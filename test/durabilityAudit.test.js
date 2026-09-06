const { describe, it } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const os = require("os");
const path = require("path");
const { createGpsPointWriter, classifyInsertManyError } = require("../lib/gpsPointWriter");
const { createPositionPipeline } = require("../lib/positionPipeline");
const { createGpsLogsWriter } = require("../lib/gpsLogsWriter");
const { loadBridgeEnv } = require("../lib/bridgeEnv");
const { createBridgeMetrics, snapshotMetrics } = require("../lib/bridgeMetrics");
const {
  INDEX_OPTIONS,
  summarizeDiagnostic,
  shouldCreateIndex,
} = require("../lib/gpspointsIdempotencyIndex");

function tmpDir(prefix) {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

function jsonlFiles(dir) {
  return fs
    .readdirSync(dir)
    .filter((name) => name.endsWith(".jsonl") && !name.endsWith(".tmp"))
    .map((name) => path.join(dir, name));
}

function readAllJsonl(dir) {
  const docs = [];
  for (const filePath of jsonlFiles(dir)) {
    const raw = fs.readFileSync(filePath, "utf8");
    for (const line of raw.split("\n")) {
      if (line.trim()) docs.push(JSON.parse(line));
    }
  }
  return docs;
}

function proxyFs(overrides) {
  return new Proxy(fs, {
    get(target, prop) {
      if (Object.prototype.hasOwnProperty.call(overrides, prop)) return overrides[prop];
      const value = target[prop];
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}

function percentile(values, p) {
  if (!values.length) return 0;
  const sorted = values.slice().sort((a, b) => a - b);
  const idx = Math.min(sorted.length - 1, Math.max(0, Math.ceil((p / 100) * sorted.length) - 1));
  return sorted[idx];
}

describe("durability: abrupt crash / kill simulation", () => {
  it("recovers journaled points after simulated kill -9", async () => {
    const spoolDir = tmpDir("kill9-");
    const first = createGpsPointWriter({
      spoolDir,
      batchSize: 50,
      flushMs: 60_000,
      insertMany: async () => {
        throw new Error("mongo down");
      },
      metrics: {},
    });
    for (let i = 0; i < 25; i++) {
      first.enqueue({ imei: "k", traccar_position_id: i, latitude: 1, longitude: 1 });
    }
    await first.flushJournal();
    assert.ok(jsonlFiles(spoolDir).length >= 1);
    first.simulateCrash();
    assert.equal(first.getMemoryDepth(), 0);

    const recovered = [];
    const second = createGpsPointWriter({
      spoolDir,
      batchSize: 50,
      flushMs: 60_000,
      insertMany: async (docs) => {
        recovered.push(...docs);
      },
      metrics: {},
    });
    await second.flushCycle();
    await second.flushAndStop(500);
    assert.equal(recovered.length, 25);
    assert.equal(new Set(recovered.map((d) => d.traccar_position_id)).size, 25);
    fs.rmSync(spoolDir, { recursive: true, force: true });
  });
});

describe("durability: spool recovery + atomic files", () => {
  it("writes spool via tmp then atomic rename and leaves no .tmp", async () => {
    const spoolDir = tmpDir("atomic-");
    const writer = createGpsPointWriter({
      spoolDir,
      flushMs: 60_000,
      insertMany: async () => {
        throw new Error("mongo down");
      },
      metrics: {},
    });
    writer.enqueue({ imei: "a", traccar_position_id: 1, latitude: 1, longitude: 1 });
    await writer.flushJournal();
    const names = fs.readdirSync(spoolDir);
    assert.equal(names.some((n) => n.endsWith(".tmp")), false);
    const files = jsonlFiles(spoolDir);
    assert.equal(files.length, 1);
    const lines = fs.readFileSync(files[0], "utf8").trim().split("\n");
    assert.equal(JSON.parse(lines[0]).traccar_position_id, 1);
    writer.simulateCrash();
    fs.rmSync(spoolDir, { recursive: true, force: true });
  });

  it("ignores leftover .tmp partial files and does not treat them as persisted", async () => {
    const spoolDir = tmpDir("tmppartial-");
    fs.writeFileSync(
      path.join(spoolDir, "hot-0000000000001-dead.jsonl.tmp"),
      '{"imei":"x","traccar_position_id":99}\n'
    );
    const recovered = [];
    const writer = createGpsPointWriter({
      spoolDir,
      flushMs: 60_000,
      insertMany: async (docs) => recovered.push(...docs),
      metrics: {},
    });
    await writer.flushCycle();
    assert.equal(recovered.length, 0);
    assert.equal(fs.existsSync(path.join(spoolDir, "hot-0000000000001-dead.jsonl.tmp")), false);
    await writer.flushAndStop(200);
    fs.rmSync(spoolDir, { recursive: true, force: true });
  });

  it("quarantines corrupt jsonl and continues with valid lines", async () => {
    const spoolDir = tmpDir("corrupt-");
    fs.writeFileSync(
      path.join(spoolDir, "pending-0000000000001-bad.jsonl"),
      "NOT_JSON\n{\"imei\":\"ok\",\"traccar_position_id\":7,\"latitude\":1,\"longitude\":1}\n"
    );
    const recovered = [];
    const writer = createGpsPointWriter({
      spoolDir,
      flushMs: 60_000,
      insertMany: async (docs) => recovered.push(...docs),
      metrics: {},
    });
    await writer.flushCycle();
    await writer.flushAndStop(200);
    assert.equal(recovered.some((d) => d.traccar_position_id === 7), true);
    assert.equal(recovered.some((d) => d._raw), false);
    fs.rmSync(spoolDir, { recursive: true, force: true });
  });
});

describe("durability: Mongo ACK before spool delete", () => {
  it("keeps spool files until insertMany acknowledges", async () => {
    const spoolDir = tmpDir("ack-");
    let allow = false;
    const writer = createGpsPointWriter({
      spoolDir,
      flushMs: 60_000,
      insertMany: async (docs) => {
        if (!allow) {
          const err = new Error("network timeout");
          err.code = 50;
          throw err;
        }
        return docs;
      },
      metrics: {},
    });
    writer.enqueue({ imei: "a", traccar_position_id: 1, latitude: 1, longitude: 1 });
    await writer.flushJournal();
    await writer.flushCycle();
    assert.ok(jsonlFiles(spoolDir).length >= 1);
    allow = true;
    await writer.flushCycle();
    assert.equal(jsonlFiles(spoolDir).length, 0);
    await writer.flushAndStop(200);
    fs.rmSync(spoolDir, { recursive: true, force: true });
  });

  it("treats idempotency duplicate after lost ACK as already persisted", async () => {
    const spoolDir = tmpDir("dupack-");
    const stored = new Set();
    let attempts = 0;
    const metrics = {};
    const writer = createGpsPointWriter({
      spoolDir,
      flushMs: 60_000,
      insertMany: async (docs) => {
        attempts += 1;
        if (attempts === 1) {
          for (const doc of docs) stored.add(`${doc.imei}:${doc.traccar_position_id}`);
          const err = new Error("connection interrupted before ACK");
          err.code = 11600;
          throw err;
        }
        const writeErrors = [];
        docs.forEach((doc, index) => {
          const key = `${doc.imei}:${doc.traccar_position_id}`;
          if (stored.has(key)) {
            writeErrors.push({
              index,
              code: 11000,
              keyPattern: { imei: 1, traccar_position_id: 1 },
              errmsg: "E11000 duplicate key error collection: gpspoints index: imei_1_traccar_position_id_1_unique_partial",
            });
          } else {
            stored.add(key);
          }
        });
        if (writeErrors.length) {
          const err = new Error("E11000 duplicate key");
          err.code = 11000;
          err.writeErrors = writeErrors;
          throw err;
        }
      },
      metrics,
    });
    writer.enqueue({ imei: "a", traccar_position_id: 42, latitude: 1, longitude: 1 });
    await writer.flushJournal();
    await writer.flushCycle();
    await writer.flushCycle();
    await writer.flushAndStop(200);
    assert.equal(jsonlFiles(spoolDir).length, 0);
    assert.ok((metrics.gpspoints_duplicate_already_persisted_total || 0) >= 1);
    assert.equal(metrics.persistence_dropped || 0, 0);
    fs.rmSync(spoolDir, { recursive: true, force: true });
  });
});

describe("durability: partial batch failure", () => {
  it("acks success+idempotency dups, retries retryable, quarantines invalid", async () => {
    const spoolDir = tmpDir("partial-");
    const metrics = {};
    const writer = createGpsPointWriter({
      spoolDir,
      batchSize: 10,
      flushMs: 60_000,
      insertMany: async (docs) => {
        const err = new Error("partial");
        err.writeErrors = [
          {
            index: 1,
            code: 11000,
            keyPattern: { imei: 1, traccar_position_id: 1 },
            errmsg: "E11000 duplicate key traccar_position_id",
          },
          { index: 2, code: 50, errmsg: "timeout" },
          { index: 3, code: 121, errmsg: "DocumentValidationFailure" },
        ];
        throw err;
      },
      metrics,
    });
    writer.enqueue({ imei: "a", traccar_position_id: 1, latitude: 1, longitude: 1 });
    writer.enqueue({ imei: "a", traccar_position_id: 2, latitude: 1, longitude: 1 });
    writer.enqueue({ imei: "a", traccar_position_id: 3, latitude: 1, longitude: 1 });
    writer.enqueue({ imei: "a", traccar_position_id: 4, latitude: 1, longitude: 1 });
    await writer.flushJournal();
    await writer.flushCycle();
    const remaining = readAllJsonl(spoolDir);
    assert.equal(remaining.length, 1);
    assert.equal(remaining[0].traccar_position_id, 3);
    assert.ok((metrics.gpspoints_duplicate_already_persisted_total || 0) >= 1);
    assert.ok((metrics.gpspoints_quarantined_total || 0) >= 1);
    assert.equal(metrics.persistence_dropped || 0, 0);
    writer.simulateCrash();
    fs.rmSync(spoolDir, { recursive: true, force: true });
  });
});

describe("durability: disk errors are visible and never silent drops", () => {
  it("permission error keeps RAM points and increments write failures", async () => {
    const spoolDir = tmpDir("eacces-");
    const err = Object.assign(new Error("EACCES"), { code: "EACCES" });
    const metrics = {};
    const writer = createGpsPointWriter({
      spoolDir,
      flushMs: 60_000,
      fsImpl: proxyFs({
        writeFileSync() {
          throw err;
        },
      }),
      insertMany: async () => {
        throw new Error("mongo down");
      },
      metrics,
    });
    writer.enqueue({ imei: "a", traccar_position_id: 1, latitude: 1, longitude: 1 });
    await writer.flushJournal();
    assert.equal(writer.getMemoryDepth(), 1);
    assert.ok((metrics.gpspoints_spool_write_failures || 0) >= 1);
    assert.equal(metrics.persistence_dropped || 0, 0);
    const stats = writer.getStats();
    assert.equal(stats.gpspoints_health, "critical");
    writer.simulateCrash();
    fs.rmSync(spoolDir, { recursive: true, force: true });
  });

  it("disk full simulated error does not discard GPS", async () => {
    const spoolDir = tmpDir("enospc-");
    const err = Object.assign(new Error("ENOSPC"), { code: "ENOSPC" });
    const persisted = [];
    const metrics = {};
    const writer = createGpsPointWriter({
      spoolDir,
      flushMs: 60_000,
      fsImpl: proxyFs({
        writeFileSync() {
          throw err;
        },
      }),
      insertMany: async (docs) => {
        persisted.push(...docs);
      },
      metrics,
    });
    writer.enqueue({ imei: "a", traccar_position_id: 8, latitude: 1, longitude: 1 });
    await writer.flushJournal();
    await writer.flushCycle();
    assert.equal(persisted.length, 1);
    assert.ok((metrics.gpspoints_spool_write_failures || 0) >= 1);
    assert.equal(metrics.persistence_dropped || 0, 0);
    await writer.flushAndStop(200);
    fs.rmSync(spoolDir, { recursive: true, force: true });
  });
});

describe("durability: large spool fairness + realtime independence", () => {
  it("does not drain entire old spool before new points", async () => {
    const spoolDir = tmpDir("fair-");
    const persisted = [];
    const writer = createGpsPointWriter({
      spoolDir,
      flushMs: 60_000,
      drainNewFilesPerCycle: 2,
      drainOldFilesPerCycle: 1,
      maxMongoBatchesPerCycle: 3,
      batchSize: 50,
      insertMany: async (docs) => {
        persisted.push(...docs);
      },
      metrics: {},
    });
    for (let i = 1; i <= 20; i++) {
      fs.writeFileSync(
        path.join(spoolDir, `pending-${String(i).padStart(13, "0")}-old.jsonl`),
        JSON.stringify({ imei: "old", traccar_position_id: i, tag: `old-${i}` }) + "\n"
      );
    }
    writer.enqueue({ imei: "new", traccar_position_id: 1000, tag: "new-1" });
    await writer.flushJournal();
    await writer.flushCycle();
    assert.ok(persisted.some((d) => d.tag === "new-1"));
    assert.ok(persisted.length < 21);
    assert.ok(jsonlFiles(spoolDir).length > 0);
    writer.simulateCrash();
    fs.rmSync(spoolDir, { recursive: true, force: true });
  });

  it("live emit happens before slow persistence and command stays fast under backlog", async () => {
    const spoolDir = tmpDir("live-");
    for (let i = 1; i <= 30; i++) {
      fs.writeFileSync(
        path.join(spoolDir, `pending-${String(i).padStart(13, "0")}-old.jsonl`),
        JSON.stringify({ imei: "old", traccar_position_id: i }) + "\n"
      );
    }
    const liveTimes = [];
    const commandTimes = [];
    const writer = createGpsPointWriter({
      spoolDir,
      flushMs: 60_000,
      drainNewFilesPerCycle: 1,
      drainOldFilesPerCycle: 1,
      maxMongoBatchesPerCycle: 2,
      insertMany: async (docs) => {
        await new Promise((r) => setTimeout(r, 80));
        return docs;
      },
      metrics: {},
    });
    const pipeline = createPositionPipeline({
      now: () => Date.now(),
      emitLive: () => liveTimes.push(Date.now()),
      emitCommand: () => commandTimes.push(Date.now()),
      enqueueArchive: (doc) => writer.enqueue(doc),
      enqueueAnalytics: () => {},
    });
    void writer.flushCycle();
    const t0 = Date.now();
    pipeline.handleResolvedGps({
      imei: "1",
      positions: [{ id: 1, fixTime: new Date().toISOString(), latitude: 1, longitude: 1 }],
      buildPayload: (p) => p,
    });
    const liveMs = Date.now() - t0;
    const cmdSamples = [];
    for (let i = 0; i < 20; i++) {
      const c0 = Date.now();
      pipeline.handleCommand("1", { attributes: { result: "OK!" } }, { ok: true });
      cmdSamples.push(Date.now() - c0);
    }
    assert.equal(liveTimes.length, 1);
    assert.ok(liveMs < 20, `live waited ${liveMs}ms`);
    assert.ok(percentile(cmdSamples, 50) < 5);
    assert.ok(percentile(cmdSamples, 95) < 10);
    assert.ok(percentile(cmdSamples, 99) < 15);
    await new Promise((r) => setTimeout(r, 250));
    writer.simulateCrash();
    fs.rmSync(spoolDir, { recursive: true, force: true });
  });
});

describe("durability: gpslogs remains OFF by default", () => {
  it("env default is false and writer performs zero GpsLog inserts", async () => {
    const env = loadBridgeEnv();
    assert.equal(env.GPSLOGS_WRITE_ENABLED, false);
    let writes = 0;
    const writer = createGpsLogsWriter({
      enabled: env.GPSLOGS_WRITE_ENABLED,
      GpsLog: {
        insertMany: async (docs) => {
          writes += docs.length;
          return docs;
        },
      },
      metrics: {},
    });
    await writer.writeMany([{ type: "gps" }, { type: "alarm" }, { type: "gps" }]);
    assert.equal(writes, 0);
  });
});

describe("durability: classifyInsertManyError", () => {
  it("does not treat unrelated unique indexes as traccar_position_id idempotency", () => {
    const docs = [{ imei: "a", traccar_position_id: 1 }, { imei: "a", traccar_position_id: 2 }];
    const classified = classifyInsertManyError(docs, {
      code: 11000,
      writeErrors: [
        {
          index: 0,
          code: 11000,
          keyPattern: { imei: 1, packet_date: 1 },
          errmsg: "E11000 duplicate key index: imei_1_packet_date_1",
        },
      ],
    });
    assert.equal(classified.duplicates.length, 0);
    assert.equal(classified.unexpectedDuplicates.length, 1);
    assert.equal(classified.acked.length, 1);
  });
});

describe("durability: health snapshot fields", () => {
  it("exposes spool and persistence fields without hiding dropped invariant", () => {
    const metrics = createBridgeMetrics();
    const snap = snapshotMetrics(metrics, {
      gpspoints_spool_files: 2,
      event_loop_lag_ms: 1,
    });
    assert.equal(snap.persistence_dropped, 0);
    assert.equal(snap.gpslogs_write_enabled, false);
    assert.equal(snap.gpspoints_spool_files, 2);
    assert.equal("event_loop_lag_ms" in snap, true);
  });
});

describe("durability: idempotency index readiness helpers", () => {
  it("refuses unique index creation when duplicates exist", () => {
    assert.equal(INDEX_OPTIONS.unique, true);
    assert.deepEqual(INDEX_OPTIONS.partialFilterExpression, {
      traccar_position_id: { $exists: true, $type: "number" },
    });
    const dirty = summarizeDiagnostic({
      total: 10,
      withTraccarPositionId: 8,
      withoutTraccarPositionId: 2,
      duplicateGroups: [{ n: 3 }],
    });
    assert.equal(shouldCreateIndex(dirty), false);
    assert.equal(dirty.duplicated_rows, 3);
    const clean = summarizeDiagnostic({ total: 10, withTraccarPositionId: 8, duplicateGroups: [] });
    assert.equal(shouldCreateIndex(clean), true);
  });
});

describe("durability: fake-mongo persist throughput vs enqueue", () => {
  it("reports mongo_acknowledged separately from received for 10k points", async () => {
    const spoolDir = tmpDir("load10k-");
    const metrics = createBridgeMetrics();
    const writer = createGpsPointWriter({
      spoolDir,
      batchSize: 250,
      flushMs: 60_000,
      maxMongoBatchesPerCycle: 10_000,
      drainNewFilesPerCycle: 10_000,
      drainOldFilesPerCycle: 0,
      insertMany: async (docs) => docs,
      metrics,
    });
    const tIngress = Date.now();
    for (let i = 0; i < 10_000; i++) {
      writer.enqueue({
        imei: String(i % 50),
        traccar_position_id: i,
        latitude: 24,
        longitude: 46,
      });
    }
    const ingressMs = Date.now() - tIngress;
    await writer.flushJournal();
    const tMongo = Date.now();
    for (let i = 0; i < 80 && jsonlFiles(spoolDir).length; i++) {
      await writer.flushCycle();
    }
    const mongoMs = Date.now() - tMongo;
    await writer.flushAndStop(2000);
    assert.equal(metrics.gpspoints_received_total, 10_000);
    assert.equal(metrics.gpspoints_mongo_acknowledged_total, 10_000);
    assert.equal(metrics.persistence_dropped, 0);
    console.log(
      JSON.stringify({
        received_total: metrics.gpspoints_received_total,
        live_processed_total: null,
        archive_enqueued_total: metrics.gpspoints_received_total,
        durably_spooled_total: metrics.gpspoints_spooled_total,
        mongo_attempted_total: metrics.gpspoints_mongo_attempted_total,
        mongo_acknowledged_total: metrics.gpspoints_mongo_acknowledged_total,
        duplicate_already_persisted_total: metrics.gpspoints_duplicate_already_persisted_total,
        retry_pending_total: 0,
        lost_total: metrics.persistence_dropped,
        ingress_ms: ingressMs,
        mongo_ack_ms: mongoMs,
        rss_mb: Math.round(process.memoryUsage().rss / 1048576),
        heap_used_mb: Math.round(process.memoryUsage().heapUsed / 1048576),
      })
    );
    fs.rmSync(spoolDir, { recursive: true, force: true });
  });
});
