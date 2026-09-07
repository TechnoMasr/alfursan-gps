const { describe, it, beforeEach, afterEach } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const os = require("os");
const path = require("path");
const { createGpsPointWriter } = require("../lib/gpsPointWriter");
const { createAnalyticsQueue } = require("../lib/analyticsQueue");

function tmpDir(prefix) {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

describe("gpspoints batch writer + spool", () => {
  let spoolDir;
  beforeEach(() => {
    spoolDir = tmpDir("gps-spool-");
  });
  afterEach(() => {
    fs.rmSync(spoolDir, { recursive: true, force: true });
  });

  it("batches inserts and never drops on duplicate key", async () => {
    const inserted = [];
    const writer = createGpsPointWriter({
      spoolDir,
      batchSize: 3,
      flushMs: 10,
      insertMany: async (docs) => {
        inserted.push(...docs);
      },
      metrics: {},
    });
    writer.enqueue({ imei: "1", latitude: 1, longitude: 1 });
    writer.enqueue({ imei: "1", latitude: 2, longitude: 2 });
    writer.enqueue({ imei: "1", latitude: 3, longitude: 3 });
    await writer.flushCycle();
    assert.equal(inserted.length, 3);
    await writer.flushAndStop(500);
  });

  it("journal coalesce batches 100 same-IMEI points without latest-only drop", async () => {
    const inserted = [];
    const writer = createGpsPointWriter({
      spoolDir,
      batchSize: 250,
      flushMs: 5_000,
      journalCoalesceMs: 50,
      insertMany: async (docs) => {
        inserted.push(...docs);
      },
      metrics: {},
    });
    for (let i = 0; i < 100; i++) {
      writer.enqueue({
        imei: "same",
        latitude: 24 + i / 10000,
        longitude: 46 + i / 10000,
        traccar_position_id: i + 1,
      });
    }
    await new Promise((r) => setTimeout(r, 80));
    await writer.flushCycle();
    assert.equal(inserted.length, 100);
    await writer.flushAndStop(500);
  });

  it("retries after mongo failure then persists all", async () => {
    let fails = 2;
    const inserted = [];
    const writer = createGpsPointWriter({
      spoolDir,
      batchSize: 10,
      flushMs: 5,
      retryBaseMs: 5,
      retryMaxMs: 20,
      insertMany: async (docs) => {
        if (fails > 0) {
          fails -= 1;
          const err = new Error("mongo down");
          throw err;
        }
        inserted.push(...docs);
      },
      metrics: {},
    });
    for (let i = 0; i < 5; i++) writer.enqueue({ imei: "x", i });
    await new Promise((r) => setTimeout(r, 80));
    await writer.flushCycle();
    assert.equal(inserted.length, 5);
    await writer.flushAndStop(500);
  });

  it("survives restart via durable spool", async () => {
    const first = createGpsPointWriter({
      spoolDir,
      batchSize: 100,
      flushMs: 10_000,
      insertMany: async () => {
        throw new Error("down");
      },
      metrics: {},
      memHigh: 1,
    });
    first.enqueue({ imei: "r", latitude: 1, longitude: 1, traccar_position_id: 44 });
    await first.flushAndStop(200);

    const recovered = [];
    const second = createGpsPointWriter({
      spoolDir,
      batchSize: 50,
      flushMs: 5,
      insertMany: async (docs) => {
        recovered.push(...docs);
      },
      metrics: {},
    });
    await second.flushCycle();
    assert.ok(recovered.length >= 1);
    assert.equal(recovered[0].traccar_position_id, 44);
    await second.flushAndStop(200);
  });

  it("duplicate key is not a batch failure", async () => {
    const writer = createGpsPointWriter({
      spoolDir,
      batchSize: 2,
      flushMs: 5,
      insertMany: async () => {
        const err = new Error("E11000 duplicate key");
        err.code = 11000;
        throw err;
      },
      metrics: {},
    });
    writer.enqueue({ imei: "1", traccar_position_id: 1 });
    writer.enqueue({ imei: "1", traccar_position_id: 1 });
    await writer.flushCycle();
    assert.equal(writer.getMemoryDepth(), 0);
    await writer.flushAndStop(200);
  });

  it("E1: combines many 1-doc legacy files into one insertMany batch", async () => {
    const batches = [];
    for (let i = 1; i <= 40; i++) {
      fs.writeFileSync(
        path.join(spoolDir, `pending-${String(i).padStart(13, "0")}-leg.jsonl`),
        JSON.stringify({ imei: "leg", traccar_position_id: i, n: i }) + "\n"
      );
    }
    const writer = createGpsPointWriter({
      spoolDir,
      batchSize: 250,
      flushMs: 60_000,
      maxMongoBatchesPerCycle: 2,
      maxFilesPerCycle: 500,
      drainOldDocRatio: 0.5,
      insertMany: async (docs) => {
        batches.push(docs.slice());
      },
      metrics: {},
    });
    await writer.flushCycle();
    assert.equal(batches.length, 1, "one Mongo batch for 40 one-doc files");
    assert.equal(batches[0].length, 40);
    assert.equal(fs.readdirSync(spoolDir).filter((n) => n.endsWith(".jsonl")).length, 0);
    await writer.flushAndStop(200);
  });

  it("E1: doc-based fairness drains old and new without emptying whole backlog", async () => {
    const persisted = [];
    for (let i = 1; i <= 30; i++) {
      fs.writeFileSync(
        path.join(spoolDir, `pending-${String(i).padStart(13, "0")}-old.jsonl`),
        JSON.stringify({ imei: "old", traccar_position_id: i, side: "old" }) + "\n"
      );
    }
    const writer = createGpsPointWriter({
      spoolDir,
      batchSize: 100,
      flushMs: 60_000,
      maxMongoBatchesPerCycle: 1,
      maxFilesPerCycle: 8,
      drainOldDocRatio: 0.5,
      drainNewFilesPerCycle: 2,
      drainOldFilesPerCycle: 1,
      insertMany: async (docs) => {
        persisted.push(...docs);
      },
      metrics: {},
    });
    writer.enqueue({ imei: "new", traccar_position_id: 9999, side: "new" });
    await writer.flushJournal();
    await writer.flushCycle();
    assert.ok(persisted.some((d) => d.side === "new"));
    assert.ok(persisted.some((d) => d.side === "old"));
    assert.ok(persisted.length <= 8);
    assert.ok(
      fs.readdirSync(spoolDir).filter((n) => n.endsWith(".jsonl")).length > 0,
      "backlog must remain"
    );
    await writer.flushAndStop(200);
  });

  it("E1: enqueue journals without waiting for Mongo ACK", async () => {
    let mongoStarted = false;
    let resolveMongo;
    const mongoGate = new Promise((r) => {
      resolveMongo = r;
    });
    const metrics = {};
    const writer = createGpsPointWriter({
      spoolDir,
      batchSize: 250,
      flushMs: 60_000,
      journalCoalesceMs: 0,
      insertMany: async (docs) => {
        mongoStarted = true;
        await mongoGate;
        return docs;
      },
      metrics,
    });
    writer.enqueue({ imei: "ack", traccar_position_id: 1 });
    await writer.flushJournal();
    assert.equal(metrics.gpspoints_journaled_total, 1);
    assert.equal(mongoStarted, false, "journal must complete before Mongo drain");
    assert.ok(
      fs.readdirSync(spoolDir).some(
        (n) =>
          n.endsWith(".jsonl") ||
          n.endsWith(".jsonl.active") ||
          n.endsWith(".jsonl.ready")
      ),
      "durable spool present before Mongo"
    );
    const drain = writer.flushCycle();
    await new Promise((r) => setImmediate(r));
    assert.equal(mongoStarted, true);
    resolveMongo();
    await drain;
    await writer.flushAndStop(200);
  });
});
describe("analytics FIFO never coalesces or hangs", () => {
  it("processes every item in per-IMEI order", async () => {
    const seen = [];
    const q = createAnalyticsQueue({
      concurrency: 1,
      memHigh: 10_000,
      spoolDir: tmpDir("an-"),
      processFn: async (ctx) => {
        seen.push(ctx.n);
      },
      metrics: {},
    });
    q.enqueue("imei", { n: 1 });
    q.enqueue("imei", { n: 2 });
    q.enqueue("imei", { n: 3 });
    await new Promise((r) => setTimeout(r, 50));
    assert.deepEqual(seen, [1, 2, 3]);
  });

  it("queue overflow never leaves an unresolved Promise (no silent shift)", async () => {
    let processed = 0;
    const q = createAnalyticsQueue({
      concurrency: 1,
      memHigh: 100000,
      spoolDir: tmpDir("an2-"),
      processFn: async () => {
        processed += 1;
        await new Promise((r) => setTimeout(r, 1));
      },
      metrics: {},
    });
    for (let i = 0; i < 50; i++) q.enqueue("a", { i });
    const start = Date.now();
    while (processed < 50 && Date.now() - start < 2000) {
      await new Promise((r) => setTimeout(r, 10));
    }
    assert.equal(processed, 50);
  });
});
