const { describe, it, beforeEach, afterEach } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const os = require("os");
const path = require("path");
const {
  createTraccarRawIngressWriter,
  buildTraccarRawIngressDoc,
} = require("../lib/traccarRawIngressWriter");

function tmpDir(prefix) {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function makeDoc(i) {
  return buildTraccarRawIngressDoc(
    {
      device: { uniqueId: `imei-${i % 7}`, id: i },
      position: {
        id: 0,
        latitude: 24 + i / 10000,
        longitude: 46 + i / 10000,
        fixTime: new Date(1_700_000_000_000 + i * 1000).toISOString(),
        attributes: {},
      },
    },
    Date.now() + i
  );
}

function trackingFs(spoolDir) {
  const syncHits = [];
  const real = fs;
  const impl = {
    promises: real.promises,
    appendFileSync(...args) {
      syncHits.push("appendFileSync");
      return real.appendFileSync(...args);
    },
    writeFileSync(...args) {
      syncHits.push("writeFileSync");
      return real.writeFileSync(...args);
    },
    renameSync(...args) {
      syncHits.push("renameSync");
      return real.renameSync(...args);
    },
    readFileSync(...args) {
      syncHits.push("readFileSync");
      return real.readFileSync(...args);
    },
    readdirSync(...args) {
      syncHits.push("readdirSync");
      return real.readdirSync(...args);
    },
  };
  return { impl, syncHits, spoolDir };
}

describe("traccarRawIngressWriter P0-A", () => {
  let spoolDir;
  beforeEach(() => {
    spoolDir = tmpDir("raw-ingress-");
  });
  afterEach(() => {
    fs.rmSync(spoolDir, { recursive: true, force: true });
  });

  it("sustained ingress with async Mongo latency drains while queue stays non-empty", async () => {
    const metrics = {};
    const batches = [];
    let inFlight = 0;
    let maxInFlight = 0;

    const writer = createTraccarRawIngressWriter({
      spoolDir,
      batchSize: 10,
      flushMs: 5,
      maxQueueDepth: 200,
      maxMongoBatchesPerCycle: 2,
      retryBaseMs: 5,
      retryMaxMs: 20,
      metrics,
      insertMany: async (docs) => {
        inFlight += 1;
        maxInFlight = Math.max(maxInFlight, inFlight);
        await sleep(15);
        batches.push(docs.length);
        inFlight -= 1;
      },
    });
    await writer.whenReady();

    const total = 200;
    for (let i = 0; i < total; i++) {
      const r = writer.enqueue(makeDoc(i));
      assert.equal(r.accepted, true, `enqueue ${i} rejected`);
      if (i % 20 === 0) await sleep(0);
    }

    // Reproduce prior deadlock condition: producers keep work in flight while drain runs.
    const deadline = Date.now() + 8000;
    while (Date.now() < deadline) {
      const attempted = metrics.raw_ingress_mongo_attempted_total || 0;
      const persisted = metrics.raw_ingress_persisted_total || 0;
      if (attempted > 0 && persisted >= total && (metrics.raw_ingress_queue_depth || 0) === 0) {
        break;
      }
      await sleep(20);
    }

    assert.ok(
      (metrics.raw_ingress_mongo_attempted_total || 0) > 0,
      "mongo_attempted_total must leave zero"
    );
    assert.ok((metrics.raw_ingress_persisted_total || 0) >= total, "all docs persisted");
    assert.ok(batches.length >= 2, `expected multiple Mongo batches, got ${batches.length}`);
    assert.ok(
      batches.some((n) => n === 10) || batches.every((n) => n <= 10),
      "batches should be useful/bounded"
    );
    assert.equal(metrics.raw_ingress_queue_depth || 0, 0, "memory+active journal must drain");
    assert.ok(
      (metrics.raw_ingress_queue_rejected_total || 0) === 0,
      "queue must not pin at max under this load"
    );
    assert.equal(maxInFlight, 1, "single drain owner: no concurrent insertMany chains");

    await writer.flushAndStop(2000);
  });

  it("enqueue wakes writer and increments attempted/persisted", async () => {
    const metrics = {};
    const inserted = [];
    const writer = createTraccarRawIngressWriter({
      spoolDir,
      batchSize: 5,
      flushMs: 5,
      metrics,
      insertMany: async (docs) => {
        inserted.push(...docs);
      },
    });
    await writer.whenReady();

    for (let i = 0; i < 5; i++) writer.enqueue(makeDoc(i));
    await sleep(80);
    await writer.flushCycle({ force: true });
    await sleep(40);

    assert.equal(metrics.raw_ingress_accepted_total, 5);
    assert.ok(metrics.raw_ingress_mongo_attempted_total >= 5);
    assert.ok(metrics.raw_ingress_persisted_total >= 5);
    assert.equal(inserted.length, 5);
    await writer.flushAndStop(1000);
  });

  it("startup spool replay progresses while new packets are journaled", async () => {
    const prior = path.join(spoolDir, "raw-0000000000001-aaaaaaaaaaaa.jsonl");
    const priorDocs = [makeDoc(9001), makeDoc(9002), makeDoc(9003)];
    fs.writeFileSync(prior, priorDocs.map((d) => JSON.stringify(d)).join("\n") + "\n");

    const metrics = {};
    const inserted = [];
    const writer = createTraccarRawIngressWriter({
      spoolDir,
      batchSize: 2,
      flushMs: 5,
      maxMongoBatchesPerCycle: 1,
      metrics,
      insertMany: async (docs) => {
        await sleep(10);
        inserted.push(...docs);
      },
    });
    await writer.whenReady();

    for (let i = 0; i < 6; i++) {
      assert.equal(writer.enqueue(makeDoc(i)).accepted, true);
    }

    const deadline = Date.now() + 5000;
    while (Date.now() < deadline) {
      if ((metrics.raw_ingress_persisted_total || 0) >= priorDocs.length + 6) break;
      await sleep(20);
    }

    assert.ok(
      (metrics.raw_ingress_persisted_total || 0) >= priorDocs.length + 6,
      `expected old+new persisted, got ${metrics.raw_ingress_persisted_total}`
    );
    assert.ok(inserted.some((d) => d.imei === priorDocs[0].imei));
    assert.ok((metrics.raw_ingress_spooled_total || 0) >= 6);
    await writer.flushAndStop(2000);
  });

  it("Mongo failure retains spool, retries, and does not deadlock new ingress", async () => {
    const metrics = {};
    let fails = 2;
    const inserted = [];
    const writer = createTraccarRawIngressWriter({
      spoolDir,
      batchSize: 5,
      flushMs: 5,
      retryBaseMs: 5,
      retryMaxMs: 20,
      metrics,
      insertMany: async (docs) => {
        if (fails > 0) {
          fails -= 1;
          throw new Error("mongo down");
        }
        inserted.push(...docs);
      },
    });
    await writer.whenReady();

    for (let i = 0; i < 5; i++) writer.enqueue(makeDoc(i));

    const deadline = Date.now() + 5000;
    while (Date.now() < deadline) {
      if (inserted.length >= 5) break;
      await sleep(20);
    }

    assert.equal(inserted.length, 5);
    assert.ok((metrics.raw_ingress_persist_failures || 0) >= 1);
    assert.ok((metrics.raw_ingress_mongo_attempted_total || 0) >= 5);
    assert.equal(metrics.raw_ingress_persisted_total, 5);

    // Still accepts after recovery
    assert.equal(writer.enqueue(makeDoc(99)).accepted, true);
    await sleep(80);
    await writer.flushAndStop(2000);
    assert.ok((metrics.raw_ingress_persisted_total || 0) >= 6);
  });

  it("hot-path enqueue does not use synchronous filesystem APIs", async () => {
    const { impl, syncHits } = trackingFs(spoolDir);
    const metrics = {};
    const writer = createTraccarRawIngressWriter({
      spoolDir,
      batchSize: 50,
      flushMs: 50,
      metrics,
      fsImpl: impl,
      insertMany: async () => {},
    });
    await writer.whenReady();
    syncHits.length = 0;

    for (let i = 0; i < 3; i++) writer.enqueue(makeDoc(i));
    await sleep(30);

    assert.deepEqual(syncHits, [], `sync FS used on hot path: ${syncHits.join(",")}`);
    await writer.flushAndStop(1000);
  });

  it("flushAndStop journals accepted queue without rotating during active append", async () => {
    const metrics = {};
    const inserted = [];
    let appendGate = null;
    let releaseAppend = null;
    const realFs = fs;
    const fsImpl = {
      promises: {
        mkdir: (...a) => realFs.promises.mkdir(...a),
        readdir: (...a) => realFs.promises.readdir(...a),
        readFile: (...a) => realFs.promises.readFile(...a),
        writeFile: (...a) => realFs.promises.writeFile(...a),
        unlink: (...a) => realFs.promises.unlink(...a),
        rename: async (...a) => realFs.promises.rename(...a),
        appendFile: async (...a) => {
          if (appendGate) await appendGate;
          return realFs.promises.appendFile(...a);
        },
      },
    };

    const writer = createTraccarRawIngressWriter({
      spoolDir,
      batchSize: 100,
      flushMs: 10_000,
      metrics,
      fsImpl,
      insertMany: async (docs) => {
        inserted.push(...docs);
      },
    });
    await writer.whenReady();

    appendGate = new Promise((r) => {
      releaseAppend = r;
    });
    assert.equal(writer.enqueue(makeDoc(1)).accepted, true);
    await sleep(10);
    // Append held; shutdown must wait / not drop accepted work.
    const stopPromise = writer.flushAndStop(2000);
    await sleep(30);
    releaseAppend();
    const stop = await stopPromise;
    assert.ok(inserted.length >= 1, "accepted doc must reach Mongo or remain counted");
    assert.ok(stop.elapsed_ms <= 2000);
  });
});
