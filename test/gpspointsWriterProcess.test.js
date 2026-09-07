const { describe, it, beforeEach, afterEach } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const os = require("os");
const path = require("path");
const {
  createGpsPointWriter,
  readGpspointsWriterHeartbeat,
} = require("../lib/gpsPointWriter");

function tmpDir(prefix) {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

function readyCount(dir) {
  return fs.readdirSync(dir).filter((n) => n.endsWith(".jsonl.ready")).length;
}

describe("E3 dedicated gpspoints-writer isolation", () => {
  let spoolDir;
  beforeEach(() => {
    spoolDir = tmpDir("e3-writer-");
  });
  afterEach(() => {
    fs.rmSync(spoolDir, { recursive: true, force: true });
  });

  it("producer journals and ACKs while drain process is stopped", async () => {
    const producer = createGpsPointWriter({
      spoolDir,
      mode: "producer",
      segmentMaxDocs: 1000,
      segmentSealMs: 60_000,
      journalCoalesceMs: 0,
      flushMs: 60_000,
      metrics: {},
    });
    for (let i = 0; i < 20; i++) {
      producer.enqueue({ imei: "iso", traccar_position_id: i, latitude: 1, longitude: 1 });
    }
    assert.equal(producer.journalNow(), true);
    producer.sealActiveSegment("test");
    assert.ok(readyCount(spoolDir) >= 1);
    // No drain process running — backlog remains.
    assert.equal(readyCount(spoolDir), 1);
    await producer.flushAndStop(200);
  });

  it("drain writer claims backlog after restart and persists to Mongo", async () => {
    const producer = createGpsPointWriter({
      spoolDir,
      mode: "producer",
      segmentMaxDocs: 50,
      segmentSealMs: 60_000,
      journalCoalesceMs: 0,
      flushMs: 60_000,
      metrics: {},
    });
    for (let i = 0; i < 12; i++) {
      producer.enqueue({ imei: "drain", traccar_position_id: i });
    }
    producer.journalNow();
    producer.sealActiveSegment("test");
    await producer.flushAndStop(200);

    const persisted = [];
    const drainer = createGpsPointWriter({
      spoolDir,
      mode: "drain",
      flushMs: 60_000,
      insertMany: async (docs) => {
        persisted.push(...docs);
      },
      metrics: {},
    });
    await drainer.flushCycle();
    assert.equal(persisted.length, 12);
    assert.equal(readyCount(spoolDir), 0);
    await drainer.flushAndStop(200);
  });

  it("failure domain: Mongo down — producer still journals; later drain recovers", async () => {
    const producerMetrics = {};
    const producer = createGpsPointWriter({
      spoolDir,
      mode: "producer",
      segmentMaxDocs: 100,
      segmentSealMs: 60_000,
      journalCoalesceMs: 0,
      flushMs: 60_000,
      metrics: producerMetrics,
    });
    for (let i = 0; i < 8; i++) {
      producer.enqueue({ imei: "fd", traccar_position_id: i });
    }
    assert.equal(producer.journalNow(), true);
    assert.equal(producerMetrics.gpspoints_journaled_total, 8);
    producer.sealActiveSegment("test");
    await producer.flushAndStop(200);

    const failDrain = createGpsPointWriter({
      spoolDir,
      mode: "drain",
      flushMs: 60_000,
      insertMany: async () => {
        throw new Error("mongo unavailable");
      },
      metrics: {},
    });
    await failDrain.flushCycle();
    assert.ok(readyCount(spoolDir) >= 1);
    failDrain.simulateCrash();

    const recovered = [];
    const okDrain = createGpsPointWriter({
      spoolDir,
      mode: "drain",
      flushMs: 60_000,
      insertMany: async (docs) => {
        recovered.push(...docs);
      },
      metrics: {},
    });
    await okDrain.flushCycle();
    assert.equal(recovered.length, 8);
    await okDrain.flushAndStop(200);
  });

  it("dual-drain lock forbids two concurrent drain owners", async () => {
    const a = createGpsPointWriter({
      spoolDir,
      mode: "drain",
      drainOwnerId: process.pid,
      flushMs: 60_000,
      insertMany: async () => {},
      metrics: {},
    });
    let threw = null;
    try {
      createGpsPointWriter({
        spoolDir,
        mode: "drain",
        drainOwnerId: process.pid + 1_000_003,
        flushMs: 60_000,
        insertMany: async () => {},
        metrics: {},
      });
    } catch (err) {
      threw = err;
    }
    assert.ok(threw, "second drain must fail");
    assert.match(String(threw.message), /dual-consumer|drain already owned/i);
    await a.flushAndStop(200);
  });

  it("heartbeat file is readable for /health without shared memory", async () => {
    const drainer = createGpsPointWriter({
      spoolDir,
      mode: "drain",
      heartbeatMs: 50,
      flushMs: 60_000,
      insertMany: async () => {},
      metrics: {},
    });
    await new Promise((r) => setTimeout(r, 80));
    const hb = readGpspointsWriterHeartbeat(spoolDir);
    assert.equal(hb.gpspoints_writer_alive, true);
    assert.ok(hb.gpspoints_writer_last_heartbeat);
    await drainer.flushAndStop(200);
  });

  it("at-least-once: Mongo success then crash before delete may replay", async () => {
    let calls = 0;
    const seen = [];
    const producer = createGpsPointWriter({
      spoolDir,
      mode: "producer",
      segmentMaxDocs: 5,
      segmentSealMs: 60_000,
      journalCoalesceMs: 0,
      flushMs: 60_000,
      metrics: {},
    });
    for (let i = 0; i < 5; i++) producer.enqueue({ imei: "al", traccar_position_id: i });
    producer.journalNow();
    await producer.flushAndStop(200);

    // Simulate: drain claimed, mongo ok, crash before unlink by leaving draining file manually.
    const ready = fs.readdirSync(spoolDir).find((n) => n.endsWith(".jsonl.ready"));
    assert.ok(ready);
    const readyPath = path.join(spoolDir, ready);
    const drainingPath = readyPath.replace(/\.jsonl\.ready$/i, ".jsonl.draining.99999");
    fs.renameSync(readyPath, drainingPath);

    const drainer = createGpsPointWriter({
      spoolDir,
      mode: "drain",
      flushMs: 60_000,
      insertMany: async (docs) => {
        calls += 1;
        seen.push(...docs);
      },
      metrics: {},
    });
    // Startup reclaim: stale draining → ready, then drain.
    await drainer.flushCycle();
    assert.equal(seen.length, 5);
    assert.ok(calls >= 1);
    await drainer.flushAndStop(200);
  });
});
