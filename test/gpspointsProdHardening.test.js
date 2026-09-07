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

function listSpool(dir) {
  return fs.readdirSync(dir).filter((n) => !n.endsWith(".tmp") && n !== "quarantine");
}

function countActiveReady(dir) {
  const names = listSpool(dir);
  return {
    active: names.filter((n) => n.endsWith(".jsonl.active")).length,
    ready: names.filter((n) => n.endsWith(".jsonl.ready")).length,
    files: names.filter(
      (n) =>
        n.endsWith(".jsonl.active") ||
        n.endsWith(".jsonl.ready") ||
        /^(hot|pending)-/i.test(n)
    ).length,
  };
}

describe("E2/E3 production hardening", () => {
  let spoolDir;
  beforeEach(() => {
    spoolDir = tmpDir("harden-");
  });
  afterEach(() => {
    fs.rmSync(spoolDir, { recursive: true, force: true });
  });

  it("1-2. low traffic does not seal every 200ms; stays open until docs/bytes/5s", async () => {
    const writer = createGpsPointWriter({
      spoolDir,
      mode: "producer",
      segmentMaxDocs: 1000,
      segmentMaxBytes: 2 * 1024 * 1024,
      segmentMaxAgeMs: 5000,
      journalCoalesceMs: 0,
      flushMs: 60_000,
      metrics: {},
    });
    for (let i = 0; i < 12; i++) {
      writer.enqueue({ imei: "low", traccar_position_id: i });
      writer.journalNow();
      await new Promise((r) => setTimeout(r, 25)); // ~400ms total << 5s
    }
    const mid = countActiveReady(spoolDir);
    assert.equal(mid.active, 1, "active segment must remain open");
    assert.equal(mid.ready, 0, "must not rotate every ~200ms");
    assert.ok(mid.files <= 1);
    await writer.flushAndStop(200);
  });

  it("3. ACK/journal after append without waiting for seal", async () => {
    const metrics = {};
    const writer = createGpsPointWriter({
      spoolDir,
      mode: "producer",
      segmentMaxAgeMs: 60_000,
      journalCoalesceMs: 0,
      flushMs: 60_000,
      metrics,
    });
    writer.enqueue({ imei: "ack", traccar_position_id: 1 });
    assert.equal(writer.journalNow(), true);
    assert.equal(metrics.gpspoints_journaled_total, 1);
    assert.equal(countActiveReady(spoolDir).active, 1);
    assert.equal(countActiveReady(spoolDir).ready, 0);
    await writer.flushAndStop(200);
  });

  it("4. process restart recovers active segment (no silent delete)", async () => {
    const first = createGpsPointWriter({
      spoolDir,
      mode: "producer",
      segmentMaxAgeMs: 60_000,
      journalCoalesceMs: 0,
      flushMs: 60_000,
      metrics: {},
    });
    for (let i = 0; i < 9; i++) first.enqueue({ imei: "rec", traccar_position_id: i });
    first.journalNow();
    assert.equal(countActiveReady(spoolDir).active, 1);
    first.simulateCrash();

    const recovered = [];
    const second = createGpsPointWriter({
      spoolDir,
      mode: "full",
      segmentMaxAgeMs: 60_000,
      flushMs: 60_000,
      insertMany: async (docs) => {
        recovered.push(...docs);
      },
      metrics: {},
    });
    // Adopt/seal then drain.
    second.sealActiveSegment("test");
    await second.flushCycle();
    assert.equal(recovered.length, 9);
    await second.flushAndStop(200);
  });

  it("5. shutdown seals active segment without Mongo", async () => {
    const writer = createGpsPointWriter({
      spoolDir,
      mode: "producer",
      segmentMaxAgeMs: 60_000,
      journalCoalesceMs: 0,
      flushMs: 60_000,
      metrics: {},
    });
    writer.enqueue({ imei: "sd", traccar_position_id: 1 });
    writer.journalNow();
    assert.equal(countActiveReady(spoolDir).active, 1);
    await writer.flushAndStop(500);
    assert.equal(countActiveReady(spoolDir).active, 0);
    assert.equal(countActiveReady(spoolDir).ready, 1);
  });

  it("6-7. external writer status metrics separate from producer Mongo counters", async () => {
    const producerMetrics = {};
    const producer = createGpsPointWriter({
      spoolDir,
      mode: "producer",
      segmentMaxDocs: 10,
      segmentMaxAgeMs: 60_000,
      journalCoalesceMs: 0,
      flushMs: 60_000,
      metrics: producerMetrics,
    });
    for (let i = 0; i < 10; i++) producer.enqueue({ imei: "sep", traccar_position_id: i });
    producer.journalNow();
    await producer.flushAndStop(200);

    assert.equal(producerMetrics.gpspoints_journaled_total, 10);
    assert.equal(producerMetrics.gpspoints_mongo_attempted_total || 0, 0);
    assert.equal(producerMetrics.gpspoints_persisted_total || 0, 0);

    const writerMetrics = {};
    const drainer = createGpsPointWriter({
      spoolDir,
      mode: "drain",
      heartbeatMs: 50,
      flushMs: 60_000,
      insertMany: async (docs) => docs,
      metrics: writerMetrics,
    });
    await drainer.flushCycle();
    // Force status publish.
    await new Promise((r) => setTimeout(r, 80));

    const hb = readGpspointsWriterHeartbeat(spoolDir);
    assert.equal(hb.gpspoints_writer_alive, true);
    assert.ok(hb.gpspoints_writer_persisted_total >= 10);
    assert.ok(hb.gpspoints_writer_mongo_attempted_total >= 10);
    assert.ok(hb.gpspoints_writer_mongo_flush_count >= 1);
    assert.ok(hb.gpspoints_writer_batch_docs_max >= 1);
    assert.ok(hb.gpspoints_writer_last_persisted_at);

    // Producer-local names remain separate / zero on producer metrics object.
    assert.equal(producerMetrics.gpspoints_persisted_total || 0, 0);
    assert.equal(producerMetrics.gpspoints_mongo_flush_count || 0, 0);

    await drainer.flushAndStop(200);
  });

  it("8. missing/corrupt status file does not crash health reader", () => {
    const missing = readGpspointsWriterHeartbeat(spoolDir);
    assert.equal(missing.gpspoints_writer_alive, false);
    assert.equal(missing.gpspoints_writer_persisted_total, 0);

    fs.writeFileSync(path.join(spoolDir, "gpspoints-writer.heartbeat.json"), "{not-json", "utf8");
    const corrupt = readGpspointsWriterHeartbeat(spoolDir);
    assert.equal(corrupt.gpspoints_writer_alive, false);
    assert.equal(corrupt.gpspoints_writer_mongo_attempted_total, 0);
  });

  it("rotation by explicit short age still works when configured", async () => {
    const writer = createGpsPointWriter({
      spoolDir,
      mode: "producer",
      segmentMaxDocs: 10_000,
      segmentMaxAgeMs: 40,
      journalCoalesceMs: 0,
      flushMs: 60_000,
      metrics: {},
    });
    writer.enqueue({ imei: "age", traccar_position_id: 1 });
    writer.journalNow();
    assert.equal(countActiveReady(spoolDir).active, 1);
    await new Promise((r) => setTimeout(r, 80));
    assert.equal(countActiveReady(spoolDir).ready, 1);
    await writer.flushAndStop(200);
  });
});
