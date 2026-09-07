const { describe, it, beforeEach, afterEach } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const os = require("os");
const path = require("path");
const { createGpsPointWriter } = require("../lib/gpsPointWriter");

function tmpDir(prefix) {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

function listSpool(dir) {
  return fs.readdirSync(dir).filter((n) => !n.endsWith(".tmp") && n !== "quarantine");
}

function listBy(dir, pred) {
  return listSpool(dir).filter(pred).map((n) => path.join(dir, n));
}

describe("E2 segmented GPSPoints journal", () => {
  let spoolDir;
  beforeEach(() => {
    spoolDir = tmpDir("e2-seg-");
  });
  afterEach(() => {
    fs.rmSync(spoolDir, { recursive: true, force: true });
  });

  it("1. many points append to one active/ready segment", async () => {
    const writer = createGpsPointWriter({
      spoolDir,
      mode: "full",
      batchSize: 250,
      segmentMaxDocs: 500,
      segmentMaxBytes: 10 * 1024 * 1024,
      segmentSealMs: 60_000,
      journalCoalesceMs: 0,
      flushMs: 60_000,
      insertMany: async () => {
        throw new Error("mongo down");
      },
      metrics: {},
    });
    for (let i = 0; i < 40; i++) {
      writer.enqueue({ imei: "a", traccar_position_id: i, latitude: 1, longitude: 1 });
    }
    await writer.flushJournal();
    const actives = listBy(spoolDir, (n) => n.endsWith(".jsonl.active"));
    const readies = listBy(spoolDir, (n) => n.endsWith(".jsonl.ready"));
    assert.equal(actives.length + readies.length, 1);
    const file = actives[0] || readies[0];
    const lines = fs.readFileSync(file, "utf8").split("\n").filter((l) => l.trim());
    assert.equal(lines.length, 40);
    writer.simulateCrash();
  });

  it("2. ACK/journal completes without Mongo", async () => {
    let mongo = false;
    const metrics = {};
    const writer = createGpsPointWriter({
      spoolDir,
      mode: "producer",
      segmentSealMs: 60_000,
      journalCoalesceMs: 0,
      flushMs: 60_000,
      insertMany: async () => {
        mongo = true;
      },
      metrics,
    });
    writer.enqueue({ imei: "ack", traccar_position_id: 1 });
    const ok = writer.journalNow();
    assert.equal(ok, true);
    assert.equal(metrics.gpspoints_journaled_total, 1);
    assert.equal(mongo, false);
    assert.ok(listBy(spoolDir, (n) => n.endsWith(".jsonl.active")).length === 1);
    await writer.flushAndStop(200);
  });

  it("3. rotation by doc threshold", async () => {
    const writer = createGpsPointWriter({
      spoolDir,
      mode: "producer",
      segmentMaxDocs: 5,
      segmentMaxBytes: 10 * 1024 * 1024,
      segmentSealMs: 60_000,
      journalCoalesceMs: 0,
      flushMs: 60_000,
      metrics: {},
    });
    for (let i = 0; i < 5; i++) {
      writer.enqueue({ imei: "d", traccar_position_id: i });
    }
    writer.journalNow();
    assert.equal(listBy(spoolDir, (n) => n.endsWith(".jsonl.ready")).length, 1);
    assert.equal(listBy(spoolDir, (n) => n.endsWith(".jsonl.active")).length, 0);
    await writer.flushAndStop(200);
  });

  it("4. rotation by byte threshold", async () => {
    const writer = createGpsPointWriter({
      spoolDir,
      mode: "producer",
      segmentMaxDocs: 10_000,
      segmentMaxBytes: 200,
      segmentSealMs: 60_000,
      journalCoalesceMs: 0,
      flushMs: 60_000,
      metrics: {},
    });
    for (let i = 0; i < 20; i++) {
      writer.enqueue({
        imei: "bytes",
        traccar_position_id: i,
        pad: "x".repeat(40),
      });
    }
    writer.journalNow();
    assert.ok(listBy(spoolDir, (n) => n.endsWith(".jsonl.ready")).length >= 1);
    await writer.flushAndStop(200);
  });

  it("5. rotation by time deadline", async () => {
    const writer = createGpsPointWriter({
      spoolDir,
      mode: "producer",
      segmentMaxDocs: 10_000,
      segmentMaxBytes: 10 * 1024 * 1024,
      segmentSealMs: 30,
      journalCoalesceMs: 0,
      flushMs: 60_000,
      metrics: {},
    });
    writer.enqueue({ imei: "t", traccar_position_id: 1 });
    writer.journalNow();
    assert.equal(listBy(spoolDir, (n) => n.endsWith(".jsonl.active")).length, 1);
    await new Promise((r) => setTimeout(r, 80));
    assert.equal(listBy(spoolDir, (n) => n.endsWith(".jsonl.ready")).length, 1);
    await writer.flushAndStop(200);
  });

  it("6. restart recovers active segment", async () => {
    const first = createGpsPointWriter({
      spoolDir,
      mode: "producer",
      segmentMaxDocs: 10_000,
      segmentSealMs: 60_000,
      journalCoalesceMs: 0,
      flushMs: 60_000,
      metrics: {},
    });
    for (let i = 0; i < 7; i++) first.enqueue({ imei: "r", traccar_position_id: i });
    first.journalNow();
    assert.ok(listBy(spoolDir, (n) => n.endsWith(".jsonl.active")).length === 1);
    first.simulateCrash();

    const recovered = [];
    const second = createGpsPointWriter({
      spoolDir,
      mode: "full",
      segmentSealMs: 60_000,
      flushMs: 60_000,
      insertMany: async (docs) => {
        recovered.push(...docs);
      },
      metrics: {},
    });
    second.sealActiveSegment("test");
    await second.flushCycle();
    assert.equal(recovered.length, 7);
    await second.flushAndStop(200);
  });

  it("7. sealed segments survive restart", async () => {
    const first = createGpsPointWriter({
      spoolDir,
      mode: "producer",
      segmentMaxDocs: 3,
      segmentSealMs: 60_000,
      journalCoalesceMs: 0,
      flushMs: 60_000,
      metrics: {},
    });
    for (let i = 0; i < 3; i++) first.enqueue({ imei: "s", traccar_position_id: i });
    first.journalNow();
    assert.equal(listBy(spoolDir, (n) => n.endsWith(".jsonl.ready")).length, 1);
    first.simulateCrash();

    const recovered = [];
    const second = createGpsPointWriter({
      spoolDir,
      mode: "drain",
      flushMs: 60_000,
      insertMany: async (docs) => {
        recovered.push(...docs);
      },
      metrics: {},
    });
    await second.flushCycle();
    assert.equal(recovered.length, 3);
    await second.flushAndStop(200);
  });

  it("8. legacy hot/pending files still drain", async () => {
    for (let i = 1; i <= 5; i++) {
      fs.writeFileSync(
        path.join(spoolDir, `pending-${String(i).padStart(13, "0")}-leg.jsonl`),
        JSON.stringify({ imei: "leg", traccar_position_id: i }) + "\n"
      );
    }
    const recovered = [];
    const writer = createGpsPointWriter({
      spoolDir,
      mode: "drain",
      batchSize: 250,
      maxFilesPerCycle: 500,
      flushMs: 60_000,
      insertMany: async (docs) => {
        recovered.push(...docs);
      },
      metrics: {},
    });
    await writer.flushCycle();
    assert.equal(recovered.length, 5);
    await writer.flushAndStop(200);
  });

  it("9. new ready segments drain correctly", async () => {
    const producer = createGpsPointWriter({
      spoolDir,
      mode: "producer",
      segmentMaxDocs: 10,
      segmentSealMs: 60_000,
      journalCoalesceMs: 0,
      flushMs: 60_000,
      metrics: {},
    });
    for (let i = 0; i < 10; i++) producer.enqueue({ imei: "n", traccar_position_id: i });
    producer.journalNow();
    await producer.flushAndStop(200);

    const recovered = [];
    const drainer = createGpsPointWriter({
      spoolDir,
      mode: "drain",
      flushMs: 60_000,
      insertMany: async (docs) => {
        recovered.push(...docs);
      },
      metrics: {},
    });
    await drainer.flushCycle();
    assert.equal(recovered.length, 10);
    await drainer.flushAndStop(200);
  });

  it("10. no one-file-per-point regression", async () => {
    const writer = createGpsPointWriter({
      spoolDir,
      mode: "producer",
      segmentMaxDocs: 1000,
      segmentSealMs: 60_000,
      journalCoalesceMs: 20,
      flushMs: 60_000,
      metrics: {},
    });
    for (let i = 0; i < 50; i++) {
      writer.enqueue({ imei: "one", traccar_position_id: i });
    }
    await new Promise((r) => setTimeout(r, 60));
    writer.journalNow();
    const files = listSpool(spoolDir).filter(
      (n) => n.endsWith(".jsonl.active") || n.endsWith(".jsonl.ready")
    );
    assert.equal(files.length, 1);
    await writer.flushAndStop(200);
  });

  it("11. Mongo failure keeps segment on disk", async () => {
    const writer = createGpsPointWriter({
      spoolDir,
      mode: "full",
      segmentMaxDocs: 5,
      segmentSealMs: 60_000,
      journalCoalesceMs: 0,
      flushMs: 60_000,
      insertMany: async () => {
        throw new Error("mongo down");
      },
      metrics: {},
    });
    for (let i = 0; i < 5; i++) writer.enqueue({ imei: "m", traccar_position_id: i });
    writer.journalNow();
    await writer.flushCycle();
    const remaining = listSpool(spoolDir).filter(
      (n) =>
        n.endsWith(".jsonl.ready") ||
        n.includes(".draining.") ||
        n.startsWith("pending-")
    );
    assert.ok(remaining.length >= 1);
    writer.simulateCrash();
  });

  it("12. shutdown seals active segment", async () => {
    const writer = createGpsPointWriter({
      spoolDir,
      mode: "producer",
      segmentMaxDocs: 10_000,
      segmentSealMs: 60_000,
      journalCoalesceMs: 0,
      flushMs: 60_000,
      metrics: {},
    });
    writer.enqueue({ imei: "z", traccar_position_id: 1 });
    writer.journalNow();
    assert.equal(listBy(spoolDir, (n) => n.endsWith(".jsonl.active")).length, 1);
    await writer.flushAndStop(500);
    assert.equal(listBy(spoolDir, (n) => n.endsWith(".jsonl.active")).length, 0);
    assert.equal(listBy(spoolDir, (n) => n.endsWith(".jsonl.ready")).length, 1);
  });
});
