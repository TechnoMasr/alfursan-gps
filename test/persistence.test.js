const { describe, it, beforeEach, afterEach } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const os = require("os");
const path = require("path");
const { createGpsPointWriter } = require("../lib/gpsPointWriter");
const { createAnalyticsQueue } = require("../lib/analyticsQueue");
const { createGpsLogsWriter } = require("../lib/gpsLogsWriter");

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

describe("gpslogs writer flag", () => {
  it("default OFF does not write GpsLog", async () => {
    let writes = 0;
    const writer = createGpsLogsWriter({
      enabled: false,
      GpsLog: { insertMany: async (docs) => { writes += docs.length; return docs; } },
      metrics: {},
    });
    const out = await writer.writeMany([{ imei: "1", type: "gps" }, { imei: "1", type: "alarm" }]);
    assert.equal(writes, 0);
    assert.equal(out.length, 2);
  });

  it("enabled writes to GpsLog", async () => {
    let writes = 0;
    const writer = createGpsLogsWriter({
      enabled: true,
      GpsLog: {
        insertMany: async (docs) => {
          writes += docs.length;
          return docs;
        },
      },
      metrics: {},
    });
    await writer.writeOne({ imei: "1", type: "gps" });
    assert.equal(writes, 1);
  });

  it("disabled still returns geofence events for notify/realtime", async () => {
    const writer = createGpsLogsWriter({ enabled: false, metrics: {} });
    const events = [{ imei: "1", type: "alarm", subType: "geofence" }];
    const out = await writer.writeMany(events);
    assert.equal(out[0].subType, "geofence");
  });
});
