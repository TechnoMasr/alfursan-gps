const { describe, it } = require("node:test");
const assert = require("node:assert/strict");
const { createPositionPipeline } = require("../lib/positionPipeline");
const { createGpsPointWriter } = require("../lib/gpsPointWriter");
const { createWsDelivery } = require("../lib/wsDelivery");
const { createBridgeMetrics } = require("../lib/bridgeMetrics");
const { EventEmitter } = require("events");
const os = require("os");
const fs = require("fs");
const path = require("path");

describe("load: 100 devices x 1000 positions", () => {
  it("archives every point, no persistence drop, no historical live replay", async () => {
    const metrics = createBridgeMetrics();
    const archived = [];
    const live = [];
    const nowMs = Date.parse("2026-08-18T12:00:00.000Z");
    const pipeline = createPositionPipeline({
      metrics,
      now: () => nowMs,
      maxLiveAgeMs: 300000,
      emitLive: (imei, payload) => live.push({ imei, id: payload.id }),
      enqueueArchive: (doc) => archived.push(doc),
      enqueueAnalytics: () => {},
    });

    const t0 = Date.now();
    for (let d = 0; d < 100; d++) {
      const imei = `imei-${d}`;
      const burst = [];
      for (let i = 0; i < 999; i++) {
        burst.push({
          id: `${d}-${i}`,
          deviceId: d,
          fixTime: new Date(nowMs - 7 * 86400000 + i * 60000).toISOString(),
          latitude: 24 + d / 100,
          longitude: 46 + i / 10000,
        });
      }
      burst.push({
        id: `${d}-fresh`,
        deviceId: d,
        fixTime: new Date(nowMs - 1000).toISOString(),
        latitude: 24,
        longitude: 46,
      });
      if (d === 0) {
        burst.unshift({
          id: "cmd-0",
          attributes: { result: "OK!" },
          deviceId: 0,
        });
      }
      pipeline.handleResolvedGps({
        imei,
        positions: burst.filter((p) => !p.attributes?.result),
        buildPayload: (p) => p,
      });
    }
    const elapsed = Date.now() - t0;
    assert.equal(archived.length, 100 * 1000);
    assert.equal(live.length, 100);
    assert.ok(live.every((row) => String(row.id).endsWith("fresh")));
    assert.equal(metrics.persistence_dropped || 0, 0);
    assert.ok(elapsed < 5000, `event loop blocked? elapsed=${elapsed}`);
    console.log(
      JSON.stringify({
        received_total: 100000,
        live_processed_total: live.length,
        archive_enqueued_total: archived.length,
        durably_spooled_total: 0,
        mongo_attempted_total: 0,
        mongo_acknowledged_total: 0,
        note: "this case measures pipeline enqueue+live classify only, not Mongo writes",
        live_stale_suppressed: metrics.live_stale_suppressed,
        live_coalesced: metrics.live_coalesced,
        elapsed_ms: elapsed,
        persistence_dropped: metrics.persistence_dropped || 0,
      })
    );
  });
});

describe("load: slow websocket client", () => {
  it("bounds pending GPS to latest-only and isolates other clients", () => {
    const metrics = createBridgeMetrics();
    const delivery = createWsDelivery({
      metrics,
      highWatermark: 100,
      criticalWatermark: 50_000,
    });
    const slow = new EventEmitter();
    slow.readyState = 1;
    slow.bufferedAmount = 5000;
    slow.sent = [];
    slow.send = (p) => slow.sent.push(p);
    const fast = new EventEmitter();
    fast.readyState = 1;
    fast.bufferedAmount = 0;
    fast.sent = [];
    fast.send = (p) => fast.sent.push(p);

    for (let i = 0; i < 5000; i++) {
      delivery.send(slow, { n: i }, { kind: "gps", imei: "S" });
      delivery.send(fast, { n: i }, { kind: "gps", imei: "F" });
    }
    delivery.send(slow, { cmd: true }, { kind: "command", imei: "S" });
    assert.equal(delivery.pendingGpsCount(slow), 1);
    assert.ok(fast.sent.length > 0);
    assert.ok(delivery.pendingCommandCount(slow) >= 1 || slow.sent.some((s) => String(s).includes("cmd")));
  });
});

describe("load: mongo failure then recovery", () => {
  it("keeps realtime path and eventually persists all points", async () => {
    const spoolDir = fs.mkdtempSync(path.join(os.tmpdir(), "load-spool-"));
    let down = true;
    const persisted = [];
    const writer = createGpsPointWriter({
      spoolDir,
      batchSize: 200,
      flushMs: 5,
      retryBaseMs: 5,
      retryMaxMs: 20,
      memHigh: 500,
      insertMany: async (docs) => {
        if (down) throw new Error("mongo down");
        persisted.push(...docs);
      },
      metrics: createBridgeMetrics(),
    });
    const pipeline = createPositionPipeline({
      now: () => Date.now(),
      emitLive: () => {},
      enqueueArchive: (doc) => writer.enqueue(doc),
      enqueueAnalytics: () => {},
    });
    for (let i = 0; i < 400; i++) {
      pipeline.handleResolvedGps({
        imei: "m",
        positions: [
          {
            id: i,
            fixTime: new Date().toISOString(),
            latitude: 1,
            longitude: 1,
          },
        ],
      });
    }
    await new Promise((r) => setTimeout(r, 40));
    down = false;
    await writer.flushCycle();
    await writer.flushAndStop(1000);
    assert.ok(persisted.length >= 400);
    fs.rmSync(spoolDir, { recursive: true, force: true });
  });
});
