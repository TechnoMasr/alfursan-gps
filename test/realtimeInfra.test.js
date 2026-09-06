const { describe, it } = require("node:test");
const assert = require("node:assert/strict");
const { EventEmitter } = require("events");
const { createWsDelivery } = require("../lib/wsDelivery");
const { attachSubscriberHeartbeat } = require("../lib/subscriberHeartbeat");
const { createDeviceResolver } = require("../lib/deviceResolve");
const { createBridgeMetrics, snapshotMetrics } = require("../lib/bridgeMetrics");
const { buildGpsStatusUpdatePipeline } = require("../deviceStatus");

function fakeSocket({ buffered = 0 } = {}) {
  const ws = new EventEmitter();
  ws.readyState = 1;
  ws.bufferedAmount = buffered;
  ws.sent = [];
  ws.send = (payload) => {
    ws.sent.push(payload);
    ws.bufferedAmount += String(payload).length;
  };
  ws.terminate = () => {
    ws.readyState = 3;
    ws.terminated = true;
  };
  ws.ping = () => {
    ws.pinged = (ws.pinged || 0) + 1;
  };
  return ws;
}

describe("websocket backpressure", () => {
  it("keeps only latest GPS per IMEI when buffer is high", () => {
    const metrics = createBridgeMetrics();
    const delivery = createWsDelivery({
      metrics,
      highWatermark: 10,
      lowWatermark: 1,
      criticalWatermark: 10_000,
    });
    const ws = fakeSocket({ buffered: 50 });
    delivery.send(ws, { id: 1 }, { kind: "gps", imei: "A" });
    delivery.send(ws, { id: 2 }, { kind: "gps", imei: "A" });
    delivery.send(ws, { id: 3 }, { kind: "gps", imei: "A" });
    assert.equal(delivery.pendingGpsCount(ws), 1);
    assert.equal(ws._pendingGps.get("A").id, 3);
    assert.ok(metrics.live_backpressure_suppressed >= 3);
    assert.ok(metrics.live_coalesced >= 2);
  });

  it("does not coalesce command responses behind GPS", () => {
    const delivery = createWsDelivery({
      metrics: createBridgeMetrics(),
      highWatermark: 10,
      criticalWatermark: 10_000,
    });
    const ws = fakeSocket({ buffered: 50 });
    delivery.send(ws, { id: "g1" }, { kind: "gps", imei: "A" });
    delivery.send(ws, { id: "c1" }, { kind: "command", imei: "A" });
    delivery.send(ws, { id: "c2" }, { kind: "command", imei: "A" });
    assert.equal(delivery.pendingCommandCount(ws), 2);
    assert.equal(delivery.pendingGpsCount(ws), 1);
  });

  it("terminates unhealthy socket at critical watermark", () => {
    const metrics = createBridgeMetrics();
    const delivery = createWsDelivery({
      metrics,
      highWatermark: 10,
      criticalWatermark: 100,
    });
    const ws = fakeSocket({ buffered: 500 });
    delivery.send(ws, { id: 1 }, { kind: "gps", imei: "A" });
    assert.equal(ws.terminated, true);
    assert.equal(metrics.subscriber_terminated_backpressure, 1);
  });
});

describe("subscriber heartbeat", () => {
  it("terminates sockets that never pong and cleans pending GPS", () => {
    const metrics = createBridgeMetrics();
    const dead = fakeSocket();
    const alive = fakeSocket();
    const clients = [dead, alive];
    const hb = attachSubscriberHeartbeat({
      intervalMs: 20,
      metrics,
      getClients: () => clients,
      onDead: (ws) => {
        ws.terminated = true;
        ws.readyState = 3;
      },
    });
    hb.initSocket(dead);
    hb.initSocket(alive);
    dead.isAlive = false;
    alive.ping = () => {
      alive.isAlive = true;
    };
    alive.isAlive = true;
    return new Promise((resolve) => {
      setTimeout(() => {
        assert.equal(dead.terminated, true);
        assert.equal(alive.terminated, undefined);
        hb.stop();
        resolve();
      }, 50);
    });
  });
});

describe("device fetch in-flight sharing", () => {
  it("50 unknown-device positions share one fetch", async () => {
    let fetches = 0;
    const resolver = createDeviceResolver({
      cooldownMs: 60_000,
      fetchById: async () => {
        fetches += 1;
        await new Promise((r) => setTimeout(r, 20));
        return "imei-1";
      },
    });
    const results = await Promise.all(
      Array.from({ length: 50 }, () => resolver.resolve(9, "test"))
    );
    assert.equal(fetches, 1);
    assert.ok(results.every((r) => r === "imei-1"));
  });
});

describe("device status pipeline", () => {
  it("does not set last_position_at for attrsType 19", () => {
    const pipeline = buildGpsStatusUpdatePipeline({
      update: { last_type: "gps" },
      fixAt: new Date("2026-08-18T12:00:00.000Z"),
      fixValid: true,
      ingressAt: new Date("2026-08-18T12:01:00.000Z"),
      ingressValid: true,
      attrsTypeNum: 19,
      lat: 1,
      lon: 2,
      speed: 0,
      direction: 0,
      hasCoords: true,
    });
    assert.equal(pipeline[0].$set.last_position_at, undefined);
  });

  it("does set last_position_at when attrsType is not 19", () => {
    const ingressAt = new Date("2026-08-18T12:01:00.000Z");
    const pipeline = buildGpsStatusUpdatePipeline({
      update: { last_type: "gps" },
      fixAt: new Date("2026-08-18T12:00:00.000Z"),
      fixValid: true,
      ingressAt,
      ingressValid: true,
      attrsTypeNum: 1,
      lat: 1,
      lon: 2,
      speed: 10,
      direction: 90,
      hasCoords: true,
    });
    assert.equal(pipeline[0].$set.last_position_at, ingressAt);
  });

  it("coords update is conditioned on newer last_fix_at", () => {
    const fixAt = new Date("2026-08-11T12:00:00.000Z");
    const pipeline = buildGpsStatusUpdatePipeline({
      update: { last_type: "gps" },
      fixAt,
      fixValid: true,
      ingressAt: new Date("2026-08-18T12:00:00.000Z"),
      ingressValid: true,
      attrsTypeNum: 1,
      lat: 99,
      lon: 99,
      speed: 5,
      direction: 1,
      hasCoords: true,
    });
    assert.equal(pipeline[0].$set.last_lat.$cond[1], 99);
    assert.ok(pipeline[0].$set.last_fix_at.$cond);
  });
});

describe("health snapshot names", () => {
  it("exposes gpslogs_write_enabled and persistence_dropped separately from live coalescing", () => {
    const metrics = createBridgeMetrics();
    metrics.gpslogs_write_enabled = false;
    const snap = snapshotMetrics(metrics);
    assert.equal(snap.gpslogs_write_enabled, false);
    assert.equal(snap.persistence_dropped, 0);
    assert.equal("live_coalesced" in snap, true);
    assert.equal("coalesced_dropped" in snap, false);
  });

  it("exposes final http-forward ingress and live pipeline counters", () => {
    const metrics = createBridgeMetrics();
    metrics.forward_positions_received_total = 3;
    metrics.forward_queue_depth = 1;
    metrics.live_eligible_total = 3;
    metrics.tenant_gps_emitted_total = 1;
    const snap = snapshotMetrics(metrics);
    assert.equal(snap.traccar_ingress, "http-forward");
    assert.equal(snap.forward_positions_received_total, 3);
    assert.equal(snap.forward_queue_depth, 1);
    assert.equal(snap.live_eligible_total, 3);
    assert.equal(snap.tenant_gps_emitted_total, 1);
    assert.equal(snap.live_fix_stale_device_fresh_total, 0);
  });
});

