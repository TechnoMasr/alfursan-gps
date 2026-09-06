const { describe, it } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const os = require("os");
const path = require("path");
const { createTenantRoomGpsThrottle } = require("../lib/tenantRoomGpsThrottle");
const { countSubscriberMetrics } = require("../lib/subscriberCounts");
const { parseDebugImeis, createImeiDebugger } = require("../lib/imeiDebug");
const { createGpsPointWriter } = require("../lib/gpsPointWriter");
const { createBridgeMetrics } = require("../lib/bridgeMetrics");

function gpsPayload({ imei, speed, lat = 24.7, lng = 46.6 }) {
  return {
    type: "gps",
    data: {
      imei,
      speed,
      packet_date: new Date().toISOString(),
      gps: { latitude: lat, longitude: lng, speed },
      attributes: { type: 1 },
    },
  };
}

describe("tenant-room throttle", () => {
  it("moving speed>0 never takes the stationary deadband branch", () => {
    const emitted = [];
    const metrics = createBridgeMetrics();
    const throttle = createTenantRoomGpsThrottle({
      metrics,
      emit: (imei, room, payload) => emitted.push({ imei, room, speed: payload.data.speed }),
    });
    const t0 = Date.now();
    throttle.push("A", "tenant:1", gpsPayload({ imei: "A", speed: 40, lat: 24.7, lng: 46.6 }));
    throttle.push("A", "tenant:1", gpsPayload({ imei: "A", speed: 40, lat: 24.70001, lng: 46.60001 }));
    assert.equal(emitted.length, 2);
    assert.equal(metrics.tenant_room_gps_stationary_suppressed, 0);
    assert.ok(Date.now() - t0 < 1000);
  });

  it("stationary deadband suppresses small drift", () => {
    let now = 1_000;
    const emitted = [];
    const metrics = createBridgeMetrics();
    const throttle = createTenantRoomGpsThrottle({
      metrics,
      now: () => now,
      emit: (imei) => emitted.push(imei),
      setTimeoutFn: () => 1,
      clearTimeoutFn: () => {},
    });
    throttle.push("S", "tenant:1", gpsPayload({ imei: "S", speed: 0, lat: 24.7, lng: 46.6 }));
    now += 1000;
    const r = throttle.push("S", "tenant:1", gpsPayload({ imei: "S", speed: 0, lat: 24.70001, lng: 46.60001 }));
    assert.equal(r.reason, "stationary_deadband");
    assert.equal(emitted.length, 1);
    assert.equal(metrics.tenant_room_gps_stationary_suppressed, 1);
  });

  it("throttle is per IMEI not global", () => {
    const emitted = [];
    const throttle = createTenantRoomGpsThrottle({
      windowMs: 30_000,
      maxPerWindow: 2,
      metrics: createBridgeMetrics(),
      emit: (imei) => emitted.push(imei),
    });
    throttle.push("A", "tenant:1", gpsPayload({ imei: "A", speed: 40 }));
    throttle.push("A", "tenant:1", gpsPayload({ imei: "A", speed: 40 }));
    throttle.push("A", "tenant:1", gpsPayload({ imei: "A", speed: 40 }));
    throttle.push("B", "tenant:1", gpsPayload({ imei: "B", speed: 40 }));
    assert.deepEqual(emitted.filter((i) => i === "B"), ["B"]);
    assert.equal(emitted.filter((i) => i === "A").length, 2);
  });

  it("tenant A does not affect tenant B", () => {
    const rooms = [];
    const throttle = createTenantRoomGpsThrottle({
      maxPerWindow: 1,
      metrics: createBridgeMetrics(),
      emit: (imei, room) => rooms.push(room),
    });
    throttle.push("A", "tenant:1", gpsPayload({ imei: "A", speed: 40 }));
    throttle.push("B", "tenant:2", gpsPayload({ imei: "B", speed: 40 }));
    assert.deepEqual(rooms, ["tenant:1", "tenant:2"]);
  });

  it("200 moving + 1800 stationary load keeps moving emits", () => {
    const movingEmits = new Set();
    const throttle = createTenantRoomGpsThrottle({
      metrics: createBridgeMetrics(),
      emit: (imei, room, payload) => {
        if (payload.data.speed > 0) movingEmits.add(imei);
      },
    });
    for (let i = 0; i < 200; i++) {
      throttle.push(`m${i}`, `tenant:${(i % 10) + 1}`, gpsPayload({ imei: `m${i}`, speed: 40 }));
    }
    for (let i = 0; i < 1800; i++) {
      throttle.push(`s${i}`, `tenant:${(i % 10) + 1}`, gpsPayload({ imei: `s${i}`, speed: 0 }));
    }
    assert.equal(movingEmits.size, 200);
  });
});

describe("subscriber unique sockets", () => {
  it("does not double-count a socket in tenant room + command channel", () => {
    const ws = { id: 1 };
    const deviceSubscribers = new Map();
    const rooms = new Map([
      ["tenant:1", new Set([ws])],
      ["command_response_chanel", new Set([ws])],
    ]);
    const m = countSubscriberMetrics({ deviceSubscribers, rooms });
    assert.equal(m.subscriber_unique_sockets, 1);
    assert.equal(m.subscriber_room_memberships, 2);
    assert.equal(m.tenant_room_listeners, 1);
    assert.equal(m.command_response_channel_listeners, 1);
  });
});

describe("targeted IMEI diagnostic", () => {
  it("logs only the configured IMEI", () => {
    const lines = [];
    const dbg = createImeiDebugger({
      envValue: "111,222",
      log: { info: (...a) => lines.push(a) },
    });
    dbg.logPosition({ imei: "111", live_decision: "sent" });
    dbg.logPosition({ imei: "999", live_decision: "sent" });
    assert.equal(lines.length, 1);
    assert.equal(parseDebugImeis("111,222").size, 2);
  });
});

describe("spool incremental stats", () => {
  it("does not readdir on every journal write", async () => {
    const spoolDir = fs.mkdtempSync(path.join(os.tmpdir(), "spool-perf-"));
    for (let i = 0; i < 80; i++) {
      fs.writeFileSync(path.join(spoolDir, `hot-1-${i}.jsonl`), "{}\n");
    }
    const writer = createGpsPointWriter({
      spoolDir,
      flushMs: 60_000,
      journalCoalesceMs: 0,
      spoolReconcileMs: 0,
      insertMany: async () => {
        throw new Error("down");
      },
      metrics: {},
    });
    const before = writer.getReaddirCount();
    writer.enqueue({ imei: "x", latitude: 1, longitude: 1, traccar_position_id: 1 });
    await writer.flushJournal();
    const after = writer.getReaddirCount();
    assert.equal(after, before);
    writer.simulateCrash();
    fs.rmSync(spoolDir, { recursive: true, force: true });
  });
});
