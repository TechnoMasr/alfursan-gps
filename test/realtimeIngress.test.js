const { describe, it } = require("node:test");
const assert = require("node:assert/strict");
const { createRealtimeIngress } = require("../lib/realtimeIngress");

describe("realtime ingress pipeline", () => {
  it("runs command fast-path before batch persistence and device/event fanout", async () => {
    const calls = [];
    const ingress = createRealtimeIngress({
      metrics: {},
      groupByDeviceId: (rows) => new Map([["7", rows]]),
      resolveImeiFromCache: () => "imei-7",
      bumpTraccarPositionsReceived: (count) => calls.push(["positions", count]),
      tryProcessCommandResponseIngressSync: (pos) => {
        calls.push(["command-sync", pos.id]);
        return true;
      },
      processCommandResponseIngressDeferred: async () => calls.push(["command-deferred"]),
      processGpsBurst: (imei, rows) => calls.push(["burst", imei, rows.map((r) => r.id)]),
      persistPosition: (pos) => calls.push(["fallback", pos.id]),
      warmImeiCacheFromTraccarDevice: (dev) => calls.push(["warm", dev.id]),
      persistTraccarDeviceStatus: async (dev) => calls.push(["device", dev.id]),
      persistEvent: async (evt) => calls.push(["event", evt.id]),
      hasLegacySubscribers: () => true,
      setImmediateImpl: (fn) => fn(),
    });

    await ingress.handleMessage({
      positions: [{ id: "cmd", attributes: { result: "ok" } }, { id: "gps", latitude: 1, longitude: 2 }],
      devices: [{ id: "d1", uniqueId: "imei-7" }],
      events: [{ id: "e1" }],
    });

    assert.deepEqual(calls.slice(0, 3), [
      ["positions", 2],
      ["command-sync", "cmd"],
      ["burst", "imei-7", ["gps"]],
    ]);
    assert.ok(calls.some((row) => row[0] === "device"));
    assert.ok(calls.some((row) => row[0] === "event"));
  });
});
