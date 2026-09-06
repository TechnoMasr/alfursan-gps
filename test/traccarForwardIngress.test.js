const { describe, it } = require("node:test");
const assert = require("node:assert/strict");
const {
  verifyForwardBearer,
  isJsonContentType,
  normalizeForwardPayload,
} = require("../lib/traccarForwardIngress");
const { createTraccarForwardQueue } = require("../lib/traccarForwardQueue");
const { resolveRuntimeDeviceIdByImei } = require("../lib/traccarRuntimeDevices");

describe("traccar forward ingress adapter", () => {
  it("validates bearer token without accepting missing secrets", () => {
    assert.deepEqual(verifyForwardBearer("Bearer secret", "secret"), { ok: true });
    assert.equal(verifyForwardBearer("Bearer nope", "secret").ok, false);
    assert.equal(verifyForwardBearer("Bearer secret", "").reason, "forward_token_not_configured");
  });

  it("accepts only application/json content type", () => {
    assert.equal(isJsonContentType({ headers: { "content-type": "application/json" } }), true);
    assert.equal(isJsonContentType({ headers: { "content-type": "application/json; charset=utf-8" } }), true);
    assert.equal(isJsonContentType({ headers: { "content-type": "text/plain" } }), false);
  });

  it("normalizes a Traccar-style device + position payload with direct uniqueId", () => {
    const out = normalizeForwardPayload({
      device: { id: 7, uniqueId: "359339080000001", name: "359339080000001", model: "SEEWORLD" },
      position: {
        id: 99,
        protocol: "gt06",
        serverTime: "2026-09-06T01:00:00Z",
        deviceTime: "2026-09-06T00:59:59Z",
        fixTime: "2026-09-06T00:59:58Z",
        latitude: "24.7136",
        longitude: "46.6753",
        speed: "10",
        course: "91",
        attributes: { ignition: true },
      },
    });

    assert.equal(out.ok, true);
    assert.equal(out.items.length, 1);
    assert.equal(out.items[0].imei, "359339080000001");
    assert.equal(out.items[0].runtimeDeviceId, 7);
    assert.equal(out.items[0].runtimeDevice.model, "SEEWORLD");
    assert.equal(out.items[0].position.deviceId, 7);
    assert.equal(out.items[0].position.deviceModel, "SEEWORLD");
    assert.equal(out.items[0].position.latitude, 24.7136);
    assert.equal(out.items[0].position.longitude, 46.6753);
    assert.equal(out.items[0].position.speed, 10);
    assert.equal(out.items[0].position.attributes.ignition, true);
  });

  it("normalizes alternate wrapped forwarded payloads", () => {
    const out = normalizeForwardPayload({
      data: {
        positions: [
          {
            device: { id: 11, uniqueId: "imei-11" },
            lat: "1.25",
            lng: "2.5",
            direction: 180,
            attributes: { alarm: "overspeed" },
          },
        ],
      },
    });
    assert.equal(out.ok, true);
    assert.equal(out.items[0].imei, "imei-11");
    assert.equal(out.items[0].runtimeDeviceId, 11);
    assert.equal(out.items[0].position.longitude, 2.5);
    assert.equal(out.items[0].position.course, 180);
    assert.equal(out.items[0].position.attributes.alarm, "overspeed");
  });

  it("allows missing uniqueId when deviceId is present for cache fallback", () => {
    const out = normalizeForwardPayload({
      position: {
        deviceId: 7,
        latitude: 1,
        longitude: 2,
        fixTime: "2026-09-06T00:00:00Z",
      },
    });
    assert.equal(out.ok, true);
    assert.equal(out.items[0].imei, null);
    assert.equal(out.items[0].runtimeDeviceId, 7);
    assert.equal(out.items[0].position.deviceId, 7);
  });

  it("detects command responses carried in attributes.result", () => {
    const out = normalizeForwardPayload({
      imei: "123",
      position: {
        latitude: 1,
        longitude: 2,
        attributes: { result: "OK!" },
      },
    });
    assert.equal(out.items[0].hasCommandResponse, true);
    assert.equal(out.items[0].position.attributes.result, "OK!");
  });

  it("rejects invalid forwarded JSON shape", () => {
    const out = normalizeForwardPayload({ position: { latitude: 1 } });
    assert.equal(out.ok, false);
    assert.equal(out.invalid[0].reason, "missing_uniqueId_or_deviceId");
  });

  it("bounded forward queue rejects when full", async () => {
    const metrics = {};
    let release;
    const first = new Promise((resolve) => {
      release = resolve;
    });
    const q = createTraccarForwardQueue({
      maxDepth: 1,
      metrics,
      processFn: async () => first,
      setImmediateFn: () => {},
    });
    assert.equal(q.enqueue({ id: 1 }).accepted, true);
    assert.equal(q.enqueue({ id: 2 }).accepted, false);
    assert.equal(metrics.forward_queue_rejected_total, 1);
    release();
    await q.flushAndStop(10);
  });
});

describe("traccar runtime device command lookup", () => {
  it("resolves command device id by IMEI with uniqueId query", async () => {
    const calls = [];
    const cache = new Map();
    const id = await resolveRuntimeDeviceIdByImei({
      imei: "123",
      cache,
      client: {
        get: async (path, opts) => {
          calls.push({ path, opts });
          return { data: [{ id: 42, uniqueId: "123" }] };
        },
      },
    });
    assert.equal(id, 42);
    assert.equal(calls[0].path, "/api/devices");
    assert.deepEqual(calls[0].opts.params, { uniqueId: "123" });
    assert.equal(cache.get(42), "123");
  });

  it("uses newly resolved runtime id after simulated Traccar restart", async () => {
    const ids = [42, 77];
    const client = {
      get: async () => ({ data: [{ id: ids.shift(), uniqueId: "123" }] }),
    };
    assert.equal(await resolveRuntimeDeviceIdByImei({ imei: "123", client }), 42);
    assert.equal(await resolveRuntimeDeviceIdByImei({ imei: "123", client }), 77);
  });

  it("does not use persistent numeric device id assumptions", async () => {
    const id = await resolveRuntimeDeviceIdByImei({
      imei: "123",
      client: {
        get: async () => ({ data: [{ id: 9, uniqueId: "123" }] }),
      },
    });
    assert.equal(id, 9);
  });

  it("returns clear error when device is not registered in runtime", async () => {
    await assert.rejects(
      resolveRuntimeDeviceIdByImei({
        imei: "missing",
        client: { get: async () => ({ data: [] }) },
      }),
      /device_not_registered_in_traccar_runtime/
    );
  });
});
