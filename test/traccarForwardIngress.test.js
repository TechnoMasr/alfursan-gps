const { describe, it } = require("node:test");
const assert = require("node:assert/strict");
const {
  normalizeIngressMode,
  verifyForwardBearer,
  isJsonContentType,
  normalizeForwardPayload,
  positionShadowSignature,
} = require("../lib/traccarForwardIngress");

describe("traccar forward ingress adapter", () => {
  it("normalizes ingress mode with safe ws fallback", () => {
    assert.equal(normalizeIngressMode("forward"), "forward");
    assert.equal(normalizeIngressMode("forward-shadow"), "forward-shadow");
    assert.equal(normalizeIngressMode("ws"), "ws");
    assert.equal(normalizeIngressMode("surprise"), "ws");
  });

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
      device: { id: 7, uniqueId: "359339080000001" },
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
    assert.equal(out.items[0].position.deviceId, 7);
    assert.equal(out.items[0].position.latitude, 24.7136);
    assert.equal(out.items[0].position.longitude, 46.6753);
    assert.equal(out.items[0].position.attributes.ignition, true);
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

  it("uses stable shadow signatures for comparison metrics", () => {
    const a = positionShadowSignature({
      deviceId: 7,
      fixTime: "2026-09-06T00:00:00.000Z",
      latitude: 24.1234564,
      longitude: 46.1234564,
      speed: 4,
      attributes: { alarm: "overspeed" },
    });
    const b = positionShadowSignature({
      deviceId: 7,
      fixTime: "2026-09-06T00:00:00.000Z",
      latitude: 24.12345649,
      longitude: 46.12345649,
      speed: 4,
      attributes: { alarm: "overspeed" },
    });
    assert.equal(a, b);
  });
});
