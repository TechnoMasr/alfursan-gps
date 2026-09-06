const { describe, it } = require("node:test");
const assert = require("node:assert/strict");
const { createPositionPipeline } = require("../lib/positionPipeline");

describe("fresh live delivery before slow persistence", () => {
  it("subscriber gets GPS immediately while archive waits 5000ms", async () => {
    const liveTimes = [];
    const archived = [];
    const pipeline = createPositionPipeline({
      now: () => 1_000_000,
      maxLiveAgeMs: 300000,
      emitLive: () => liveTimes.push(Date.now()),
      enqueueArchive: (doc) => {
        archived.push(doc);
      },
      enqueueAnalytics: () => {},
    });

    const t0 = Date.now();
    pipeline.handleResolvedGps({
      imei: "123",
      positions: [{ fixTime: new Date(1_000_000 - 1000).toISOString(), latitude: 1, longitude: 2, id: 9 }],
      buildPayload: (p) => ({ type: "gps", data: p }),
    });
    const liveDelay = Date.now() - t0;
    assert.ok(liveTimes.length === 1, "live should fire synchronously");
    assert.ok(liveDelay < 50, `live delay ${liveDelay}ms should not wait persistence`);
    assert.equal(archived.length, 1);

    await new Promise((r) => setTimeout(r, 20));
    assert.ok(Date.now() - t0 < 5000);
  });
});

describe("historical storage without live movement", () => {
  it("stores 7-day-old point and does not emit live GPS", () => {
    const live = [];
    const archive = [];
    const pipeline = createPositionPipeline({
      now: () => Date.parse("2026-08-18T12:00:00.000Z"),
      emitLive: (imei, payload) => live.push(payload),
      enqueueArchive: (doc) => archive.push(doc),
      enqueueAnalytics: () => {},
    });
    pipeline.handleResolvedGps({
      imei: "123",
      positions: [
        {
          fixTime: "2026-08-11T12:00:00.000Z",
          latitude: 10,
          longitude: 20,
          id: 1,
        },
      ],
    });
    assert.equal(archive.length, 1);
    assert.equal(live.length, 0);
  });
});

describe("out-of-order live vs archive", () => {
  it("archives 4 points and live-emits 12:00, 12:05, not 12:03, then 12:06", () => {
    const live = [];
    const archive = [];
    const nowMs = Date.parse("2026-08-18T12:10:00.000Z");
    const pipeline = createPositionPipeline({
      now: () => nowMs,
      maxLiveAgeMs: 30 * 60 * 1000,
      emitLive: (imei, payload) => live.push(payload.fixTime),
      enqueueArchive: (doc) => archive.push(doc.position.fixTime),
      enqueueAnalytics: () => {},
    });
    const send = (fixTime) =>
      pipeline.handleResolvedGps({
        imei: "123",
        positions: [{ fixTime, latitude: 1, longitude: 1 }],
        buildPayload: (p) => p,
      });
    send("2026-08-18T12:00:00.000Z");
    send("2026-08-18T12:05:00.000Z");
    send("2026-08-18T12:03:00.000Z");
    send("2026-08-18T12:06:00.000Z");
    assert.equal(archive.length, 4);
    assert.deepEqual(live, [
      "2026-08-18T12:00:00.000Z",
      "2026-08-18T12:05:00.000Z",
      "2026-08-18T12:06:00.000Z",
    ]);
  });
});

describe("historical burst then fresh", () => {
  it("does not replay history on the live map", () => {
    const live = [];
    const archive = [];
    const nowMs = Date.parse("2026-08-18T12:00:00.000Z");
    const pipeline = createPositionPipeline({
      now: () => nowMs,
      emitLive: (imei, payload) => live.push(payload.id),
      enqueueArchive: (doc) => archive.push(doc),
      enqueueAnalytics: () => {},
    });
    const historical = [];
    for (let i = 0; i < 100; i++) {
      historical.push({
        id: i,
        fixTime: new Date(nowMs - 7 * 24 * 3600 * 1000 + i * 60000).toISOString(),
        latitude: 1,
        longitude: 1,
      });
    }
    historical.push({
      id: "fresh",
      fixTime: new Date(nowMs - 1000).toISOString(),
      latitude: 2,
      longitude: 2,
    });
    pipeline.handleResolvedGps({
      imei: "123",
      positions: historical,
      buildPayload: (p) => p,
    });
    assert.equal(archive.length, 101);
    assert.deepEqual(live, ["fresh"]);
  });
});

describe("frozen fixTime with moving deviceTime", () => {
  it("legacy fallback-off stops live after 5 minutes", () => {
    const live = [];
    const t0 = Date.parse("2026-08-18T12:00:00.000Z");
    const pipeline = createPositionPipeline({
      now: () => t0 + 6 * 60_000,
      maxLiveAgeMs: 300000,
      allowDeviceTimeFallback: false,
      emitLive: (_imei, payload) => live.push(payload.id),
      enqueueArchive: () => {},
    });
    pipeline.handleResolvedGps({
      imei: "frozen",
      positions: [
        {
          id: "late",
          fixTime: new Date(t0).toISOString(),
          deviceTime: new Date(t0 + 6 * 60_000).toISOString(),
          serverTime: new Date(t0 + 6 * 60_000).toISOString(),
          latitude: 24.72,
          longitude: 46.62,
          speed: 40,
        },
      ],
      buildPayload: (p) => p,
    });
    assert.equal(live.length, 0);
  });

  it("deviceTime fallback keeps live after 10 minutes of frozen fixTime", () => {
    const live = [];
    const archiveFixTimes = [];
    const t0 = Date.parse("2026-08-18T12:00:00.000Z");
    const frozenFix = new Date(t0).toISOString();
    let nowMs = t0;
    const pipeline = createPositionPipeline({
      now: () => nowMs,
      maxLiveAgeMs: 300000,
      allowDeviceTimeFallback: true,
      emitLive: (_imei, payload) => live.push(payload.deviceTime),
      enqueueArchive: (doc) => archiveFixTimes.push(doc.position.fixTime),
    });
    for (let elapsed = 0; elapsed <= 10 * 60_000; elapsed += 30_000) {
      nowMs = t0 + elapsed;
      pipeline.handleResolvedGps({
        imei: "frozen",
        positions: [
          {
            id: elapsed,
            fixTime: frozenFix,
            deviceTime: new Date(nowMs).toISOString(),
            serverTime: new Date(nowMs).toISOString(),
            latitude: 24.7 + elapsed / 1e8,
            longitude: 46.6 + elapsed / 1e8,
            speed: 40,
            outdated: false,
          },
        ],
        buildPayload: (p) => p,
      });
    }
    assert.equal(live.length, 21);
    assert.equal(archiveFixTimes.length, 21);
    assert.ok(archiveFixTimes.every((t) => t === frozenFix));
    assert.equal(live[live.length - 1], new Date(t0 + 10 * 60_000).toISOString());
  });

  it("stale fix + stale device + fresh server never emits live", () => {
    const live = [];
    const nowMs = Date.parse("2026-08-18T12:00:00.000Z");
    const pipeline = createPositionPipeline({
      now: () => nowMs,
      emitLive: () => live.push(1),
      enqueueArchive: () => {},
    });
    pipeline.handleResolvedGps({
      imei: "hist",
      positions: [
        {
          fixTime: new Date(nowMs - 60 * 60_000).toISOString(),
          deviceTime: new Date(nowMs - 60 * 60_000).toISOString(),
          serverTime: new Date(nowMs).toISOString(),
          latitude: 1,
          longitude: 1,
          speed: 50,
        },
      ],
    });
    assert.equal(live.length, 0);
  });

  it("week-old journey with moving coords is archived and never live", () => {
    const live = [];
    const archive = [];
    const nowMs = Date.parse("2026-08-18T12:00:00.000Z");
    const pipeline = createPositionPipeline({
      now: () => nowMs,
      emitLive: () => live.push(1),
      enqueueArchive: (doc) => archive.push(doc),
    });
    const weekAgo = nowMs - 7 * 24 * 60 * 60_000;
    pipeline.handleResolvedGps({
      imei: "replay",
      positions: Array.from({ length: 8 }, (_, i) => ({
        id: i,
        fixTime: new Date(weekAgo + i * 60_000).toISOString(),
        deviceTime: new Date(weekAgo + i * 60_000).toISOString(),
        serverTime: new Date(nowMs).toISOString(),
        latitude: 24 + i * 0.01,
        longitude: 46 + i * 0.01,
        speed: 80,
      })),
    });
    assert.equal(archive.length, 8);
    assert.equal(live.length, 0);
  });
});

describe("deferred persistence batch", () => {
  it("emits live before handing the batch to persistence", () => {
    const calls = [];
    const pipeline = createPositionPipeline({
      now: () => Date.parse("2026-08-18T12:00:00.000Z"),
      emitLive: () => calls.push("live"),
      onPersistenceBatch: () => calls.push("batch"),
      deferPersistence: true,
      enqueueArchive: () => {
        throw new Error("archive should be deferred");
      },
      enqueueAnalytics: () => {},
    });

    pipeline.handleResolvedGps({
      imei: "123",
      positions: [
        {
          fixTime: "2026-08-18T11:59:59.000Z",
          latitude: 1,
          longitude: 1,
        },
      ],
      buildPayload: (p) => p,
    });

    assert.deepEqual(calls, ["live", "batch"]);
  });
});
