const { describe, it } = require("node:test");
const assert = require("node:assert/strict");
const {
  classifyLiveFix,
  isLiveEligible,
  isArchiveEligible,
  pickLatestLiveFromBurst,
  createLiveFixTracker,
} = require("../lib/liveEligibility");

const NOW = Date.parse("2026-08-18T12:00:00.000Z");

describe("live vs archive eligibility", () => {
  it("historical GPS is not live-eligible but is archive-eligible", () => {
    const oldPoint = {
      fixTime: "2026-08-11T12:00:00.000Z",
      deviceTime: "2026-08-11T12:00:00.000Z",
      serverTime: "2026-08-18T12:00:00.000Z",
    };
    assert.equal(isLiveEligible(oldPoint, { nowMs: NOW }), false);
    assert.equal(isArchiveEligible(oldPoint, { nowMs: NOW }), true);
    assert.equal(classifyLiveFix(oldPoint, { nowMs: NOW }).decision, "historical");
  });

  it("does not treat serverTime as the live clock", () => {
    const weekOldFixNowReceived = {
      fixTime: "2026-08-11T12:00:00.000Z",
      serverTime: new Date(NOW).toISOString(),
    };
    const cls = classifyLiveFix(weekOldFixNowReceived, { nowMs: NOW, maxLiveAgeMs: 300000 });
    assert.equal(cls.decision, "historical");
    assert.equal(cls.liveEligible, false);
    assert.equal(cls.archiveEligible, true);
  });

  it("fresh point within max age is live", () => {
    const fresh = { fixTime: new Date(NOW - 10_000).toISOString() };
    const cls = classifyLiveFix(fresh, { nowMs: NOW, maxLiveAgeMs: 300000 });
    assert.equal(cls.decision, "fresh");
    assert.equal(cls.liveEligible, true);
  });

  it("out-of-order after a newer live fix is archived not live", () => {
    const cls = classifyLiveFix(
      { fixTime: new Date(NOW - 20_000).toISOString() },
      { nowMs: NOW, lastLiveFixMs: NOW - 5_000, maxLiveAgeMs: 300000 }
    );
    assert.equal(cls.decision, "out_of_order");
    assert.equal(cls.archiveEligible, true);
    assert.equal(cls.liveEligible, false);
  });

  it("missing_time is not live, still archive-eligible", () => {
    const cls = classifyLiveFix({}, { nowMs: NOW });
    assert.equal(cls.decision, "missing_time");
    assert.equal(cls.liveEligible, false);
    assert.equal(cls.archiveEligible, true);
  });

  it("future beyond tolerance is not live", () => {
    const cls = classifyLiveFix(
      { fixTime: new Date(NOW + 10 * 60_000).toISOString() },
      { nowMs: NOW, futureToleranceMs: 120000 }
    );
    assert.equal(cls.decision, "future_invalid");
    assert.equal(cls.liveEligible, false);
  });

  it("burst coalesces to a single live GPS", () => {
    const list = [];
    for (let i = 0; i < 100; i++) {
      list.push({ fixTime: new Date(NOW - 20000 + i * 100).toISOString(), id: i });
    }
    const picked = pickLatestLiveFromBurst(list, { nowMs: NOW, maxLiveAgeMs: 300000 });
    assert.equal(picked.liveIndex, 99);
    assert.equal(picked.livePosition.id, 99);
  });

  it("latest-wins tracker rejects older after newer", () => {
    const t = createLiveFixTracker();
    assert.equal(t.acceptLive("123", NOW), true);
    assert.equal(t.acceptLive("123", NOW - 1000), false);
    assert.equal(t.acceptLive("123", NOW + 1000), true);
  });

  it("legacy policy: frozen fixTime stops live after 5 minutes", () => {
    const t0 = NOW - 10 * 60_000;
    const now = t0 + 6 * 60_000;
    const cls = classifyLiveFix(
      {
        fixTime: new Date(t0).toISOString(),
        deviceTime: new Date(now).toISOString(),
        serverTime: new Date(now).toISOString(),
        latitude: 24.71,
        longitude: 46.61,
        speed: 40,
      },
      {
        nowMs: now,
        maxLiveAgeMs: 300000,
        allowDeviceTimeFallback: false,
        previous: { latitude: 24.7, longitude: 46.6 },
      }
    );
    assert.equal(cls.liveEligible, false);
    assert.equal(cls.decision, "historical");
  });

  it("deviceTime fallback: frozen fixTime + fresh deviceTime stays live for 10 minutes", () => {
    const t0 = NOW;
    let car = {
      latitude: 24.7,
      longitude: 46.6,
      speed: 40,
      fixTime: new Date(t0).toISOString(),
    };
    const tracker = createLiveFixTracker();
    for (let elapsed = 0; elapsed <= 10 * 60_000; elapsed += 30_000) {
      const now = t0 + elapsed;
      const pos = {
        ...car,
        latitude: 24.7 + elapsed / 1e8,
        longitude: 46.6 + elapsed / 1e8,
        fixTime: new Date(t0).toISOString(),
        deviceTime: new Date(now).toISOString(),
        serverTime: new Date(now).toISOString(),
        speed: 40,
        outdated: false,
      };
      const cls = tracker.classify("imei-frozen", pos, {
        nowMs: now,
        maxLiveAgeMs: 300000,
        allowDeviceTimeFallback: true,
      });
      assert.equal(cls.liveEligible, true, `elapsed=${elapsed} decision=${cls.decision}`);
      if (elapsed <= 300000) assert.equal(cls.effectiveLiveSource, "fixTime");
      else {
        assert.equal(cls.effectiveLiveSource, "deviceTime");
        assert.equal(cls.decision, "fresh_device_time");
      }
      assert.equal(tracker.acceptLive("imei-frozen", cls.effectiveLiveMs, { source: cls.effectiveLiveSource, fixMs: cls.fixMs }), true);
      car = pos;
    }
  });

  it("stale fix + stale device + fresh serverTime is never live", () => {
    const cls = classifyLiveFix(
      {
        fixTime: new Date(NOW - 60 * 60_000).toISOString(),
        deviceTime: new Date(NOW - 60 * 60_000).toISOString(),
        serverTime: new Date(NOW).toISOString(),
        speed: 50,
        latitude: 1,
        longitude: 1,
      },
      { nowMs: NOW, maxLiveAgeMs: 300000 }
    );
    assert.equal(cls.liveEligible, false);
    assert.equal(cls.decision, "historical");
    assert.equal(cls.effectiveLiveSource, null);
  });

  it("week-old fix and deviceTime stay archive-only even with current serverTime", () => {
    const cls = classifyLiveFix(
      {
        fixTime: new Date(NOW - 7 * 24 * 60 * 60_000).toISOString(),
        deviceTime: new Date(NOW - 7 * 24 * 60 * 60_000).toISOString(),
        serverTime: new Date(NOW).toISOString(),
        speed: 60,
        latitude: 24,
        longitude: 46,
      },
      { nowMs: NOW, maxLiveAgeMs: 300000 }
    );
    assert.equal(cls.archiveEligible, true);
    assert.equal(cls.liveEligible, false);
  });

  it("does not fallback when outdated=true", () => {
    const now = NOW;
    const cls = classifyLiveFix(
      {
        fixTime: new Date(now - 10 * 60_000).toISOString(),
        deviceTime: new Date(now).toISOString(),
        serverTime: new Date(now).toISOString(),
        speed: 50,
        latitude: 24.8,
        longitude: 46.8,
        outdated: true,
      },
      {
        nowMs: now,
        maxLiveAgeMs: 300000,
        previous: { latitude: 24.7, longitude: 46.6 },
      }
    );
    assert.equal(cls.liveEligible, false);
    assert.equal(cls.decision, "historical");
  });

  it("stale fix + fresh deviceTime + movement uses deviceTime as live clock only", () => {
    const now = NOW;
    const frozenFix = new Date(now - 10 * 60_000).toISOString();
    const cls = classifyLiveFix(
      {
        fixTime: frozenFix,
        deviceTime: new Date(now).toISOString(),
        serverTime: new Date(now).toISOString(),
        speed: 50,
        latitude: 24.81,
        longitude: 46.81,
        outdated: false,
      },
      {
        nowMs: now,
        maxLiveAgeMs: 300000,
        previous: { latitude: 24.7, longitude: 46.6, fixTime: frozenFix },
      }
    );
    assert.equal(cls.liveEligible, true);
    assert.equal(cls.effectiveLiveSource, "deviceTime");
    assert.equal(cls.effectiveLiveMs, now);
    assert.equal(cls.fixMs, Date.parse(frozenFix));
  });

  it("acceptLive orders on effectiveLiveMs, not frozen fixMs", () => {
    const t0 = NOW;
    const tracker = createLiveFixTracker();
    const frozenFix = new Date(t0).toISOString();
    const accepted = [];
    for (let elapsed = 0; elapsed <= 10 * 60_000; elapsed += 30_000) {
      const now = t0 + elapsed;
      const pos = {
        fixTime: frozenFix,
        deviceTime: new Date(now).toISOString(),
        serverTime: new Date(now).toISOString(),
        latitude: 24.7 + elapsed / 1e8,
        longitude: 46.6 + elapsed / 1e8,
        speed: 40,
        outdated: false,
      };
      const cls = tracker.classify("imei-order", pos, {
        nowMs: now,
        maxLiveAgeMs: 300000,
        allowDeviceTimeFallback: true,
      });
      assert.equal(cls.liveEligible, true, `elapsed=${elapsed}`);
      assert.equal(cls.fixMs, t0);
      if (tracker.peekEffective("imei-order") > cls.fixMs) {
        assert.equal(cls.effectiveLiveSource, "deviceTime");
        assert.equal(
          tracker.acceptLive("imei-order", cls.fixMs, { source: "fixTime", fixMs: cls.fixMs }),
          false
        );
      }
      assert.equal(
        tracker.acceptLive("imei-order", cls.effectiveLiveMs, {
          source: cls.effectiveLiveSource,
          fixMs: cls.fixMs,
        }),
        true
      );
      accepted.push(cls.effectiveLiveMs);
    }
    assert.equal(accepted.length, 21);
    assert.equal(tracker.peekEffective("imei-order"), t0 + 10 * 60_000);
    assert.equal(tracker.peekFix("imei-order"), t0);
  });

  it("future deviceTime is archive-only", () => {
    const cls = classifyLiveFix(
      {
        fixTime: new Date(NOW - 10 * 60_000).toISOString(),
        deviceTime: new Date(NOW + 10 * 60_000).toISOString(),
        serverTime: new Date(NOW).toISOString(),
        speed: 40,
        latitude: 1,
        longitude: 1,
      },
      { nowMs: NOW, maxLiveAgeMs: 300000, deviceTimeFutureToleranceMs: 120000 }
    );
    assert.equal(cls.liveEligible, false);
    assert.equal(cls.archiveEligible, true);
    assert.equal(cls.decision, "future_invalid");
  });

  it("rejects deviceTime fallback when device-server skew exceeds limit", () => {
    const cls = classifyLiveFix(
      {
        fixTime: new Date(NOW - 10 * 60_000).toISOString(),
        deviceTime: new Date(NOW).toISOString(),
        serverTime: new Date(NOW - 5 * 60_000).toISOString(),
        speed: 40,
        latitude: 24.8,
        longitude: 46.8,
      },
      {
        nowMs: NOW,
        maxLiveAgeMs: 300000,
        deviceServerMaxSkewMs: 180000,
        previous: { latitude: 24.7, longitude: 46.6 },
      }
    );
    assert.equal(cls.liveEligible, false);
    assert.equal(cls.fallbackRejectReason, "device_server_skew");
    assert.equal(cls.deviceFallbackRejected, true);
  });

  it("week-old buffered history with changing coords and speed is not live", () => {
    const tracker = createLiveFixTracker();
    const live = [];
    const base = NOW - 7 * 24 * 60 * 60_000;
    for (let i = 0; i < 5; i++) {
      const fix = new Date(base + i * 30_000).toISOString();
      const pos = {
        fixTime: fix,
        deviceTime: fix,
        serverTime: new Date(NOW).toISOString(),
        speed: 80,
        latitude: 24 + i * 0.01,
        longitude: 46 + i * 0.01,
      };
      const cls = tracker.classify("hist-move", pos, { nowMs: NOW, maxLiveAgeMs: 300000 });
      assert.equal(cls.archiveEligible, true);
      assert.equal(cls.liveEligible, false);
      if (cls.liveEligible) live.push(i);
    }
    assert.equal(live.length, 0);
  });
});
