const { describe, it } = require("node:test");
const assert = require("node:assert/strict");
const {
  computeSegments,
  buildAndPersistTravelStats,
  buildTravelDoc,
  chunkArray,
  normalizeThresholds,
  resolveTravelDay,
  DEFAULT_THRESHOLDS,
  STOP_COUNT_THRESHOLD_MIN,
} = require("../travelStatsService");
const { startOfTodayUTC } = require("../lib/businessDay");
const { createOverlapGate } = require("../lib/reportSchedulerRuntime");

function pt(imei, lat, lon, speed, t) {
  return {
    imei,
    latitude: lat,
    longitude: lon,
    speed,
    packet_date: new Date(t),
    date: new Date(t),
  };
}

function createFakeTravelDb({ points = [], travel = [] } = {}) {
  const state = {
    points: points.map((p) => ({ ...p })),
    travel: travel.map((t) => ({ ...t })),
    reads: 0,
    distinctCalls: 0,
    findCalls: 0,
    bulkWrites: 0,
    findQueries: [],
  };

  function matchDoc(query, doc) {
    if (query.$or) return query.$or.some((c) => matchDoc(c, doc));
    for (const [k, v] of Object.entries(query)) {
      if (v && typeof v === "object" && !(v instanceof Date) && !Array.isArray(v)) {
        if (v.$in && !v.$in.includes(doc[k])) return false;
        else if (v.$gte != null || v.$lt != null) {
          const t = new Date(doc[k]).getTime();
          if (v.$gte != null && !(t >= new Date(v.$gte).getTime())) return false;
          if (v.$lt != null && !(t < new Date(v.$lt).getTime())) return false;
        }
      } else if (doc[k] !== v) return false;
    }
    return true;
  }

  const GpsPoint = {
    async distinct(field, q = {}) {
      state.reads += 1;
      state.distinctCalls += 1;
      const set = new Set();
      for (const p of state.points) {
        if (Object.keys(q).length && !matchDoc(q, p)) continue;
        if (p[field]) set.add(p[field]);
      }
      return [...set];
    },
    find(q) {
      state.findQueries.push(q);
      const builder = {
        sort() {
          return this;
        },
        lean() {
          return this._exec();
        },
        then(resolve, reject) {
          return this._exec().then(resolve, reject);
        },
        async _exec() {
          state.reads += 1;
          state.findCalls += 1;
          const rows = state.points.filter((d) => matchDoc(q, d));
          rows.sort((a, b) => {
            const ai = String(a.imei).localeCompare(String(b.imei));
            if (ai !== 0) return ai;
            return new Date(a.packet_date) - new Date(b.packet_date);
          });
          return rows.map((r) => ({ ...r }));
        },
      };
      return builder;
    },
  };

  const TravelStat = {
    async bulkWrite(ops) {
      state.bulkWrites += 1;
      state.reads += 0;
      for (const op of ops) {
        const { filter, update } = op.updateOne;
        let row = state.travel.find(
          (t) =>
            t.imei === filter.imei &&
            new Date(t.day).getTime() === new Date(filter.day).getTime() &&
            t.stop_threshold_min === filter.stop_threshold_min
        );
        if (!row) {
          row = {
            imei: filter.imei,
            day: filter.day,
            stop_threshold_min: filter.stop_threshold_min,
          };
          state.travel.push(row);
        }
        Object.assign(row, update.$set || {});
      }
    },
    async findOneAndUpdate() {
      throw new Error("legacy findOneAndUpdate must not be used");
    },
  };

  return { state, GpsPoint, TravelStat };
}

/** Classic moving → stop → moving pattern for threshold tests */
function buildStopScenario(imei) {
  const t0 = Date.parse("2026-09-07T10:00:00Z");
  return [
    pt(imei, 30.0, 31.0, 40, t0),
    pt(imei, 30.01, 31.0, 40, t0 + 5 * 60_000),
    // stop for 12 minutes
    pt(imei, 30.01, 31.0, 0, t0 + 5 * 60_000 + 1_000),
    pt(imei, 30.01, 31.0, 0, t0 + 17 * 60_000),
    // resume
    pt(imei, 30.02, 31.0, 40, t0 + 18 * 60_000),
    pt(imei, 30.03, 31.0, 40, t0 + 23 * 60_000),
  ];
}

describe("travel computeSegments semantics", () => {
  it("speed>0 opens segment; stopThreshold closes; open segment flushed", () => {
    const points = [
      pt("a", 30, 31, 40, "2026-09-07T10:00:00Z"),
      pt("a", 30.01, 31, 40, "2026-09-07T10:05:00Z"),
      pt("a", 30.01, 31, 0, "2026-09-07T10:06:00Z"),
      pt("a", 30.01, 31, 0, "2026-09-07T10:12:00Z"), // 6 min stop
    ];
    const closed = computeSegments(points, 5);
    assert.equal(closed.segments.length, 1);
    assert.ok(closed.totalKm > 0);

    const stillOpen = computeSegments(points.slice(0, 2), 5);
    assert.equal(stillOpen.segments.length, 1); // flushed at end
  });

  it("threshold boundary exactly equal closes like old code", () => {
    const t0 = Date.parse("2026-09-07T10:00:00Z");
    const points = [
      pt("b", 30, 31, 40, t0),
      pt("b", 30.01, 31, 40, t0 + 60_000),
      pt("b", 30.01, 31, 0, t0 + 60_000),
      pt("b", 30.01, 31, 0, t0 + 60_000 + 5 * 60_000), // exactly 5 min stop
      pt("b", 30.02, 31, 40, t0 + 60_000 + 5 * 60_000 + 60_000), // resume
    ];
    // stopDur >= 5 → close before resume → 2 segments
    assert.equal(computeSegments(points, 5).segments.length, 2);
    // stopDur >= 5.0001 false → same segment continues → 1 segment
    assert.equal(computeSegments(points, 5.0001).segments.length, 1);
  });

  it("zero movement yields empty segments / zero distance", () => {
    const points = [
      pt("z", 30, 31, 0, "2026-09-07T10:00:00Z"),
      pt("z", 30, 31, 0, "2026-09-07T10:30:00Z"),
    ];
    const r = computeSegments(points, 1);
    assert.equal(r.segments.length, 0);
    assert.equal(r.totalKm, 0);
  });

  it("continuous moving is one segment", () => {
    const points = [
      pt("c", 30, 31, 50, "2026-09-07T10:00:00Z"),
      pt("c", 30.01, 31, 50, "2026-09-07T10:10:00Z"),
      pt("c", 30.02, 31, 50, "2026-09-07T10:20:00Z"),
    ];
    const r = computeSegments(points, 1);
    assert.equal(r.segments.length, 1);
    assert.ok(r.totalKm > 0);
    assert.ok(r.totalDriveMin > 0);
  });

  it("out-of-order times do not add distance (jump bad_time)", () => {
    const points = [
      pt("o", 30, 31, 40, "2026-09-07T10:10:00Z"),
      pt("o", 30.05, 31, 40, "2026-09-07T10:00:00Z"),
    ];
    const r = computeSegments(points, 1);
    assert.equal(r.totalKm, 0);
  });

  it("stopCount uses fixed 3-minute rule independent of stopThresholdMin", () => {
    const t0 = Date.parse("2026-09-07T10:00:00Z");
    const points = [
      pt("s", 30, 31, 40, t0),
      pt("s", 30, 31, 0, t0 + 60_000),
      pt("s", 30, 31, 0, t0 + 60_000 + STOP_COUNT_THRESHOLD_MIN * 60_000),
      pt("s", 30, 31, 40, t0 + 60_000 + STOP_COUNT_THRESHOLD_MIN * 60_000 + 60_000),
    ];
    const r1 = computeSegments(points, 1);
    const r60 = computeSegments(points, 60);
    assert.ok(r1.stopCount >= 1);
    assert.equal(r1.stopCount, r60.stopCount);
  });
});

describe("travel batch multi-threshold", () => {
  it("discovers IMEIs once and fetches points once per chunk (not ×7)", async () => {
    const thresholds = [1, 3, 5, 10, 15, 30, 60];
    const points = [];
    for (let i = 0; i < 12; i++) {
      points.push(...buildStopScenario(`d${i}`));
    }
    const fake = createFakeTravelDb({ points });
    const metrics = {};
    await buildAndPersistTravelStats({
      ...fake,
      stopThresholdsMinutes: thresholds,
      businessDay: "utc",
      dayUtc: new Date("2026-09-07T00:00:00Z"),
      chunkSize: 4,
      metrics,
    });
    assert.equal(fake.state.distinctCalls, 1);
    // 12 devices / chunk 4 = 3 find calls (not 12*7 or 3*7)
    assert.equal(fake.state.findCalls, 3);
    assert.equal(fake.state.bulkWrites, 3);
    assert.equal(metrics.analytics_travel_thresholds_processed, 7);
    assert.equal(fake.state.travel.length, 12 * 7);
  });

  it("all thresholds derived from same fetched dataset match computeSegments", async () => {
    const imei = "parity";
    const points = buildStopScenario(imei);
    const fake = createFakeTravelDb({ points });
    const thresholds = DEFAULT_THRESHOLDS;
    await buildAndPersistTravelStats({
      ...fake,
      stopThresholdsMinutes: thresholds,
      businessDay: "utc",
      dayUtc: new Date("2026-09-07T00:00:00Z"),
      imeis: [imei],
      chunkSize: 10,
    });
    for (const th of thresholds) {
      const expected = buildTravelDoc(imei, new Date("2026-09-07T00:00:00Z"), th, points);
      const got = fake.state.travel.find((t) => t.stop_threshold_min === th);
      assert.ok(got);
      assert.equal(got.total_segments, expected.total_segments);
      assert.ok(Math.abs(got.total_distance_km - expected.total_distance_km) < 1e-9);
      assert.ok(Math.abs(got.total_driving_minutes - expected.total_driving_minutes) < 1e-9);
      assert.equal(got.total_stop_count, expected.total_stop_count);
    }
  });

  it("12-min stop closes for thresholds ≤12 and stays open for larger until EOF flush", () => {
    const points = buildStopScenario("th");
    // 12 min stop between moves → thresholds 1..10 close mid-stream (2 segs);
    // 15+ never close on stop → one segment flushed at end
    assert.equal(computeSegments(points, 10).segments.length, 2);
    assert.equal(computeSegments(points, 15).segments.length, 1);
  });

  it("chunk size does not change results", async () => {
    const points = [];
    for (let i = 0; i < 9; i++) points.push(...buildStopScenario(`c${i}`));
    const a = createFakeTravelDb({ points: points.map((p) => ({ ...p })) });
    const b = createFakeTravelDb({ points: points.map((p) => ({ ...p })) });
    const opts = {
      stopThresholdsMinutes: [1, 5, 15],
      businessDay: "utc",
      dayUtc: new Date("2026-09-07T00:00:00Z"),
    };
    await buildAndPersistTravelStats({ ...a, ...opts, chunkSize: 3 });
    await buildAndPersistTravelStats({ ...b, ...opts, chunkSize: 9 });
    assert.equal(a.state.travel.length, b.state.travel.length);
    for (const row of a.state.travel) {
      const other = b.state.travel.find(
        (t) => t.imei === row.imei && t.stop_threshold_min === row.stop_threshold_min
      );
      assert.ok(other);
      assert.equal(other.total_segments, row.total_segments);
      assert.ok(Math.abs(other.total_distance_km - row.total_distance_km) < 1e-9);
    }
  });

  it("uses bulkWrite not per-row findOneAndUpdate", async () => {
    const fake = createFakeTravelDb({ points: buildStopScenario("bw") });
    await buildAndPersistTravelStats({
      ...fake,
      stopThresholdsMinutes: [1, 3],
      businessDay: "utc",
      dayUtc: new Date("2026-09-07T00:00:00Z"),
      imeis: ["bw"],
    });
    assert.equal(fake.state.bulkWrites, 1);
    assert.equal(fake.state.travel.length, 2);
  });

  it("one device failure does not corrupt another; job surfaces partial failure", async () => {
    const good = "good";
    const bad = "bad";
    const points = [...buildStopScenario(good), ...buildStopScenario(bad)];
    const fake = createFakeTravelDb({ points });
    const real = computeSegments;
    // Monkeypatch via module — travelStatsService calls local computeSegments
    // Use poisoned points for bad: null coords still runs; force throw by wrapping GpsPoint.find
    const origFind = fake.GpsPoint.find.bind(fake.GpsPoint);
    fake.GpsPoint.find = (q) => {
      const builder = origFind(q);
      const origExec = builder._exec.bind(builder);
      builder._exec = async () => {
        const rows = await origExec();
        return rows.map((r) => {
          if (r.imei === bad) {
            return new Proxy(r, {
              get(target, prop) {
                if (prop === "speed") throw new Error("boom-bad");
                return target[prop];
              },
            });
          }
          return r;
        });
      };
      return builder;
    };

    await assert.rejects(
      () =>
        buildAndPersistTravelStats({
          ...fake,
          stopThresholdsMinutes: [1],
          businessDay: "utc",
          dayUtc: new Date("2026-09-07T00:00:00Z"),
          imeis: [good, bad],
          log: { warn() {} },
        }),
      /partial failure/
    );
    const g = fake.state.travel.find((t) => t.imei === good);
    assert.ok(g);
    assert.ok(g.total_segments >= 1);
    assert.equal(fake.state.travel.some((t) => t.imei === bad), false);
    void real;
  });

  it("Cairo day differs from UTC; utc rollback available", () => {
    const noon = new Date("2026-09-07T12:00:00Z");
    const cairo = resolveTravelDay({ now: noon, businessDay: "cairo" });
    const utc = resolveTravelDay({ now: noon, businessDay: "utc" });
    assert.equal(utc.dayStart.toISOString(), startOfTodayUTC(noon).toISOString());
    assert.notEqual(cairo.dayStart.toISOString(), utc.dayStart.toISOString());
  });

  it("normalizeThresholds preserves single-threshold legacy API", () => {
    assert.deepEqual(normalizeThresholds({ stopThresholdMin: 5 }), [5]);
    assert.deepEqual(normalizeThresholds({ stopThresholdsMinutes: [1, 3] }), [1, 3]);
  });

  it("chunkArray and overlap gate", async () => {
    assert.deepEqual(chunkArray([1, 2, 3, 4, 5], 2), [[1, 2], [3, 4], [5]]);
    const metrics = {};
    const gate = createOverlapGate("travel", metrics);
    let release;
    const hold = new Promise((r) => {
      release = r;
    });
    const first = gate.run(() => hold);
    const second = await gate.run(async () => {});
    assert.equal(second.skipped, true);
    release();
    await first;
  });
});
