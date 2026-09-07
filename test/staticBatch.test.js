const { describe, it } = require("node:test");
const assert = require("node:assert/strict");
const {
  buildAndPersistStaticStats,
  STATIC_MILEAGE_THRESHOLD_KM,
  DEFAULT_CHUNK,
} = require("../staticStatsService");

function createFakeStaticDb({ mileage = [], points = [], staticRows = [] } = {}) {
  const state = {
    mileage: mileage.map((r) => ({ ...r })),
    points: points.map((p) => ({ ...p })),
    staticRows: staticRows.map((r) => ({ ...r })),
    reads: 0,
    mileageFinds: 0,
    pointFinds: 0,
    bulkWrites: 0,
  };

  function match(q, doc) {
    for (const [k, v] of Object.entries(q || {})) {
      const dv = doc[k];
      if (v && typeof v === "object" && !(v instanceof Date) && !Array.isArray(v)) {
        if (Object.prototype.hasOwnProperty.call(v, "$in")) {
          if (!v.$in.includes(dv)) return false;
          continue;
        }
        const t = new Date(dv).getTime();
        if (v.$gte != null && !(t >= new Date(v.$gte).getTime())) return false;
        if (v.$gt != null && !(t > new Date(v.$gt).getTime())) return false;
        if (v.$lt != null && !(t < new Date(v.$lt).getTime())) return false;
        if (v.$lte != null && !(t <= new Date(v.$lte).getTime())) return false;
        continue;
      }
      if (k === "day") {
        if (+new Date(dv) !== +new Date(v)) return false;
        continue;
      }
      if (dv !== v) return false;
    }
    return true;
  }

  const DailyMileage = {
    find(q) {
      state.reads += 1;
      state.mileageFinds += 1;
      const rows = state.mileage.filter((d) => match(q, d));
      return {
        select() {
          return this;
        },
        lean: async () => rows.map((r) => ({ ...r })),
      };
    },
  };

  const GpsPoint = {
    async distinct(field, q) {
      state.reads += 1;
      const set = new Set();
      for (const p of state.points) {
        if (q && !match(q, p)) continue;
        if (p[field]) set.add(p[field]);
      }
      return [...set];
    },
    find(q) {
      state.reads += 1;
      state.pointFinds += 1;
      const rows = state.points.filter((d) => match(q, d));
      return {
        sort() {
          return this;
        },
        select() {
          return this;
        },
        lean: async () => {
          rows.sort((a, b) => {
            const ai = String(a.imei).localeCompare(String(b.imei));
            if (ai) return ai;
            return new Date(a.packet_date) - new Date(b.packet_date);
          });
          return rows.map((r) => ({ ...r }));
        },
      };
    },
  };

  const StaticStat = {
    async bulkWrite(ops) {
      state.bulkWrites += 1;
      for (const op of ops) {
        const { filter, update } = op.updateOne;
        let row = state.staticRows.find(
          (r) =>
            r.imei === filter.imei && +new Date(r.day) === +new Date(filter.day)
        );
        if (!row) {
          row = { imei: filter.imei, day: filter.day };
          state.staticRows.push(row);
        }
        Object.assign(row, update.$set || {});
      }
    },
    async findOneAndUpdate() {
      throw new Error("legacy findOneAndUpdate must not be used");
    },
  };

  return { state, DailyMileage, GpsPoint, StaticStat };
}

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

describe("static batch materialization", () => {
  const day = new Date("2026-09-07T00:00:00.000Z");

  it("uses DailyMileage in one find, not per IMEI", async () => {
    const mileage = [];
    for (let i = 0; i < 20; i++) {
      mileage.push({ imei: `d${i}`, day, km: i < 10 ? 0.1 : 2.0 });
    }
    const fake = createFakeStaticDb({ mileage });
    const metrics = {};
    await buildAndPersistStaticStats({
      ...fake,
      dayUtc: day,
      businessDay: "utc",
      chunkSize: 5,
      metrics,
    });
    assert.equal(fake.state.mileageFinds, 1);
    assert.equal(fake.state.pointFinds, 0);
    assert.equal(fake.state.bulkWrites, 4);
    assert.equal(metrics.analytics_static_daily_mileage_hits, 20);
    assert.equal(metrics.analytics_static_gps_fallback_devices || 0, 0);
    assert.equal(fake.state.staticRows.length, 20);
    assert.ok(fake.state.staticRows.every((r) => "daily_mileage_km" in r && "is_static" in r));
  });

  it("GpsPoint fallback only for missing DailyMileage", async () => {
    const mileage = [{ imei: "hit", day, km: 0.2 }];
    const points = [
      pt("miss", 30, 31, 40, "2026-09-07T10:00:00Z"),
      pt("miss", 30.01, 31, 40, "2026-09-07T10:10:00Z"),
    ];
    const fake = createFakeStaticDb({ mileage, points });
    const metrics = {};
    await buildAndPersistStaticStats({
      ...fake,
      dayUtc: day,
      businessDay: "utc",
      imeis: ["hit", "miss"],
      metrics,
    });
    assert.equal(metrics.analytics_static_daily_mileage_hits, 1);
    assert.equal(metrics.analytics_static_gps_fallback_devices, 1);
    assert.equal(fake.state.pointFinds, 1);
    assert.ok(
      metrics.analytics_static_points_processed >= 2,
      `points_processed=${metrics.analytics_static_points_processed}`
    );
    const hit = fake.state.staticRows.find((r) => r.imei === "hit");
    const miss = fake.state.staticRows.find((r) => r.imei === "miss");
    assert.equal(hit.is_static, true);
    assert.equal(hit.daily_mileage_km, 0.2);
    assert.ok(miss, "miss row missing");
    assert.ok(
      miss.daily_mileage_km > 0.5,
      `miss km=${miss.daily_mileage_km} rows=${JSON.stringify(fake.state.staticRows)}`
    );
    assert.equal(miss.is_static, false);
  });

  it("km boundary 0.5 preserved", async () => {
    const fake = createFakeStaticDb({
      mileage: [
        { imei: "eq", day, km: 0.5 },
        { imei: "under", day, km: 0.49 },
        { imei: "over", day, km: 0.51 },
      ],
    });
    await buildAndPersistStaticStats({
      ...fake,
      dayUtc: day,
      businessDay: "utc",
    });
    assert.equal(STATIC_MILEAGE_THRESHOLD_KM, 0.5);
    assert.equal(fake.state.staticRows.find((r) => r.imei === "eq").is_static, true);
    assert.equal(fake.state.staticRows.find((r) => r.imei === "under").is_static, true);
    assert.equal(fake.state.staticRows.find((r) => r.imei === "over").is_static, false);
  });

  it("chunk size does not alter results", async () => {
    const mileage = [];
    for (let i = 0; i < 9; i++) mileage.push({ imei: `c${i}`, day, km: i % 2 ? 0.1 : 3 });
    const a = createFakeStaticDb({ mileage: mileage.map((r) => ({ ...r })) });
    const b = createFakeStaticDb({ mileage: mileage.map((r) => ({ ...r })) });
    await buildAndPersistStaticStats({ ...a, dayUtc: day, businessDay: "utc", chunkSize: 3 });
    await buildAndPersistStaticStats({ ...b, dayUtc: day, businessDay: "utc", chunkSize: 9 });
    assert.equal(a.state.staticRows.length, b.state.staticRows.length);
    for (const row of a.state.staticRows) {
      const other = b.state.staticRows.find((r) => r.imei === row.imei);
      assert.equal(other.is_static, row.is_static);
      assert.equal(other.daily_mileage_km, row.daily_mileage_km);
    }
  });

  it("default chunk is larger than Travel/Idle", () => {
    assert.ok(DEFAULT_CHUNK >= 250);
  });
});
