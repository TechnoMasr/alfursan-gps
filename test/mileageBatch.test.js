const { describe, it } = require("node:test");
const assert = require("node:assert/strict");
const {
  sumMileageKm,
  computeDailyFromPoints,
} = require("../lib/mileageCalc");
const { calcDistanceDiffSafe } = require("../gpsJumpGuard");
const {
  resolveMileageDay,
  businessDayRange,
  startOfTodayUTC,
} = require("../lib/businessDay");
const {
  updateIncrementalMileage,
  buildAndPersistDailyReport,
  buildDailyMileageReport,
  chunkArray,
  resolveOverspeedSource,
  KM_TO_MILES,
} = require("../mileageService");
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

/** Tiny in-memory Mongo stand-in that counts reads/writes. */
function createFakeDb({ points = [], statuses = [], acc = [], overspeed = [], daily = [] } = {}) {
  const state = {
    points: points.map((p) => ({ ...p })),
    statuses: statuses.map((s) => ({ ...s })),
    acc: acc.map((a) => ({ ...a })),
    overspeed: overspeed.map((o) => ({ ...o })),
    daily: daily.map((d) => ({ ...d })),
    reads: 0,
    writes: 0,
    bulkWrites: 0,
  };

  function matchOr(query, doc) {
    if (!query.$or) return null;
    return query.$or.some((clause) => matchDoc(clause, doc));
  }

  function matchDoc(query, doc) {
    if (query.$or) return matchOr(query, doc);
    for (const [k, v] of Object.entries(query)) {
      if (k === "$or") continue;
      if (v && typeof v === "object" && !Array.isArray(v) && !(v instanceof Date)) {
        if (v.$in) {
          if (!v.$in.includes(doc[k])) return false;
        } else if (v.$gt != null) {
          if (!(new Date(doc[k]) > new Date(v.$gt))) return false;
        } else if (v.$gte != null || v.$lt != null || v.$lte != null) {
          const t = new Date(doc[k]).getTime();
          if (v.$gte != null && !(t >= new Date(v.$gte).getTime())) return false;
          if (v.$lt != null && !(t < new Date(v.$lt).getTime())) return false;
          if (v.$lte != null && !(t <= new Date(v.$lte).getTime())) return false;
        } else {
          return false;
        }
      } else if (doc[k] !== v) {
        return false;
      }
    }
    return true;
  }

  function findBuilder(collection) {
    return {
      sort() {
        return this;
      },
      select() {
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
        const rows = state[collection].filter((d) => matchDoc(this._q, d));
        rows.sort((a, b) => {
          const ai = String(a.imei).localeCompare(String(b.imei));
          if (ai !== 0) return ai;
          return new Date(a.packet_date) - new Date(b.packet_date);
        });
        return rows.map((r) => ({ ...r }));
      },
      _q: {},
    };
  }

  const GpsPoint = {
    async distinct(field, q = {}) {
      state.reads += 1;
      const set = new Set();
      for (const p of state.points) {
        if (Object.keys(q).length && !matchDoc(q, p)) continue;
        if (p[field]) set.add(p[field]);
      }
      return [...set];
    },
    find(q) {
      const b = findBuilder("points");
      b._q = q;
      return b;
    },
    aggregate(pipeline) {
      state.reads += 1;
      return (async () => {
        const match = pipeline.find((s) => s.$match)?.$match || {};
        const group = pipeline.find((s) => s.$group)?.$group;
        let rows = state.points.filter((d) => matchDoc(match, d));
        if (group && group._id === "$imei") {
          rows.sort((a, b) => {
            const ai = String(a.imei).localeCompare(String(b.imei));
            if (ai !== 0) return ai;
            return new Date(b.packet_date) - new Date(a.packet_date);
          });
          const out = [];
          const seen = new Set();
          for (const r of rows) {
            if (seen.has(r.imei)) continue;
            seen.add(r.imei);
            out.push({
              _id: r.imei,
              latitude: r.latitude,
              longitude: r.longitude,
              speed: r.speed,
              packet_date: r.packet_date,
              date: r.date,
            });
          }
          return out;
        }
        return [];
      })();
    },
  };

  const DeviceStatus = {
    async distinct(field) {
      state.reads += 1;
      return [...new Set(state.statuses.map((s) => s[field]).filter(Boolean))];
    },
    find(q) {
      state.reads += 1;
      const rows = state.statuses.filter((d) => matchDoc(q, d));
      return {
        select() {
          return this;
        },
        lean: async () => rows.map((r) => ({ ...r })),
        then(resolve, reject) {
          return this.lean().then(resolve, reject);
        },
      };
    },
    async bulkWrite(ops) {
      state.writes += 1;
      state.bulkWrites += 1;
      for (const op of ops) {
        const { filter, update } = op.updateOne;
        let row = state.statuses.find((s) => s.imei === filter.imei);
        if (!row) {
          row = { imei: filter.imei, km_total: 0, miles_total: 0, last_mileage_at: null };
          state.statuses.push(row);
        }
        if (update.$inc) {
          for (const [k, v] of Object.entries(update.$inc)) {
            row[k] = (Number(row[k]) || 0) + v;
          }
        }
        if (update.$set) Object.assign(row, update.$set);
      }
    },
  };

  const AccEvent = {
    aggregate(pipeline) {
      state.reads += 1;
      return (async () => {
        const match = pipeline.find((s) => s.$match)?.$match || {};
        const rows = state.acc.filter((d) => matchDoc(match, d));
        const map = new Map();
        for (const r of rows) {
          const key = `${r.imei}|${r.acc_status}`;
          map.set(key, (map.get(key) || 0) + 1);
        }
        return [...map.entries()].map(([key, n]) => {
          const [imei, acc_status] = key.split("|");
          return { _id: { imei, acc_status }, n };
        });
      })();
    },
  };

  const OverspeedAlert = {
    aggregate(pipeline) {
      state.reads += 1;
      return (async () => {
        const match = pipeline.find((s) => s.$match)?.$match || {};
        const rows = state.overspeed.filter((d) => matchDoc(match, d));
        const map = new Map();
        for (const r of rows) map.set(r.imei, (map.get(r.imei) || 0) + 1);
        return [...map.entries()].map(([imei, n]) => ({ _id: imei, n }));
      })();
    },
  };

  const DailyMileage = {
    async bulkWrite(ops) {
      state.writes += 1;
      state.bulkWrites += 1;
      for (const op of ops) {
        const { filter, update } = op.updateOne;
        let row = state.daily.find(
          (d) =>
            d.imei === filter.imei &&
            new Date(d.day).getTime() === new Date(filter.day).getTime()
        );
        if (!row) {
          row = { imei: filter.imei, day: filter.day };
          state.daily.push(row);
        }
        Object.assign(row, update.$set || {});
      }
    },
  };

  return { state, GpsPoint, DeviceStatus, AccEvent, OverspeedAlert, DailyMileage };
}

describe("mileage calc parity / jump / OOO", () => {
  it("matches sequential haversine with speed edge and jump filter", () => {
    const a = pt("x", 30.0, 31.0, 40, "2026-09-07T10:00:00Z");
    const b = pt("x", 30.01, 31.0, 40, "2026-09-07T10:05:00Z");
    const c = pt("x", 30.02, 31.0, 0, "2026-09-07T10:10:00Z");
    const { kmSum } = sumMileageKm([a, b, c]);
    let expected = 0;
    for (const [p, n] of [
      [a, b],
      [b, c],
    ]) {
      const res = calcDistanceDiffSafe(
        { lat: p.latitude, lon: p.longitude, date: p.packet_date },
        { lat: n.latitude, lon: n.longitude, date: n.packet_date }
      );
      const ps = Number(p.speed) || 0;
      const ns = Number(n.speed) || 0;
      if (!res.isJump && (ps > 0 || ns > 0)) expected += res.distanceKm;
    }
    assert.ok(Math.abs(kmSum - expected) < 1e-9);
  });

  it("out-of-order time yields no distance (jump bad_time)", () => {
    const a = pt("x", 30.0, 31.0, 40, "2026-09-07T10:10:00Z");
    const b = pt("x", 30.01, 31.0, 40, "2026-09-07T10:00:00Z");
    const { kmSum } = sumMileageKm([a, b]);
    assert.equal(kmSum, 0);
  });

  it("zero movement / single point yields 0", () => {
    assert.equal(sumMileageKm([pt("x", 30, 31, 0, "2026-09-07T10:00:00Z")]).kmSum, 0);
    const a = pt("x", 30, 31, 0, "2026-09-07T10:00:00Z");
    const b = pt("x", 30, 31, 0, "2026-09-07T10:05:00Z");
    assert.equal(sumMileageKm([a, b]).kmSum, 0);
  });

  it("previous boundary point is included", () => {
    const prev = pt("x", 30.0, 31.0, 50, "2026-09-07T09:55:00Z");
    const a = pt("x", 30.01, 31.0, 50, "2026-09-07T10:00:00Z");
    const withPrev = sumMileageKm([a], { prevPoint: prev });
    const without = sumMileageKm([a]);
    assert.ok(withPrev.kmSum > 0);
    assert.equal(without.kmSum, 0);
  });
});

describe("mileage batch incremental", () => {
  it("Mongo reads do not scale 1:1 with device count", async () => {
    const t0 = Date.parse("2026-09-07T08:00:00Z");
    const points = [];
    const statuses = [];
    for (let i = 0; i < 20; i++) {
      const imei = `d${i}`;
      statuses.push({ imei, last_mileage_at: new Date(t0), km_total: 0, miles_total: 0 });
      points.push(pt(imei, 30, 31 + i * 0.0001, 40, t0));
      points.push(pt(imei, 30.01, 31 + i * 0.0001, 40, t0 + 600_000));
    }
    const fake = createFakeDb({ points, statuses });
    const metrics = {};
    await updateIncrementalMileage({
      ...fake,
      metrics,
      chunkSize: 5,
      imeis: statuses.map((s) => s.imei),
    });
    // Old N+1 ≈ 20 * ~4 = 80+. New: O(chunks). With chunk=5 → 4 chunks.
    assert.ok(fake.state.reads < 40, `reads=${fake.state.reads}`);
    assert.ok(fake.state.bulkWrites >= 1);
    assert.equal(metrics.analytics_mileage_chunks_processed, 4);
    assert.ok(fake.state.statuses.every((s) => s.km_total > 0));
    assert.ok(fake.state.statuses.every((s) => s.last_mileage_at > new Date(t0)));
  });

  it("second run is idempotent (no double-add)", async () => {
    const t0 = Date.parse("2026-09-07T08:00:00Z");
    const imei = "idem";
    const points = [
      pt(imei, 30, 31, 40, t0),
      pt(imei, 30.02, 31, 40, t0 + 600_000),
    ];
    const statuses = [{ imei, last_mileage_at: new Date(t0), km_total: 0, miles_total: 0 }];
    const fake = createFakeDb({ points, statuses });
    await updateIncrementalMileage({ ...fake, imeis: [imei], chunkSize: 10 });
    const km1 = fake.state.statuses[0].km_total;
    const last1 = fake.state.statuses[0].last_mileage_at;
    await updateIncrementalMileage({ ...fake, imeis: [imei], chunkSize: 10 });
    assert.equal(fake.state.statuses[0].km_total, km1);
    assert.equal(
      new Date(fake.state.statuses[0].last_mileage_at).getTime(),
      new Date(last1).getTime()
    );
  });

  it("chunk boundaries do not alter results", async () => {
    const t0 = Date.parse("2026-09-07T08:00:00Z");
    const mk = () => {
      const points = [];
      const statuses = [];
      for (let i = 0; i < 9; i++) {
        const imei = `c${i}`;
        statuses.push({ imei, last_mileage_at: new Date(t0), km_total: 0, miles_total: 0 });
        points.push(pt(imei, 30, 31, 40, t0));
        points.push(pt(imei, 30.015, 31, 40, t0 + 300_000));
      }
      return createFakeDb({ points, statuses });
    };
    const a = mk();
    const b = mk();
    await updateIncrementalMileage({
      ...a,
      imeis: a.state.statuses.map((s) => s.imei),
      chunkSize: 3,
    });
    await updateIncrementalMileage({
      ...b,
      imeis: b.state.statuses.map((s) => s.imei),
      chunkSize: 9,
    });
    for (let i = 0; i < 9; i++) {
      assert.ok(
        Math.abs(a.state.statuses[i].km_total - b.state.statuses[i].km_total) < 1e-9
      );
    }
  });

  it("one device failure does not corrupt another", async () => {
    const t0 = Date.parse("2026-09-07T08:00:00Z");
    const good = "good";
    const bad = "bad";
    const points = [
      pt(good, 30, 31, 40, t0),
      pt(good, 30.01, 31, 40, t0 + 300_000),
      pt(bad, 30, 31, 40, t0),
      pt(bad, 30.01, 31, 40, t0 + 300_000),
    ];
    const statuses = [
      { imei: good, last_mileage_at: new Date(t0), km_total: 0, miles_total: 0 },
      { imei: bad, last_mileage_at: new Date(t0), km_total: 0, miles_total: 0 },
    ];
    const fake = createFakeDb({ points, statuses });
    const mileageCalc = require("../lib/mileageCalc");
    const real = mileageCalc.sumMileageKm;
    mileageCalc.sumMileageKm = (pts, opts) => {
      const sample = opts?.prevPoint || pts?.[0];
      // Identify bad device via longitude fingerprint used only for bad points... 
      // Instead: if any point has imei bad (select keeps imei)
      if ((pts || []).some((p) => p.imei === bad) || opts?.prevPoint?.imei === bad) {
        throw new Error("boom-bad-device");
      }
      return real(pts, opts);
    };
    try {
      await updateIncrementalMileage({
        ...fake,
        imeis: [good, bad],
        chunkSize: 10,
        log: { warn() {} },
      });
      const g = fake.state.statuses.find((s) => s.imei === good);
      const b = fake.state.statuses.find((s) => s.imei === bad);
      assert.ok(g.km_total > 0);
      assert.equal(b.km_total, 0);
    } finally {
      mileageCalc.sumMileageKm = real;
    }
  });

  it("DeviceStatus bulkWrite uses $inc/$set only", async () => {
    const t0 = Date.parse("2026-09-07T08:00:00Z");
    const imei = "bulk";
    const fake = createFakeDb({
      points: [pt(imei, 30, 31, 40, t0), pt(imei, 30.01, 31, 40, t0 + 300_000)],
      statuses: [
        {
          imei,
          last_mileage_at: new Date(t0),
          km_total: 5,
          miles_total: 5 * KM_TO_MILES,
          last_speed: 99,
          other_field: "keep",
        },
      ],
    });
    let seen;
    const orig = fake.DeviceStatus.bulkWrite.bind(fake.DeviceStatus);
    fake.DeviceStatus.bulkWrite = async (ops, opts) => {
      seen = ops;
      return orig(ops, opts);
    };
    await updateIncrementalMileage({ ...fake, imeis: [imei] });
    assert.equal(seen.length, 1);
    assert.ok(seen[0].updateOne.update.$inc.km_total > 0);
    assert.ok(seen[0].updateOne.update.$set.last_mileage_at);
    assert.equal(fake.state.statuses[0].other_field, "keep");
    assert.equal(fake.state.statuses[0].last_speed, 99);
    assert.ok(fake.state.statuses[0].km_total > 5);
  });
});

describe("mileage daily batch / ACC / overspeed", () => {
  it("materializes DailyMileage and uses AccEvent counts", async () => {
    const dayStart = new Date("2026-09-07T00:00:00.000Z");
    const imei = "acc1";
    const fake = createFakeDb({
      points: [
        pt(imei, 30, 31, 40, "2026-09-07T01:00:00Z"),
        pt(imei, 30.01, 31, 40, "2026-09-07T01:10:00Z"),
      ],
      acc: [
        { imei, acc_status: "on", start_time: new Date("2026-09-07T02:00:00Z") },
        { imei, acc_status: "on", start_time: new Date("2026-09-07T03:00:00Z") },
        { imei, acc_status: "off", start_time: new Date("2026-09-07T04:00:00Z") },
      ],
      overspeed: [{ imei, start_time: new Date("2026-09-07T05:00:00Z") }],
    });
    const rows = await buildAndPersistDailyReport(dayStart, {
      ...fake,
      businessDay: "utc",
      overspeedSource: "alerts",
      imeis: [imei],
    });
    assert.equal(rows.length, 1);
    assert.equal(rows[0].acc_on_count, 2);
    assert.equal(rows[0].acc_off_count, 1);
    assert.equal(rows[0].overspeed_count, 1);
    assert.equal(fake.state.daily.length, 1);
    assert.equal(fake.state.bulkWrites, 1);
  });

  it("TARGET overspeed uses OverspeedAlert only; legacy_max keeps Math.max", async () => {
    const dayStart = new Date("2026-09-07T00:00:00.000Z");
    const imei = "os1";
    const points = [
      pt(imei, 30, 31, 130, "2026-09-07T01:00:00Z"),
      pt(imei, 30.01, 31, 130, "2026-09-07T01:05:00Z"),
      pt(imei, 30.02, 31, 130, "2026-09-07T01:10:00Z"),
    ];
    const overspeed = [{ imei, start_time: new Date("2026-09-07T01:00:00Z") }];
    const a = createFakeDb({ points, overspeed });
    const { rows: alertRows } = await buildDailyMileageReport(dayStart, {
      ...a,
      businessDay: "utc",
      overspeedSource: "alerts",
      imeis: [imei],
    });
    assert.equal(alertRows[0].overspeed_count, 1);

    const b = createFakeDb({ points, overspeed });
    const { rows: legacyRows } = await buildDailyMileageReport(dayStart, {
      ...b,
      businessDay: "utc",
      overspeedSource: "legacy_max",
      imeis: [imei],
    });
    assert.equal(legacyRows[0].overspeed_count, 3);
    assert.equal(resolveOverspeedSource("alerts"), "alerts");
  });

  it("Cairo business day range differs from UTC midnight", () => {
    const noonUtc = new Date("2026-09-07T12:00:00Z");
    const cairo = resolveMileageDay({ now: noonUtc, businessDay: "cairo" });
    const utc = resolveMileageDay({ now: noonUtc, businessDay: "utc" });
    assert.equal(utc.dayStart.toISOString(), startOfTodayUTC(noonUtc).toISOString());
    // Cairo midnight is not the same UTC instant as UTC midnight (Egypt UTC+2/+3)
    assert.notEqual(cairo.dayStart.toISOString(), utc.dayStart.toISOString());
    const range = businessDayRange(noonUtc);
    assert.ok(range.dayEnd > range.dayStart);
    assert.equal(range.ymd, "2026-09-07");
  });
});

describe("mileage helpers + overlap", () => {
  it("chunkArray batches devices", () => {
    assert.deepEqual(chunkArray([1, 2, 3, 4, 5], 2), [[1, 2], [3, 4], [5]]);
  });

  it("overlap gate still skips concurrent work", async () => {
    const metrics = {};
    const gate = createOverlapGate("mileage", metrics);
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
