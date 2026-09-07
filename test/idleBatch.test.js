const { describe, it } = require("node:test");
const assert = require("node:assert/strict");
const {
  computeIdle,
  isAccOn,
  buildAndPersistIdleStats,
  handleIdleNotifySample,
  chunkArray,
  resolveIdleDay,
  DEFAULT_CHUNK,
} = require("../idleStatsService");
const { startOfTodayUTC } = require("../lib/businessDay");
const { createOverlapGate } = require("../lib/reportSchedulerRuntime");

function pt(imei, speed, t, extra = {}) {
  return {
    imei,
    latitude: 30,
    longitude: 31,
    speed,
    packet_date: new Date(t),
    date: new Date(t),
    ...extra,
  };
}

function createFakeIdleDb({ points = [], idle = [] } = {}) {
  const state = {
    points: points.map((p) => ({ ...p })),
    idle: idle.map((r) => ({ ...r })),
    reads: 0,
    distinctCalls: 0,
    findCalls: 0,
    bulkWrites: 0,
    deletesInBulk: 0,
    upsertsInBulk: 0,
  };

  function matchDoc(query, doc) {
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

  const IdleStat = {
    async bulkWrite(ops) {
      state.bulkWrites += 1;
      for (const op of ops) {
        if (op.deleteOne) {
          state.deletesInBulk += 1;
          const { filter } = op.deleteOne;
          state.idle = state.idle.filter(
            (r) =>
              !(
                r.imei === filter.imei &&
                new Date(r.day).getTime() === new Date(filter.day).getTime()
              )
          );
          continue;
        }
        if (op.updateOne) {
          state.upsertsInBulk += 1;
          const { filter, update } = op.updateOne;
          let row = state.idle.find(
            (r) =>
              r.imei === filter.imei &&
              new Date(r.day).getTime() === new Date(filter.day).getTime()
          );
          if (!row) {
            row = { imei: filter.imei, day: filter.day };
            state.idle.push(row);
          }
          Object.assign(row, update.$set || {});
        }
      }
    },
    async deleteOne() {
      throw new Error("legacy deleteOne must not be used");
    },
    async findOneAndUpdate() {
      throw new Error("legacy findOneAndUpdate must not be used");
    },
  };

  return { state, GpsPoint, IdleStat };
}

/** 10 min idle with ignition on, then move */
function idleScenario(imei, { ignition = true, speed = 0 } = {}) {
  const t0 = Date.parse("2026-09-07T10:00:00Z");
  return [
    pt(imei, speed, t0, { ignition }),
    pt(imei, speed, t0 + 5 * 60_000, { ignition }),
    pt(imei, speed, t0 + 10 * 60_000, { ignition }),
    pt(imei, 40, t0 + 11 * 60_000, { ignition: true }),
  ];
}

describe("idle ignition / computeIdle semantics", () => {
  it("isAccOn: true/false/null behave as HEAD", () => {
    assert.equal(isAccOn({ ignition: true }), true);
    assert.equal(isAccOn({ ignition: false }), false);
    assert.equal(isAccOn({ ignition: null }), false);
    assert.equal(isAccOn({}), false);
    assert.equal(isAccOn({ acc_status: "on" }), true);
    assert.equal(isAccOn({ acc_status: "off" }), false);
  });

  it("requireAccOn + ignition null → not idle", () => {
    const points = idleScenario("n", { ignition: null });
    // Explicit null on points
    for (const p of points.slice(0, 3)) p.ignition = null;
    const r = computeIdle(points, 0, 5, 1, true, 600);
    assert.equal(r.idleSeconds, 0);
    assert.equal(r.idleCount, 0);
  });

  it("ignition true + low speed counts idle", () => {
    const points = idleScenario("y", { ignition: true, speed: 0 });
    const r = computeIdle(points, 0, 5, 1, true, 600);
    assert.ok(r.idleSeconds >= 5 * 60);
    assert.ok(r.idleCount >= 1);
  });

  it("ignition false never idle even at speed 0", () => {
    const points = idleScenario("f", { ignition: false, speed: 0 });
    const r = computeIdle(points, 0, 5, 1, true, 600);
    assert.equal(r.idleSeconds, 0);
  });

  it("movement closes idle; exact min duration boundary", () => {
    const t0 = Date.parse("2026-09-07T10:00:00Z");
    const exact = [
      pt("e", 0, t0, { ignition: true }),
      pt("e", 0, t0 + 5 * 60_000, { ignition: true }),
      pt("e", 40, t0 + 5 * 60_000, { ignition: true }),
    ];
    const ok = computeIdle(exact, 0, 5, 1, true, 600);
    assert.equal(ok.idleCount, 1);
    assert.ok(Math.abs(ok.idleSeconds - 300) < 0.01);

    const short = [
      pt("e", 0, t0, { ignition: true }),
      pt("e", 0, t0 + 4 * 60_000, { ignition: true }),
      pt("e", 40, t0 + 4 * 60_000 + 1, { ignition: true }),
    ];
    assert.equal(computeIdle(short, 0, 5, 1, true, 600).idleCount, 0);
  });

  it("open interval at end of day is counted when >= min", () => {
    const t0 = Date.parse("2026-09-07T10:00:00Z");
    const points = [
      pt("o", 0, t0, { ignition: true }),
      pt("o", 0, t0 + 6 * 60_000, { ignition: true }),
    ];
    const r = computeIdle(points, 0, 5, 1, true, 600);
    assert.equal(r.idleCount, 1);
    assert.ok(r.idleSeconds >= 360);
  });

  it("maxGapSeconds splits idle periods", () => {
    const t0 = Date.parse("2026-09-07T10:00:00Z");
    const points = [
      pt("g", 0, t0, { ignition: true }),
      pt("g", 0, t0 + 3 * 60_000, { ignition: true }),
      // gap > 600s
      pt("g", 0, t0 + 3 * 60_000 + 700_000, { ignition: true }),
      pt("g", 0, t0 + 3 * 60_000 + 700_000 + 6 * 60_000, { ignition: true }),
    ];
    const r = computeIdle(points, 0, 5, 1, true, 600);
    // first stretch 3 min < 5 → drop; second 6 min → count
    assert.equal(r.idleCount, 1);
  });
});

describe("idle batch materialization", () => {
  it("discovers once and fetches once per chunk (not per IMEI)", async () => {
    const points = [];
    for (let i = 0; i < 12; i++) {
      points.push(...idleScenario(`d${i}`, { ignition: true }));
    }
    const fake = createFakeIdleDb({ points });
    const metrics = {};
    await buildAndPersistIdleStats({
      ...fake,
      idleSpeedKph: 0,
      idleMinutes: 5,
      requireAccOn: true,
      businessDay: "utc",
      dayUtc: new Date("2026-09-07T00:00:00Z"),
      chunkSize: 4,
      metrics,
    });
    assert.equal(fake.state.distinctCalls, 1);
    assert.equal(fake.state.findCalls, 3);
    assert.equal(fake.state.bulkWrites, 3);
    assert.ok(fake.state.reads < 20);
    assert.equal(fake.state.idle.length, 12);
    assert.equal(metrics.analytics_idle_chunks_processed, 3);
  });

  it("no-idle device issues deleteOne in bulkWrite", async () => {
    const imei = "noidle";
    const day = new Date("2026-09-07T00:00:00Z");
    const fake = createFakeIdleDb({
      points: [
        pt(imei, 40, "2026-09-07T10:00:00Z", { ignition: true }),
        pt(imei, 50, "2026-09-07T10:10:00Z", { ignition: true }),
      ],
      idle: [{ imei, day, idle_duration_seconds: 999, idle_count: 1 }],
    });
    await buildAndPersistIdleStats({
      ...fake,
      idleSpeedKph: 0,
      idleMinutes: 5,
      businessDay: "utc",
      dayUtc: day,
      imeis: [imei],
    });
    assert.equal(fake.state.deletesInBulk, 1);
    assert.equal(fake.state.idle.length, 0);
  });

  it("chunk size does not change results", async () => {
    const points = [];
    for (let i = 0; i < 9; i++) points.push(...idleScenario(`c${i}`));
    const a = createFakeIdleDb({ points: points.map((p) => ({ ...p })) });
    const b = createFakeIdleDb({ points: points.map((p) => ({ ...p })) });
    const opts = {
      idleSpeedKph: 0,
      idleMinutes: 5,
      businessDay: "utc",
      dayUtc: new Date("2026-09-07T00:00:00Z"),
    };
    await buildAndPersistIdleStats({ ...a, ...opts, chunkSize: 3 });
    await buildAndPersistIdleStats({ ...b, ...opts, chunkSize: 9 });
    assert.equal(a.state.idle.length, b.state.idle.length);
    for (const row of a.state.idle) {
      const other = b.state.idle.find((r) => r.imei === row.imei);
      assert.ok(other);
      assert.equal(other.idle_count, row.idle_count);
      assert.ok(Math.abs(other.idle_duration_seconds - row.idle_duration_seconds) < 1e-6);
    }
  });

  it("one device failure does not corrupt another", async () => {
    const good = "good";
    const bad = "bad";
    const points = [...idleScenario(good), ...idleScenario(bad)];
    const fake = createFakeIdleDb({ points });
    const origFind = fake.GpsPoint.find.bind(fake.GpsPoint);
    fake.GpsPoint.find = (q) => {
      const builder = origFind(q);
      const origExec = builder._exec.bind(builder);
      builder._exec = async () => {
        const rows = await origExec();
        return rows.map((r) =>
          r.imei === bad
            ? new Proxy(r, {
                get(t, prop) {
                  if (prop === "speed") throw new Error("boom-bad");
                  return t[prop];
                },
              })
            : r
        );
      };
      return builder;
    };
    await assert.rejects(
      () =>
        buildAndPersistIdleStats({
          ...fake,
          idleSpeedKph: 0,
          idleMinutes: 5,
          businessDay: "utc",
          dayUtc: new Date("2026-09-07T00:00:00Z"),
          imeis: [good, bad],
          log: { warn() {} },
        }),
      /partial failure/
    );
    assert.ok(fake.state.idle.some((r) => r.imei === good));
    assert.equal(fake.state.idle.some((r) => r.imei === bad), false);
  });

  it("Cairo vs utc day boundaries", () => {
    const noon = new Date("2026-09-07T12:00:00Z");
    const cairo = resolveIdleDay({ now: noon, businessDay: "cairo" });
    const utc = resolveIdleDay({ now: noon, businessDay: "utc" });
    assert.equal(utc.dayStart.toISOString(), startOfTodayUTC(noon).toISOString());
    assert.notEqual(cairo.dayStart.toISOString(), utc.dayStart.toISOString());
    assert.equal(DEFAULT_CHUNK, 150);
  });

  it("overlap gate still works", async () => {
    const metrics = {};
    const gate = createOverlapGate("idle", metrics);
    let release;
    const hold = new Promise((r) => {
      release = r;
    });
    const first = gate.run(() => hold);
    const second = await gate.run(async () => {});
    assert.equal(second.skipped, true);
    release();
    await first;
    assert.deepEqual(chunkArray([1, 2, 3, 4], 2), [
      [1, 2],
      [3, 4],
    ]);
  });

  it("handleIdleNotifySample live path remains independent", () => {
    let confirmed = 0;
    const imei = "live";
    handleIdleNotifySample(imei, {
      speed: 0,
      accOn: true,
      packetDate: new Date("2026-09-07T10:00:00Z"),
      lat: 1,
      lon: 2,
    }, { idleSpeedKph: 0, idleMinutes: 5, requireAccOn: true, onIdleConfirmed: () => {} });

    handleIdleNotifySample(
      imei,
      {
        speed: 0,
        accOn: true,
        packetDate: new Date("2026-09-07T10:06:00Z"),
        lat: 1,
        lon: 2,
      },
      {
        idleSpeedKph: 0,
        idleMinutes: 5,
        requireAccOn: true,
        onIdleConfirmed: async () => {
          confirmed += 1;
        },
      }
    );
    // notification is setImmediate — give it a tick
    return new Promise((resolve) => {
      setImmediate(() => {
        assert.equal(confirmed, 1);
        // null/false acc does not notify
        handleIdleNotifySample(
          imei,
          { speed: 0, accOn: false, packetDate: new Date() },
          { requireAccOn: true, onIdleConfirmed: async () => {
            confirmed += 1;
          } }
        );
        assert.equal(confirmed, 1);
        resolve();
      });
    });
  });
});
