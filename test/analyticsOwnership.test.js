const { describe, it, beforeEach, afterEach } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const os = require("os");
const path = require("path");
const {
  createOverlapGate,
  createReportSchedulerOwner,
  readAnalyticsWorkerHeartbeat,
  normalizeOwner,
  OWNER_ANALYTICS,
  OWNER_BRIDGE,
} = require("../lib/reportSchedulerRuntime");
const { startGlobalReportSchedulers } = require("../lib/globalReportSchedulers");

function tmpDir(prefix) {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

describe("A1 report scheduler ownership", () => {
  let spoolDir;
  beforeEach(() => {
    spoolDir = tmpDir("a1-analytics-");
  });
  afterEach(() => {
    fs.rmSync(spoolDir, { recursive: true, force: true });
  });

  it("normalizeOwner defaults to analytics", () => {
    assert.equal(normalizeOwner(undefined), OWNER_ANALYTICS);
    assert.equal(normalizeOwner("bridge"), OWNER_BRIDGE);
    assert.equal(normalizeOwner("ANALYTICS"), OWNER_ANALYTICS);
  });

  it("shouldStartSchedulers is true only for matching owner", () => {
    const analytics = createReportSchedulerOwner({
      owner: OWNER_ANALYTICS,
      spoolDir,
      metrics: {},
    });
    assert.equal(analytics.shouldStartSchedulers(OWNER_ANALYTICS), true);
    assert.equal(analytics.shouldStartSchedulers(OWNER_BRIDGE), false);

    const bridge = createReportSchedulerOwner({
      owner: OWNER_BRIDGE,
      spoolDir,
      metrics: {},
    });
    assert.equal(bridge.shouldStartSchedulers(OWNER_BRIDGE), true);
    assert.equal(bridge.shouldStartSchedulers(OWNER_ANALYTICS), false);
  });

  it("lock forbids dual ownership while holder is alive", () => {
    const a = createReportSchedulerOwner({
      owner: OWNER_ANALYTICS,
      expectedOwner: OWNER_ANALYTICS,
      spoolDir,
      pid: process.pid,
      metrics: {},
      log: { error() {}, warn() {}, info() {} },
    });
    a.acquireLock();
    const b = createReportSchedulerOwner({
      owner: OWNER_BRIDGE,
      expectedOwner: OWNER_BRIDGE,
      spoolDir,
      pid: process.pid + 999999,
      metrics: {},
      log: { error() {}, warn() {}, info() {} },
    });
    assert.throws(() => b.acquireLock(), /dual-owner forbidden/);
    a.releaseLock();
  });

  it("stale lock is reclaimed when holder pid is dead", () => {
    const lockPath = path.join(spoolDir, "report-schedulers.lock");
    fs.mkdirSync(spoolDir, { recursive: true });
    fs.writeFileSync(
      lockPath,
      JSON.stringify({
        pid: 2147483000,
        owner: "analytics",
        started_at: new Date().toISOString(),
      })
    );
    const owner = createReportSchedulerOwner({
      owner: OWNER_ANALYTICS,
      expectedOwner: OWNER_ANALYTICS,
      spoolDir,
      pid: process.pid,
      metrics: {},
      log: { error() {}, warn() {}, info() {} },
    });
    assert.equal(owner.acquireLock(), true);
    owner.releaseLock();
  });
});

describe("A1 overlap guards", () => {
  it("prevents overlapping runs and increments metric", async () => {
    const metrics = {};
    const gate = createOverlapGate("mileage", metrics);
    let resolveFirst;
    const first = gate.run(
      () =>
        new Promise((resolve) => {
          resolveFirst = resolve;
        })
    );
    const second = await gate.run(async () => {});
    assert.equal(second.skipped, true);
    assert.equal(second.reason, "overlap");
    assert.equal(metrics.analytics_overlap_prevented_total, 1);
    resolveFirst();
    await first;
    const third = await gate.run(async () => {});
    assert.equal(third.skipped, false);
  });
});

describe("A1 heartbeat / health read", () => {
  let spoolDir;
  beforeEach(() => {
    spoolDir = tmpDir("a1-hb-");
  });
  afterEach(() => {
    fs.rmSync(spoolDir, { recursive: true, force: true });
  });

  it("writes heartbeat and /health reader marks alive", () => {
    const metrics = {};
    const runtime = createReportSchedulerOwner({
      owner: OWNER_ANALYTICS,
      expectedOwner: OWNER_ANALYTICS,
      spoolDir,
      metrics,
      heartbeatMs: 60_000,
      log: { warn() {} },
    });
    runtime.acquireLock();
    runtime.writeHeartbeat();
    const hb = readAnalyticsWorkerHeartbeat(spoolDir);
    assert.equal(hb.analytics_worker_alive, true);
    assert.ok(hb.analytics_worker_last_heartbeat);
    assert.equal(hb.analytics_worker_owner, OWNER_ANALYTICS);
    runtime.releaseLock();
  });

  it("missing heartbeat is not alive", () => {
    const hb = readAnalyticsWorkerHeartbeat(spoolDir);
    assert.equal(hb.analytics_worker_alive, false);
    assert.equal(hb.analytics_jobs_running, 0);
  });
});

describe("A1 stagger + static after mileage", () => {
  let spoolDir;
  beforeEach(() => {
    spoolDir = tmpDir("a1-stagger-");
  });
  afterEach(() => {
    fs.rmSync(spoolDir, { recursive: true, force: true });
  });

  it("skips when processRole does not match owner", () => {
    const ctrl = startGlobalReportSchedulers({
      processRole: OWNER_BRIDGE,
      owner: OWNER_ANALYTICS,
      spoolDir,
      metrics: {},
      log: { info() {}, error() {}, warn() {} },
      updateIncrementalMileage: async () => {},
      buildAndPersistDailyReport: async () => {},
      buildAndPersistTravelStats: async () => {},
      buildAndPersistIdleStats: async () => {},
      buildAndPersistStaticStats: async () => {},
    });
    assert.equal(ctrl.started, false);
    assert.equal(ctrl.reason, "owner_mismatch");
  });

  it("staggers startup and never starts static before mileage completes", async () => {
    const order = [];
    const pendingTimeouts = [];
    const setTimeoutFn = (fn, ms) => {
      const handle = { ms, fn, cleared: false };
      pendingTimeouts.push(handle);
      return handle;
    };
    const clearTimeoutFn = (handle) => {
      if (handle) handle.cleared = true;
    };
    const setIntervalFn = () => ({ interval: true });
    const clearIntervalFn = () => {};

    let releaseMileage;
    const mileageHold = new Promise((resolve) => {
      releaseMileage = resolve;
    });

    const ctrl = startGlobalReportSchedulers({
      processRole: OWNER_ANALYTICS,
      owner: OWNER_ANALYTICS,
      spoolDir,
      metrics: {},
      log: { info() {}, error() {}, warn() {} },
      intervalMs: 60_000,
      staggerMileageMs: 10,
      staggerIdleMs: 20,
      staggerTravelMs: 30,
      setTimeoutFn,
      setIntervalFn,
      clearTimeoutFn,
      clearIntervalFn,
      updateIncrementalMileage: async () => {
        order.push("mileage_inc_start");
        await mileageHold;
        order.push("mileage_inc_done");
      },
      buildAndPersistDailyReport: async () => {
        order.push("daily");
      },
      buildAndPersistStaticStats: async () => {
        order.push("static");
      },
      buildAndPersistIdleStats: async () => {
        order.push("idle");
      },
      buildAndPersistTravelStats: async () => {
        order.push("travel");
      },
    });
    assert.equal(ctrl.started, true);
    assert.equal(pendingTimeouts.map((t) => t.ms).join(","), "10,20,30");

    // Fire mileage startup only; static is chained after.
    await pendingTimeouts[0].fn();
    assert.ok(order.includes("mileage_inc_start"));
    assert.equal(order.includes("static"), false);

    releaseMileage();
    await new Promise((r) => setImmediate(r));
    await new Promise((r) => setImmediate(r));
    // allow chained static
    await new Promise((r) => setTimeout(r, 20));
    assert.ok(order.includes("daily"));
    assert.ok(order.includes("static"));
    const mileageIdx = order.indexOf("mileage_inc_done");
    const staticIdx = order.indexOf("static");
    assert.ok(mileageIdx >= 0 && staticIdx > mileageIdx);

    ctrl.stop();
  });

  it("overlap gate skips concurrent mileage via controller", async () => {
    const metrics = {};
    let release;
    const hold = new Promise((r) => {
      release = r;
    });
    const ctrl = startGlobalReportSchedulers({
      processRole: OWNER_ANALYTICS,
      owner: OWNER_ANALYTICS,
      spoolDir,
      metrics,
      log: { info() {}, error() {}, warn() {} },
      intervalMs: 60_000,
      staggerMileageMs: 60_000,
      staggerIdleMs: 60_000,
      staggerTravelMs: 60_000,
      setTimeoutFn: () => ({}),
      setIntervalFn: () => ({}),
      clearTimeoutFn: () => {},
      clearIntervalFn: () => {},
      updateIncrementalMileage: async () => {
        await hold;
      },
      buildAndPersistDailyReport: async () => {},
      buildAndPersistTravelStats: async () => {},
      buildAndPersistIdleStats: async () => {},
      buildAndPersistStaticStats: async () => {},
    });
    const first = ctrl.runMileage();
    const second = await ctrl.runMileage();
    assert.equal(second.skipped, true);
    assert.equal(metrics.analytics_overlap_prevented_total, 1);
    release();
    await first;
    ctrl.stop();
  });
});
