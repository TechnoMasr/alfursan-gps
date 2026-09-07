/**
 * Starts the four global report materializers with:
 * - single ownership lock
 * - per-job overlap guards
 * - staggered first runs (no startup storm)
 * - Static after mileage first completion (no DailyMileage race)
 *
 * Does not change report algorithms.
 * Service modules are required lazily so unit tests can inject fakes without Mongo.
 */
const {
  createReportSchedulerOwner,
  normalizeOwner,
  OWNER_ANALYTICS,
} = require("./reportSchedulerRuntime");

const DEFAULT_INTERVAL_MS = 20 * 60 * 1000;

function startGlobalReportSchedulers(options = {}) {
  const {
    processRole = OWNER_ANALYTICS,
    owner = process.env.REPORT_SCHEDULER_OWNER,
    metrics = {},
    log = console,
    intervalMs = Number(process.env.REPORT_SCHEDULER_INTERVAL_MS || DEFAULT_INTERVAL_MS) ||
      DEFAULT_INTERVAL_MS,
    staggerMileageMs = Number(process.env.REPORT_STAGGER_MILEAGE_MS || 30_000) || 30_000,
    staggerIdleMs = Number(process.env.REPORT_STAGGER_IDLE_MS || 60_000) || 60_000,
    staggerTravelMs = Number(process.env.REPORT_STAGGER_TRAVEL_MS || 90_000) || 90_000,
    stopThresholdsMinutes = [1, 3, 5, 10, 15, 30, 60],
    idleSpeedKph = 0,
    idleMinutes = 5,
    requireAccOn = true,
    maxGapSeconds = 10 * 60,
    mileageThresholdKm = 0.5,
    timers = [],
    spoolDir,
    setTimeoutFn = setTimeout,
    setIntervalFn = setInterval,
    clearTimeoutFn = clearTimeout,
    clearIntervalFn = clearInterval,
  } = options;

  const updateIncrementalMileage =
    options.updateIncrementalMileage ||
    require("../mileageService").updateIncrementalMileage;
  const buildAndPersistDailyReport =
    options.buildAndPersistDailyReport ||
    require("../mileageService").buildAndPersistDailyReport;
  const buildAndPersistTravelStats =
    options.buildAndPersistTravelStats ||
    require("../travelStatsService").buildAndPersistTravelStats;
  const buildAndPersistIdleStats =
    options.buildAndPersistIdleStats ||
    require("../idleStatsService").buildAndPersistIdleStats;
  const buildAndPersistStaticStats =
    options.buildAndPersistStaticStats ||
    require("../staticStatsService").buildAndPersistStaticStats;

  const runtime = createReportSchedulerOwner({
    owner,
    expectedOwner: processRole,
    metrics,
    log,
    heartbeatMs: Number(process.env.ANALYTICS_HEARTBEAT_MS || 1000) || 1000,
    ...(spoolDir ? { spoolDir } : {}),
  });

  if (!runtime.shouldStartSchedulers(processRole)) {
    log.info?.(
      `[analytics] skipping global report schedulers: REPORT_SCHEDULER_OWNER=${runtime.owner} processRole=${normalizeOwner(processRole)}`
    );
    return { started: false, reason: "owner_mismatch", runtime };
  }

  runtime.acquireLock();
  runtime.startHeartbeat();
  log.info?.(
    `[analytics] global report schedulers started owner=${runtime.owner} pid=${process.pid} intervalMs=${intervalMs}`
  );

  async function runMileage() {
    return runtime.gates.mileage.run(async () => {
      await updateIncrementalMileage();
      await buildAndPersistDailyReport();
    });
  }

  async function runStatic() {
    return runtime.gates.static.run(async () => {
      // Never race DailyMileage: wait while mileage gate is busy.
      const deadline = Date.now() + 15 * 60 * 1000;
      while (runtime.gates.mileage.running && Date.now() < deadline) {
        await new Promise((r) => setTimeout(r, 250));
      }
      await buildAndPersistStaticStats({ mileageThresholdKm });
    });
  }

  async function runIdle() {
    return runtime.gates.idle.run(async () => {
      await buildAndPersistIdleStats({
        idleSpeedKph,
        idleMinutes,
        requireAccOn,
        maxGapSeconds,
      });
    });
  }

  async function runTravel() {
    return runtime.gates.travel.run(async () => {
      for (const t of stopThresholdsMinutes) {
        await buildAndPersistTravelStats({ stopThresholdMin: t });
      }
    });
  }

  function scheduleInterval(fn, label) {
    const id = setIntervalFn(() => {
      void fn().catch((err) => log.error?.(`[analytics] ${label} error:`, err.message));
    }, intervalMs);
    if (typeof id.unref === "function") id.unref();
    timers.push(id);
    return id;
  }

  // Staggered first runs — mileage first, then static chained after mileage succeeds.
  const tMileage = setTimeoutFn(() => {
    void runMileage()
      .then(() => runStatic())
      .catch((err) => log.error?.("[analytics] mileage/static startup error:", err.message));
  }, Math.max(0, staggerMileageMs));
  if (typeof tMileage.unref === "function") tMileage.unref();
  timers.push(tMileage);

  const tIdle = setTimeoutFn(() => {
    void runIdle().catch((err) => log.error?.("[analytics] idle startup error:", err.message));
  }, Math.max(0, staggerIdleMs));
  if (typeof tIdle.unref === "function") tIdle.unref();
  timers.push(tIdle);

  const tTravel = setTimeoutFn(() => {
    void runTravel().catch((err) =>
      log.error?.("[analytics] travel startup error:", err.message)
    );
  }, Math.max(0, staggerTravelMs));
  if (typeof tTravel.unref === "function") tTravel.unref();
  timers.push(tTravel);

  // Steady-state intervals: mileage then static sequenced on same tick for Static safety.
  scheduleInterval(async () => {
    await runMileage();
    await runStatic();
  }, "mileage+static");
  scheduleInterval(runIdle, "idle");
  scheduleInterval(runTravel, "travel");

  function stop() {
    for (const id of timers) {
      try {
        clearTimeoutFn(id);
        clearIntervalFn(id);
      } catch {
        /* ignore */
      }
    }
    timers.length = 0;
    runtime.stopHeartbeat();
    runtime.releaseLock();
  }

  return {
    started: true,
    runtime,
    stop,
    runMileage,
    runStatic,
    runIdle,
    runTravel,
  };
}

module.exports = {
  startGlobalReportSchedulers,
  DEFAULT_INTERVAL_MS,
};
