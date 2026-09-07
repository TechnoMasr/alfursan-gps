/**
 * Global report-scheduler ownership, lock, heartbeat, and overlap helpers.
 *
 * Exactly one process may run mileage/travel/idle/static materializers:
 *   REPORT_SCHEDULER_OWNER=analytics  → workers/analytics-worker.js (default)
 *   REPORT_SCHEDULER_OWNER=bridge     → realtime bridge (rollback only)
 *
 * Never start the same schedulers in both processes.
 */
const fs = require("fs");
const path = require("path");

const OWNER_ANALYTICS = "analytics";
const OWNER_BRIDGE = "bridge";

function normalizeOwner(raw) {
  const v = String(raw || OWNER_ANALYTICS).trim().toLowerCase();
  if (v === OWNER_BRIDGE) return OWNER_BRIDGE;
  return OWNER_ANALYTICS;
}

function pidAlive(pid) {
  const n = Number(pid);
  if (!Number.isInteger(n) || n <= 0) return false;
  try {
    process.kill(n, 0);
    return true;
  } catch {
    return false;
  }
}

function createOverlapGate(name, metrics = {}) {
  let running = false;
  return {
    get running() {
      return running;
    },
    async run(fn) {
      if (running) {
        metrics.analytics_overlap_prevented_total =
          (metrics.analytics_overlap_prevented_total || 0) + 1;
        metrics[`analytics_overlap_prevented_${name}`] =
          (metrics[`analytics_overlap_prevented_${name}`] || 0) + 1;
        return { skipped: true, reason: "overlap" };
      }
      running = true;
      metrics.analytics_jobs_running = (metrics.analytics_jobs_running || 0) + 1;
      metrics.analytics_jobs_started_total = (metrics.analytics_jobs_started_total || 0) + 1;
      const started = Date.now();
      try {
        await fn();
        metrics.analytics_jobs_completed_total =
          (metrics.analytics_jobs_completed_total || 0) + 1;
        metrics[`analytics_job_${name}_last_success_at`] = new Date().toISOString();
        metrics[`analytics_job_${name}_last_duration_ms`] = Date.now() - started;
        return { skipped: false };
      } catch (err) {
        metrics.analytics_jobs_failed_total = (metrics.analytics_jobs_failed_total || 0) + 1;
        metrics[`analytics_job_${name}_last_error`] = String(err?.message || err);
        throw err;
      } finally {
        running = false;
        metrics.analytics_jobs_running = Math.max(
          0,
          (metrics.analytics_jobs_running || 1) - 1
        );
      }
    },
  };
}

function createReportSchedulerOwner(options = {}) {
  const {
    owner = normalizeOwner(process.env.REPORT_SCHEDULER_OWNER),
    expectedOwner,
    spoolDir = path.join(__dirname, "..", "data", "analytics-spool"),
    fsImpl = fs,
    now = () => Date.now(),
    log = console,
    metrics = {},
    heartbeatMs = 1000,
    pid = process.pid,
  } = options;

  const resolvedOwner = normalizeOwner(owner);
  const required = expectedOwner ? normalizeOwner(expectedOwner) : null;
  const resolvedSpoolDir = path.resolve(spoolDir);
  const lockPath = path.join(resolvedSpoolDir, "report-schedulers.lock");
  const heartbeatPath = path.join(resolvedSpoolDir, "analytics-worker.heartbeat.json");
  let lockHeld = false;
  let heartbeatTimer = null;
  const gates = {
    mileage: createOverlapGate("mileage", metrics),
    travel: createOverlapGate("travel", metrics),
    idle: createOverlapGate("idle", metrics),
    static: createOverlapGate("static", metrics),
  };

  function ensureDir() {
    fsImpl.mkdirSync(resolvedSpoolDir, { recursive: true });
  }

  function assertExpectedOwner() {
    if (required && resolvedOwner !== required) {
      throw new Error(
        `report scheduler owner mismatch: process expects ${required} but REPORT_SCHEDULER_OWNER=${resolvedOwner}`
      );
    }
  }

  function shouldStartSchedulers(processRole) {
    const role = normalizeOwner(processRole);
    return resolvedOwner === role;
  }

  function acquireLock() {
    assertExpectedOwner();
    ensureDir();
    const payload = JSON.stringify({
      pid,
      owner: resolvedOwner,
      started_at: new Date(now()).toISOString(),
    });
    try {
      fsImpl.writeFileSync(lockPath, payload, { flag: "wx" });
      lockHeld = true;
      return true;
    } catch (err) {
      if (err?.code !== "EEXIST") throw err;
      let existing = null;
      try {
        existing = JSON.parse(fsImpl.readFileSync(lockPath, "utf8"));
      } catch {
        existing = null;
      }
      const otherPid = Number(existing?.pid);
      if (otherPid === Number(pid)) {
        lockHeld = true;
        return true;
      }
      if (otherPid && !pidAlive(otherPid)) {
        try {
          fsImpl.unlinkSync(lockPath);
        } catch {
          /* ignore */
        }
        fsImpl.writeFileSync(lockPath, payload, { flag: "wx" });
        lockHeld = true;
        log.warn?.("[analytics] reclaimed stale report-scheduler lock", {
          stale_pid: otherPid,
        });
        return true;
      }
      const msg =
        "report-scheduler dual-owner forbidden: another process holds report-schedulers.lock (set REPORT_SCHEDULER_OWNER consistently; never run bridge+analytics schedulers together)";
      log.error?.(msg, { lock: lockPath, holder: existing });
      throw new Error(msg);
    }
  }

  function releaseLock() {
    if (!lockHeld) return;
    try {
      const raw = fsImpl.readFileSync(lockPath, "utf8");
      const existing = JSON.parse(raw);
      if (Number(existing?.pid) === Number(pid)) fsImpl.unlinkSync(lockPath);
    } catch {
      /* ignore */
    }
    lockHeld = false;
  }

  function writeHeartbeat(extra = {}) {
    try {
      ensureDir();
      const ts = new Date(now()).toISOString();
      const body = JSON.stringify({
        alive: true,
        pid,
        owner: resolvedOwner,
        ts,
        jobs_running: metrics.analytics_jobs_running || 0,
        jobs_started_total: metrics.analytics_jobs_started_total || 0,
        jobs_completed_total: metrics.analytics_jobs_completed_total || 0,
        jobs_failed_total: metrics.analytics_jobs_failed_total || 0,
        overlap_prevented_total: metrics.analytics_overlap_prevented_total || 0,
        mileage_running: gates.mileage.running,
        travel_running: gates.travel.running,
        idle_running: gates.idle.running,
        static_running: gates.static.running,
        last_success_mileage: metrics.analytics_job_mileage_last_success_at || null,
        last_success_travel: metrics.analytics_job_travel_last_success_at || null,
        last_success_idle: metrics.analytics_job_idle_last_success_at || null,
        last_success_static: metrics.analytics_job_static_last_success_at || null,
        mileage_devices_processed: metrics.analytics_mileage_devices_processed || 0,
        mileage_chunks_processed: metrics.analytics_mileage_chunks_processed || 0,
        mileage_points_processed: metrics.analytics_mileage_points_processed || 0,
        mileage_mongo_reads: metrics.analytics_mileage_mongo_reads || 0,
        mileage_mongo_writes: metrics.analytics_mileage_mongo_writes || 0,
        mileage_bulk_writes: metrics.analytics_mileage_bulk_writes || 0,
        mileage_duration_ms: metrics.analytics_mileage_duration_ms || 0,
        mileage_last_success_at: metrics.analytics_mileage_last_success_at || null,
        travel_devices_processed: metrics.analytics_travel_devices_processed || 0,
        travel_chunks_processed: metrics.analytics_travel_chunks_processed || 0,
        travel_points_processed: metrics.analytics_travel_points_processed || 0,
        travel_thresholds_processed: metrics.analytics_travel_thresholds_processed || 0,
        travel_mongo_reads: metrics.analytics_travel_mongo_reads || 0,
        travel_mongo_writes: metrics.analytics_travel_mongo_writes || 0,
        travel_bulk_writes: metrics.analytics_travel_bulk_writes || 0,
        travel_duration_ms: metrics.analytics_travel_duration_ms || 0,
        travel_max_points_per_device: metrics.analytics_travel_max_points_per_device || 0,
        travel_last_success_at: metrics.analytics_travel_last_success_at || null,
        idle_devices_processed: metrics.analytics_idle_devices_processed || 0,
        idle_chunks_processed: metrics.analytics_idle_chunks_processed || 0,
        idle_points_processed: metrics.analytics_idle_points_processed || 0,
        idle_mongo_reads: metrics.analytics_idle_mongo_reads || 0,
        idle_mongo_writes: metrics.analytics_idle_mongo_writes || 0,
        idle_bulk_writes: metrics.analytics_idle_bulk_writes || 0,
        idle_duration_ms: metrics.analytics_idle_duration_ms || 0,
        idle_max_points_per_device: metrics.analytics_idle_max_points_per_device || 0,
        idle_records_upserted: metrics.analytics_idle_records_upserted || 0,
        idle_records_deleted: metrics.analytics_idle_records_deleted || 0,
        idle_last_success_at: metrics.analytics_idle_last_success_at || null,
        static_devices_processed: metrics.analytics_static_devices_processed || 0,
        static_chunks_processed: metrics.analytics_static_chunks_processed || 0,
        static_daily_mileage_hits: metrics.analytics_static_daily_mileage_hits || 0,
        static_gps_fallback_devices: metrics.analytics_static_gps_fallback_devices || 0,
        static_points_processed: metrics.analytics_static_points_processed || 0,
        static_mongo_reads: metrics.analytics_static_mongo_reads || 0,
        static_mongo_writes: metrics.analytics_static_mongo_writes || 0,
        static_bulk_writes: metrics.analytics_static_bulk_writes || 0,
        static_duration_ms: metrics.analytics_static_duration_ms || 0,
        static_last_success_at: metrics.analytics_static_last_success_at || null,
        ...extra,
      });
      const tmp = `${heartbeatPath}.tmp`;
      fsImpl.writeFileSync(tmp, body, "utf8");
      fsImpl.renameSync(tmp, heartbeatPath);
      metrics.analytics_worker_alive = true;
      metrics.analytics_worker_last_heartbeat = ts;
    } catch (err) {
      log.warn?.("[analytics] heartbeat write failed", err.message);
    }
  }

  function startHeartbeat() {
    if (heartbeatTimer) return;
    writeHeartbeat();
    heartbeatTimer = setInterval(() => writeHeartbeat(), heartbeatMs);
    if (typeof heartbeatTimer.unref === "function") heartbeatTimer.unref();
  }

  function stopHeartbeat() {
    if (heartbeatTimer) {
      clearInterval(heartbeatTimer);
      heartbeatTimer = null;
    }
  }

  return {
    owner: resolvedOwner,
    spoolDir: resolvedSpoolDir,
    lockPath,
    heartbeatPath,
    gates,
    shouldStartSchedulers,
    acquireLock,
    releaseLock,
    writeHeartbeat,
    startHeartbeat,
    stopHeartbeat,
    metrics,
  };
}

function numOr(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

/**
 * Read analytics worker heartbeat for /health. Never throws.
 */
function readAnalyticsWorkerHeartbeat(spoolDir, fsImpl = fs, clock = Date.now) {
  const heartbeatPath = path.join(
    path.resolve(spoolDir),
    "analytics-worker.heartbeat.json"
  );
  const empty = {
    analytics_worker_alive: false,
    analytics_worker_last_heartbeat: null,
    analytics_worker_heartbeat_age_ms: null,
    analytics_worker_pid: null,
    analytics_worker_owner: null,
    analytics_jobs_running: 0,
    analytics_jobs_started_total: 0,
    analytics_jobs_completed_total: 0,
    analytics_jobs_failed_total: 0,
    analytics_overlap_prevented_total: 0,
  };
  try {
    const data = JSON.parse(fsImpl.readFileSync(heartbeatPath, "utf8"));
    if (!data || typeof data !== "object") return empty;
    const ts = Date.parse(data.ts || "");
    const ageMs = Number.isFinite(ts) ? Math.max(0, clock() - ts) : null;
    return {
      analytics_worker_alive: ageMs != null && ageMs < 15_000,
      analytics_worker_last_heartbeat: data.ts || null,
      analytics_worker_heartbeat_age_ms: ageMs,
      analytics_worker_pid: data.pid || null,
      analytics_worker_owner: data.owner || null,
      analytics_jobs_running: numOr(data.jobs_running),
      analytics_jobs_started_total: numOr(data.jobs_started_total),
      analytics_jobs_completed_total: numOr(data.jobs_completed_total),
      analytics_jobs_failed_total: numOr(data.jobs_failed_total),
      analytics_overlap_prevented_total: numOr(data.overlap_prevented_total),
      analytics_last_success_mileage: data.last_success_mileage || null,
      analytics_last_success_travel: data.last_success_travel || null,
      analytics_last_success_idle: data.last_success_idle || null,
      analytics_last_success_static: data.last_success_static || null,
      analytics_mileage_devices_processed: numOr(data.mileage_devices_processed),
      analytics_mileage_chunks_processed: numOr(data.mileage_chunks_processed),
      analytics_mileage_points_processed: numOr(data.mileage_points_processed),
      analytics_mileage_mongo_reads: numOr(data.mileage_mongo_reads),
      analytics_mileage_mongo_writes: numOr(data.mileage_mongo_writes),
      analytics_mileage_bulk_writes: numOr(data.mileage_bulk_writes),
      analytics_mileage_duration_ms: numOr(data.mileage_duration_ms),
      analytics_mileage_last_success_at: data.mileage_last_success_at || null,
      analytics_travel_devices_processed: numOr(data.travel_devices_processed),
      analytics_travel_chunks_processed: numOr(data.travel_chunks_processed),
      analytics_travel_points_processed: numOr(data.travel_points_processed),
      analytics_travel_thresholds_processed: numOr(data.travel_thresholds_processed),
      analytics_travel_mongo_reads: numOr(data.travel_mongo_reads),
      analytics_travel_mongo_writes: numOr(data.travel_mongo_writes),
      analytics_travel_bulk_writes: numOr(data.travel_bulk_writes),
      analytics_travel_duration_ms: numOr(data.travel_duration_ms),
      analytics_travel_max_points_per_device: numOr(data.travel_max_points_per_device),
      analytics_travel_last_success_at: data.travel_last_success_at || null,
      analytics_idle_devices_processed: numOr(data.idle_devices_processed),
      analytics_idle_chunks_processed: numOr(data.idle_chunks_processed),
      analytics_idle_points_processed: numOr(data.idle_points_processed),
      analytics_idle_mongo_reads: numOr(data.idle_mongo_reads),
      analytics_idle_mongo_writes: numOr(data.idle_mongo_writes),
      analytics_idle_bulk_writes: numOr(data.idle_bulk_writes),
      analytics_idle_duration_ms: numOr(data.idle_duration_ms),
      analytics_idle_max_points_per_device: numOr(data.idle_max_points_per_device),
      analytics_idle_records_upserted: numOr(data.idle_records_upserted),
      analytics_idle_records_deleted: numOr(data.idle_records_deleted),
      analytics_idle_last_success_at: data.idle_last_success_at || null,
      analytics_static_devices_processed: numOr(data.static_devices_processed),
      analytics_static_chunks_processed: numOr(data.static_chunks_processed),
      analytics_static_daily_mileage_hits: numOr(data.static_daily_mileage_hits),
      analytics_static_gps_fallback_devices: numOr(data.static_gps_fallback_devices),
      analytics_static_points_processed: numOr(data.static_points_processed),
      analytics_static_mongo_reads: numOr(data.static_mongo_reads),
      analytics_static_mongo_writes: numOr(data.static_mongo_writes),
      analytics_static_bulk_writes: numOr(data.static_bulk_writes),
      analytics_static_duration_ms: numOr(data.static_duration_ms),
      analytics_static_last_success_at: data.static_last_success_at || null,
    };
  } catch {
    return empty;
  }
}

module.exports = {
  OWNER_ANALYTICS,
  OWNER_BRIDGE,
  normalizeOwner,
  createOverlapGate,
  createReportSchedulerOwner,
  readAnalyticsWorkerHeartbeat,
  pidAlive,
};
