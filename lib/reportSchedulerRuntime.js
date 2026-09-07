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
