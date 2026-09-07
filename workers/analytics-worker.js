/**
 * Dedicated analytics / global report materialization process (A1).
 *
 * Owns: mileage, travel, idle, static schedulers (exactly one owner).
 * Must NOT: WS, Traccar ingress, raw archive, GPSPoints writer, per-packet business.
 */
const path = require("path");

require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

process.env.REPORT_SCHEDULER_OWNER =
  process.env.REPORT_SCHEDULER_OWNER || "analytics";

if (!process.env.MONGO_MAX_POOL_SIZE) {
  process.env.MONGO_MAX_POOL_SIZE = String(
    process.env.ANALYTICS_MONGO_MAX_POOL || 8
  );
}
if (!process.env.MONGO_MIN_POOL_SIZE) {
  process.env.MONGO_MIN_POOL_SIZE = String(
    process.env.ANALYTICS_MONGO_MIN_POOL || 1
  );
}

// Connect Mongo with bounded pool (mongo.js reads MONGO_* at load).
require("../mongo");

const {
  startGlobalReportSchedulers,
} = require("../lib/globalReportSchedulers");
const { OWNER_ANALYTICS, normalizeOwner } = require("../lib/reportSchedulerRuntime");

const spoolDir = process.env.ANALYTICS_SPOOL_DIR
  ? path.isAbsolute(process.env.ANALYTICS_SPOOL_DIR)
    ? process.env.ANALYTICS_SPOOL_DIR
    : path.join(__dirname, "..", process.env.ANALYTICS_SPOOL_DIR)
  : path.join(__dirname, "..", "data", "analytics-spool");

const metrics = {
  analytics_jobs_started_total: 0,
  analytics_jobs_completed_total: 0,
  analytics_jobs_failed_total: 0,
  analytics_jobs_running: 0,
  analytics_overlap_prevented_total: 0,
};

const owner = normalizeOwner(process.env.REPORT_SCHEDULER_OWNER);
if (owner !== OWNER_ANALYTICS) {
  console.error(
    `[alfursan-analytics] refusing to start: REPORT_SCHEDULER_OWNER=${owner} (expected analytics). Use bridge rollback mode only on the bridge process.`
  );
  process.exit(1);
}

let controller = null;
try {
  controller = startGlobalReportSchedulers({
    processRole: OWNER_ANALYTICS,
    owner,
    metrics,
    log: console,
    spoolDir,
  });
} catch (err) {
  console.error("[alfursan-analytics] failed to start schedulers:", err.message);
  process.exit(1);
}

if (!controller?.started) {
  console.error("[alfursan-analytics] schedulers not started:", controller?.reason);
  process.exit(1);
}

console.log(
  `[alfursan-analytics] online pid=${process.pid} spool=${controller.runtime.spoolDir}`
);

async function shutdown(signal) {
  console.log(`[alfursan-analytics] ${signal} — stopping schedulers`);
  try {
    controller?.stop?.();
  } catch (err) {
    console.warn("[alfursan-analytics] stop error:", err.message);
  }
  process.exit(0);
}

process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("SIGTERM", () => void shutdown("SIGTERM"));
