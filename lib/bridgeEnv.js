/**
 * Single source of truth for Traccar bridge env (no duplicated hardcoded policy).
 */

const path = require("path");

const PROJECT_ROOT = path.resolve(__dirname, "..");

function boolEnv(name, defaultValue) {
  const raw = process.env[name];
  if (raw == null || raw === "") return defaultValue;
  return String(raw) === "1" || String(raw).toLowerCase() === "true";
}

function intEnv(name, defaultValue) {
  const n = Number(process.env[name]);
  return Number.isFinite(n) ? n : defaultValue;
}

function strEnv(name, defaultValue) {
  const raw = process.env[name];
  if (raw == null || raw === "") return defaultValue;
  return String(raw);
}

function resolveProjectPath(raw, fallbackRelative) {
  const value = raw == null || raw === "" ? fallbackRelative : raw;
  if (path.isAbsolute(value)) return value;
  return path.resolve(PROJECT_ROOT, value);
}

function loadBridgeEnv(overrides = {}) {
  const env = {
    TRACCAR_BASE_URL: strEnv("TRACCAR_BASE_URL", "http://127.0.0.1:8082"),
    TRACCAR_SERVICE_TOKEN: strEnv("TRACCAR_SERVICE_TOKEN", ""),
    TRACCAR_FORWARD_TOKEN: strEnv("TRACCAR_FORWARD_TOKEN", ""),
    TRACCAR_FORWARD_QUEUE_MAX: intEnv("TRACCAR_FORWARD_QUEUE_MAX", 5000),
    TRACCAR_RAW_RETENTION_DAYS: intEnv("TRACCAR_RAW_RETENTION_DAYS", 7),

    MAX_LIVE_FIX_AGE_MS: intEnv("MAX_LIVE_FIX_AGE_MS", 300_000),
    LIVE_FIX_FUTURE_TOLERANCE_MS: intEnv("LIVE_FIX_FUTURE_TOLERANCE_MS", 120_000),
    LIVE_DEVICE_TIME_FALLBACK: boolEnv("LIVE_DEVICE_TIME_FALLBACK", true),
    LIVE_DEVICE_TIME_MAX_AGE_MS: intEnv("LIVE_DEVICE_TIME_MAX_AGE_MS", 300_000),
    LIVE_DEVICE_TIME_FUTURE_TOLERANCE_MS: intEnv("LIVE_DEVICE_TIME_FUTURE_TOLERANCE_MS", 120_000),
    LIVE_DEVICE_SERVER_MAX_SKEW_MS: intEnv("LIVE_DEVICE_SERVER_MAX_SKEW_MS", 180_000),

    SUBSCRIBER_HEARTBEAT_MS: intEnv("SUBSCRIBER_HEARTBEAT_MS", 30_000),
    WS_BUFFER_HIGH_WATERMARK_BYTES: intEnv("WS_BUFFER_HIGH_WATERMARK_BYTES", 1_048_576),
    WS_BUFFER_LOW_WATERMARK_BYTES: intEnv("WS_BUFFER_LOW_WATERMARK_BYTES", 262_144),
    WS_BUFFER_CRITICAL_BYTES: intEnv("WS_BUFFER_CRITICAL_BYTES", 8_388_608),

    GPSPOINT_BATCH_SIZE: intEnv("GPSPOINT_BATCH_SIZE", 250),
    GPSPOINT_FLUSH_MS: intEnv("GPSPOINT_FLUSH_MS", 100),
    GPSPOINT_MEM_HIGH: intEnv("GPSPOINT_MEM_HIGH", 10_000),
    GPSPOINT_RETRY_BASE_MS: intEnv("GPSPOINT_RETRY_BASE_MS", 250),
    GPSPOINT_RETRY_MAX_MS: intEnv("GPSPOINT_RETRY_MAX_MS", 30_000),
    GPSPOINT_SPOOL_DIR: resolveProjectPath(
      strEnv("GPSPOINT_SPOOL_DIR", ""),
      path.join("data", "gpspoints-spool")
    ),
    GPSPOINT_SPOOL_WARN_BYTES: intEnv("GPSPOINT_SPOOL_WARN_BYTES", 512 * 1024 * 1024),
    GPSPOINT_SPOOL_CRITICAL_BYTES: intEnv("GPSPOINT_SPOOL_CRITICAL_BYTES", 2 * 1024 * 1024 * 1024),
    MIN_DISK_FREE_BYTES: intEnv("MIN_DISK_FREE_BYTES", 1024 * 1024 * 1024),
    GPSPOINT_JOURNAL_COALESCE_MS: intEnv("GPSPOINT_JOURNAL_COALESCE_MS", 50),
    GPSPOINT_SPOOL_RECONCILE_MS: intEnv("GPSPOINT_SPOOL_RECONCILE_MS", 30_000),
    GPSPOINT_DRAIN_NEW_FILES_PER_CYCLE: intEnv("GPSPOINT_DRAIN_NEW_FILES_PER_CYCLE", 2),
    GPSPOINT_DRAIN_OLD_FILES_PER_CYCLE: intEnv("GPSPOINT_DRAIN_OLD_FILES_PER_CYCLE", 1),
    GPSPOINT_MAX_MONGO_BATCHES_PER_CYCLE: intEnv("GPSPOINT_MAX_MONGO_BATCHES_PER_CYCLE", 4),

    ANALYTICS_CONCURRENCY: intEnv("ANALYTICS_CONCURRENCY", 8),
    ANALYTICS_MEM_HIGH: intEnv("ANALYTICS_MEM_HIGH", 20_000),
    ANALYTICS_SPOOL_DIR: strEnv("ANALYTICS_SPOOL_DIR", "data/analytics-spool"),

    PERSISTENCE_IPC_MAX_QUEUE_BATCHES: intEnv("PERSISTENCE_IPC_MAX_QUEUE_BATCHES", 512),
    PERSISTENCE_IPC_MAX_BATCH_SIZE: intEnv("PERSISTENCE_IPC_MAX_BATCH_SIZE", 2000),
    PERSISTENCE_IPC_SPOOL_DIR: resolveProjectPath(
      strEnv("PERSISTENCE_IPC_SPOOL_DIR", ""),
      path.join("data", "persistence-ipc-spool")
    ),

    BRIDGE_LATENCY_DEBUG: boolEnv("BRIDGE_LATENCY_DEBUG", false),
    BRIDGE_SHUTDOWN_TIMEOUT_MS: intEnv("BRIDGE_SHUTDOWN_TIMEOUT_MS", 8_000),

    DEVICE_FETCH_COOLDOWN_MS: intEnv("DEVICE_FETCH_COOLDOWN_MS", 60_000),
    BASE_GAP_MIN: intEnv("BASE_GAP_MIN", 1),
  };
  return { ...env, ...overrides };
}

module.exports = {
  boolEnv,
  intEnv,
  strEnv,
  loadBridgeEnv,
  PROJECT_ROOT,
  resolveProjectPath,
};
