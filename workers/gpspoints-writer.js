/**
 * Dedicated GPSPoints Mongo drain process (P0-E3).
 *
 * Owns: ready/legacy spool → insertMany, retry, quarantine, heartbeat.
 * Must NOT: WebSocket, business processing, reports, model sync, tenant broadcast.
 *
 * Handoff: shared filesystem spool (no Redis). Dual-drain forbidden via lock file.
 */
const path = require("path");

require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

// Bounded pool for this writer only (before mongo.js connects).
if (!process.env.MONGO_MAX_POOL_SIZE) {
  process.env.MONGO_MAX_POOL_SIZE = String(
    process.env.GPSPOINT_WRITER_MONGO_MAX_POOL || 8
  );
}

const { GpsPoint } = require("../mongo");
const { createGpsPointWriter } = require("../lib/gpsPointWriter");

const spoolDir = process.env.GPSPOINT_SPOOL_DIR || undefined;
const pollMs = Number(process.env.GPSPOINT_WRITER_POLL_MS || 100) || 100;

const metrics = {
  gpspoints_received_total: 0,
  gpspoints_persisted_total: 0,
  gpspoints_writer_retries: 0,
  gpspoints_writer_quarantined: 0,
};

const writer = createGpsPointWriter({
  mode: "drain",
  insertMany: (docs) => GpsPoint.insertMany(docs, { ordered: false }),
  spoolDir,
  batchSize: Number(process.env.GPSPOINT_BATCH_SIZE || 250) || 250,
  flushMs: pollMs,
  maxMongoBatchesPerCycle:
    Number(process.env.GPSPOINT_MAX_MONGO_BATCHES_PER_CYCLE || 16) || 16,
  maxFilesPerCycle: Number(process.env.GPSPOINT_DRAIN_MAX_FILES_PER_CYCLE || 500) || 500,
  drainOldDocRatio: Number(process.env.GPSPOINT_DRAIN_OLD_DOC_RATIO || 0.5) || 0.5,
  drainNewFilesPerCycle: Number(process.env.GPSPOINT_DRAIN_NEW_FILES_PER_CYCLE || 2) || 2,
  drainOldFilesPerCycle: Number(process.env.GPSPOINT_DRAIN_OLD_FILES_PER_CYCLE || 1) || 1,
  heartbeatMs: Number(process.env.GPSPOINT_WRITER_HEARTBEAT_MS || 2000) || 2000,
  metrics,
  log: console,
});

console.log(
  `[gpspoints-writer] started pid=${process.pid} spool=${writer.getSpoolDir()} pollMs=${pollMs}`
);

const pollTimer = setInterval(() => {
  void writer.flushCycle().catch((err) => {
    console.warn("[gpspoints-writer] flush error:", err.message);
  });
}, pollMs);
if (typeof pollTimer.unref === "function") pollTimer.unref();

// Optional wake hint — correctness does not depend on fs.watch alone.
let watch = null;
try {
  watch = require("fs").watch(writer.getSpoolDir(), { persistent: false }, () => {
    void writer.flushCycle().catch(() => {});
  });
} catch (err) {
  console.warn("[gpspoints-writer] fs.watch unavailable; poll-only mode:", err.message);
}

async function shutdown(signal) {
  console.log(`[gpspoints-writer] ${signal} — draining then exit`);
  clearInterval(pollTimer);
  try {
    watch?.close?.();
  } catch {
    /* ignore */
  }
  try {
    await writer.flushAndStop(Number(process.env.BRIDGE_SHUTDOWN_TIMEOUT_MS || 8000) || 8000);
  } catch (err) {
    console.warn("[gpspoints-writer] shutdown flush error:", err.message);
  }
  process.exit(0);
}

process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("SIGTERM", () => void shutdown("SIGTERM"));
