/**
 * Local-only gpspoints load against mongodb://127.0.0.1:27017
 * Never uses production URI / mongo.js.
 *
 *   node scripts/local-gpspoints-load.js
 */
const fs = require("fs");
const os = require("os");
const path = require("path");
const mongoose = require("mongoose");
const { createGpsPointWriter } = require("../lib/gpsPointWriter");
const { createBridgeMetrics } = require("../lib/bridgeMetrics");
const { createEventLoopLagMonitor } = require("../lib/eventLoopLag");

const URI = process.env.MONGO_TEST_URI || "mongodb://127.0.0.1:27017/gps_bridge_audit_test";
const SIZES = (process.env.LOAD_SIZES || "10000,50000")
  .split(",")
  .map((n) => Number(n.trim()))
  .filter((n) => n > 0);

async function runSize(coll, size) {
  const spoolDir = fs.mkdtempSync(path.join(os.tmpdir(), `local-load-${size}-`));
  const metrics = createBridgeMetrics();
  const lag = createEventLoopLagMonitor();
  const writer = createGpsPointWriter({
    spoolDir,
    batchSize: 250,
    flushMs: 60_000,
    maxMongoBatchesPerCycle: 10_000,
    drainNewFilesPerCycle: 10_000,
    drainOldFilesPerCycle: 0,
    insertMany: (docs) => coll.insertMany(docs, { ordered: false }),
    metrics,
  });
  const liveLatencies = [];
  const tIngress = Date.now();
  for (let i = 0; i < size; i++) {
    const t0 = Date.now();
    writer.enqueue({
      imei: `local-${i % 50}`,
      traccar_position_id: i,
      latitude: 24 + (i % 10) / 1000,
      longitude: 46,
      packet_date: new Date(),
      date: new Date(),
    });
    liveLatencies.push(Date.now() - t0);
  }
  const ingressMs = Date.now() - tIngress;
  await writer.flushJournal();
  const tMongo = Date.now();
  for (let i = 0; i < 200; i++) {
    if (writer.getSpoolDepth() === 0 && writer.getMemoryDepth() === 0) break;
    await writer.flushCycle();
  }
  await writer.flushAndStop(30_000);
  const mongoMs = Date.now() - tMongo;
  const count = await coll.countDocuments({ imei: /^local-/ });
  const lagSnap = lag.snapshot();
  lag.stop();
  fs.rmSync(spoolDir, { recursive: true, force: true });
  liveLatencies.sort((a, b) => a - b);
  const pct = (p) => liveLatencies[Math.min(liveLatencies.length - 1, Math.ceil((p / 100) * liveLatencies.length) - 1)];
  return {
    size,
    received_total: metrics.gpspoints_received_total,
    durably_spooled_total: metrics.gpspoints_spooled_total,
    mongo_attempted_total: metrics.gpspoints_mongo_attempted_total,
    mongo_acknowledged_total: metrics.gpspoints_mongo_acknowledged_total,
    mongo_count_documents: count,
    lost_total: metrics.persistence_dropped,
    ingress_ms: ingressMs,
    mongo_ack_ms: mongoMs,
    ingress_per_sec: Math.round(size / (ingressMs / 1000)),
    mongo_per_sec: Math.round(size / (mongoMs / 1000)),
    live_enqueue_p50_ms: pct(50),
    live_enqueue_p95_ms: pct(95),
    live_enqueue_p99_ms: pct(99),
    event_loop_lag_p99_ms: lagSnap.event_loop_lag_p99_ms,
    rss_mb: Math.round(process.memoryUsage().rss / 1048576),
    heap_used_mb: Math.round(process.memoryUsage().heapUsed / 1048576),
  };
}

async function main() {
  if (/95\.216\.21\.242/.test(URI)) {
    throw new Error("Refusing production Mongo URI");
  }
  await mongoose.connect(URI, { serverSelectionTimeoutMS: 3000 });
  const coll = mongoose.connection.collection("gpspoints_audit");
  await coll.deleteMany({});
  const results = [];
  for (const size of SIZES) {
    await coll.deleteMany({});
    const row = await runSize(coll, size);
    results.push(row);
    console.log(JSON.stringify(row));
  }
  await coll.deleteMany({});
  await mongoose.connection.db.dropDatabase();
  await mongoose.disconnect();
  console.log(JSON.stringify({ local_mongo_load: results }, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
