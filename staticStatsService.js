/**
 * StaticStat materialization — prefer batched DailyMileage; GpsPoint fallback only when missing.
 *
 * Business rule UNCHANGED: is_static <=> daily_mileage_km <= 0.5
 * Collection/fields/key UNCHANGED for Laravel.
 */
const { sumMileageKm } = require("./lib/mileageCalc");
const { resolveMileageDay, startOfTodayUTC } = require("./lib/businessDay");

const STATIC_MILEAGE_THRESHOLD_KM = 0.5;
const SCHEDULE_MINUTES = 20;
/** DailyMileage docs are light — larger default than Travel/Idle point chunks. */
const DEFAULT_CHUNK = 500;
const FALLBACK_CHUNK = 100;

function mongoModels() {
  return require("./mongo");
}

function bump(metrics, key, by = 1) {
  if (!metrics) return;
  metrics[key] = (metrics[key] || 0) + by;
}

function chunkArray(arr, size) {
  const out = [];
  const n = Math.max(1, Number(size) || DEFAULT_CHUNK);
  for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n));
  return out;
}

function calculatePointMileageForPoints(points) {
  return sumMileageKm(points || []).kmSum;
}

async function calculatePointMileage(imei, dayStart, dayEnd, deps = {}) {
  const GpsPoint = deps.GpsPoint || mongoModels().GpsPoint;
  const points = await GpsPoint.find({
    imei,
    packet_date: { $gte: dayStart, $lt: dayEnd },
  })
    .sort({ packet_date: 1 })
    .lean();
  return calculatePointMileageForPoints(points);
}

async function fetchFallbackKmByImei(imeis, dayStart, dayEnd, deps) {
  const { GpsPoint, metrics } = deps;
  if (!imeis.length) return new Map();
  bump(metrics, "analytics_static_mongo_reads", 1);
  const points = await GpsPoint.find({
    imei: { $in: imeis },
    packet_date: { $gte: dayStart, $lt: dayEnd },
  })
    .sort({ imei: 1, packet_date: 1 })
    .select({ imei: 1, latitude: 1, longitude: 1, speed: 1, packet_date: 1, date: 1 })
    .lean();
  bump(metrics, "analytics_static_points_processed", points.length);

  const byImei = new Map();
  for (const p of points) {
    const list = byImei.get(p.imei) || [];
    list.push(p);
    byImei.set(p.imei, list);
  }
  const kmByImei = new Map();
  for (const imei of imeis) {
    kmByImei.set(imei, calculatePointMileageForPoints(byImei.get(imei) || []));
  }
  return kmByImei;
}

/**
 * Build + persist StaticStat. Output shape identical to pre-batch HEAD.
 */
async function buildAndPersistStaticStats(options = {}) {
  const models =
    options.DailyMileage && options.StaticStat && options.GpsPoint
      ? null
      : mongoModels();
  const DailyMileage = options.DailyMileage || models.DailyMileage;
  const StaticStat = options.StaticStat || models.StaticStat;
  const GpsPoint = options.GpsPoint || models.GpsPoint;
  const metrics = options.metrics || {};
  const log = options.log || console;
  const mileageThresholdKm = options.mileageThresholdKm ?? STATIC_MILEAGE_THRESHOLD_KM;
  const chunkSize =
    Number(options.chunkSize || process.env.STATIC_CHUNK_SIZE || DEFAULT_CHUNK) ||
    DEFAULT_CHUNK;
  const fallbackChunkSize =
    Number(options.fallbackChunkSize || process.env.STATIC_FALLBACK_CHUNK_SIZE || FALLBACK_CHUNK) ||
    FALLBACK_CHUNK;

  const started = Date.now();
  const { dayStart, dayEnd } = resolveMileageDay({
    dayUtc: options.dayUtc,
    businessDay: options.businessDay || process.env.STATIC_BUSINESS_DAY,
    now: options.now,
  });

  bump(metrics, "analytics_static_mongo_reads", 1);
  let mileageRows = await DailyMileage.find({ day: dayStart })
    .select({ imei: 1, km: 1 })
    .lean();
  const mileageByImei = new Map(
    mileageRows.filter((r) => r?.imei).map((r) => [r.imei, r.km])
  );

  let imeis = options.imeis?.length
    ? options.imeis
    : [...mileageByImei.keys()];
  if (!imeis.length) {
    bump(metrics, "analytics_static_mongo_reads", 1);
    imeis = await GpsPoint.distinct("imei", {
      packet_date: { $gte: dayStart, $lt: dayEnd },
    });
  }

  const missing = imeis.filter((imei) => !mileageByImei.has(imei));
  const fallbackKm = new Map();
  for (const chunk of chunkArray(missing, fallbackChunkSize)) {
    const part = await fetchFallbackKmByImei(chunk, dayStart, dayEnd, {
      GpsPoint,
      metrics,
    });
    for (const [k, v] of part) fallbackKm.set(k, v);
    bump(metrics, "analytics_static_gps_fallback_devices", chunk.length);
  }

  bump(metrics, "analytics_static_daily_mileage_hits", imeis.length - missing.length);

  const failures = [];
  for (const chunk of chunkArray(imeis, chunkSize)) {
    const ops = [];
    for (const imei of chunk) {
      try {
        let km = mileageByImei.has(imei) ? mileageByImei.get(imei) : fallbackKm.get(imei);
        if (km == null) km = 0;
        const isStatic = Number(km || 0) <= mileageThresholdKm;
        ops.push({
          updateOne: {
            filter: { imei, day: dayStart },
            update: {
              $set: {
                imei,
                day: dayStart,
                daily_mileage_km: km || 0,
                is_static: isStatic,
              },
            },
            upsert: true,
          },
        });
      } catch (err) {
        failures.push({ imei, error: String(err?.message || err) });
        log.warn?.("[static] device failed", imei, err.message);
      }
    }
    if (ops.length) {
      bump(metrics, "analytics_static_mongo_writes", 1);
      bump(metrics, "analytics_static_bulk_writes", 1);
      await StaticStat.bulkWrite(ops, { ordered: false });
    }
    bump(metrics, "analytics_static_chunks_processed", 1);
    bump(metrics, "analytics_static_devices_processed", chunk.length);
  }

  metrics.analytics_static_duration_ms = Date.now() - started;
  metrics.analytics_static_last_success_at = new Date().toISOString();

  if (failures.length) {
    const err = new Error(
      `static partial failure: ${failures.length}/${imeis.length} devices failed`
    );
    err.failures = failures;
    throw err;
  }
}

function startStaticStatsScheduler({
  mileageThresholdKm = STATIC_MILEAGE_THRESHOLD_KM,
  intervalMinutes = SCHEDULE_MINUTES,
} = {}) {
  runOnce();
  setInterval(runOnce, intervalMinutes * 60 * 1000);

  async function runOnce() {
    try {
      await buildAndPersistStaticStats({ mileageThresholdKm });
    } catch (err) {
      console.error("Static stats job error:", err.message);
    }
  }
}

module.exports = {
  startStaticStatsScheduler,
  buildAndPersistStaticStats,
  calculatePointMileage,
  calculatePointMileageForPoints,
  chunkArray,
  STATIC_MILEAGE_THRESHOLD_KM,
  DEFAULT_CHUNK,
  FALLBACK_CHUNK,
  startOfTodayUTC,
};
