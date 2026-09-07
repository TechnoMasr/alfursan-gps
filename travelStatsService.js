/**
 * TravelStat materialization — one GpsPoint read per IMEI-chunk for ALL stop thresholds.
 *
 * Segment semantics unchanged from pre-batch HEAD (computeSegments).
 * Business day: Africa/Cairo via lib/businessDay (TRAVEL_BUSINESS_DAY=utc rollback).
 */
const { calcDistanceDiffSafe } = require("./gpsJumpGuard");
const { resolveMileageDay, startOfTodayUTC } = require("./lib/businessDay");

const DEFAULT_INTERVAL_MIN = 20;
const STOP_COUNT_THRESHOLD_MIN = 3;
/** Full-day tracks denser than incremental mileage — default below mileage's 250. */
const DEFAULT_CHUNK = 150;
const DEFAULT_THRESHOLDS = [1, 3, 5, 10, 15, 30, 60];

function mongoModels() {
  return require("./mongo");
}

function pointDate(point) {
  return point?.packet_date || point?.date;
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

function resolveTravelDay(options = {}) {
  const mode =
    options.businessDay ||
    process.env.TRAVEL_BUSINESS_DAY ||
    process.env.MILEAGE_BUSINESS_DAY ||
    "cairo";
  return resolveMileageDay({
    dayUtc: options.dayUtc,
    now: options.now,
    businessDay: mode,
  });
}

function normalizeThresholds(options = {}) {
  if (Array.isArray(options.stopThresholdsMinutes) && options.stopThresholdsMinutes.length) {
    return options.stopThresholdsMinutes.map(Number).filter((n) => Number.isFinite(n));
  }
  if (options.stopThresholdMin != null) {
    return [Number(options.stopThresholdMin)];
  }
  return DEFAULT_THRESHOLDS.slice();
}

/**
 * CURRENT HEAD segment algorithm — do not change without contract approval.
 *
 * - speed > 0 opens/continues a travel segment
 * - speed == 0 accumulates stop; closes open segment when stopDur >= stopThresholdMin
 * - distance via calcDistanceDiffSafe; counted if !isJump && (prevSpeed>0 || speed>0)
 * - driving_minutes: wall-clock delta between consecutive points when either speed > 0
 * - stopCount uses fixed STOP_COUNT_THRESHOLD_MIN=3 (independent of stopThresholdMin)
 * - open segment at end of day is materialized
 */
function computeSegments(points, stopThresholdMin) {
  const segments = [];
  let seg = null;
  let totalKm = 0;
  let totalDriveMin = 0;
  let zeroStart = null;
  let zeroCounted = false;
  let stopCount = 0;
  let last = null;

  for (const p of points) {
    const t = new Date(pointDate(p));
    const speed = Number(p.speed) || 0;
    const lat = p.latitude;
    const lon = p.longitude;

    if (seg && last) {
      const res = calcDistanceDiffSafe(
        { lat: last.latitude, lon: last.longitude, date: pointDate(last) },
        { lat, lon, date: pointDate(p) }
      );
      const prevSpeed = Number(last.speed) || 0;
      if (!res.isJump && (prevSpeed > 0 || speed > 0)) seg.distance_km += res.distanceKm;
    }

    if (seg && last) {
      const deltaMin = Math.max(0, (t - new Date(pointDate(last))) / 60000);
      const prevSpeed = Number(last.speed) || 0;
      if (prevSpeed > 0 || speed > 0) seg.driving_minutes += deltaMin;
    }

    if (speed > 0) {
      if (zeroStart && !zeroCounted) {
        const stopDur = (t - zeroStart) / 60000;
        if (stopDur >= STOP_COUNT_THRESHOLD_MIN) stopCount += 1;
      }
      zeroStart = null;
      zeroCounted = false;

      if (!seg) {
        seg = {
          start_at: t,
          end_at: t,
          distance_km: 0,
          driving_minutes: 0,
          start_loc: { lat, lon },
          end_loc: { lat, lon },
        };
      }
      seg.end_at = t;
      seg.end_loc = { lat, lon };
    } else {
      if (!zeroStart) zeroStart = t;
      const stopDur = (t - zeroStart) / 60000;
      if (!zeroCounted && stopDur >= STOP_COUNT_THRESHOLD_MIN) {
        stopCount += 1;
        zeroCounted = true;
      }
      if (stopDur >= stopThresholdMin && seg) {
        segments.push(seg);
        totalKm += seg.distance_km;
        totalDriveMin += seg.driving_minutes;
        seg = null;
      }
    }

    last = p;
  }

  if (seg) {
    segments.push(seg);
    totalKm += seg.distance_km;
    totalDriveMin += seg.driving_minutes;
  }

  if (zeroStart && !zeroCounted) {
    const endT = last ? new Date(pointDate(last)) : zeroStart;
    const stopDur = (endT - zeroStart) / 60000;
    if (stopDur >= STOP_COUNT_THRESHOLD_MIN) stopCount += 1;
  }

  return { segments, totalKm, totalDriveMin, stopCount };
}

function buildTravelDoc(imei, dayStart, stopThresholdMin, points) {
  const { segments, totalKm, totalDriveMin, stopCount } = computeSegments(
    points,
    stopThresholdMin
  );
  return {
    imei,
    day: dayStart,
    stop_threshold_min: stopThresholdMin,
    segments,
    total_distance_km: totalKm,
    total_driving_minutes: totalDriveMin,
    total_segments: segments.length,
    total_stop_count: stopCount || 0,
  };
}

/**
 * Build + persist TravelStat for one or many stop thresholds.
 *
 * Single IMEI discovery per run. Per chunk: one GpsPoint query, CPU over all
 * thresholds on the same in-memory points, one TravelStat.bulkWrite.
 */
async function buildAndPersistTravelStats(options = {}) {
  const models = options.GpsPoint && options.TravelStat ? null : mongoModels();
  const GpsPoint = options.GpsPoint || models.GpsPoint;
  const TravelStat = options.TravelStat || models.TravelStat;
  const metrics = options.metrics || {};
  const log = options.log || console;
  const thresholds = normalizeThresholds(options);
  const chunkSize =
    Number(options.chunkSize || process.env.TRAVEL_CHUNK_SIZE || DEFAULT_CHUNK) ||
    DEFAULT_CHUNK;

  const started = Date.now();
  const { dayStart, dayEnd } = resolveTravelDay(options);

  bump(metrics, "analytics_travel_mongo_reads", 1);
  const imeiList = options.imeis?.length
    ? options.imeis
    : await GpsPoint.distinct("imei", {
        packet_date: { $gte: dayStart, $lt: dayEnd },
      });

  bump(metrics, "analytics_travel_thresholds_processed", thresholds.length);

  const results = [];
  const failures = [];
  let maxPointsPerDevice = 0;

  for (const chunk of chunkArray(imeiList, chunkSize)) {
    bump(metrics, "analytics_travel_mongo_reads", 1);
    const points = await GpsPoint.find({
      imei: { $in: chunk },
      packet_date: { $gte: dayStart, $lt: dayEnd },
    })
      .sort({ imei: 1, packet_date: 1 })
      .lean();

    bump(metrics, "analytics_travel_points_processed", points.length);

    const byImei = new Map();
    for (const p of points) {
      const list = byImei.get(p.imei) || [];
      list.push(p);
      byImei.set(p.imei, list);
    }

    const ops = [];
    const chunkDocs = [];

    for (const imei of chunk) {
      try {
        const devicePoints = byImei.get(imei) || [];
        if (devicePoints.length > maxPointsPerDevice) {
          maxPointsPerDevice = devicePoints.length;
        }
        // Seven (or N) CPU passes over the SAME array — Mongo not re-queried.
        for (const stopThresholdMin of thresholds) {
          const doc = buildTravelDoc(imei, dayStart, stopThresholdMin, devicePoints);
          chunkDocs.push(doc);
          ops.push({
            updateOne: {
              filter: {
                imei: doc.imei,
                day: dayStart,
                stop_threshold_min: stopThresholdMin,
              },
              update: {
                $set: {
                  imei: doc.imei,
                  day: dayStart,
                  stop_threshold_min: stopThresholdMin,
                  segments: doc.segments,
                  total_distance_km: doc.total_distance_km,
                  total_driving_minutes: doc.total_driving_minutes,
                  total_segments: doc.total_segments,
                  total_stop_count: doc.total_stop_count,
                },
              },
              upsert: true,
            },
          });
        }
      } catch (err) {
        failures.push({ imei, error: String(err?.message || err) });
        log.warn?.("[travel] device calculation failed", imei, err.message);
      }
    }

    if (ops.length) {
      bump(metrics, "analytics_travel_mongo_writes", 1);
      bump(metrics, "analytics_travel_bulk_writes", 1);
      await TravelStat.bulkWrite(ops, { ordered: false });
      results.push(...chunkDocs);
    }

    bump(metrics, "analytics_travel_chunks_processed", 1);
    bump(metrics, "analytics_travel_devices_processed", chunk.length);
  }

  metrics.analytics_travel_max_points_per_device = Math.max(
    metrics.analytics_travel_max_points_per_device || 0,
    maxPointsPerDevice
  );
  metrics.analytics_travel_duration_ms = Date.now() - started;
  metrics.analytics_travel_last_success_at = new Date().toISOString();

  if (failures.length) {
    const err = new Error(
      `travel partial failure: ${failures.length}/${imeiList.length} devices failed`
    );
    err.failures = failures;
    err.results = results;
    throw err;
  }

  return results;
}

function startTravelStatsScheduler({
  stopThresholdsMinutes = DEFAULT_THRESHOLDS,
  intervalMinutes = DEFAULT_INTERVAL_MIN,
} = {}) {
  runOnce();
  setInterval(runOnce, intervalMinutes * 60 * 1000);

  async function runOnce() {
    try {
      await buildAndPersistTravelStats({ stopThresholdsMinutes });
    } catch (err) {
      console.error("Travel stats job error:", err.message);
    }
  }
}

module.exports = {
  buildAndPersistTravelStats,
  startTravelStatsScheduler,
  computeSegments,
  buildTravelDoc,
  chunkArray,
  normalizeThresholds,
  resolveTravelDay,
  DEFAULT_CHUNK,
  DEFAULT_THRESHOLDS,
  STOP_COUNT_THRESHOLD_MIN,
  startOfTodayUTC,
  pointDate,
};
