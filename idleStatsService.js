/**
 * IdleStat materialization — chunked GpsPoint reads + bulkWrite.
 *
 * computeIdle / isAccOn / handleIdleNotifySample semantics unchanged from HEAD.
 * Live notify path (handleIdleNotifySample) is separate — do not conflate.
 *
 * Business day: Africa/Cairo (IDLE_BUSINESS_DAY=utc rollback).
 */
const { resolveMileageDay, startOfTodayUTC } = require("./lib/businessDay");

const IDLE_SPEED_KPH = 5;
const IDLE_MINUTES = 5;
const IDLE_FUEL_LPH = 1;
const SCHEDULE_MINUTES = 20;
/** Full-day tracks — same conservative default as Travel. */
const DEFAULT_CHUNK = 150;

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

function resolveIdleDay(options = {}) {
  const mode =
    options.businessDay ||
    process.env.IDLE_BUSINESS_DAY ||
    process.env.MILEAGE_BUSINESS_DAY ||
    "cairo";
  return resolveMileageDay({
    dayUtc: options.dayUtc,
    now: options.now,
    businessDay: mode,
  });
}

/**
 * CURRENT HEAD: unknown/null ignition → false when requireAccOn.
 * Do NOT infer ignition from speed.
 */
function isAccOn(p) {
  if (p.ignition === true) return true;
  if (p.ignition === false) return false;
  if (p.acc_status === "on") return true;
  if (p.acc_status === "off") return false;
  if (p.accOn === true) return true;
  if (p.accOn === false) return false;
  if (p.statusDecoded && typeof p.statusDecoded.accOn === "boolean") {
    return p.statusDecoded.accOn;
  }
  if (typeof p.acc === "boolean") return p.acc;
  return false;
}

/**
 * CURRENT HEAD idle interval algorithm — preserve exactly.
 */
function computeIdle(
  points,
  idleSpeedKph,
  idleMinutes,
  fuelLph,
  requireAccOn = true,
  maxGapSeconds = 10 * 60
) {
  const minSeconds = idleMinutes * 60;
  let idleStart = null;
  let lastTs = null;
  let prevTs = null;
  let idleSeconds = 0;
  let idleCount = 0;
  let firstStart = null;
  let lastEnd = null;

  for (const p of points) {
    const ts = new Date(pointDate(p));
    const accOn = requireAccOn ? isAccOn(p) : true;
    const speed = Number(p.speed) || 0;
    const isIdle = accOn && speed <= idleSpeedKph;

    if (prevTs && idleStart && lastTs) {
      const gapSec = (ts.getTime() - prevTs.getTime()) / 1000;
      if (gapSec > maxGapSeconds) {
        const dur = (lastTs.getTime() - idleStart.getTime()) / 1000;
        if (dur >= minSeconds) {
          idleSeconds += dur;
          idleCount += 1;
          if (!firstStart) firstStart = idleStart;
          lastEnd = lastTs;
        }
        idleStart = null;
        lastTs = null;
      }
    }

    if (isIdle) {
      if (!idleStart) idleStart = ts;
      lastTs = ts;
    } else {
      if (idleStart && lastTs) {
        const dur = (ts - idleStart) / 1000;
        if (dur >= minSeconds) {
          idleSeconds += dur;
          idleCount += 1;
          if (!firstStart) firstStart = idleStart;
          lastEnd = ts;
        }
      }
      idleStart = null;
      lastTs = null;
    }

    prevTs = ts;
  }

  if (idleStart && lastTs) {
    const dur = (lastTs - idleStart) / 1000;
    if (dur >= minSeconds) {
      idleSeconds += dur;
      idleCount += 1;
      if (!firstStart) firstStart = idleStart;
      lastEnd = lastTs;
    }
  }

  const fuelWaste = fuelLph > 0 ? (idleSeconds / 3600) * fuelLph : undefined;
  return { idleSeconds, idleCount, firstStart, lastEnd, fuelWaste };
}

function buildIdleDoc(
  imei,
  dayStart,
  stats,
  { idleSpeedKph, idleMinutes, requireAccOn, maxGapSeconds }
) {
  return {
    imei,
    day: dayStart,
    idle_speed_kph: idleSpeedKph,
    idle_minutes: idleMinutes,
    require_acc_on: requireAccOn,
    max_gap_seconds: maxGapSeconds,
    idle_duration_seconds: Number(stats.idleSeconds) || 0,
    idle_count: Number(stats.idleCount) || 0,
    first_idle_start: stats.firstStart,
    last_idle_end: stats.lastEnd,
    fuel_waste_liters: stats.fuelWaste,
  };
}

/**
 * Chunked IdleStat rebuild. One discovery, one GpsPoint read/chunk, bulkWrite.
 */
async function buildAndPersistIdleStats(options = {}) {
  const models = options.GpsPoint && options.IdleStat ? null : mongoModels();
  const GpsPoint = options.GpsPoint || models.GpsPoint;
  const IdleStat = options.IdleStat || models.IdleStat;
  const metrics = options.metrics || {};
  const log = options.log || console;

  const idleSpeedKph = options.idleSpeedKph ?? IDLE_SPEED_KPH;
  const idleMinutes = options.idleMinutes ?? IDLE_MINUTES;
  const fuelLph = options.fuelLph ?? IDLE_FUEL_LPH;
  const requireAccOn = options.requireAccOn !== undefined ? options.requireAccOn : true;
  const maxGapSeconds = options.maxGapSeconds ?? 10 * 60;
  const chunkSize =
    Number(options.chunkSize || process.env.IDLE_CHUNK_SIZE || DEFAULT_CHUNK) ||
    DEFAULT_CHUNK;

  const started = Date.now();
  const { dayStart, dayEnd } = resolveIdleDay(options);

  bump(metrics, "analytics_idle_mongo_reads", 1);
  const imeis = options.imeis?.length
    ? options.imeis
    : await GpsPoint.distinct("imei", {
        packet_date: { $gte: dayStart, $lt: dayEnd },
      });

  const results = [];
  const failures = [];
  let maxPointsPerDevice = 0;
  let upserted = 0;
  let deleted = 0;

  for (const chunk of chunkArray(imeis, chunkSize)) {
    bump(metrics, "analytics_idle_mongo_reads", 1);
    const points = await GpsPoint.find({
      imei: { $in: chunk },
      packet_date: { $gte: dayStart, $lt: dayEnd },
    })
      .sort({ imei: 1, packet_date: 1 })
      .lean();

    bump(metrics, "analytics_idle_points_processed", points.length);

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

        const stats = computeIdle(
          devicePoints,
          idleSpeedKph,
          idleMinutes,
          fuelLph,
          requireAccOn,
          maxGapSeconds
        );
        const idleSeconds = Number(stats.idleSeconds) || 0;
        const idleCount = Number(stats.idleCount) || 0;

        if (idleSeconds <= 0 && idleCount <= 0) {
          ops.push({
            deleteOne: { filter: { imei, day: dayStart } },
          });
          deleted += 1;
          continue;
        }

        const doc = buildIdleDoc(imei, dayStart, stats, {
          idleSpeedKph,
          idleMinutes,
          requireAccOn,
          maxGapSeconds,
        });
        chunkDocs.push(doc);
        ops.push({
          updateOne: {
            filter: { imei, day: dayStart },
            update: { $set: doc },
            upsert: true,
          },
        });
        upserted += 1;
      } catch (err) {
        failures.push({ imei, error: String(err?.message || err) });
        log.warn?.("[idle] device calculation failed", imei, err.message);
      }
    }

    if (ops.length) {
      bump(metrics, "analytics_idle_mongo_writes", 1);
      bump(metrics, "analytics_idle_bulk_writes", 1);
      await IdleStat.bulkWrite(ops, { ordered: false });
      results.push(...chunkDocs);
    }

    bump(metrics, "analytics_idle_chunks_processed", 1);
    bump(metrics, "analytics_idle_devices_processed", chunk.length);
  }

  metrics.analytics_idle_max_points_per_device = Math.max(
    metrics.analytics_idle_max_points_per_device || 0,
    maxPointsPerDevice
  );
  metrics.analytics_idle_records_upserted =
    (metrics.analytics_idle_records_upserted || 0) + upserted;
  metrics.analytics_idle_records_deleted =
    (metrics.analytics_idle_records_deleted || 0) + deleted;
  metrics.analytics_idle_duration_ms = Date.now() - started;
  metrics.analytics_idle_last_success_at = new Date().toISOString();

  if (failures.length) {
    const err = new Error(
      `idle partial failure: ${failures.length}/${imeis.length} devices failed`
    );
    err.failures = failures;
    err.results = results;
    throw err;
  }

  return results;
}

const idleNotifyStateByImei = new Map();

/**
 * Live idle notification — business path only. Unchanged from HEAD.
 * Not used by scheduled IdleStat materialization.
 */
function handleIdleNotifySample(imei, sample, opts = {}) {
  if (!imei || !sample) return;

  const idleSpeedKph = opts.idleSpeedKph ?? 0;
  const idleMinutes = opts.idleMinutes ?? 5;
  const requireAccOn = opts.requireAccOn !== false;
  const onIdleConfirmed = opts.onIdleConfirmed;

  const speed = Number(sample.speed) || 0;
  const accOk = requireAccOn ? sample.accOn === true : true;
  const t = sample.packetDate instanceof Date ? sample.packetDate : new Date(sample.packetDate);
  const lowSpeed = speed <= idleSpeedKph;

  if (!accOk || !lowSpeed) {
    idleNotifyStateByImei.set(imei, { idleStart: null, notified: false });
    return;
  }

  const st = idleNotifyStateByImei.get(imei) || { idleStart: null, notified: false };
  if (!st.idleStart) {
    idleNotifyStateByImei.set(imei, { idleStart: t, notified: false });
    return;
  }

  const elapsedMin = (t.getTime() - st.idleStart.getTime()) / 60000;
  if (elapsedMin >= idleMinutes && !st.notified && typeof onIdleConfirmed === "function") {
    idleNotifyStateByImei.set(imei, { idleStart: st.idleStart, notified: true });
    setImmediate(async () => {
      try {
        await onIdleConfirmed({
          imei,
          idleStart: st.idleStart,
          packetDate: t,
          lat: sample.lat,
          lon: sample.lon,
          idleMinutes,
        });
      } catch (e) {
        console.error("onIdleConfirmed error:", e.message);
      }
    });
  }
}

function startIdleStatsScheduler({
  idleSpeedKph = IDLE_SPEED_KPH,
  idleMinutes = IDLE_MINUTES,
  fuelLph = IDLE_FUEL_LPH,
  requireAccOn = true,
  maxGapSeconds = 10 * 60,
  intervalMinutes = SCHEDULE_MINUTES,
} = {}) {
  runOnce();
  setInterval(runOnce, intervalMinutes * 60 * 1000);

  async function runOnce() {
    try {
      await buildAndPersistIdleStats({
        idleSpeedKph,
        idleMinutes,
        fuelLph,
        requireAccOn,
        maxGapSeconds,
      });
    } catch (err) {
      console.error("Idle stats job error:", err.message);
    }
  }
}

module.exports = {
  startIdleStatsScheduler,
  buildAndPersistIdleStats,
  computeIdle,
  isAccOn,
  handleIdleNotifySample,
  buildIdleDoc,
  chunkArray,
  resolveIdleDay,
  DEFAULT_CHUNK,
  IDLE_SPEED_KPH,
  IDLE_MINUTES,
  startOfTodayUTC,
  pointDate,
};
