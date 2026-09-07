/**
 * Mileage materialization — batched Mongo access (chunk-scale, not per-device N+1).
 *
 * Semantics preserved from pre-batch HEAD for distance / cursor / jump / speed edge.
 * Daily overspeed: TARGET = OverspeedAlert only (see REPORTING-DATA-CONTRACT).
 * Daily day key: Africa/Cairo → UTC range (MILEAGE_BUSINESS_DAY=utc for legacy keys).
 */
const { sumMileageKm, computeDailyFromPoints } = require("./lib/mileageCalc");
const { resolveMileageDay, startOfTodayUTC } = require("./lib/businessDay");

const SCHEDULE_MINUTES = 20;
const KM_TO_MILES = 0.621371;
const DEFAULT_CHUNK = 250;
const EPOCH = new Date(0);

function mongoModels() {
  return require("./mongo");
}

function chunkArray(arr, size) {
  const out = [];
  const n = Math.max(1, Number(size) || DEFAULT_CHUNK);
  for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n));
  return out;
}

function bump(metrics, key, by = 1) {
  if (!metrics) return;
  metrics[key] = (metrics[key] || 0) + by;
}

function resolveOverspeedSource(raw) {
  const v = String(raw || process.env.MILEAGE_OVERSPEED_SOURCE || "alerts")
    .trim()
    .toLowerCase();
  if (v === "legacy_max" || v === "legacy") return "legacy_max";
  return "alerts";
}

async function listMileageImeis(deps) {
  const { DeviceStatus: DS, GpsPoint: GP } = deps;
  bump(deps.metrics, "analytics_mileage_mongo_reads", 2);
  const [fromStatus, fromPoints] = await Promise.all([
    DS.distinct("imei"),
    GP.distinct("imei"),
  ]);
  return Array.from(new Set([...fromStatus, ...fromPoints])).filter(Boolean);
}

/**
 * One prior point per IMEI: latest with packet_date <= lastAt.
 * Single aggregation per chunk (not per device).
 */
async function fetchPriorPointsByImei(imeiLastAtPairs, deps) {
  const need = imeiLastAtPairs.filter(
    (x) => x.lastAt && x.lastAt.getTime() > EPOCH.getTime()
  );
  if (!need.length) return new Map();
  const { GpsPoint: GP, metrics } = deps;
  bump(metrics, "analytics_mileage_mongo_reads", 1);
  const rows = await GP.aggregate([
    {
      $match: {
        $or: need.map(({ imei, lastAt }) => ({
          imei,
          packet_date: { $lte: lastAt },
        })),
      },
    },
    { $sort: { imei: 1, packet_date: -1 } },
    {
      $group: {
        _id: "$imei",
        latitude: { $first: "$latitude" },
        longitude: { $first: "$longitude" },
        speed: { $first: "$speed" },
        packet_date: { $first: "$packet_date" },
        date: { $first: "$date" },
      },
    },
  ]);
  const map = new Map();
  for (const r of rows) {
    map.set(r._id, {
      latitude: r.latitude,
      longitude: r.longitude,
      speed: r.speed,
      packet_date: r.packet_date,
      date: r.date,
    });
  }
  return map;
}

/**
 * New points since each IMEI cursor — one find with $or, then group in memory.
 */
async function fetchNewPointsByImei(imeiLastAtPairs, deps) {
  if (!imeiLastAtPairs.length) return new Map();
  const { GpsPoint: GP, metrics } = deps;
  bump(metrics, "analytics_mileage_mongo_reads", 1);
  const points = await GP.find({
    $or: imeiLastAtPairs.map(({ imei, lastAt }) => ({
      imei,
      packet_date: { $gt: lastAt },
    })),
  })
    .sort({ imei: 1, packet_date: 1 })
    .select({ latitude: 1, longitude: 1, speed: 1, packet_date: 1, date: 1, imei: 1 })
    .lean();
  const byImei = new Map();
  for (const p of points) {
    const list = byImei.get(p.imei) || [];
    list.push(p);
    byImei.set(p.imei, list);
  }
  bump(metrics, "analytics_mileage_points_processed", points.length);
  return byImei;
}

function computeIncrementalUpdates(imeis, statusByImei, pointsByImei, priorByImei) {
  const { sumMileageKm: sumKm, pointDate: pDate } = require("./lib/mileageCalc");
  const updates = [];
  for (const imei of imeis) {
    try {
      const status = statusByImei.get(imei);
      const lastAt = status?.last_mileage_at ? new Date(status.last_mileage_at) : EPOCH;
      const points = pointsByImei.get(imei) || [];
      if (!points.length) continue;
      const prevPoint = priorByImei.get(imei) || null;
      const { kmSum, maxDate } = sumKm(points, { prevPoint });
      if (kmSum <= 0) continue;
      updates.push({
        imei,
        kmSum,
        maxDate: maxDate || pDate(points[points.length - 1]) || lastAt,
      });
    } catch (err) {
      // Isolate per-IMEI failures — do not abort the chunk.
      updates.push({ imei, error: err });
    }
  }
  return updates;
}

async function bulkUpdateDeviceMileage(updates, deps) {
  const ok = updates.filter((u) => u && !u.error && u.kmSum > 0);
  if (!ok.length) return 0;
  const { DeviceStatus: DS, metrics } = deps;
  bump(metrics, "analytics_mileage_mongo_writes", 1);
  bump(metrics, "analytics_mileage_bulk_writes", 1);
  const ops = ok.map((u) => ({
    updateOne: {
      filter: { imei: u.imei },
      update: {
        $set: { last_mileage_at: u.maxDate },
        $inc: { km_total: u.kmSum, miles_total: u.kmSum * KM_TO_MILES },
      },
      upsert: true,
    },
  }));
  await DS.bulkWrite(ops, { ordered: false });
  return ok.length;
}

async function processIncrementalChunk(imeis, deps) {
  const { DeviceStatus: DS, metrics } = deps;
  bump(metrics, "analytics_mileage_mongo_reads", 1);
  const statuses = await DS.find({ imei: { $in: imeis } })
    .select({ imei: 1, last_mileage_at: 1, km_total: 1 })
    .lean();
  const statusByImei = new Map(statuses.map((s) => [s.imei, s]));
  const pairs = imeis.map((imei) => {
    const lastAt = statusByImei.get(imei)?.last_mileage_at
      ? new Date(statusByImei.get(imei).last_mileage_at)
      : EPOCH;
    return { imei, lastAt };
  });

  const pointsByImei = await fetchNewPointsByImei(pairs, deps);
  const activePairs = pairs.filter((p) => (pointsByImei.get(p.imei) || []).length > 0);
  const priorByImei = await fetchPriorPointsByImei(activePairs, deps);
  const updates = computeIncrementalUpdates(
    imeis,
    statusByImei,
    pointsByImei,
    priorByImei
  );
  const errors = updates.filter((u) => u.error);
  for (const e of errors) {
    deps.log?.warn?.("[mileage] incremental device error", e.imei, e.error?.message);
  }
  const written = await bulkUpdateDeviceMileage(updates, deps);
  bump(metrics, "analytics_mileage_devices_processed", imeis.length);
  bump(metrics, "analytics_mileage_chunks_processed", 1);
  return { written, errors: errors.length };
}

/**
 * Incremental fleet mileage. Mongo ops scale with chunks, not devices.
 */
async function updateIncrementalMileage(options = {}) {
  const models = options.GpsPoint ? null : mongoModels();
  const deps = {
    GpsPoint: options.GpsPoint || models.GpsPoint,
    DeviceStatus: options.DeviceStatus || models.DeviceStatus,
    metrics: options.metrics || {},
    log: options.log || console,
  };
  const chunkSize =
    Number(options.chunkSize || process.env.MILEAGE_CHUNK_SIZE || DEFAULT_CHUNK) ||
    DEFAULT_CHUNK;
  const started = Date.now();
  const imeis = options.imeis || (await listMileageImeis(deps));
  const chunks = chunkArray(imeis, chunkSize);
  let written = 0;
  for (const chunk of chunks) {
    const r = await processIncrementalChunk(chunk, deps);
    written += r.written;
  }
  deps.metrics.analytics_mileage_duration_ms = Date.now() - started;
  deps.metrics.analytics_mileage_last_success_at = new Date().toISOString();
  return { imeis: imeis.length, chunks: chunks.length, written, chunkSize };
}

async function fetchDayPointsByImei(imeis, dayStart, dayEnd, deps) {
  if (!imeis.length) return new Map();
  const { GpsPoint: GP, metrics } = deps;
  bump(metrics, "analytics_mileage_mongo_reads", 1);
  const points = await GP.find({
    imei: { $in: imeis },
    packet_date: { $gte: dayStart, $lt: dayEnd },
  })
    .sort({ imei: 1, packet_date: 1 })
    .lean();
  const byImei = new Map();
  for (const p of points) {
    const list = byImei.get(p.imei) || [];
    list.push(p);
    byImei.set(p.imei, list);
  }
  bump(metrics, "analytics_mileage_points_processed", points.length);
  return byImei;
}

async function fetchAccCountsByImei(imeis, dayStart, dayEnd, deps) {
  if (!imeis.length) return new Map();
  const { AccEvent: AE, metrics } = deps;
  bump(metrics, "analytics_mileage_mongo_reads", 1);
  const rows = await AE.aggregate([
    {
      $match: {
        imei: { $in: imeis },
        start_time: { $gte: dayStart, $lt: dayEnd },
      },
    },
    {
      $group: {
        _id: { imei: "$imei", acc_status: "$acc_status" },
        n: { $sum: 1 },
      },
    },
  ]);
  const map = new Map();
  for (const r of rows) {
    const imei = r._id.imei;
    const cur = map.get(imei) || { acc_on_count: 0, acc_off_count: 0 };
    if (r._id.acc_status === "on") cur.acc_on_count = r.n;
    if (r._id.acc_status === "off") cur.acc_off_count = r.n;
    map.set(imei, cur);
  }
  return map;
}

async function fetchOverspeedAlertCountsByImei(imeis, dayStart, dayEnd, deps) {
  if (!imeis.length) return new Map();
  const { OverspeedAlert: OA, metrics } = deps;
  bump(metrics, "analytics_mileage_mongo_reads", 1);
  const rows = await OA.aggregate([
    {
      $match: {
        imei: { $in: imeis },
        start_time: { $gte: dayStart, $lt: dayEnd },
      },
    },
    { $group: { _id: "$imei", n: { $sum: 1 } } },
  ]);
  return new Map(rows.map((r) => [r._id, r.n]));
}

async function buildDailyMileageReport(dayUtc, options = {}) {
  const models = options.GpsPoint ? null : mongoModels();
  const deps = {
    GpsPoint: options.GpsPoint || models.GpsPoint,
    AccEvent: options.AccEvent || models.AccEvent,
    OverspeedAlert: options.OverspeedAlert || models.OverspeedAlert,
    metrics: options.metrics || {},
    log: options.log || console,
  };
  const { dayStart, dayEnd, ymd } = resolveMileageDay({
    dayUtc,
    businessDay: options.businessDay,
    now: options.now,
  });
  const overspeedSource = resolveOverspeedSource(options.overspeedSource);
  const chunkSize =
    Number(options.chunkSize || process.env.MILEAGE_CHUNK_SIZE || DEFAULT_CHUNK) ||
    DEFAULT_CHUNK;

  bump(deps.metrics, "analytics_mileage_mongo_reads", 1);
  const imeis =
    options.imeis ||
    (await deps.GpsPoint.distinct("imei", {
      packet_date: { $gte: dayStart, $lt: dayEnd },
    }));

  const rows = [];
  for (const chunk of chunkArray(imeis, chunkSize)) {
    const pointsByImei = await fetchDayPointsByImei(chunk, dayStart, dayEnd, deps);
    const accByImei = await fetchAccCountsByImei(chunk, dayStart, dayEnd, deps);
    const alertByImei = await fetchOverspeedAlertCountsByImei(
      chunk,
      dayStart,
      dayEnd,
      deps
    );

    for (const imei of chunk) {
      try {
        const points = pointsByImei.get(imei) || [];
        const daily = computeDailyFromPoints(points);
        const alertCount = alertByImei.get(imei) || 0;
        let overspeed_count = alertCount;
        if (overspeedSource === "legacy_max") {
          overspeed_count = Math.max(daily.overspeedCount, alertCount);
        }
        const acc = accByImei.get(imei) || { acc_on_count: 0, acc_off_count: 0 };
        rows.push({
          imei,
          date: ymd || dayStart.toISOString().slice(0, 10),
          miles: daily.km * KM_TO_MILES,
          overspeed_count,
          total_stop_minutes: daily.stopMinutes,
          total_stop_count: daily.stopCount,
          acc_on_count: acc.acc_on_count,
          acc_off_count: acc.acc_off_count,
          _km: daily.km,
          _legacy_gps_overspeed: daily.overspeedCount,
        });
      } catch (err) {
        deps.log?.warn?.("[mileage] daily device error", imei, err.message);
      }
    }
    bump(deps.metrics, "analytics_mileage_chunks_processed", 1);
    bump(deps.metrics, "analytics_mileage_devices_processed", chunk.length);
  }

  return {
    rows,
    dayStart,
    dayEnd,
    overspeedSource,
    ymd,
  };
}

async function buildAndPersistDailyReport(dayUtc, options = {}) {
  const models = options.DailyMileage ? null : mongoModels();
  const deps = {
    ...options,
    DailyMileage: options.DailyMileage || models.DailyMileage,
    metrics: options.metrics || {},
  };
  const { rows, dayStart } = await buildDailyMileageReport(dayUtc, options);

  const chunkSize =
    Number(options.chunkSize || process.env.MILEAGE_CHUNK_SIZE || DEFAULT_CHUNK) ||
    DEFAULT_CHUNK;
  for (const chunk of chunkArray(rows, chunkSize)) {
    if (!chunk.length) continue;
    bump(deps.metrics, "analytics_mileage_mongo_writes", 1);
    bump(deps.metrics, "analytics_mileage_bulk_writes", 1);
    const ops = chunk.map((row) => ({
      updateOne: {
        filter: { imei: row.imei, day: dayStart },
        update: {
          $set: {
            km: row._km != null ? row._km : row.miles / KM_TO_MILES,
            miles: row.miles,
            overspeed_count: row.overspeed_count,
            total_stop_minutes: row.total_stop_minutes,
            total_stop_count: row.total_stop_count ?? 0,
            acc_on_count: row.acc_on_count,
            acc_off_count: row.acc_off_count,
          },
        },
        upsert: true,
      },
    }));
    await deps.DailyMileage.bulkWrite(ops, { ordered: false });
  }
  deps.metrics.analytics_mileage_last_success_at = new Date().toISOString();
  // Preserve prior call-site expectation: array of row objects
  return rows;
}

function startMileageScheduler() {
  updateIncrementalMileage().catch((err) => console.error("Mileage job error:", err.message));
  buildAndPersistDailyReport().catch((err) =>
    console.error("Daily mileage report error:", err.message)
  );
  setInterval(() => {
    updateIncrementalMileage().catch((err) => console.error("Mileage job error:", err.message));
    buildAndPersistDailyReport().catch((err) =>
      console.error("Daily mileage report error:", err.message)
    );
  }, SCHEDULE_MINUTES * 60 * 1000);
}

module.exports = {
  startMileageScheduler,
  updateIncrementalMileage,
  buildDailyMileageReport,
  buildAndPersistDailyReport,
  chunkArray,
  sumMileageKm,
  computeDailyFromPoints,
  resolveOverspeedSource,
  KM_TO_MILES,
  DEFAULT_CHUNK,
  startOfTodayUTC,
};
