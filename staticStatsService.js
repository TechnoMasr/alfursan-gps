const { GpsPoint, DailyMileage, StaticStat } = require("./mongo");
const { calcDistanceDiffSafe } = require("./gpsJumpGuard");

const STATIC_MILEAGE_THRESHOLD_KM = 0.5;
const SCHEDULE_MINUTES = 20;

function pointDate(point) {
  return point?.packet_date || point?.date;
}

async function calculatePointMileage(imei, dayStart, dayEnd) {
  const points = await GpsPoint.find({
    imei,
    packet_date: { $gte: dayStart, $lt: dayEnd },
  }).sort({ packet_date: 1 }).lean();
  let km = 0;
  for (let i = 1; i < points.length; i++) {
    const prev = points[i - 1];
    const p = points[i];
    const res = calcDistanceDiffSafe(
      { lat: prev.latitude, lon: prev.longitude, date: pointDate(prev) },
      { lat: p.latitude, lon: p.longitude, date: pointDate(p) }
    );
    const prevSpeed = Number(prev.speed) || 0;
    const speed = Number(p.speed) || 0;
    if (!res.isJump && (prevSpeed > 0 || speed > 0)) km += res.distanceKm;
  }
  return km;
}

async function buildAndPersistStaticStats({ dayUtc, mileageThresholdKm = STATIC_MILEAGE_THRESHOLD_KM }) {
  const dayStart = dayUtc ? new Date(dayUtc) : startOfTodayUTC();
  const dayEnd = new Date(dayStart);
  dayEnd.setUTCDate(dayEnd.getUTCDate() + 1);

  const mileageRows = await DailyMileage.find({ day: dayStart }).lean();
  let imeis = mileageRows.map((r) => r.imei).filter(Boolean);
  if (!imeis.length) {
    imeis = await GpsPoint.distinct("imei", { packet_date: { $gte: dayStart, $lt: dayEnd } });
  }

  for (const imei of imeis) {
    let km = mileageRows.find((r) => r.imei === imei)?.km;
    if (km == null) km = await calculatePointMileage(imei, dayStart, dayEnd);
    const isStatic = Number(km || 0) <= mileageThresholdKm;

    await StaticStat.findOneAndUpdate(
      { imei, day: dayStart },
      { $set: { imei, day: dayStart, daily_mileage_km: km || 0, is_static: isStatic } },
      { upsert: true }
    );
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

function startOfTodayUTC() {
  const now = new Date();
  return new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
}

module.exports = {
  startStaticStatsScheduler,
  buildAndPersistStaticStats,
  calculatePointMileage,
};
