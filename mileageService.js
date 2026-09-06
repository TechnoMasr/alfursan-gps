const { GpsPoint, DeviceStatus, DailyMileage, AccEvent, OverspeedAlert } = require("./mongo");
const { calcDistanceDiffSafe } = require("./gpsJumpGuard");

const SCHEDULE_MINUTES = 20;
const OVERSPEED_LIMIT_KMH = 120;
const KM_TO_MILES = 0.621371;
const STOP_COUNT_THRESHOLD_MIN = 3;

function pointDate(point) {
  return point?.packet_date || point?.date;
}

async function updateIncrementalMileage() {
  const imeisFromStatus = await DeviceStatus.distinct("imei");
  const imeisFromPoints = await GpsPoint.distinct("imei");
  const imeis = Array.from(new Set([...imeisFromStatus, ...imeisFromPoints])).filter(Boolean);

  for (const imei of imeis) {
    const status = await DeviceStatus.findOne({ imei });
    const lastAt = status?.last_mileage_at || new Date(0);
    const points = await GpsPoint.find({ imei, packet_date: { $gt: lastAt } })
      .sort({ packet_date: 1 })
      .select({ latitude: 1, longitude: 1, speed: 1, packet_date: 1, date: 1 })
      .lean();
    if (!points.length) continue;
    const prevPoint = await GpsPoint.findOne({ imei, packet_date: { $lte: lastAt } })
      .sort({ packet_date: -1 })
      .select({ latitude: 1, longitude: 1, speed: 1, packet_date: 1, date: 1 })
      .lean();
    const calcPoints = prevPoint ? [prevPoint, ...points] : points;
    if (calcPoints.length < 2) continue;

    let kmSum = 0;
    let maxDate = lastAt;
    for (let i = 1; i < calcPoints.length; i++) {
      const prev = calcPoints[i - 1];
      const p = calcPoints[i];
      const res = calcDistanceDiffSafe(
        { lat: prev.latitude, lon: prev.longitude, date: pointDate(prev) },
        { lat: p.latitude, lon: p.longitude, date: pointDate(p) }
      );
      const prevSpeed = Number(prev.speed) || 0;
      const speed = Number(p.speed) || 0;
      if (!res.isJump && (prevSpeed > 0 || speed > 0)) kmSum += res.distanceKm;
      maxDate = pointDate(p) || maxDate;
    }
    if (kmSum <= 0) continue;

    await DeviceStatus.findOneAndUpdate(
      { imei },
      {
        $set: { last_mileage_at: maxDate },
        $inc: { km_total: kmSum, miles_total: kmSum * KM_TO_MILES },
      },
      { upsert: true }
    );
  }
}

async function buildDailyMileageReport(dayUtc) {
  const start = dayUtc ? new Date(dayUtc) : startOfTodayUTC();
  const end = new Date(start);
  end.setUTCDate(end.getUTCDate() + 1);

  const imeis = await GpsPoint.distinct("imei", {
    packet_date: { $gte: start, $lt: end },
  });

  const rows = [];
  for (const imei of imeis) {
    const points = await GpsPoint.find({
      imei,
      packet_date: { $gte: start, $lt: end },
    }).sort({ packet_date: 1 }).lean();

    let km = 0;
    let overspeedCount = 0;
    let stopMinutes = 0;
    let stopCount = 0;
    let zeroStart = null;
    let zeroCounted = false;

    for (let i = 0; i < points.length; i++) {
      const p = points[i];
      const currentAt = new Date(pointDate(p));
      const speed = Number(p.speed) || 0;

      if (i > 0) {
        const prev = points[i - 1];
        const res = calcDistanceDiffSafe(
          { lat: prev.latitude, lon: prev.longitude, date: pointDate(prev) },
          { lat: p.latitude, lon: p.longitude, date: pointDate(p) }
        );
        const prevSpeed = Number(prev.speed) || 0;
        if (!res.isJump && (prevSpeed > 0 || speed > 0)) km += res.distanceKm;
      }

      if (speed > OVERSPEED_LIMIT_KMH) overspeedCount += 1;

      if (speed === 0) {
        if (!zeroStart) zeroStart = currentAt;
        const nextTime = i < points.length - 1 ? new Date(pointDate(points[i + 1])) : zeroStart;
        stopMinutes += Math.max(0, (nextTime - currentAt) / 60000);
        const stopDur = (currentAt - zeroStart) / 60000;
        if (!zeroCounted && stopDur >= STOP_COUNT_THRESHOLD_MIN) {
          stopCount += 1;
          zeroCounted = true;
        }
      } else {
        if (zeroStart) {
          const stopDur = (currentAt - zeroStart) / 60000;
          if (!zeroCounted && stopDur >= STOP_COUNT_THRESHOLD_MIN) stopCount += 1;
        }
        zeroStart = null;
        zeroCounted = false;
      }
    }

    if (zeroStart && !zeroCounted) stopCount += 1;

    const [persistedOverspeedCount, accOnCount, accOffCount] = await Promise.all([
      OverspeedAlert.countDocuments({ imei, start_time: { $gte: start, $lt: end } }),
      AccEvent.countDocuments({ imei, acc_status: "on", start_time: { $gte: start, $lt: end } }),
      AccEvent.countDocuments({ imei, acc_status: "off", start_time: { $gte: start, $lt: end } }),
    ]);

    rows.push({
      imei,
      date: start.toISOString().slice(0, 10),
      miles: km * KM_TO_MILES,
      overspeed_count: Math.max(overspeedCount, persistedOverspeedCount),
      total_stop_minutes: stopMinutes,
      total_stop_count: stopCount,
      acc_on_count: accOnCount,
      acc_off_count: accOffCount,
    });
  }

  return rows;
}

async function buildAndPersistDailyReport(dayUtc) {
  const rows = await buildDailyMileageReport(dayUtc);
  const dayStart = dayUtc ? new Date(dayUtc) : startOfTodayUTC();
  for (const row of rows) {
    await DailyMileage.findOneAndUpdate(
      { imei: row.imei, day: dayStart },
      {
        $set: {
          km: row.miles / KM_TO_MILES,
          miles: row.miles,
          overspeed_count: row.overspeed_count,
          total_stop_minutes: row.total_stop_minutes,
          total_stop_count: row.total_stop_count ?? 0,
          acc_on_count: row.acc_on_count,
          acc_off_count: row.acc_off_count,
        },
      },
      { upsert: true }
    );
  }
  return rows;
}

function startOfTodayUTC() {
  const now = new Date();
  return new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
}

function startMileageScheduler() {
  updateIncrementalMileage().catch((err) => console.error("Mileage job error:", err.message));
  buildAndPersistDailyReport().catch((err) => console.error("Daily mileage report error:", err.message));
  setInterval(() => {
    updateIncrementalMileage().catch((err) => console.error("Mileage job error:", err.message));
    buildAndPersistDailyReport().catch((err) => console.error("Daily mileage report error:", err.message));
  }, SCHEDULE_MINUTES * 60 * 1000);
}

module.exports = {
  startMileageScheduler,
  updateIncrementalMileage,
  buildDailyMileageReport,
  buildAndPersistDailyReport,
};
