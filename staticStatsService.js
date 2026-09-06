const { GpsLog, DailyMileage, StaticStat } = require('./mongo');

const STATIC_MILEAGE_THRESHOLD_KM = 0.5;
const SCHEDULE_MINUTES = 20;

/**
 * يبني إحصائيات السكون لليوم المحدد (UTC) ويخزّنها في StaticStat.
 */
async function buildAndPersistStaticStats({ dayUtc, mileageThresholdKm = STATIC_MILEAGE_THRESHOLD_KM }) {
  const dayStart = dayUtc ? new Date(dayUtc) : startOfTodayUTC();
  const dayEnd = new Date(dayStart);
  dayEnd.setUTCDate(dayEnd.getUTCDate() + 1);

  // جرّب استخدام DailyMileage أولاً
  const mileageRows = await DailyMileage.find({ day: dayStart }).lean();
  let imeis = mileageRows.map(r => r.imei).filter(Boolean);

  // إن لم تتوفر أميال اليوم، استخرج IMEIs من gpslogs
  if (!imeis.length) {
    imeis = await GpsLog.distinct('imei', {
      date: { $gte: dayStart, $lt: dayEnd },
      type: { $in: ['gps', 'alarm'] },
    });
  }

  for (const imei of imeis) {
    let km = mileageRows.find(r => r.imei === imei)?.km;

    if (km == null) {
      // احسب المسافة من gpslogs عند الحاجة
      const agg = await GpsLog.aggregate([
        {
          $match: {
            imei,
            date: { $gte: dayStart, $lt: dayEnd },
            type: { $in: ['gps', 'alarm'] },
            distanceDiff: { $gte: 0 },
          },
        },
        {
          $group: {
            _id: null,
            kmSum: { $sum: '$distanceDiff' },
          },
        }
      ]);
      km = agg?.[0]?.kmSum || 0;
    }

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
      console.error('Static stats job error:', err.message);
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
};

