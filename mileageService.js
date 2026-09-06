const mongoose = require('mongoose');
const { GpsLog, DeviceStatus, DailyMileage } = require('./mongo');

const SCHEDULE_MINUTES = 20; // سيتم زيادتها لاحقاً إلى 3 ساعات
const OVERSPEED_LIMIT_KMH = 120; // حد السرعة الافتراضي لاحتساب التجاوزات
const KM_TO_MILES = 0.621371;
const STOP_COUNT_THRESHOLD_MIN = 3; // حساب عدد التوقفات عند بلوغ 3 دقائق سرعة صفر
const { calcDistanceDiffSafe } = require('./gpsJumpGuard');

/**
 * يجمع المسافة التراكمية لكل IMEI بدون إعادة الحساب
 * يعتمد على distanceDiff (كم) ويحوّلها إلى أميال.
 */
async function updateIncrementalMileage() {
  const imeisFromStatus = await DeviceStatus.distinct('imei');
  const imeisFromLogs = await GpsLog.distinct('imei');
  const imeis = Array.from(new Set([...imeisFromStatus, ...imeisFromLogs])).filter(Boolean);

  for (const imei of imeis) {
    const status = await DeviceStatus.findOne({ imei });
    const lastAt = status?.last_mileage_at || new Date(0);

    const agg = await GpsLog.aggregate([
      {
        $match: {
          imei,
          type: { $in: ['gps', 'alarm'] },
          distanceDiff: { $gt: 0 },
          jump_detected: { $ne: true }, // لا تجمع القفزات القديمة/المعلّمة
          date: { $gt: lastAt },
        }
      },
      {
        $group: {
          _id: null,
          kmSum: { $sum: '$distanceDiff' },
          maxDate: { $max: '$date' },
        }
      }
    ]);

    if (!agg.length) continue;

    const kmSum = agg[0].kmSum || 0;
    const maxDate = agg[0].maxDate || lastAt;
    if (kmSum <= 0) continue;

    const milesToAdd = kmSum * KM_TO_MILES;

    await DeviceStatus.findOneAndUpdate(
      { imei },
      {
        $set: { last_mileage_at: maxDate },
        $inc: { km_total: kmSum, miles_total: milesToAdd },
      },
      { upsert: true }
    );
  }
}

/**
 * تقرير يومي: لكل IMEI يحسب أميال اليوم، عدد التجاوزات، ومدة التوقف.
 * stopMinutes محسوبة تقريبياً من الفارق بين النقاط المتتالية ذات السرعة 0.
 */
async function buildDailyMileageReport(dayUtc) {
  const start = dayUtc ? new Date(dayUtc) : startOfTodayUTC();
  const end = new Date(start);
  end.setUTCDate(end.getUTCDate() + 1);

  const imeis = await GpsLog.distinct('imei', {
    date: { $gte: start, $lt: end },
    type: { $in: ['gps', 'alarm'] },
  });

  const rows = [];
  for (const imei of imeis) {
    const points = await GpsLog.find({
      imei,
      date: { $gte: start, $lt: end },
      type: { $in: ['gps', 'alarm'] },
    }).sort({ date: 1 }).lean();

    let km = 0;
    let overspeedCount = 0;
    let stopMinutes = 0;
    let stopCount = 0;
    let accOnCount = 0;
    let accOffCount = 0;
    let zeroStart = null;
    let zeroCounted = false;

    for (let i = 0; i < points.length; i++) {
      const p = points[i];
      const speed = Number(p.speed) || 0;
      // ⚠️ لا تعتمد على distanceDiff المخزّن فقط (قد يحتوي قفزات قديمة).
      // نحسب فرق المسافة بشكل آمن بين النقطتين المتتاليتين.
      if (i > 0) {
        const prev = points[i - 1];
        const res = calcDistanceDiffSafe(
          { lat: prev.latitude ?? prev.gps?.latitude, lon: prev.longitude ?? prev.gps?.longitude, date: prev.date },
          { lat: p.latitude ?? p.gps?.latitude, lon: p.longitude ?? p.gps?.longitude, date: p.date }
        );
        const prevSpeed = Number(prev.speed) || 0;
        // Driving mileage: نضيف فقط عندما يوجد حركة (speed>0) في أحد النقطتين
        if (!res.isJump && (prevSpeed > 0 || speed > 0)) {
          km += res.distanceKm;
        }
      }

      const isOverspeed = speed > OVERSPEED_LIMIT_KMH
        || (p.type === 'alarm' && (p.alarmType === 6 || /overspeed/i.test(p.alarmText || '')));
      if (isOverspeed) overspeedCount += 1;

      // عدّ ACC On/Off (من acc_status أو accOn)
      const accStatus = p.acc_status || (p.accOn === true ? 'on' : p.accOn === false ? 'off' : undefined);
      if (accStatus === 'on') accOnCount += 1;
      if (accStatus === 'off') accOffCount += 1;

      if (speed === 0) {
        if (!zeroStart) zeroStart = new Date(p.date);
        const nextTime = i < points.length - 1 ? new Date(points[i + 1].date) : zeroStart;
        const diffMin = Math.max(0, (nextTime - new Date(p.date)) / 60000);
        stopMinutes += diffMin;

        const stopDur = (new Date(p.date) - zeroStart) / 60000;
        if (!zeroCounted && stopDur >= STOP_COUNT_THRESHOLD_MIN) {
          stopCount += 1;
          zeroCounted = true;
        }
      } else {
        // سرعة > 0: أغلق التوقف السابق
        if (zeroStart) {
          const stopDur = (new Date(p.date) - zeroStart) / 60000;
          if (!zeroCounted && stopDur >= STOP_COUNT_THRESHOLD_MIN) {
            stopCount += 1;
          }
        }
        zeroStart = null;
        zeroCounted = false;
      }
    }

    // لو انتهى اليوم على توقف غير محسوب
    if (zeroStart) {
      if (!zeroCounted) stopCount += 1;
    }

    rows.push({
      imei,
      date: start.toISOString().slice(0, 10),
      miles: km * KM_TO_MILES,
      overspeed_count: overspeedCount,
      total_stop_minutes: stopMinutes,
      total_stop_count: stopCount,
      acc_on_count: accOnCount,
      acc_off_count: accOffCount,
    });


    
  }

  return rows;
}

/**
 * يبني التقرير اليومي ويخزّنه في DailyMileage (upsert per imei/day).
 */
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
  // شغّل مرة عند الإقلاع
  updateIncrementalMileage().catch(err => console.error('Mileage job error:', err.message));
  buildAndPersistDailyReport().catch(err => console.error('Daily mileage report error:', err.message));
  // جدولة دورية
  setInterval(() => {
    updateIncrementalMileage().catch(err => console.error('Mileage job error:', err.message));
    buildAndPersistDailyReport().catch(err => console.error('Daily mileage report error:', err.message));
  }, SCHEDULE_MINUTES * 60 * 1000);
}

module.exports = {
  startMileageScheduler,
  updateIncrementalMileage,
  buildDailyMileageReport,
  buildAndPersistDailyReport,
};

