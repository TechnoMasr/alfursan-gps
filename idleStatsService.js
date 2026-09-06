const { GpsLog, IdleStat } = require('./mongo');

// إعدادات افتراضية
const IDLE_SPEED_KPH = 5;          // حد السرعة للسلانسيه
const IDLE_MINUTES = 5;            // حد زمن السلانسيه
const IDLE_FUEL_LPH = 1;           // استهلاك وقود تقديري باللتر/ساعة (اختياري)
const SCHEDULE_MINUTES = 20;       // نفس الوتيرة الحالية للسكيجولرات الأخرى

/**
 * احسب إحصائيات السلانسيه ليوم محدد (UTC) ثم خزّنها في IdleStat.
 */
async function buildAndPersistIdleStats({
  dayUtc,
  idleSpeedKph = IDLE_SPEED_KPH,
  idleMinutes = IDLE_MINUTES,
  fuelLph = IDLE_FUEL_LPH,
  requireAccOn = true,
  maxGapSeconds = 10 * 60,
}) {
  const start = dayUtc ? new Date(dayUtc) : startOfTodayUTC();
  const end = new Date(start);
  end.setUTCDate(end.getUTCDate() + 1);

  // في Traccar نعتبر الخمول من نقاط positions (gps/alarm) وليس heartbeat
  const types = requireAccOn ? ['gps', 'alarm', 'heartbeat'] : ['gps', 'alarm'];

  const imeis = await GpsLog.distinct('imei', {
    date: { $gte: start, $lt: end },
    type: { $in: types },
  });

  const results = [];

  for (const imei of imeis) {
    const points = await GpsLog.find({
      imei,
      date: { $gte: start, $lt: end },
      type: { $in: types },
    }).sort({ date: 1 }).lean();

    const stats = computeIdle(points, idleSpeedKph, idleMinutes, fuelLph, requireAccOn, maxGapSeconds);
    const idleSeconds = Number(stats.idleSeconds) || 0;
    const idleCount = Number(stats.idleCount) || 0;

    // لا نخزّن سجلات "صفر" لأنها لا تضيف قيمة وتشوّش التقارير
    if (idleSeconds <= 0 && idleCount <= 0) {
      await IdleStat.deleteOne({ imei, day: start });
      continue;
    }

    const doc = {
      imei,
      day: start,
      idle_speed_kph: idleSpeedKph,
      idle_minutes: idleMinutes,
      require_acc_on: requireAccOn,
      max_gap_seconds: maxGapSeconds,
      idle_duration_seconds: idleSeconds,
      idle_count: idleCount,
      first_idle_start: stats.firstStart,
      last_idle_end: stats.lastEnd,
      fuel_waste_liters: stats.fuelWaste,
    };

    await IdleStat.findOneAndUpdate(
      { imei, day: start },
      { $set: doc },
      { upsert: true }
    );

    results.push(doc);
  }

  return results;
}

/**
 * يحسب فترات الخمول (سرعة قليلة/صفر) ويجمعها.
 * - requireAccOn=true: نفس السلوك القديم (ACC ON + speed<=threshold)
 * - requireAccOn=false: يعتمد على speed فقط (مناسب لـ Traccar عندما تريد idle=0 speed)
 * - maxGapSeconds: يمنع احتساب فترات طويلة عند وجود فجوات كبيرة في وصول النقاط
 */
function computeIdle(points, idleSpeedKph, idleMinutes, fuelLph, requireAccOn = true, maxGapSeconds = 10 * 60) {
  const minSeconds = idleMinutes * 60;
  let idleStart = null;
  let lastTs = null;
  let prevTs = null;
  let idleSeconds = 0;
  let idleCount = 0;
  let firstStart = null;
  let lastEnd = null;

  for (let i = 0; i < points.length; i++) {
    const p = points[i];
    const ts = new Date(p.date);
    const accOn = isAccOn(p);
    const speed = Number(p.speed) || 0;
    const isIdle = (requireAccOn ? accOn : true) && speed <= idleSpeedKph;

    // Gap guard: إذا انقطعت النقاط فترة كبيرة، اقفل الفترة عند آخر نقطة بدل تمديدها لساعات
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

  // لو اليوم انتهى ونحن في سلانسيه
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

function isAccOn(p) {
  if (p.acc_status === 'on') return true;
  if (p.acc_status === 'off') return false;
  if (p.accOn === true) return true;
  if (p.accOn === false) return false;
  if (p.statusDecoded && typeof p.statusDecoded.accOn === 'boolean') return p.statusDecoded.accOn;
  if (typeof p.acc === 'boolean') return p.acc; // لبعض الردود النصية
  return false;
}

function startOfTodayUTC() {
  const now = new Date();
  return new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
}

/**
 * حالة خمول لكل IMEI لإرسال إشعار مرة واحدة عند استيفاء الشرط (محرك يعمل + سرعة منخفضة مدة كافية).
 * منفصل عن التقرير الدوري buildAndPersistIdleStats حتى لا نكسر السلوك الحالي.
 */
const idleNotifyStateByImei = new Map();

/**
 * @param {string} imei
 * @param {{ speed: number, accOn: boolean, lat: number|null, lon: number|null, packetDate: Date }} sample
 * @param {{ idleSpeedKph?: number, idleMinutes?: number, requireAccOn?: boolean, onIdleConfirmed?: Function }} opts
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

  // ليس وضع خمول: إعادة ضبط لدورة قادمة
  if (!accOk || !lowSpeed) {
    idleNotifyStateByImei.set(imei, { idleStart: null, notified: false });
    return;
  }

  let st = idleNotifyStateByImei.get(imei) || { idleStart: null, notified: false };

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
      await buildAndPersistIdleStats({ idleSpeedKph, idleMinutes, fuelLph, requireAccOn, maxGapSeconds });
    } catch (err) {
      console.error('Idle stats job error:', err.message);
    }
  }
}

module.exports = {
  startIdleStatsScheduler,
  buildAndPersistIdleStats,
  computeIdle,
  handleIdleNotifySample,
};

