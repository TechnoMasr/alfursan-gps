const { GpsPoint, TravelStat } = require("./mongo");
const { calcDistanceDiffSafe } = require("./gpsJumpGuard");

const DEFAULT_INTERVAL_MIN = 20;
const STOP_COUNT_THRESHOLD_MIN = 3;

function pointDate(point) {
  return point?.packet_date || point?.date;
}

async function buildAndPersistTravelStats({ dayUtc, stopThresholdMin = 1, imeis = [] }) {
  const start = dayUtc ? new Date(dayUtc) : startOfTodayUTC();
  const end = new Date(start);
  end.setUTCDate(end.getUTCDate() + 1);

  const imeiList = imeis.length
    ? imeis
    : await GpsPoint.distinct("imei", { packet_date: { $gte: start, $lt: end } });

  const results = [];
  for (const imei of imeiList) {
    const points = await GpsPoint.find({
      imei,
      packet_date: { $gte: start, $lt: end },
    }).sort({ packet_date: 1 }).lean();

    const { segments, totalKm, totalDriveMin, stopCount } = computeSegments(points, stopThresholdMin);
    const doc = {
      imei,
      day: start,
      stop_threshold_min: stopThresholdMin,
      segments,
      total_distance_km: totalKm,
      total_driving_minutes: totalDriveMin,
      total_segments: segments.length,
      total_stop_count: stopCount || 0,
    };

    await TravelStat.findOneAndUpdate(
      { imei, day: start, stop_threshold_min: stopThresholdMin },
      { $set: doc },
      { upsert: true }
    );
    results.push(doc);
  }

  return results;
}

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

function startOfTodayUTC() {
  const now = new Date();
  return new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
}

function startTravelStatsScheduler({
  stopThresholdsMinutes = [1, 2, 3, 4, 5, 10, 15, 20, 25, 30, 45, 60, 360, 720],
  intervalMinutes = DEFAULT_INTERVAL_MIN,
} = {}) {
  runAll(stopThresholdsMinutes);
  setInterval(() => runAll(stopThresholdsMinutes), intervalMinutes * 60 * 1000);

  async function runAll(thresholds) {
    for (const t of thresholds) {
      try {
        await buildAndPersistTravelStats({ stopThresholdMin: t });
      } catch (err) {
        console.error(`Travel stats job error (stop ${t} min):`, err.message);
      }
    }
  }
}

module.exports = {
  buildAndPersistTravelStats,
  startTravelStatsScheduler,
  computeSegments,
};
