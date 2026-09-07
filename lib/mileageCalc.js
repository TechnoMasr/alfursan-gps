/**
 * Pure mileage segment math — shared by incremental + daily (and tests).
 * Semantics match CURRENT HEAD mileageService (jump guard + speed edge).
 */
const { calcDistanceDiffSafe } = require("../gpsJumpGuard");

function pointDate(point) {
  return point?.packet_date || point?.date;
}

/**
 * Sum km over ordered points. Optional leading prevPoint (boundary before window).
 * Distance added when !isJump && (prevSpeed > 0 || speed > 0).
 */
function sumMileageKm(points, { prevPoint = null } = {}) {
  const calcPoints = prevPoint ? [prevPoint, ...points] : points;
  if (!calcPoints || calcPoints.length < 2) {
    return { kmSum: 0, maxDate: null, pairs: 0 };
  }
  let kmSum = 0;
  let maxDate = null;
  let pairs = 0;
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
    pairs += 1;
  }
  return { kmSum, maxDate, pairs };
}

const STOP_COUNT_THRESHOLD_MIN = 3;

/**
 * Daily metrics from an ordered day window of points (no prior boundary).
 * Overspeed from GpsPoint speed>120 is LEGACY only — callers decide whether to use it.
 */
function computeDailyFromPoints(points, { overspeedLimitKmh = 120 } = {}) {
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

    if (speed > overspeedLimitKmh) overspeedCount += 1;

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

  return { km, overspeedCount, stopMinutes, stopCount };
}

module.exports = {
  pointDate,
  sumMileageKm,
  computeDailyFromPoints,
  STOP_COUNT_THRESHOLD_MIN,
};
