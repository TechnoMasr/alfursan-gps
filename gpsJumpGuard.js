function toRad(x) { return x * Math.PI / 180; }

function haversineKm(lat1, lon1, lat2, lon2) {
  const R = 6371;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a = Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) *
    Math.sin(dLon / 2) ** 2;
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

/**
 * Calculate distance diff with GPS jump guard.
 *
 * Problem: device can "teleport" (e.g., shipped while offline). If we blindly compute
 * haversine between last point and new point we will inflate mileage/trips/reports.
 *
 * Heuristics used:
 * - Reject very large relocations after long offline gaps (offline relocation).
 * - Reject points with impossible implied speed.
 *
 * Returns:
 * - distanceKm: the accepted distance (0 when jump)
 * - isJump: boolean
 * - reason: string for debugging/storage
 * - rawKm: raw haversine distance
 * - dtMin / impliedKmh: useful for debugging
 */
function calcDistanceDiffSafe(prev, curr, opts = {}) {
  const {
    maxImpliedKmh = 180,
    offlineGapHours = 6,
    offlineRelocationKm = 50,
  } = opts;

  if (!prev || !curr) return { distanceKm: 0, isJump: false, reason: 'missing_point', rawKm: 0, dtMin: null, impliedKmh: null };

  const lat1 = Number(prev.lat);
  const lon1 = Number(prev.lon);
  const lat2 = Number(curr.lat);
  const lon2 = Number(curr.lon);

  if (!Number.isFinite(lat1) || !Number.isFinite(lon1) || !Number.isFinite(lat2) || !Number.isFinite(lon2)) {
    return { distanceKm: 0, isJump: true, reason: 'invalid_coords', rawKm: 0, dtMin: null, impliedKmh: null };
  }

  const t1 = prev.date ? new Date(prev.date).getTime() : null;
  const t2 = curr.date ? new Date(curr.date).getTime() : null;
  if (!Number.isFinite(t1) || !Number.isFinite(t2) || t2 <= t1) {
    // out-of-order or missing time; safest is to not add distance
    const rawKm = haversineKm(lat1, lon1, lat2, lon2);
    return { distanceKm: 0, isJump: true, reason: 'bad_time', rawKm, dtMin: null, impliedKmh: null };
  }

  const rawKm = haversineKm(lat1, lon1, lat2, lon2);
  const dtMin = (t2 - t1) / 60000;
  const dtHours = dtMin / 60;
  const impliedKmh = dtHours > 0 ? (rawKm / dtHours) : Infinity;

  // Offline relocation: big distance after long offline gap should not be counted as driving mileage
  if (dtHours >= offlineGapHours && rawKm >= offlineRelocationKm) {
    return { distanceKm: 0, isJump: true, reason: 'offline_relocation', rawKm, dtMin, impliedKmh };
  }

  // Impossible implied speed => jump
  if (rawKm >= 2 && impliedKmh > maxImpliedKmh) {
    return { distanceKm: 0, isJump: true, reason: 'implied_speed', rawKm, dtMin, impliedKmh };
  }

  return { distanceKm: rawKm, isJump: false, reason: 'ok', rawKm, dtMin, impliedKmh };
}

module.exports = { calcDistanceDiffSafe };

