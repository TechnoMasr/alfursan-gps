/**
 * Trip runtime state recovery after process restart.
 *
 * Preserves applyTripLogic segmentation rules (speed>0, BASE_GAP_MIN).
 * Reconstructs enough memory so an open Trip does not duplicate on first packet.
 *
 * Tradeoff when DeviceStatus.last_speed is missing:
 *   assume prevSpeed=0 if an open Trip exists (enables correct close-on-resume),
 *   which may close+reopen only when stop gap >= BASE_GAP_MIN — matching normal rules.
 */
function bump(metrics, key, by = 1) {
  if (!metrics) return;
  metrics[key] = (metrics[key] || 0) + by;
}

function toDate(value) {
  if (!value) return null;
  const d = value instanceof Date ? value : new Date(value);
  return Number.isNaN(d.getTime()) ? null : d;
}

function numOrNull(value) {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

/**
 * Build in-memory trip state from durable open Trip + optional DeviceStatus.
 * Pure (no I/O) — callers fetch docs once per IMEI cache miss.
 */
function buildRecoveredTripState({ openTrip = null, deviceStatus = null } = {}) {
  if (!openTrip) {
    return {
      currentTripId: null,
      currentDistKm: 0,
      lastNonZeroAt: null,
      prevSpeed: null,
      prevPacketAt: null,
      recovered: true,
      recoveryKind: "none",
    };
  }

  const lastSpeed = numOrNull(deviceStatus?.last_speed);
  const lastPacketAt =
    toDate(deviceStatus?.last_packet_at) ||
    toDate(deviceStatus?.last_gps_at) ||
    toDate(openTrip.end_at) ||
    toDate(openTrip.start_at);

  // Prefer status speed; if unknown with open trip, assume stopped (0) so
  // resume-after-gap close/start works; short-gap resume continues same trip.
  const prevSpeed = lastSpeed != null ? lastSpeed : 0;

  const lastNonZeroAt =
    toDate(openTrip.end_at) || toDate(openTrip.start_at) || lastPacketAt;

  return {
    currentTripId: openTrip._id || openTrip.id || null,
    currentDistKm: Number(openTrip.distance_km) || 0,
    lastNonZeroAt,
    prevSpeed,
    prevPacketAt: lastPacketAt,
    recovered: true,
    recoveryKind: lastSpeed != null ? "open_with_status" : "open_assume_stopped",
  };
}

/**
 * Lazy once-per-IMEI recovery into `stateMap`.
 * One Trip find + optional DeviceStatus find — never per subsequent packet.
 */
async function ensureTripRuntimeState({
  imei,
  stateMap,
  Trip,
  DeviceStatus = null,
  metrics = null,
  log = console,
} = {}) {
  if (!imei) return null;
  if (stateMap.has(imei)) return stateMap.get(imei);

  bump(metrics, "trip_recovery_loaded_total", 1);
  let openTrip = null;
  let deviceStatus = null;
  try {
    openTrip = await Trip.findOne({ imei, is_open: true }).sort({ start_at: -1 }).lean();
    if (DeviceStatus) {
      deviceStatus = await DeviceStatus.findOne({ imei })
        .select({ last_speed: 1, last_packet_at: 1, last_gps_at: 1 })
        .lean();
    }
  } catch (err) {
    bump(metrics, "trip_recovery_failures_total", 1);
    log.warn?.("[trip-recovery] load failed", imei, err.message);
    const fallback = {
      currentTripId: null,
      currentDistKm: 0,
      lastNonZeroAt: null,
      prevSpeed: null,
      prevPacketAt: null,
      recovered: true,
      recoveryKind: "load_failed",
    };
    stateMap.set(imei, fallback);
    return fallback;
  }

  const st = buildRecoveredTripState({ openTrip, deviceStatus });
  if (openTrip) {
    bump(metrics, "trip_recovery_open_restored_total", 1);
  } else {
    bump(metrics, "trip_recovery_missing_total", 1);
  }
  if (st.recoveryKind === "open_assume_stopped") {
    bump(metrics, "trip_recovery_duplicate_prevented_total", 1);
  }
  stateMap.set(imei, st);
  return st;
}

module.exports = {
  buildRecoveredTripState,
  ensureTripRuntimeState,
};
