/**
 * Overspeed report service: tracks when a device exceeds speed limit and persists
 * one record per "overspeed session" (start → end) to overspeed_alerts collection.
 * Uses alert_speed_limit_value from MongoDB devicestatuses. All persistence is async.
 */
const mongoose = require("mongoose");
const { OverspeedAlert } = require("./mongo");
const { calcDistanceDiffSafe } = require("./gpsJumpGuard");

const activeOverspeed = new Map();
const speedLimitCache = new Map();
const SPEED_LIMIT_CACHE_MS = Number(process.env.OVERSPEED_LIMIT_CACHE_MS || 60_000) || 60_000;

function num(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function isFiniteCoord(lat, lon) {
  return Number.isFinite(lat) && Number.isFinite(lon);
}

/**
 * Get current speed limit (km/h) for device from devicestatuses.alert_speed_limit_value.
 * @returns {Promise<number|null>} limit in km/h or null if not set
 */
async function getSpeedLimit(imei) {
  if (!imei) return null;
  const key = String(imei);
  const cached = speedLimitCache.get(key);
  if (cached && cached.expiresAt > Date.now()) return cached.value;
  try {
    const doc = await mongoose.connection
      .collection("devicestatuses")
      .findOne({ imei }, { projection: { alert_speed_limit_value: 1 } });
    const v = doc?.alert_speed_limit_value;
    if (v === undefined || v === null || v === "") {
      speedLimitCache.set(key, { value: null, expiresAt: Date.now() + SPEED_LIMIT_CACHE_MS });
      return null;
    }
    const n = Number(v);
    const value = Number.isFinite(n) && n > 0 ? n : null;
    speedLimitCache.set(key, { value, expiresAt: Date.now() + SPEED_LIMIT_CACHE_MS });
    return value;
  } catch (err) {
    return null;
  }
}

function clearSpeedLimitCache(imei = null) {
  if (imei == null) speedLimitCache.clear();
  else speedLimitCache.delete(String(imei));
}

/**
 * Persist one overspeed alert record (async, non-blocking).
 * hooks.afterPersist: called after a successful overspeed alert write.
 */
function persistOverspeedAlert(payload, hooks) {
  setImmediate(async () => {
    try {
      await OverspeedAlert.create(payload);
      if (hooks && typeof hooks.afterPersist === "function") {
        try {
          await hooks.afterPersist(payload);
        } catch (e) {
          console.error("Overspeed afterPersist hook error:", e.message);
        }
      }
    } catch (err) {
      console.error("OverspeedAlert persist error:", err.message);
    }
  });
}

/**
 * Normalize alarm code for comparison (e.g. "hardAcceleration" -> "hardacceleration").
 */
function normalizeAlarmCode(code) {
  return String(code || "").toLowerCase().replace(/\s+/g, "");
}

/**
 * Handle one position sample or alarm event: update overspeed state and optionally close + persist.
 * Call this asynchronously (e.g. from setImmediate) so it does not block position pipeline.
 * When alarmCodes contains 'overspeed' we force overspeed on; when 'lowspeed' we force off.
 *
 * @param {string} imei
 * @param {number} speedKmh - speed in km/h (use 0 if only event, no position)
 * @param {number|null} lat
 * @param {number|null} lon
 * @param {Date} packetDate
 * @param {{ alarmCodes?: string[], hooks?: { onOverspeedStart?: Function, onOverspeedEnd?: Function } }} [options]
 */
async function handleOverspeedSample(imei, speedKmh, lat, lon, packetDate, options = {}) {
  if (!imei) return;

  const speed = Number(speedKmh) ? speedKmh : 0;
  // const speed = Number.isFinite(speedKmh) ? speedKmh : 0;
  const alarmCodes = (options.alarmCodes || []).map(normalizeAlarmCode);
  const hooks = options.hooks || {};
  const hasOverspeedAlarm = alarmCodes.includes("overspeed");
  const hasLowspeedAlarm = alarmCodes.includes("lowspeed");

  let limit = await getSpeedLimit(imei);

  let isOver;
  if (hasLowspeedAlarm) {
    isOver = false;
  } else if (hasOverspeedAlarm) {
    isOver = true;
    if (limit === null) limit = 0;
  } else {
    if (limit === null) return;
    isOver = speed > limit;
  }

  let state = activeOverspeed.get(imei);

  if (isOver) {
    if (!state) {
      state = {
        start_time: packetDate,
        start_lat: isFiniteCoord(lat, lon) ? lat : null,
        start_lon: isFiniteCoord(lat, lon) ? lon : null,
        end_time: packetDate,
        end_lat: isFiniteCoord(lat, lon) ? lat : null,
        end_lon: isFiniteCoord(lat, lon) ? lon : null,
        max_speed: speed,
        speed_limit_kmh: limit,
      };
      activeOverspeed.set(imei, state);
      // بداية جلسة تجاوز سرعة — إشعار + سجل في gps_logs (اختياري عبر hooks)
      if (typeof hooks.onOverspeedStart === "function") {
        try {
          await hooks.onOverspeedStart({
            imei,
            packetDate,
            lat: isFiniteCoord(lat, lon) ? lat : null,
            lon: isFiniteCoord(lat, lon) ? lon : null,
            speed,
            speed_limit_kmh: limit,
          });
        } catch (e) {
          console.error("onOverspeedStart hook error:", e.message);
        }
      }
    } else {
      state.end_time = packetDate;
      if (isFiniteCoord(lat, lon)) {
        state.end_lat = lat;
        state.end_lon = lon;
      }
      if (speed > state.max_speed) state.max_speed = speed;
    }
    return;
  }

  if (!state) return;

  const startTime = state.start_time instanceof Date ? state.start_time : new Date(state.start_time);
  const endTime = state.end_time instanceof Date ? state.end_time : new Date(state.end_time);
  const durationSec = Math.max(0, Math.round((endTime - startTime) / 1000));

  let distanceKm = 0;
  if (isFiniteCoord(state.start_lat, state.start_lon) && isFiniteCoord(state.end_lat, state.end_lon)) {
    const res = calcDistanceDiffSafe(
      { lat: state.start_lat, lon: state.start_lon, date: startTime },
      { lat: state.end_lat, lon: state.end_lon, date: endTime }
    );
    distanceKm = res.distanceKm;
  }

  const payload = {
    imei,
    start_time: startTime,
    end_time: endTime,
    start_lat: state.start_lat ?? null,
    start_lon: state.start_lon ?? null,
    end_lat: state.end_lat ?? null,
    end_lon: state.end_lon ?? null,
    speed_kmh: round1(state.max_speed),
    speed_limit_kmh: state.speed_limit_kmh != null ? num(state.speed_limit_kmh) : null,
    duration_sec: durationSec,
    distance_km: Math.round(distanceKm * 1000) / 1000,
  };

  activeOverspeed.delete(imei);
  persistOverspeedAlert(payload, {
    afterPersist: hooks.onOverspeedEnd
      ? async (p) => {
          try {
            await hooks.onOverspeedEnd(p);
          } catch (e) {
            console.error("onOverspeedEnd hook error:", e.message);
          }
        }
      : undefined,
  });
}

function round1(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  return Math.round(n * 10) / 10;
}

module.exports = {
  getSpeedLimit,
  handleOverspeedSample,
  clearSpeedLimitCache,
};
