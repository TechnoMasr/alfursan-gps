/**
 * ACC report service: tracks ignition state changes (from position.attributes.ignition
 * and from power events poweron/poweroff/powercut/powerrestored). On each change,
 * persists the previous state segment (start_time, end_time, duration, locations).
 * Ignores null ignition; filters out illogical short-duration toggles.
 * All persistence is async.
 */
const { AccEvent } = require("./mongo");

const accState = new Map();

/** أقل مدة (بالثواني) لاعتبار التغيير حقيقياً؛ التبديلات الأقصر تُعتبر ضوضاء وتُتجاهل */
const MIN_DURATION_SEC = 5;

function isFiniteCoord(lat, lon) {
  return Number.isFinite(lat) && Number.isFinite(lon);
}

function persistAccEvent(payload) {
  setImmediate(async () => {
    try {
      await AccEvent.create(payload);
    } catch (err) {
      console.error("AccEvent persist error:", err.message);
    }
  });
}

/**
 * Map power alarm/event type to ACC on/off.
 * poweron, powerrestored -> true; poweroff, powercut -> false.
 */
function powerEventToAccOn(eventType) {
  const t = String(eventType || "").toLowerCase().replace(/\s+/g, "");
  if (t === "poweron" || t === "powerrestored") return true;
  if (t === "poweroff" || t === "powercut") return false;
  return null;
}

/**
 * Handle one ACC sample: ignition state or power event.
 * - Ignores when accOn is not boolean (e.g. null).
 * - عند عدم وجود حالة سابقة: تخزين الحالة في الذاكرة فقط حتى يحدث تغيير (لا يُسجّل في acc_events إلا الفترات المغلقة بمدة حقيقية).
 * - When state changes: if previous segment duration < MIN_DURATION_SEC, ignore the change (no persist, no state update).
 * - When same state as before: do not reset startTime (keep segment open).
 *
 * @param {string} imei
 * @param {boolean} accOn - true = ACC on, false = ACC off
 * @param {number|null} lat
 * @param {number|null} lon
 * @param {Date} packetDate
 * @param {{ triggeredBy?: string }} [options]
 */
function handleAccSample(imei, accOn, lat, lon, packetDate, options = {}) {
  if (!imei || typeof accOn !== "boolean") return;

  const triggeredBy = options.triggeredBy || "ignition";
  const prev = accState.get(imei);
  const packetTime = packetDate instanceof Date ? packetDate : new Date(packetDate);

  if (!prev) {
    accState.set(imei, {
      accOn,
      startTime: packetTime,
      startLat: isFiniteCoord(lat, lon) ? lat : null,
      startLon: isFiniteCoord(lat, lon) ? lon : null,
      triggeredBy,
    });
    return;
  }

  if (prev.accOn !== accOn) {
    const startTime = prev.startTime instanceof Date ? prev.startTime : new Date(prev.startTime);
    const durationSec = Math.max(0, Math.round((packetTime - startTime) / 1000));

    if (durationSec < MIN_DURATION_SEC) {
      return;
    }

    const accPayload = {
      imei,
      acc_status: prev.accOn ? "on" : "off",
      start_time: startTime,
      end_time: packetTime,
      duration_sec: durationSec,
      start_lat: prev.startLat ?? null,
      start_lon: prev.startLon ?? null,
      end_lat: isFiniteCoord(lat, lon) ? lat : null,
      end_lon: isFiniteCoord(lat, lon) ? lon : null,
      triggered_by: prev.triggeredBy || "ignition",
    };
    persistAccEvent(accPayload);
  }

  if (!prev || prev.accOn !== accOn) {
    accState.set(imei, {
      accOn,
      startTime: packetTime,
      startLat: isFiniteCoord(lat, lon) ? lat : null,
      startLon: isFiniteCoord(lat, lon) ? lon : null,
      triggeredBy,
    });
  }
}

module.exports = {
  handleAccSample,
  powerEventToAccOn,
};
