/**
 * Lean GPS track points for fast replay (collection: gpspoints).
 */
const { GpsPoint } = require("./mongo");

const ATTR_TYPE_SKIP = new Set([19]); // heartbeat / non-track packets
const GPSPOINT_TRACK_TYPES = new Set(["gps", "alarm"]);

function shouldStoreGpsPoint({ type, latitude, longitude, attrsType } = {}) {
  if (!GPSPOINT_TRACK_TYPES.has(String(type || "").toLowerCase())) return false;
  const lat = Number(latitude);
  const lon = Number(longitude);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return false;
  if (lat === 0 && lon === 0) return false;
  if (attrsType != null && ATTR_TYPE_SKIP.has(Number(attrsType))) return false;
  return true;
}

function normalizeTraccarPositionId(value) {
  const id = Number(value);
  return Number.isFinite(id) && id > 0 ? id : null;
}

function normalizeIgnition(value) {
  if (value === true) return true;
  if (value === false) return false;
  return null;
}

function buildGpsPointDoc({
  imei,
  latitude,
  longitude,
  speed,
  direction,
  packet_date,
  date,
  ignition,
  traccar_position_id,
}) {
  const key = imei != null ? String(imei).trim() : "";
  if (!key) return null;
  const doc = {
    imei: key,
    latitude: Number(latitude),
    longitude: Number(longitude),
    speed: Number(speed) || 0,
    direction: Number(direction) || 0,
    packet_date: packet_date ? new Date(packet_date) : new Date(),
    date: date ? new Date(date) : new Date(),
    ignition: normalizeIgnition(ignition),
  };
  const posId = normalizeTraccarPositionId(traccar_position_id);
  if (posId != null) doc.traccar_position_id = posId;
  return doc;
}

let gpsPointWriter = null;

function setGpsPointWriter(writer) {
  gpsPointWriter = writer || null;
}

function getGpsPointWriter() {
  return gpsPointWriter;
}

/**
 * Enqueue for lossless archive. Never awaits Mongo. Never blocks realtime.
 */
function enqueueGpsPoint(fields) {
  if (!shouldStoreGpsPoint(fields)) return;
  const doc = buildGpsPointDoc(fields);
  if (!doc) return;
  if (gpsPointWriter && typeof gpsPointWriter.enqueue === "function") {
    gpsPointWriter.enqueue(doc);
    return;
  }
  setImmediate(() => {
    GpsPoint.create(doc).catch((err) => {
      console.error("[gpspoints] insert error:", err.message);
    });
  });
}

module.exports = {
  shouldStoreGpsPoint,
  buildGpsPointDoc,
  enqueueGpsPoint,
  setGpsPointWriter,
  getGpsPointWriter,
  normalizeTraccarPositionId,
  normalizeIgnition,
  ATTR_TYPE_SKIP,
  GPSPOINT_TRACK_TYPES,
};
