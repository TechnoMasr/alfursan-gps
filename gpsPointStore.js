/**
 * Lean GPS track points for fast replay (collection: gpspoints).
 * Separate from heavy gpslogs documents.
 */
const { GpsPoint, GpsLog } = require("./mongo");

const ATTR_TYPE_SKIP = new Set([19]); // heartbeat / non-track packets

function shouldStoreGpsPoint({ type, latitude, longitude, attrsType } = {}) {
  if (type !== "gps") return false;
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

function buildGpsPointDoc({
  imei,
  latitude,
  longitude,
  speed,
  direction,
  packet_date,
  date,
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

/**
 * Backfill lean points from gpslogs (type=gps) for the last N days (default 30).
 * GET /gpspoints/backfill?days=30&limit_per_batch=2000
 */
async function backfillGpsPointsFromLogs({
  days = 30,
  limitPerBatch = 2000,
  imei = null,
  dryRun = false,
  replace = false,
} = {}) {
  const daysN = Math.min(Math.max(Number(days) || 30, 1), 90);
  const batchSize = Math.min(Math.max(Number(limitPerBatch) || 2000, 100), 5000);
  const since = new Date(Date.now() - daysN * 24 * 60 * 60 * 1000);

  const match = {
    type: "gps",
    $or: [
      { packet_date: { $gte: since } },
      { packet_date: { $exists: false }, date: { $gte: since } },
    ],
    latitude: { $exists: true, $ne: null },
    longitude: { $exists: true, $ne: null },
  };
  if (imei) match.imei = String(imei).trim();

  let deleted = 0;
  if (replace && !dryRun) {
    const delFilter = { packet_date: { $gte: since } };
    if (imei) delFilter.imei = String(imei).trim();
    const delRes = await GpsPoint.deleteMany(delFilter);
    deleted = delRes?.deletedCount || 0;
  }

  let scanned = 0;
  let inserted = 0;
  let skipped = 0;
  let lastId = null;
  const startedAt = Date.now();

  // eslint-disable-next-line no-constant-condition
  while (true) {
    const q = { ...match };
    if (lastId) q._id = { $gt: lastId };

    const rows = await GpsLog.find(q)
      .sort({ _id: 1 })
      .limit(batchSize)
      .select({
        imei: 1,
        latitude: 1,
        longitude: 1,
        speed: 1,
        direction: 1,
        course: 1,
        packet_date: 1,
        date: 1,
        attributes: 1,
      })
      .lean();

    if (!rows.length) break;

    const docs = [];
    for (const row of rows) {
      scanned += 1;
      lastId = row._id;
      const attrsType = row?.attributes?.type;
      if (
        !shouldStoreGpsPoint({
          type: "gps",
          latitude: row.latitude,
          longitude: row.longitude,
          attrsType,
        })
      ) {
        skipped += 1;
        continue;
      }
      const doc = buildGpsPointDoc({
        imei: row.imei,
        latitude: row.latitude,
        longitude: row.longitude,
        speed: row.speed,
        direction: row.direction ?? row.course ?? 0,
        packet_date: row.packet_date || row.date,
        date: row.date || row.packet_date,
      });
      if (doc) docs.push(doc);
      else skipped += 1;
    }

    if (docs.length && !dryRun) {
      try {
        const res = await GpsPoint.insertMany(docs, { ordered: false });
        inserted += res.length;
      } catch (err) {
        // Duplicate key / unordered: count insertedDocs if present
        const n = err?.insertedDocs?.length ?? err?.result?.nInserted ?? 0;
        inserted += n;
        if (!n) console.warn("[gpspoints] backfill batch error:", err.message);
      }
    } else if (docs.length && dryRun) {
      inserted += docs.length;
    }

    if (rows.length < batchSize) break;
  }

  return {
    ok: true,
    days: daysN,
    since: since.toISOString(),
    imei: imei || null,
    dry_run: !!dryRun,
    replace: !!replace,
    deleted,
    scanned,
    inserted,
    skipped,
    elapsed_ms: Date.now() - startedAt,
  };
}

module.exports = {
  shouldStoreGpsPoint,
  buildGpsPointDoc,
  enqueueGpsPoint,
  setGpsPointWriter,
  getGpsPointWriter,
  backfillGpsPointsFromLogs,
  normalizeTraccarPositionId,
  ATTR_TYPE_SKIP,
};
