const path = require("path");

require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

const { createGpsPointWriter } = require("../lib/gpsPointWriter");
const { setGpsPointWriter, enqueueGpsPoint } = require("../gpsPointStore");
const { upsertDeviceStatus } = require("../deviceStatus");
const { evaluateGeofences } = require("../geofenceService");
const { handleParkingSample } = require("../parkingEventsService");
const { handleOverspeedSample } = require("../overspeedService");
const { handleAccSample, powerEventToAccOn } = require("../accReportService");
const { persistTenantNotification } = require("../notificationStore");
const { handleIdleNotifySample } = require("../idleStatsService");
const { sendPushNotification } = require("../fcm.service");
const Trip = require("../trip");
const mongoose = require("mongoose");
const { GpsPoint } = require("../mongo");

const BRIDGE_LATENCY_DEBUG = String(process.env.BRIDGE_LATENCY_DEBUG ?? "0") === "1";
const BASE_GAP_MIN = Number(process.env.BASE_GAP_MIN ?? 1) || 1;
const BUSINESS_QUEUE_MAX = Number(process.env.BUSINESS_QUEUE_MAX || 20_000) || 20_000;
const BUSINESS_CONCURRENCY = Number(process.env.BUSINESS_CONCURRENCY || 64) || 64;
const DEVICE_STATUS_PERSIST_INTERVAL_MS =
  Number(process.env.DEVICE_STATUS_PERSIST_INTERVAL_MS || 10_000) || 10_000;
const TRIP_FLUSH_INTERVAL_MS = Number(process.env.TRIP_FLUSH_INTERVAL_MS || 10_000) || 10_000;

const bridgeMetrics = {
  gpspoints_received_total: 0,
  gpspoints_spooled_total: 0,
  gpspoints_persisted_total: 0,
  gpspoints_batch_completed: 0,
  gpspoints_retry_total: 0,
  gpspoints_persist_failures: 0,
  live_device_fallback_rejected_total: 0,
  business_queue_depth: 0,
  business_queue_rejected_or_coalesced_total: 0,
  business_processing_lag_ms: 0,
  business_processed_total: 0,
  device_status_dirty_count: 0,
  device_status_flush_total: 0,
  device_status_skipped_total: 0,
  trip_dirty_count: 0,
  trip_flush_total: 0,
};

function num(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function round1(v) {
  const n = Number(v);
  return Number.isFinite(n) ? Math.round(n * 10) / 10 : 0;
}

function toDate(value, fallback = null) {
  if (value instanceof Date) return value;
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return fallback;
  return d;
}

function isFiniteCoord(lat, lon) {
  return Number.isFinite(Number(lat)) && Number.isFinite(Number(lon));
}

function isValidGpsCoord(lat, lon) {
  const a = Number(lat);
  const b = Number(lon);
  return Number.isFinite(a) && Number.isFinite(b) && !(a === 0 && b === 0);
}

const gpsPointExternalWriter =
  String(process.env.GPSPOINT_EXTERNAL_WRITER || "0") === "1" ||
  String(process.env.GPSPOINT_EXTERNAL_WRITER || "").toLowerCase() === "true";

const gpsPointWriter = createGpsPointWriter({
  mode: gpsPointExternalWriter ? "producer" : "full",
  insertMany: gpsPointExternalWriter
    ? async () => {
        throw new Error("gpspoints_external_writer_enabled_no_local_mongo_drain");
      }
    : (docs) => GpsPoint.insertMany(docs, { ordered: false }),
  spoolDir: process.env.GPSPOINT_SPOOL_DIR || undefined,
  batchSize: Number(process.env.GPSPOINT_BATCH_SIZE || 250) || 250,
  flushMs: Number(process.env.GPSPOINT_FLUSH_MS || 100) || 100,
  memHigh: Number(process.env.GPSPOINT_MEM_HIGH || 10000) || 10000,
  journalCoalesceMs: Number(process.env.GPSPOINT_JOURNAL_COALESCE_MS || 50) || 50,
  segmentMaxDocs: Number(process.env.GPSPOINT_SEGMENT_MAX_DOCS || 1000) || 1000,
  segmentMaxBytes: Number(process.env.GPSPOINT_SEGMENT_MAX_BYTES || 2097152) || 2097152,
  segmentMaxAgeMs:
    Number(
      process.env.GPSPOINT_SEGMENT_MAX_AGE_MS ||
        process.env.GPSPOINT_SEGMENT_SEAL_MS ||
        5000
    ) || 5000,
  maxMongoBatchesPerCycle:
    Number(process.env.GPSPOINT_MAX_MONGO_BATCHES_PER_CYCLE || 16) || 16,
  maxFilesPerCycle: Number(process.env.GPSPOINT_DRAIN_MAX_FILES_PER_CYCLE || 500) || 500,
  drainOldDocRatio: Number(process.env.GPSPOINT_DRAIN_OLD_DOC_RATIO || 0.5) || 0.5,
  drainNewFilesPerCycle: Number(process.env.GPSPOINT_DRAIN_NEW_FILES_PER_CYCLE || 2) || 2,
  drainOldFilesPerCycle: Number(process.env.GPSPOINT_DRAIN_OLD_FILES_PER_CYCLE || 1) || 1,
  metrics: bridgeMetrics,
  log: console,
});
if (gpsPointExternalWriter) {
  console.log(
    "[persistence-worker] GPSPOINT_EXTERNAL_WRITER=1 — journal/ACK only; Mongo drain via alfursan-gpspoints-writer"
  );
}
setGpsPointWriter(gpsPointWriter);

const tripState = new Map();
const tripFlushState = new Map();
const deviceStatusStateByImei = new Map();
const deviceStatusDirtyByImei = new Map();
const deviceStatusDocByImei = new Map();
const businessQueuesByImei = new Map();
const businessReadyImeis = [];
const businessActiveImeis = new Set();
const businessQueuedImeis = new Set();
let totalBusinessQueued = 0;
let businessRunning = 0;

const WORKER_METRIC_KEYS = [
  "gpspoints_queue_depth",
  "gpspoints_received_total",
  "gpspoints_batch_completed",
  "gpspoints_persisted_total",
  "gpspoints_retry_total",
  "gpspoints_spooled_total",
  "gpspoints_spool_depth",
  "gpspoints_spool_files",
  "gpspoints_spool_bytes",
  "gpspoints_spool_oldest_age_ms",
  "gpspoints_spool_write_failures",
  "gpspoints_journaled_total",
  "gpspoints_mongo_attempted_total",
  "gpspoints_mongo_acknowledged_total",
  "gpspoints_mongo_flush_count",
  "gpspoints_mongo_docs_per_flush_last",
  "gpspoints_mongo_docs_per_flush_avg",
  "gpspoints_mongo_docs_per_flush_max",
  "gpspoints_duplicate_already_persisted_total",
  "gpspoints_unexpected_duplicate_total",
  "gpspoints_persist_failures",
  "gpspoints_duplicates_ignored",
  "gpspoints_health",
  "gpspoint_spool_dir",
  "disk_free_bytes",
  "persistence_dropped",
  "business_queue_depth",
  "business_queue_rejected_or_coalesced_total",
  "business_processing_lag_ms",
  "business_processed_total",
  "device_status_dirty_count",
  "device_status_flush_total",
  "device_status_skipped_total",
  "trip_dirty_count",
  "trip_flush_total",
];

function publishWorkerMetrics() {
  if (!process.send) return;
  const stats = { ...gpsPointWriter.getStats(), ...bridgeMetrics };
  for (const key of WORKER_METRIC_KEYS) {
    if (Object.prototype.hasOwnProperty.call(stats, key)) {
      process.send({ type: "worker_metric", key, value: stats[key] });
    }
  }
}

function minutesDiff(a, b) {
  const ta = a instanceof Date ? a.getTime() : new Date(a).getTime();
  const tb = b instanceof Date ? b.getTime() : new Date(b).getTime();
  if (Number.isNaN(ta) || Number.isNaN(tb)) return 0;
  return Math.abs(tb - ta) / 60000;
}

async function ensureState(imei) {
  let st = tripState.get(imei);
  if (st) return st;
  const openTrip = await Trip.findOne({ imei, is_open: true }).sort({ start_at: -1 }).lean();
  st = {
    currentTripId: openTrip?._id || null,
    currentDistKm: 0,
    lastNonZeroAt: openTrip?.end_at || null,
    prevSpeed: null,
    prevPacketAt: null,
  };
  tripState.set(imei, st);
  return st;
}

async function startTrip(imei, startAt, lat, lon) {
  return Trip.create({
    imei,
    start_at: startAt,
    end_at: startAt,
    start_lat: lat ?? null,
    start_lon: lon ?? null,
    end_lat: lat ?? null,
    end_lon: lon ?? null,
    distance_km: 0,
    duration_min: 0,
    is_open: true,
  });
}

async function flushTrip(tripId) {
  const pending = tripFlushState.get(String(tripId));
  if (!pending) return;
  const update = {
    $set: {
      end_at: pending.end_at,
      end_lat: pending.end_lat,
      end_lon: pending.end_lon,
    },
  };
  if (pending.distance_km_inc > 0) {
    update.$inc = { distance_km: pending.distance_km_inc };
  }
  await Trip.updateOne({ _id: tripId }, update);
  tripFlushState.delete(String(tripId));
  bridgeMetrics.trip_flush_total += 1;
  bridgeMetrics.trip_dirty_count = tripFlushState.size;
}

async function closeTripAtLastMove(st) {
  if (!st?.currentTripId) return;
  await flushTrip(st.currentTripId);
  await Trip.updateOne({ _id: st.currentTripId }, { $set: { is_open: false, end_at: st.lastNonZeroAt || new Date() } });
  st.currentTripId = null;
}

async function appendToTrip(tripId, packetDate, lat, lon, addKm) {
  if (!tripId) return;
  const key = String(tripId);
  const existing = tripFlushState.get(key);
  const pending = existing || {
    tripId,
    distance_km_inc: 0,
    lastFlushAt: Date.now(),
  };
  pending.end_at = packetDate;
  pending.end_lat = lat ?? null;
  pending.end_lon = lon ?? null;
  pending.distance_km_inc += addKm || 0;
  tripFlushState.set(key, pending);
  bridgeMetrics.trip_dirty_count = tripFlushState.size;
  if (Date.now() - pending.lastFlushAt >= TRIP_FLUSH_INTERVAL_MS) {
    pending.lastFlushAt = Date.now();
    await flushTrip(tripId);
  }
}

async function applyTripLogic({ imei, doc, packetDate }) {
  const st = await ensureState(imei);
  const lat = doc.latitude ?? doc.gps?.latitude;
  const lon = doc.longitude ?? doc.gps?.longitude;
  const speed = Number(doc.speed) || 0;
  const isMoving = speed > 0;
  const prevSpeed = st.prevSpeed;
  const prevLastMove = st.lastNonZeroAt;

  if (st.currentTripId && prevSpeed === 0 && isMoving && prevLastMove) {
    if (minutesDiff(prevLastMove, packetDate) >= BASE_GAP_MIN) {
      await closeTripAtLastMove(st);
    }
  }

  let shouldStart = false;
  if (isMoving) {
    if (prevSpeed === null && !st.currentTripId) {
      shouldStart = true;
    } else if (prevSpeed === 0) {
      const gap = prevLastMove ? minutesDiff(prevLastMove, packetDate) : BASE_GAP_MIN;
      if (gap >= BASE_GAP_MIN) shouldStart = true;
    }
  }

  if (shouldStart) {
    const lastClosed = await Trip.findOne({ imei, is_open: false }).sort({ end_at: -1 });
    const newTrip = await startTrip(imei, packetDate, lat, lon);
    st.currentTripId = newTrip._id;
    st.currentDistKm = 0;
    if (lastClosed && lastClosed.gap_after_min == null && lastClosed.end_at) {
      const gapAfter = minutesDiff(lastClosed.end_at, packetDate);
      await Trip.updateOne({ _id: lastClosed._id }, { $set: { gap_after_min: gapAfter } });
    }
  }

  if (st.currentTripId && isMoving) {
    const addKm = Number(doc.distanceDiff) > 0 ? Number(doc.distanceDiff) : 0;
    await appendToTrip(st.currentTripId, packetDate, lat, lon, addKm);
    st.currentDistKm += addKm;
  }

  if (isMoving) st.lastNonZeroAt = packetDate;
  st.prevSpeed = speed;
  st.prevPacketAt = packetDate;
}

function deviceStatusTransitionKey({ doc, attrs } = {}) {
  return JSON.stringify({
    type: doc?.type || null,
    deviceStatus: doc?.deviceStatus || null,
    attrsType: attrs?.type ?? null,
    ignition: attrs?.ignition ?? null,
    motion: attrs?.motion ?? null,
    charge: attrs?.charge ?? null,
    blocked: attrs?.blocked ?? null,
    alarm: attrs?.alarm ?? null,
  });
}

async function persistDeviceStatus(ctx, { force = false } = {}) {
  const {
    imei,
    doc,
    attrs,
    latitude,
    longitude,
    speed,
    direction,
    packetDate,
    serverDate,
    hasValidCoords,
  } = ctx;
  const state = deviceStatusStateByImei.get(imei) || { lastPersistAt: 0, transitionKey: null };
  const key = deviceStatusTransitionKey({ doc, attrs });
  const nowMs = Date.now();
  const due = nowMs - state.lastPersistAt >= DEVICE_STATUS_PERSIST_INTERVAL_MS;
  if (!force && state.transitionKey === key && !due) {
    deviceStatusDirtyByImei.set(imei, ctx);
    bridgeMetrics.device_status_dirty_count = deviceStatusDirtyByImei.size;
    bridgeMetrics.device_status_skipped_total += 1;
    return deviceStatusDocByImei.get(imei) || null;
  }

  const statusDoc = await upsertDeviceStatus({
    imei,
    packetDate,
    serverDate,
    lat: hasValidCoords ? latitude : undefined,
    lon: hasValidCoords ? longitude : undefined,
    speed,
    type: doc.type,
    attrsType: attrs?.type,
    attrs,
    voltageUnit: attrs?.power != null ? "v" : undefined,
    direction,
    voltage: attrs?.power ?? attrs?.battery ?? attrs?.batteryLevel,
    power: attrs?.power,
    battery: attrs?.battery,
    batteryLevel: attrs?.batteryLevel ?? attrs?.battery,
    ignition: attrs?.ignition ?? null,
    motion: attrs?.motion ?? null,
    charge: attrs?.charge ?? null,
    blocked: attrs?.blocked ?? null,
    rssi: attrs?.rssi ?? null,
    alarm: attrs?.alarm ?? null,
    protocol: doc?.protocol,
    deviceId: doc?.deviceId,
    valid: doc?.valid,
    deviceStatus: doc?.deviceStatus,
    sat: attrs?.sat,
    pdop: attrs?.pdop,
    hdop: attrs?.hdop,
    status: attrs?.status,
    hours: attrs?.hours,
    distance: attrs?.distance,
    totalDistance: attrs?.totalDistance,
    ignoredAlarmOnly: ctx.ignoredAlarmOnly === true,
  });
  deviceStatusStateByImei.set(imei, { lastPersistAt: nowMs, transitionKey: key });
  deviceStatusDirtyByImei.delete(imei);
  deviceStatusDocByImei.set(imei, statusDoc);
  bridgeMetrics.device_status_dirty_count = deviceStatusDirtyByImei.size;
  bridgeMetrics.device_status_flush_total += 1;
  return statusDoc;
}

async function flushDirtyDeviceStatus(limit = 500) {
  let flushed = 0;
  for (const [imei, ctx] of deviceStatusDirtyByImei.entries()) {
    if (flushed >= limit) break;
    const state = deviceStatusStateByImei.get(imei) || { lastPersistAt: 0 };
    if (Date.now() - state.lastPersistAt < DEVICE_STATUS_PERSIST_INTERVAL_MS) continue;
    try {
      await persistDeviceStatus(ctx, { force: true });
      flushed += 1;
    } catch (err) {
      console.warn("DeviceStatus dirty flush error:", err.message);
    }
  }
  bridgeMetrics.device_status_dirty_count = deviceStatusDirtyByImei.size;
}

async function flushDirtyTrips(limit = 500) {
  let flushed = 0;
  for (const pending of Array.from(tripFlushState.values())) {
    if (flushed >= limit) break;
    if (Date.now() - pending.lastFlushAt < TRIP_FLUSH_INTERVAL_MS) continue;
    try {
      pending.lastFlushAt = Date.now();
      await flushTrip(pending.tripId);
      flushed += 1;
    } catch (err) {
      console.warn("Trip dirty flush error:", err.message);
    }
  }
  bridgeMetrics.trip_dirty_count = tripFlushState.size;
}

async function SEND_NOTIFY_TO_CLIENT(imei, title, body, data = {}) {
  try {
    if (!imei) return null;
    const deviceDetails = await mongoose.connection.collection("device_details").findOne({ imei });
    const deviceOwnerId = deviceDetails?.device_owner_id ?? null;
    const deviceName = deviceDetails?.name || "";
    const carnum = deviceDetails?.carnum || "";
    const customTitle = `${deviceName}${carnum ? " - " + carnum : ""}`.trim() || title || `تنبيه ${imei}`;

    if (deviceOwnerId) {
      const fcmTokensColl = mongoose.connection.collection("fcm_tokens");
      const fcmDocs = await fcmTokensColl.find({ user_id: deviceOwnerId }).project({ fcm_token: 1 }).toArray();
      for (const tokenDoc of fcmDocs) {
        if (!tokenDoc?.fcm_token) continue;
        try {
          await sendPushNotification({ token: tokenDoc.fcm_token, title: customTitle, body: body || customTitle, data });
        } catch {
          /* ignore push failures */
        }
      }
    }

    return persistTenantNotification({
      imei,
      user_id: deviceOwnerId,
      title: customTitle,
      body: body || customTitle,
      data,
      device_name: deviceName,
      carnum,
    });
  } catch (err) {
    console.error("SEND_NOTIFY_TO_CLIENT error:", err.message);
    return null;
  }
}

function queueImei(imei) {
  if (businessQueuedImeis.has(imei) || businessActiveImeis.has(imei)) return;
  businessQueuedImeis.add(imei);
  businessReadyImeis.push(imei);
}

function scheduleBusinessPump() {
  setImmediate(pumpBusinessQueue);
}

function enqueueBusiness(ctx) {
  const imei = ctx?.imei ? String(ctx.imei) : "";
  if (!imei) return;
  let queue = businessQueuesByImei.get(imei);
  if (!queue) {
    queue = [];
    businessQueuesByImei.set(imei, queue);
  }
  if (totalBusinessQueued >= BUSINESS_QUEUE_MAX) {
    if (queue.length) {
      queue[queue.length - 1] = { ...ctx, businessEnqueuedAt: Date.now() };
      bridgeMetrics.business_queue_rejected_or_coalesced_total += 1;
      scheduleBusinessPump();
      return;
    }
    bridgeMetrics.business_queue_rejected_or_coalesced_total += 1;
    return;
  }
  queue.push({ ...ctx, businessEnqueuedAt: Date.now() });
  totalBusinessQueued += 1;
  bridgeMetrics.business_queue_depth = totalBusinessQueued;
  queueImei(imei);
  scheduleBusinessPump();
}

function processPositionItem(ctx) {
  const { imei, doc, attrs, latitude, longitude, speedRounded, direction, packetDate, serverDate, hasValidCoords } = ctx || {};
  if (!imei || !doc) return;
  if ((doc.type === "gps" || doc.type === "alarm") && hasValidCoords) {
    enqueueGpsPoint({
      type: doc.type,
      imei,
      latitude,
      longitude,
      speed: speedRounded,
      direction,
      packet_date: packetDate,
      date: serverDate,
      ignition: typeof attrs?.ignition === "boolean" ? attrs.ignition : null,
      attrsType: attrs?.type,
      traccar_position_id: doc.traccar_position_id ?? doc?.id ?? null,
    });
  }
}

async function processBusinessItem(ctx) {
  const {
    imei,
    doc,
    attrs,
    latitude,
    longitude,
    speed,
    speedRounded,
    direction,
    packetDate,
    serverDate,
    hasValidCoords,
    ignoredAlarmOnly,
    isHistorical,
    alarmCodesForOverspeed = [],
    alarmCodesForAcc = [],
  } = ctx;

  if (!imei || !doc) return;

  setImmediate(() => {
    handleIdleNotifySample(
      imei,
      {
        speed,
        accOn: attrs?.ignition === true,
        lat: hasValidCoords ? latitude : null,
        lon: hasValidCoords ? longitude : null,
        packetDate,
      },
      {
        idleSpeedKph: 0,
        idleMinutes: 5,
        requireAccOn: true,
        onIdleConfirmed: async ({ imei: im, idleStart, packetDate: pd, lat, lon }) => {
          try {
            if (!isHistorical) {
              await SEND_NOTIFY_TO_CLIENT(im, `خمول ${im}`, "المركبة في حالة خمول (محرك يعمل وسرعة صفر/منخفضة)", {
                type: "alarm",
                subType: "idle",
                alarmType: "IDLE",
                alarmText: `Vehicle idle since ${idleStart?.toISOString?.() || ""}`,
                alarmTextAr: "المركبة في حالة خمول (محرك يعمل وسرعة صفر/منخفضة)",
                imei: im,
                latitude: lat,
                longitude: lon,
                idle_start: idleStart?.toISOString?.() || "",
              });
            }
          } catch (e) {
            console.error("Idle notify error:", e.message);
          }
        },
      }
    );
  });

  if (hasValidCoords) {
    try {
      await handleParkingSample({ imei, timestamp: packetDate, lat: latitude, lon: longitude, speed });
    } catch (err) {
      console.warn("Parking sample error:", err.message);
    }
  }

  setImmediate(() => {
    handleOverspeedSample(imei, speed, latitude, longitude, packetDate, {
      alarmCodes: alarmCodesForOverspeed,
      hooks: isHistorical ? {} : {
        onOverspeedStart: async (payload) => {
          await SEND_NOTIFY_TO_CLIENT(payload.imei, `تنبيه ${payload.imei}`, `بدء تجاوز السرعة — السرعة الحالية تقريباً ${round1(payload.speed)} كم/س`, {
            type: "alarm",
            subType: "overspeed_start",
            alarmType: "OVERSPEED",
            alarmTextAr: `بدء تجاوز السرعة (الحد ${payload.speed_limit_kmh ?? "—"} كم/س)`,
          });
        },
        onOverspeedEnd: async () => {},
      },
    }).catch((e) => console.warn("Overspeed sample error:", e.message));
  });

  setImmediate(() => {
    let accOn = undefined;
    let triggeredBy = "ignition";
    for (const code of alarmCodesForAcc) {
      const c = String(code).toLowerCase().replace(/\s+/g, "");
      const onOff = powerEventToAccOn(c);
      if (onOff !== null) {
        accOn = onOff;
        triggeredBy = c;
        break;
      }
    }
    if (accOn === undefined && typeof attrs?.ignition === "boolean") accOn = attrs.ignition;
    if (accOn !== undefined) {
      if (accOn === false && speed > 0) accOn = true;
      handleAccSample(imei, accOn, latitude, longitude, packetDate, { triggeredBy });
    }
  });

  let statusDoc = null;
  try {
    statusDoc = await persistDeviceStatus(ctx);
  } catch (err) {
    console.warn("DeviceStatus update error:", err.message);
  }

  if ((doc.type === "gps" || doc.type === "alarm") && hasValidCoords) {
    try {
      const fenceEvents = await evaluateGeofences({
        statusDoc,
        imei,
        lat: latitude,
        lon: longitude,
        speed,
        packetDate,
      });
      for (const evt of fenceEvents) {
        if (!isHistorical) {
          await SEND_NOTIFY_TO_CLIENT(imei, `تنبيه ${imei}`, evt?.alarmTextAr || evt?.alarmText || "تنبيه من الجهاز", evt);
        }
      }
    } catch (err) {
      console.error("Geofence evaluation error:", err.message);
    }
  }

  try {
    await applyTripLogic({ imei, doc, packetDate });
  } catch (err) {
    console.warn("Trip logic error:", err.message);
  }

  if (doc.type === "alarm" && !isHistorical && !ignoredAlarmOnly) {
    await SEND_NOTIFY_TO_CLIENT(imei, `تنبيه ${imei}`, doc.alarmTextAr || "تنبيه من الجهاز", {
      type: "alarm",
      subType: doc.subType || null,
      alarmType: doc.alarmType ?? null,
      alarmCodes: doc.alarmCodes || [],
      alarmText: doc.alarmText,
      alarmTextAr: doc.alarmTextAr,
      imei,
      latitude,
      longitude,
      speed: speedRounded,
    });
  }
}

async function pumpBusinessQueue() {
  while (businessRunning < BUSINESS_CONCURRENCY && businessReadyImeis.length) {
    const imei = businessReadyImeis.shift();
    businessQueuedImeis.delete(imei);
    if (businessActiveImeis.has(imei)) continue;
    const queue = businessQueuesByImei.get(imei);
    const item = queue?.shift();
    if (!item) {
      businessQueuesByImei.delete(imei);
      continue;
    }
    businessActiveImeis.add(imei);
    totalBusinessQueued = Math.max(0, totalBusinessQueued - 1);
    bridgeMetrics.business_queue_depth = totalBusinessQueued;
    businessRunning += 1;
    void processBusinessItem(item)
      .catch((err) => console.warn("Business persistence error:", err.message))
      .finally(() => {
        const lag = Date.now() - (item.businessEnqueuedAt || Date.now());
        bridgeMetrics.business_processing_lag_ms = Math.max(0, lag);
        bridgeMetrics.business_processed_total += 1;
        businessRunning -= 1;
        businessActiveImeis.delete(imei);
        const remaining = businessQueuesByImei.get(imei);
        if (remaining?.length) {
          queueImei(imei);
        } else {
          businessQueuesByImei.delete(imei);
        }
        bridgeMetrics.business_queue_depth = totalBusinessQueued;
        scheduleBusinessPump();
      });
  }
}

async function processBatch(batch) {
  const items = Array.isArray(batch?.items) ? batch.items : [];
  for (const item of items) {
    processPositionItem(item);
  }
  if (typeof gpsPointWriter.journalNow === "function") {
    const journaled = gpsPointWriter.journalNow();
    if (!journaled) {
      throw new Error("gpspoint_journal_failed");
    }
  }
  for (const item of items) {
    enqueueBusiness(item);
  }
  publishWorkerMetrics();
}

process.on("message", async (msg) => {
  if (!msg || typeof msg !== "object") return;
  if (msg.type === "batch") {
    try {
      await processBatch(msg.batch);
      if (process.send) process.send({ type: "ack", batchId: msg.batch?.batchId, count: msg.batch?.items?.length || 0 });
    } catch (err) {
      console.error("[persistence-worker] batch error:", err.message);
      if (process.send) process.send({ type: "ack", batchId: msg.batch?.batchId, count: 0, error: err.message });
    }
  }
});

const workerMetricsTimer = setInterval(publishWorkerMetrics, 5000);
if (typeof workerMetricsTimer.unref === "function") workerMetricsTimer.unref();

const dirtyFlushTimer = setInterval(() => {
  void flushDirtyDeviceStatus().then(() => flushDirtyTrips()).catch((err) => {
    console.warn("Dirty persistence flush error:", err.message);
  });
}, Math.max(1000, Math.min(DEVICE_STATUS_PERSIST_INTERVAL_MS, TRIP_FLUSH_INTERVAL_MS)));
if (typeof dirtyFlushTimer.unref === "function") dirtyFlushTimer.unref();

if (process.send) process.send({ type: "ready" });
