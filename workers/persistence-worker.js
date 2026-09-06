const path = require("path");

require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

const { configureGpsLogsWriter } = require("../lib/gpsLogsWriter");
const { createGpsPointWriter } = require("../lib/gpsPointWriter");
const { setGpsPointWriter, enqueueGpsPoint } = require("../gpsPointStore");
const { upsertDeviceStatus } = require("../deviceStatus");
const { evaluateGeofences } = require("../geofenceService");
const { handleParkingSample } = require("../parkingEventsService");
const { handleOverspeedSample } = require("../overspeedService");
const { handleAccSample, powerEventToAccOn } = require("../accReportService");
const { persistTenantNotification } = require("../notificationStore");
const { handleIdleNotifySample, startIdleStatsScheduler } = require("../idleStatsService");
const { startTravelStatsScheduler } = require("../travelStatsService");
const { startStaticStatsScheduler } = require("../staticStatsService");
const { sendPushNotification } = require("../fcm.service");
const Trip = require("../trip");
const mongoose = require("mongoose");

const GPSLOGS_WRITE_ENABLED = String(process.env.GPSLOGS_WRITE_ENABLED ?? "0") === "1";
const BRIDGE_LATENCY_DEBUG = String(process.env.BRIDGE_LATENCY_DEBUG ?? "0") === "1";
const BASE_GAP_MIN = Number(process.env.BASE_GAP_MIN ?? 1) || 1;

const bridgeMetrics = {
  gpspoints_received_total: 0,
  gpspoints_spooled_total: 0,
  gpspoints_persisted_total: 0,
  gpspoints_batch_completed: 0,
  gpspoints_retry_total: 0,
  gpspoints_persist_failures: 0,
  live_device_fallback_rejected_total: 0,
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

const gpsLogsWriter = configureGpsLogsWriter({
  enabled: GPSLOGS_WRITE_ENABLED,
  onAlarm: () => {},
  metrics: bridgeMetrics,
});

const gpsPointWriter = createGpsPointWriter({
  insertMany: (docs) => require("../mongo").GpsPoint.insertMany(docs, { ordered: false }),
  spoolDir: process.env.GPSPOINT_SPOOL_DIR || undefined,
  batchSize: Number(process.env.GPSPOINT_BATCH_SIZE || 250) || 250,
  flushMs: Number(process.env.GPSPOINT_FLUSH_MS || 100) || 100,
  memHigh: Number(process.env.GPSPOINT_MEM_HIGH || 10000) || 10000,
  metrics: bridgeMetrics,
  log: console,
});
setGpsPointWriter(gpsPointWriter);

const tripState = new Map();

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

async function closeTripAtLastMove(st) {
  if (!st?.currentTripId) return;
  await Trip.updateOne({ _id: st.currentTripId }, { $set: { is_open: false, end_at: st.lastNonZeroAt || new Date() } });
  st.currentTripId = null;
}

async function appendToTrip(tripId, packetDate, lat, lon, addKm) {
  if (!tripId) return;
  await Trip.updateOne(
    { _id: tripId },
    {
      $set: { end_at: packetDate, end_lat: lat ?? null, end_lon: lon ?? null },
      $inc: { distance_km: addKm || 0, duration_min: 0 },
    }
  );
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

async function processPositionItem(ctx) {
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

  gpsLogsWriter.writeOneFireAndForget(doc);
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
            gpsLogsWriter.writeOneFireAndForget({
              imei: im,
              type: "alarm",
              subType: "idle",
              alarmType: "IDLE",
              alarmText: `Vehicle idle since ${idleStart?.toISOString?.() || ""}`,
              alarmTextAr: "المركبة في حالة خمول: المحرك يعمل والسرعة منخفضة/صفر لمدة لا تقل عن 5 دقائق",
              packet_date: pd,
              date: pd,
              latitude: lat ?? undefined,
              longitude: lon ?? undefined,
              idle_start: idleStart,
            });
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
            console.error("Idle GpsLog/notify error:", e.message);
          }
        },
      }
    );
  });
  if (doc.type === "gps" && hasValidCoords) {
    enqueueGpsPoint({
      imei,
      latitude,
      longitude,
      speed: speedRounded,
      direction,
      packet_date: packetDate,
      date: serverDate,
      attrsType: attrs?.type,
      traccar_position_id: doc.traccar_position_id ?? doc?.id ?? null,
    });
  }

  if (hasValidCoords) {
    await handleParkingSample({ imei, timestamp: packetDate, lat: latitude, lon: longitude, speed });
  }

  setImmediate(() => {
    handleOverspeedSample(imei, speed, latitude, longitude, packetDate, {
      alarmCodes: alarmCodesForOverspeed,
      hooks: isHistorical ? {} : {
        onOverspeedStart: async (payload) => {
          gpsLogsWriter.writeOneFireAndForget({
            imei: payload.imei,
            type: "alarm",
            subType: "overspeed_start",
            alarmType: "OVERSPEED",
            alarmText: `Overspeed started (limit ${payload.speed_limit_kmh ?? "?"} km/h)`,
            alarmTextAr: `بدء تجاوز السرعة (الحد ${payload.speed_limit_kmh ?? "—"} كم/س)`,
            packet_date: payload.packetDate,
            date: payload.packetDate,
            latitude: payload.lat ?? undefined,
            longitude: payload.lon ?? undefined,
            speed: round1(payload.speed),
            speed_limit_kmh: payload.speed_limit_kmh,
          });
          await SEND_NOTIFY_TO_CLIENT(payload.imei, `تنبيه ${payload.imei}`, `بدء تجاوز السرعة — السرعة الحالية تقريباً ${round1(payload.speed)} كم/س`, {
            type: "alarm",
            subType: "overspeed_start",
            alarmType: "OVERSPEED",
            alarmTextAr: `بدء تجاوز السرعة (الحد ${payload.speed_limit_kmh ?? "—"} كم/س)`,
          });
        },
        onOverspeedEnd: async (payload) => {
          gpsLogsWriter.writeOneFireAndForget({
            imei: payload.imei,
            type: "alarm",
            subType: "overspeed_end",
            alarmType: "OVERSPEED",
            alarmText: `Overspeed ended: max ${payload.speed_kmh} km/h`,
            alarmTextAr: `انتهاء تجاوز السرعة — أقصى سرعة ${payload.speed_kmh} كم/س`,
            packet_date: payload.end_time,
            date: payload.end_time,
            latitude: payload.end_lat ?? undefined,
            longitude: payload.end_lon ?? undefined,
            speed: payload.speed_kmh,
            duration_sec: payload.duration_sec,
            distance_km: payload.distance_km,
          });
        },
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

  await upsertDeviceStatus({
    imei,
    packetDate,
    serverDate,
    lat: hasValidCoords ? latitude : undefined,
    lon: hasValidCoords ? longitude : undefined,
    speed,
    type: doc.type,
    attrsType: attrs?.type,
    voltageUnit: attrs?.power != null ? "v" : undefined,
    direction,
    voltage: attrs?.power ?? attrs?.battery ?? attrs?.batteryLevel,
    batteryLevel: attrs?.batteryLevel ?? attrs?.battery,
    ignition: attrs?.ignition ?? null,
    motion: attrs?.motion ?? null,
    charge: attrs?.charge ?? null,
    blocked: attrs?.blocked ?? null,
    rssi: attrs?.rssi ?? null,
    alarm: attrs?.alarm ?? null,
  });

  if ((doc.type === "gps" || doc.type === "alarm") && hasValidCoords) {
    try {
      const fenceEvents = await evaluateGeofences({
        imei,
        lat: latitude,
        lon: longitude,
        speed,
        packetDate,
      });
      for (const evt of fenceEvents) {
        gpsLogsWriter.writeOneFireAndForget(evt);
        if (!isHistorical) {
          await SEND_NOTIFY_TO_CLIENT(imei, `تنبيه ${imei}`, evt?.alarmTextAr || evt?.alarmText || "تنبيه من الجهاز", evt);
        }
      }
    } catch (err) {
      console.error("Geofence evaluation error:", err.message);
    }
  }

  await applyTripLogic({ imei, doc, packetDate });

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

async function processBatch(batch) {
  const items = Array.isArray(batch?.items) ? batch.items : [];
  for (const item of items) {
    await processPositionItem(item);
  }
  if (typeof gpsPointWriter.journalNow === "function") {
    gpsPointWriter.journalNow();
  }
  if (typeof gpsPointWriter.flushCycle === "function") {
    await gpsPointWriter.flushCycle({ force: true }).catch(() => {});
  }
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

startIdleStatsScheduler();
startTravelStatsScheduler();
startStaticStatsScheduler();

if (process.send) process.send({ type: "ready" });
