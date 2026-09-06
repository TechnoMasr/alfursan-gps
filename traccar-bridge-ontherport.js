const axios = require("axios").default;
const WebSocket = require("ws");
const mongoose = require("mongoose");
const express = require("express");
const cors = require("cors");
const http = require("http");
const path = require("path");

require("dotenv").config({ path: path.join(__dirname, ".env") });

const Trip = require("./trip");
const { GpsPoint, GpsBuffer, CommandResponse, Notification, TraccarIngressRaw } = require("./mongo");
const { normalizeTraccarPositionId } = require("./gpsPointStore");
const { upsertDeviceStatus } = require("./deviceStatus");
const { loadBridgeEnv } = require("./lib/bridgeEnv");
const {
  classifyLiveFix,
  pickLatestLiveFromBurst,
  createLiveFixTracker,
  resolveFixMs,
  recordLiveClockSplit,
} = require("./lib/liveEligibility");
const { createBridgeMetrics, recordLiveDecision, snapshotMetrics } = require("./lib/bridgeMetrics");
const { createAnalyticsQueue } = require("./lib/analyticsQueue");
const { createPersistenceIpc } = require("./lib/persistenceIpc");
const { createWsDelivery } = require("./lib/wsDelivery");
const { attachSubscriberHeartbeat } = require("./lib/subscriberHeartbeat");
const { createDeviceResolver } = require("./lib/deviceResolve");
const { createTraccarForwardQueue } = require("./lib/traccarForwardQueue");
const {
  createTraccarRawIngressWriter,
  buildTraccarRawIngressDoc,
} = require("./lib/traccarRawIngressWriter");
const { resolveRuntimeDeviceIdByImei: resolveRuntimeDeviceIdByImeiViaTraccar } = require("./lib/traccarRuntimeDevices");
const { maybeLatencyLog } = require("./lib/latencyLog");
const { createEventLoopLagMonitor } = require("./lib/eventLoopLag");
const { createImeiDebugger } = require("./lib/imeiDebug");
const { countSubscriberMetrics } = require("./lib/subscriberCounts");
const { createTenantRoomGpsThrottle } = require("./lib/tenantRoomGpsThrottle");
const { runStartupConnectivityReconciliation } = require("./lib/startupConnectivityReconciliation");
const { createTraccarModelSync } = require("./lib/traccarModelSync");
const {
  verifyForwardBearer,
  isJsonContentType,
  normalizeForwardPayload,
} = require("./lib/traccarForwardIngress");
const {
  mergeStickyAttributesForImei,
  applyStickyTelemetryToStatusSet,
} = require("./stickyTelemetry");
const { calcDistanceDiffSafe } = require("./gpsJumpGuard");
const { evaluateGeofences } = require("./geofenceService");
const { handleParkingSample } = require("./parkingEventsService");
const { handleOverspeedSample } = require("./overspeedService");
const { handleAccSample, powerEventToAccOn } = require("./accReportService");
const { sendPushNotification } = require("./fcm.service");
const { persistTenantNotification } = require("./notificationStore");
const { parseCommandResponseLegacyFields } = require("./commandResponseParse");
const { startIdleStatsScheduler, handleIdleNotifySample } = require("./idleStatsService");
const { startMileageScheduler } = require("./mileageService");
const { startTravelStatsScheduler } = require("./travelStatsService");
const { startStaticStatsScheduler } = require("./staticStatsService");

// ====== CONFIG ======
const BRIDGE_ENV = loadBridgeEnv();
const TRACCAR_BASE = normalizeTraccarBaseUrl(BRIDGE_ENV.TRACCAR_BASE_URL);
const TRACCAR_SERVICE_TOKEN = String(BRIDGE_ENV.TRACCAR_SERVICE_TOKEN || "").trim();
const TRACCAR_FORWARD_TOKEN = String(BRIDGE_ENV.TRACCAR_FORWARD_TOKEN || "").trim();
const BASE_GAP_MIN = BRIDGE_ENV.BASE_GAP_MIN;
const DEVICE_FETCH_COOLDOWN_MS = BRIDGE_ENV.DEVICE_FETCH_COOLDOWN_MS;
const MAX_LIVE_FIX_AGE_MS = BRIDGE_ENV.MAX_LIVE_FIX_AGE_MS;
const LIVE_FIX_FUTURE_TOLERANCE_MS = BRIDGE_ENV.LIVE_FIX_FUTURE_TOLERANCE_MS;
const LIVE_DEVICE_TIME_FALLBACK = BRIDGE_ENV.LIVE_DEVICE_TIME_FALLBACK;
const LIVE_DEVICE_TIME_MAX_AGE_MS = BRIDGE_ENV.LIVE_DEVICE_TIME_MAX_AGE_MS;
const LIVE_DEVICE_TIME_FUTURE_TOLERANCE_MS = BRIDGE_ENV.LIVE_DEVICE_TIME_FUTURE_TOLERANCE_MS;
const LIVE_DEVICE_SERVER_MAX_SKEW_MS = BRIDGE_ENV.LIVE_DEVICE_SERVER_MAX_SKEW_MS;
const SUBSCRIBER_HEARTBEAT_MS = BRIDGE_ENV.SUBSCRIBER_HEARTBEAT_MS;
/** Alarms suppressed from FCM and WebSocket subscribers (lowbattery is sent). */
const IGNORED_ALARM_CODES = new Set(["tampering"  , "lowbattery"]);
// ====================

function normalizeTraccarBaseUrl(value) {
  const raw = String(value || "http://127.0.0.1:8082").trim();
  return raw.replace(/\/+$/, "");
}

const states = new Map();
const deviceIdToImeiCache = new Map();
/** In-memory last GPS point per IMEI — avoids Mongo findOne on every position packet. */
const lastGpsPointByImei = new Map();
const deviceSubscribers = new Map();
/** roomName => Set<WebSocket> — tenant dashboard listens on one room per client. */
const rooms = new Map();
/** ws => Set<roomName> */
const socketRooms = new WeakMap();
/**
 * Global fast channel for command replies (all tenants/clients).
 * Spelling kept as requested: command_response_chanel
 * Avoids tenant-room GPS flood / throttle delay.
 */
const COMMAND_RESPONSE_CHANNEL = "command_response_chanel";
const DEBUG_CMD_CHANNEL =
  String(process.env.DEBUG_CMD_CHANNEL ?? "").trim() === "1";
const IMEI_ROOM_CACHE_TTL_MS = 10 * 60 * 1000;
const imeiToRoomCache = new Map();
const tenantDbNameToIdCache = new Map();
const unresolvedTenantRoomImeis = new Set();
let shuttingDown = false;
let subscribersWss = null;
const liveFixTracker = createLiveFixTracker();
const bridgeMetrics = createBridgeMetrics();
const rawIngressWriter = createTraccarRawIngressWriter({
  insertMany: (docs) => TraccarIngressRaw.insertMany(docs, { ordered: true }),
  spoolDir: path.join(__dirname, "data", "traccar-raw-ingress-spool"),
  batchSize: 250,
  flushMs: 100,
  maxQueueDepth: Math.max(1000, BRIDGE_ENV.TRACCAR_FORWARD_QUEUE_MAX || 5000),
  metrics: bridgeMetrics,
  log: console,
});
/** Per-IMEI broadcast ordering guard (tenant room + subscribers). */
const lastBroadcastPacketMsByImei = new Map();
const gpsPointWriter = {
  getSpoolDir: () => BRIDGE_ENV.GPSPOINT_SPOOL_DIR || null,
  getStats: () => ({
    gpspoint_spool_dir: BRIDGE_ENV.GPSPOINT_SPOOL_DIR || null,
    gpspoints_health: "ok",
    disk_free_bytes: null,
    gpspoints_persisted_total: 0,
    gpspoints_retry_total: 0,
    gpspoints_persist_failures: 0,
    persistence_dropped: 0,
  }),
  flushAndStop: async () => ({ elapsed_ms: 0, spool_depth: 0 }),
};
const persistenceIpc = createPersistenceIpc({
  workerModulePath: require("path").join(__dirname, "workers", "persistence-worker.js"),
  metrics: bridgeMetrics,
  maxQueueBatches: BRIDGE_ENV.PERSISTENCE_IPC_MAX_QUEUE_BATCHES || 512,
  maxBatchSize: BRIDGE_ENV.PERSISTENCE_IPC_MAX_BATCH_SIZE || 2000,
  batchMaxItems: BRIDGE_ENV.PERSISTENCE_IPC_BATCH_MAX_ITEMS || 100,
  batchMaxWaitMs: BRIDGE_ENV.PERSISTENCE_IPC_BATCH_MAX_WAIT_MS || 5,
  spoolDir: BRIDGE_ENV.PERSISTENCE_IPC_SPOOL_DIR,
});
console.log(`persistence_worker_pid=${persistenceIpc.getWorkerPid() || "starting"}`);
const eventLoopLag = createEventLoopLagMonitor();

/** Tenant-room GPS throttle (device-level subscribe is unaffected). */
const TENANT_ROOM_GPS_THROTTLE_ENABLED =
  String(process.env.TENANT_ROOM_GPS_THROTTLE_ENABLED ?? "1") !== "0";
const TENANT_ROOM_GPS_WINDOW_MS =
  Number(process.env.TENANT_ROOM_GPS_WINDOW_MS) || 30_000;
const TENANT_ROOM_GPS_MAX_PER_WINDOW =
  Number(process.env.TENANT_ROOM_GPS_MAX_PER_WINDOW) || 10;
const TENANT_ROOM_GPS_SILENCE_FLUSH_MS =
  Number(process.env.TENANT_ROOM_GPS_SILENCE_FLUSH_MS) || 30_000;
const TENANT_ROOM_GPS_STATIONARY_MIN_INTERVAL_MS =
  Number(process.env.TENANT_ROOM_GPS_STATIONARY_MIN_INTERVAL_MS) || 30_000;
const TENANT_ROOM_GPS_STATIONARY_DEADBAND_METERS =
  Number(process.env.TENANT_ROOM_GPS_STATIONARY_DEADBAND_METERS) || 15;

function recordBroadcastMetric() {
  bridgeMetrics._broadcast_window += 1;
  const now = Date.now();
  if (now - bridgeMetrics._broadcast_window_ts >= 1000) {
    bridgeMetrics.broadcast_per_sec = bridgeMetrics._broadcast_window;
    bridgeMetrics._broadcast_window = 0;
    bridgeMetrics._broadcast_window_ts = now;
  }
}

function bumpForwardPositionsReceived(n = 1) {
  bridgeMetrics.forward_positions_received_total =
    (bridgeMetrics.forward_positions_received_total || 0) + n;
  bridgeMetrics._forward_positions_window = (bridgeMetrics._forward_positions_window || 0) + n;
  const now = Date.now();
  if (!bridgeMetrics._forward_positions_window_ts) bridgeMetrics._forward_positions_window_ts = now;
  if (now - bridgeMetrics._forward_positions_window_ts >= 1000) {
    bridgeMetrics.forward_positions_per_sec = bridgeMetrics._forward_positions_window;
    bridgeMetrics._forward_positions_window = 0;
    bridgeMetrics._forward_positions_window_ts = now;
  }
}


function recordPipelineEligibility(cls) {
  bridgeMetrics.positions_received = (bridgeMetrics.positions_received || 0) + 1;
  if (cls?.liveEligible) {
    bridgeMetrics.positions_live_eligible = (bridgeMetrics.positions_live_eligible || 0) + 1;
    bridgeMetrics.live_positions_eligible_total =
      (bridgeMetrics.live_positions_eligible_total || 0) + 1;
    bridgeMetrics.live_eligible_total = (bridgeMetrics.live_eligible_total || 0) + 1;
  } else {
    bridgeMetrics.live_positions_suppressed_total =
      (bridgeMetrics.live_positions_suppressed_total || 0) + 1;
    if (cls?.decision === "historical") {
      bridgeMetrics.positions_live_historical =
        (bridgeMetrics.positions_live_historical || 0) + 1;
    }
    if (cls?.decision === "out_of_order") {
      bridgeMetrics.positions_live_out_of_order =
        (bridgeMetrics.positions_live_out_of_order || 0) + 1;
    }
  }
  const rec = bridgeMetrics.forward_positions_received_total || bridgeMetrics.positions_received || 0;
  const elig = bridgeMetrics.live_eligible_total || 0;
  bridgeMetrics.live_eligibility_ratio = rec ? Number((elig / rec).toFixed(4)) : 0;
}

const imeiDebugger = createImeiDebugger({ envValue: process.env.BRIDGE_DEBUG_IMEI, log: console });

function countSubscribers() {
  return countSubscriberMetrics({
    deviceSubscribers,
    rooms,
    commandChannel: COMMAND_RESPONSE_CHANNEL,
  }).subscriber_unique_sockets;
}

function resolveTenantRoomFromCache(imei) {
  const key = imei ? String(imei).trim() : "";
  if (!key) return null;
  const cached = imeiToRoomCache.get(key);
  if (cached && cached.expiresAt > Date.now()) return cached.roomName;
  return null;
}

function cacheTenantRoomForImei(imei, roomName) {
  const key = imei ? String(imei).trim() : "";
  const room = roomName ? String(roomName).trim() : "";
  if (!key || !room) return;
  imeiToRoomCache.set(key, { roomName: room, expiresAt: Date.now() + IMEI_ROOM_CACHE_TTL_MS });
}

async function warmImeiToRoomCacheFromMongo() {
  try {
    const coll = mongoose.connection.collection("device_details");
    const cursor = coll.find(
      {
        $or: [
          { tenant_id: { $exists: true, $ne: null } },
          { device_owner_id: { $exists: true, $ne: null } },
        ],
      },
      { projection: { imei: 1, serial_number: 1, tenant_id: 1, device_owner_id: 1 } }
    );
    const docs =
      cursor && typeof cursor.toArray === "function"
        ? await cursor.toArray()
        : cursor && cursor[Symbol.asyncIterator]
          ? cursor
          : [];
    let count = 0;
    for await (const doc of docs) {
      const tenantId = normalizeTenantId(doc.tenant_id ?? doc.device_owner_id);
      if (!tenantId) continue;
      const roomName = buildTenantRoomName(tenantId);
      const keys = [doc.imei, doc.serial_number].filter(Boolean).map(String);
      keys.forEach((k) => cacheTenantRoomForImei(k, roomName));
      count += keys.length;
    }
    console.log("[rooms] warmed imeiToRoomCache entries:", count);
  } catch (err) {
    console.warn("[rooms] warm cache failed", err.message);
  }
}
let subscribersServerStarted = false;
let traccarHttpClient = null;
let traccarModelSync = null;
let persistPositionHeavyRef = async () => {};
const analyticsQueue = {
  enqueue() {},
  getDepth: () => 0,
  flushAndStop: async () => ({ remaining: 0 }),
};
const wsDelivery = createWsDelivery({
  metrics: bridgeMetrics,
  highWatermark: BRIDGE_ENV.WS_BUFFER_HIGH_WATERMARK_BYTES,
  lowWatermark: BRIDGE_ENV.WS_BUFFER_LOW_WATERMARK_BYTES,
  criticalWatermark: BRIDGE_ENV.WS_BUFFER_CRITICAL_BYTES,
  serialize: (payload) => JSON.stringify(normalizeSubscriberPayload(payload)),
  onTerminate: (ws, reason) => {
    cleanupSubscriberSocket(ws, reason || "terminated");
    try {
      ws.terminate();
    } catch {
      /* ignore */
    }
  },
});
let fetchTraccarDeviceByIdImpl = async () => null;
const deviceResolver = createDeviceResolver({
  fetchById: (id, reason) => fetchTraccarDeviceByIdImpl(id, reason),
  cooldownMs: DEVICE_FETCH_COOLDOWN_MS,
});
const forwardQueue = createTraccarForwardQueue({
  maxDepth: BRIDGE_ENV.TRACCAR_FORWARD_QUEUE_MAX,
  metrics: bridgeMetrics,
  processFn: ({ normalized, receivedAt }) => processForwardIngressAsync(normalized, receivedAt),
});

function toDate(value, fallback = new Date()) {
  if (!value) return fallback;
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? fallback : d;
}

function num(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function isFiniteCoord(lat, lon) {
  return Number.isFinite(lat) && Number.isFinite(lon);
}

function isValidGpsCoord(lat, lon) {
  return isFiniteCoord(lat, lon) && Number(lat) !== 0 && Number(lon) !== 0;
}

function round1(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  return Math.round(n * 10) / 10;
}

function sendWsSafe(ws, payload, { kind = "gps", imei = null } = {}) {
  return wsDelivery.send(ws, payload, { kind, imei });
}

function cleanupSubscriberSocket(ws, reason = "close") {
  if (!ws) return;
  wsDelivery.clearSocketState(ws);
  removeClientFromAll(ws);
  leaveAllRooms(ws);
}

function normalizeSubscriberPayload(payload) {
  if (!payload || typeof payload !== "object" || !payload.data || typeof payload.data !== "object") {
    return payload;
  }

  let data = { ...payload.data };
  if (data.speed !== undefined) data.speed = round1(data.speed);

  if (data.gps && typeof data.gps === "object") {
    data.gps = { ...data.gps };
    if (data.gps.speed !== undefined) data.gps.speed = round1(data.gps.speed);
  }

  if (data.legacy && typeof data.legacy === "object") {
    data.legacy = { ...data.legacy };
    if (data.legacy.speed !== undefined) data.legacy.speed = round1(data.legacy.speed);
    if (data.legacy.gps && typeof data.legacy.gps === "object") {
      data.legacy.gps = { ...data.legacy.gps };
      if (data.legacy.gps.speed !== undefined) data.legacy.gps.speed = round1(data.legacy.gps.speed);
    }
  }

  return { ...payload, data };
}

function subscribeClient(ws, imei) {
  if (!imei) return;
  if (!deviceSubscribers.has(imei)) {
    deviceSubscribers.set(imei, new Set());
  }
  deviceSubscribers.get(imei).add(ws);
  if (!ws.subscriptions) ws.subscriptions = new Set();
  ws.subscriptions.add(imei);
}

function unsubscribeClient(ws, imei) {
  if (!imei) return;
  const clients = deviceSubscribers.get(imei);
  if (!clients) return;
  clients.delete(ws);
  if (clients.size === 0) {
    deviceSubscribers.delete(imei);
  }
  if (ws.subscriptions) ws.subscriptions.delete(imei);
}

function removeClientFromAll(ws) {
  if (!ws.subscriptions || ws.subscriptions.size === 0) return;
  for (const imei of ws.subscriptions) {
    const clients = deviceSubscribers.get(imei);
    if (!clients) continue;
    clients.delete(ws);
    if (clients.size === 0) deviceSubscribers.delete(imei);
  }
  ws.subscriptions.clear();
}

function buildTenantRoomName(tenantId) {
  const id = tenantId != null ? String(tenantId).trim() : "";
  if (!id) return null;
  return `tenant:${id}`;
}

function joinRoom(ws, roomName) {
  const room = roomName ? String(roomName).trim() : "";
  if (!room || !ws) return;
  if (!rooms.has(room)) rooms.set(room, new Set());
  rooms.get(room).add(ws);
  let set = socketRooms.get(ws);
  if (!set) {
    set = new Set();
    socketRooms.set(ws, set);
  }
  set.add(room);
  console.log("[rooms] join", { room });
}

function leaveRoom(ws, roomName) {
  const room = roomName ? String(roomName).trim() : "";
  if (!room || !ws) return;
  const clients = rooms.get(room);
  if (clients) {
    clients.delete(ws);
    if (clients.size === 0) rooms.delete(room);
  }
  const set = socketRooms.get(ws);
  if (set) set.delete(room);
  console.log("[rooms] leave", { room });
}

function leaveAllRooms(ws) {
  const set = socketRooms.get(ws);
  if (!set || set.size === 0) return;
  for (const room of set) {
    const clients = rooms.get(room);
    if (!clients) continue;
    clients.delete(ws);
    if (clients.size === 0) rooms.delete(room);
  }
  set.clear();
}

function emitToRoom(roomName, payload, { kind = "gps", imei = null } = {}) {
  const room = roomName ? String(roomName).trim() : "";
  if (!room) return;
  const clients = rooms.get(room);
  if (!clients || clients.size === 0) return;
  const sendKind = isCommandResponsePayload(payload) || kind === "command" ? "command" : kind;
  for (const ws of clients) {
    sendWsSafe(ws, payload, { kind: sendKind, imei });
  }
}

function isCommandResponsePayload(payload) {
  return (
    payload?.type === "command_response" ||
    payload?.data?.type === "command_response"
  );
}

function cmdChannelBp(label, extra = {}) {
  if (!DEBUG_CMD_CHANNEL) return;
  // Breakpoint-friendly trace: set DEBUG_CMD_CHANNEL=1 then attach debugger / grep logs
  console.log(`[BP:cmd-channel] ${label}`, extra);
  // eslint-disable-next-line no-debugger
  if (process.env.DEBUG_CMD_CHANNEL_BREAK === "1") debugger;
}

/** Immediate fan-out on command_response_chanel (no GPS throttle). */
function emitCommandResponseChannel(imei, payload) {
  if (!payload) return;
  const key = imei != null ? String(imei).trim() : payload?.data?.imei || null;
  const clients = rooms.get(COMMAND_RESPONSE_CHANNEL);
  cmdChannelBp("emit", {
    imei: key,
    listeners: clients ? clients.size : 0,
    responsePreview: String(payload?.data?.response || "").slice(0, 80),
  });
  if (!clients || clients.size === 0) return;
  recordBroadcastMetric();
  emitToRoom(COMMAND_RESPONSE_CHANNEL, {
    type: "command_response_channel_update",
    channel: COMMAND_RESPONSE_CHANNEL,
    imei: key,
    data: payload,
  }, { kind: "command", imei: key });
  bridgeMetrics.command_response_broadcast_total += 1;
}

function normalizeTenantId(value) {
  if (value == null || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? n : null;
}

async function resolveTenantIdByDbName(dbName) {
  const name = dbName ? String(dbName).trim() : "";
  if (!name) return null;
  if (tenantDbNameToIdCache.has(name)) return tenantDbNameToIdCache.get(name);

  try {
    const ref = await mongoose.connection.collection("device_details").findOne(
      { tenant_db_name: name, tenant_id: { $exists: true, $ne: null } },
      { projection: { tenant_id: 1 } }
    );
    const tid = normalizeTenantId(ref?.tenant_id);
    if (tid) {
      tenantDbNameToIdCache.set(name, tid);
      return tid;
    }
  } catch (err) {
    console.warn("[rooms] tenant_db_name lookup failed", { dbName: name, error: err.message });
  }
  return null;
}

async function resolveTenantRoomByImei(imei) {
  const key = imei ? String(imei).trim() : "";
  if (!key) return null;

  const cached = imeiToRoomCache.get(key);
  if (cached && cached.expiresAt > Date.now()) return cached.roomName;

  let roomName = null;
  let reason = "not_in_device_details";
  try {
    const deviceDetails = await mongoose.connection.collection("device_details").findOne(
      { $or: [{ imei: key }, { serial_number: key }] },
      { projection: { tenant_id: 1, device_owner_id: 1, tenant_db_name: 1, imei: 1 } }
    );

    if (!deviceDetails) {
      reason = "not_in_device_details";
    } else {
      const tenantId = normalizeTenantId(deviceDetails.tenant_id ?? deviceDetails.device_owner_id);
      if (tenantId) {
        roomName = buildTenantRoomName(tenantId);
      } else if (deviceDetails.tenant_db_name) {
        const fromDb = await resolveTenantIdByDbName(deviceDetails.tenant_db_name);
        if (fromDb) roomName = buildTenantRoomName(fromDb);
        else reason = "missing_tenant_id";
      } else {
        reason = "missing_tenant_id";
      }
    }
  } catch (err) {
    console.warn("[rooms] device_details lookup failed", { imei: key, error: err.message });
    return null;
  }

  if (!roomName) {
    if (!unresolvedTenantRoomImeis.has(key)) {
      unresolvedTenantRoomImeis.add(key);
      // console.warn("[rooms] cannot resolve tenant room for imei", key, { reason });
    }
    return null;
  }

  imeiToRoomCache.set(key, { roomName, expiresAt: Date.now() + IMEI_ROOM_CACHE_TTL_MS });
  return roomName;
}

function extractPayloadPacketMs(payload) {
  const data = payload?.data;
  if (!data || typeof data !== "object") return null;
  const raw = data.traccar_raw || data.legacy || data;
  const candidates = [
    data.packet_date,
    data.date,
    raw?.fixTime,
    raw?.deviceTime,
    raw?.serverTime,
  ];
  for (const v of candidates) {
    const d = toDate(v, null);
    if (d && !Number.isNaN(d.getTime())) return d.getTime();
  }
  return null;
}

function isStaleBroadcastPacket(imei, payload) {
  const key = imei ? String(imei).trim() : "";
  if (!key) return false;
  const packetMs = extractPayloadPacketMs(payload);
  if (packetMs == null) return false;
  const last = lastBroadcastPacketMsByImei.get(key) || 0;
  return last > 0 && packetMs < last;
}

function acceptBroadcastPacket(imei, payload) {
  if (isStaleBroadcastPacket(imei, payload)) return false;
  const key = imei ? String(imei).trim() : "";
  const packetMs = extractPayloadPacketMs(payload);
  if (key && packetMs != null) lastBroadcastPacketMsByImei.set(key, packetMs);
  return true;
}

function doEmitTenantRoomGps(imei, roomName, payload) {
  recordBroadcastMetric();
  bridgeMetrics.tenant_gps_emitted_total = (bridgeMetrics.tenant_gps_emitted_total || 0) + 1;
  bridgeMetrics.tenant_gps_sent = (bridgeMetrics.tenant_gps_sent || 0) + 1;
  imeiDebugger.logPosition({
    imei,
    tenant_room: roomName,
    tenant_emit: true,
    live_decision: "tenant_gps_sent",
  });
  emitToRoom(roomName, {
    type: "tenant_gps_update",
    room: roomName,
    imei: String(imei),
    data: payload,
  }, { kind: "gps", imei });
}

const tenantRoomThrottle = createTenantRoomGpsThrottle({
  enabled: TENANT_ROOM_GPS_THROTTLE_ENABLED,
  windowMs: TENANT_ROOM_GPS_WINDOW_MS,
  maxPerWindow: TENANT_ROOM_GPS_MAX_PER_WINDOW,
  silenceFlushMs: TENANT_ROOM_GPS_SILENCE_FLUSH_MS,
  stationaryMinIntervalMs: TENANT_ROOM_GPS_STATIONARY_MIN_INTERVAL_MS,
  stationaryDeadbandMeters: TENANT_ROOM_GPS_STATIONARY_DEADBAND_METERS,
  metrics: bridgeMetrics,
  emit: doEmitTenantRoomGps,
});
const tenantRoomGpsThrottleByImei = tenantRoomThrottle.slots;

function emitTenantRoomGpsThrottled(imei, roomName, payload) {
  const result = tenantRoomThrottle.push(imei, roomName, payload);
  imeiDebugger.logPosition({
    imei,
    tenant_room: roomName,
    tenant_emit: result.emitted,
    tenant_suppression_reason: result.reason,
    speed: payload?.data?.speed ?? payload?.data?.gps?.speed,
    attributes_type: payload?.data?.attributes?.type,
    packet_date: payload?.data?.packet_date,
    live_decision: result.reason,
  });
}

/** Immediate tenant-room fan-out (sync when room is cached). */
function emitToTenantRoomForPayload(imei, payload, { skipOrderCheck = false } = {}) {

  if (!imei || !payload) return;
  if (!shouldBroadcastToSubscribers(payload)) return;
  if (!skipOrderCheck && !acceptBroadcastPacket(imei, payload)) return;

  const deliver = (roomName) => {
    if (!roomName) {
      bridgeMetrics.tenant_room_unresolved = (bridgeMetrics.tenant_room_unresolved || 0) + 1;
      return;
    }
    bridgeMetrics.tenant_room_resolved = (bridgeMetrics.tenant_room_resolved || 0) + 1;
    cacheTenantRoomForImei(imei, roomName);
    const clients = rooms.get(roomName);
    if (!clients || clients.size === 0) {
      bridgeMetrics.tenant_room_no_listener = (bridgeMetrics.tenant_room_no_listener || 0) + 1;
      imeiDebugger.logPosition({
        imei,
        tenant_room: roomName,
        tenant_emit: false,
        tenant_suppression_reason: "no_listener",
      });
      return;
    }
    bridgeMetrics.tenant_room_has_listener = (bridgeMetrics.tenant_room_has_listener || 0) + 1;
    emitTenantRoomGpsThrottled(imei, roomName, payload);
  };

  const cachedRoom = resolveTenantRoomFromCache(imei);
  if (cachedRoom) {
    deliver(cachedRoom);
    return;
  }

  void resolveTenantRoomByImei(imei).then((roomName) => deliver(roomName));
}

function shouldBroadcastToSubscribers(payload) {
  const data = payload?.data;
  if (!data || typeof data !== "object") return true;

  const codes = Array.isArray(data.alarmCodes)
    ? data.alarmCodes
    : Array.isArray(data?.legacy?.alarmCodes)
      ? data.legacy.alarmCodes
      : null;

  // Block broadcast only when every alarm code is muted (e.g. tampering-only).
  if (codes && codes.length > 0 && codes.every((c) => IGNORED_ALARM_CODES.has(normalizeAlarmToken(c)))) {
    return false;
  }


  return true;
}

function broadcastToDeviceSubscribers(imei, payload) {
  const isCmd = isCommandResponsePayload(payload);
  // Fast path: dedicated channel (bypasses tenant-room GPS queue/throttle)
  if (isCmd) emitCommandResponseChannel(imei, payload);

  const clients = deviceSubscribers.get(imei);
  const hasDeviceClients = clients && clients.size > 0;
  if (!hasDeviceClients) {
    // command_response: channel only (no tenant GPS room) — keeps replies off the fleet flood
    if (!isCmd) emitToTenantRoomForPayload(imei, payload);
    return;
  }
  if (!shouldBroadcastToSubscribers(payload)) return;
  if (!isCmd && !acceptBroadcastPacket(imei, payload)) return;
  recordBroadcastMetric();
  for (const ws of clients) {
    sendWsSafe(ws, payload, { kind: isCmd ? "command" : "gps", imei });
  }
  if (!isCmd) {
    bridgeMetrics.device_gps_emitted_total = (bridgeMetrics.device_gps_emitted_total || 0) + 1;
    emitToTenantRoomForPayload(imei, payload, { skipOrderCheck: true });
  }
}

/** Socket-only path: minimal JSON (no normalizeSubscriberPayload) for low latency. */
function broadcastToDeviceSubscribersImmediate(imei, payload) {
  const isCmd = isCommandResponsePayload(payload);
  // Fast path first — replies must not wait behind GPS fan-out
  if (isCmd) emitCommandResponseChannel(imei, payload);

  const clients = deviceSubscribers.get(imei);
  const hasDeviceClients = clients && clients.size > 0;
  if (!hasDeviceClients) {
    if (!isCmd) emitToTenantRoomForPayload(imei, payload);
    return;
  }
  if (!shouldBroadcastToSubscribers(payload)) return;
  if (!isCmd && !acceptBroadcastPacket(imei, payload)) return;

  for (const ws of clients) {
    if (!ws || ws.readyState !== WebSocket.OPEN) continue;
    wsDelivery.send(ws, payload, { kind: isCmd ? "command" : "gps", imei });
  }
  recordBroadcastMetric();

  if (!isCmd) {
    bridgeMetrics.device_gps_emitted_total = (bridgeMetrics.device_gps_emitted_total || 0) + 1;
    emitToTenantRoomForPayload(imei, payload, { skipOrderCheck: true });
  }
}

/** FIFO per IMEI. Never coalesces. Never drops. Never leaves a hanging Promise. */
function enqueuePersistForImei(imei, ctx) {
  persistenceIpc.enqueue({
    batchId: ctx?.batchId || `${imei || "unknown"}-${ctx?.doc?.traccar_position_id ?? Date.now()}`,
    kind: "position_batch",
    imei,
    createdAt: new Date().toISOString(),
    receivedAt: ctx?.receivedAt || null,
    liveIndex: Number.isInteger(ctx?.liveIndex) ? ctx.liveIndex : -1,
    liveDecision: ctx?.liveDecision || null,
    items: [ctx],
  });
}

function resolveImeiFromCache(deviceId) {
  const key = Number(deviceId);
  if (!Number.isFinite(key)) return null;
  const cached = deviceIdToImeiCache.get(key);
  return cached ? String(cached) : null;
}

/** Sync cache warm from Traccar device payload (no Mongo). */
function warmImeiCacheFromTraccarDevice(traccarDevice) {
  const mysqlId = Number(traccarDevice?.id);
  const imei = traccarDevice?.uniqueId ? String(traccarDevice.uniqueId).trim() : null;
  if (!Number.isFinite(mysqlId) || !imei) return null;
  deviceIdToImeiCache.set(mysqlId, imei);
  scheduleTraccarModelSyncForDevice({
    imei,
    runtimeDeviceId: mysqlId,
    currentModel: traccarDevice?.model,
    forwardedDevice: traccarDevice,
  });
  return imei;
}

function scheduleTraccarModelSyncForDevice({ imei, runtimeDeviceId, currentModel, forwardedDevice } = {}) {
  if (!traccarModelSync) return;
  const result = traccarModelSync.schedule({
    imei,
    runtimeDeviceId,
    currentModel,
    forwardedDevice,
  });
  if (result && result.accepted === false) {
    console.warn("[traccar-model-sync] schedule skipped", {
      imei,
      runtime_id: runtimeDeviceId,
      reason: result.reason,
    });
  }
}

function positionHasCommandResponse(position) {
  const checkVar = String(position?.attributes?.result || "").trim().length > 0;

  if(checkVar){
    console.warn('checkVar' , checkVar ) ;
  }
  return checkVar ;
}

function slimTraccarRaw(traccarData) {
  if (!traccarData || typeof traccarData !== "object") return traccarData;
  const attrs = traccarData.attributes || {};
  return {
    id: traccarData.id ?? null,
    deviceId: traccarData.deviceId ?? null,
    protocol: traccarData.protocol ?? null,
    serverTime: traccarData.serverTime ?? null,
    deviceTime: traccarData.deviceTime ?? null,
    fixTime: traccarData.fixTime ?? null,
    valid: traccarData.valid ?? null,
    latitude: traccarData.latitude ?? null,
    longitude: traccarData.longitude ?? null,
    speed: traccarData.speed ?? null,
    course: traccarData.course ?? null,
    address: traccarData.address ?? null,
    attributes: {
      ignition: attrs.ignition ?? null,
      motion: attrs.motion ?? null,
      charge: attrs.charge ?? null,
      blocked: attrs.blocked ?? null,
      batteryLevel: attrs.batteryLevel ?? null,
      rssi: attrs.rssi ?? null,
      alarm: attrs.alarm ?? null,
      distance: attrs.distance ?? null,
      totalDistance: attrs.totalDistance ?? null,
      hours: attrs.hours ?? null,
    },
  };
}

function mergeLegacyAndTraccarPayload({ legacyDoc, traccarData, traccarType, source }) {
  return {
    ...traccarData,
    ...legacyDoc,
    imei: legacyDoc?.imei || traccarData?.imei || null,
    type: legacyDoc?.type || traccarType || "gps",
    bridge_source: source || "traccar",
    traccar_type: traccarType || null,
    // traccar_raw: slimTraccarRaw(traccarData),
    legacy: legacyDoc,
  };
}

function startSubscribersServer() {
  if (subscribersServerStarted) return;
  subscribersServerStarted = true;

  const app = express();
  app.use((req, res, next) => {
    if (req.path !== "/traccar/position") return next();
    const auth = verifyForwardBearer(req.headers.authorization, TRACCAR_FORWARD_TOKEN);
    if (!auth.ok) {
      bridgeMetrics.forward_unauthorized_total = (bridgeMetrics.forward_unauthorized_total || 0) + 1;
      const status = auth.reason === "forward_token_not_configured" ? 503 : 401;
      return res.status(status).json({ ok: false, error: auth.reason });
    }
    return next();
  });
  app.use(express.json());
  app.use((err, req, res, next) => {
    if (err instanceof SyntaxError && req?.path === "/traccar/position") {
      bridgeMetrics.forward_invalid_total = (bridgeMetrics.forward_invalid_total || 0) + 1;
      return res.status(400).json({ ok: false, error: "invalid_json" });
    }
    return next(err);
  });
  app.use(cors({ origin: "*" }));
  app.get("/health", (req, res) => {
    const counts = countSubscriberMetrics({
      deviceSubscribers,
      rooms,
      commandChannel: COMMAND_RESPONSE_CHANNEL,
    });
    bridgeMetrics.subscriber_count = counts.subscriber_unique_sockets;
    Object.assign(bridgeMetrics, counts);
    bridgeMetrics.traccar_ingress = "http-forward";
    bridgeMetrics.forward_queue_depth = forwardQueue.getDepth();
    Object.assign(bridgeMetrics, rawIngressWriter.getStats());
    const localWriterStats = gpsPointWriter.getStats();
    const writerStats = {
      ...localWriterStats,
      gpspoint_spool_dir:
        bridgeMetrics.gpspoint_spool_dir || localWriterStats.gpspoint_spool_dir,
      gpspoints_health:
        bridgeMetrics.gpspoints_health || localWriterStats.gpspoints_health || "ok",
      disk_free_bytes:
        bridgeMetrics.disk_free_bytes ?? localWriterStats.disk_free_bytes ?? null,
    };
    const lag = eventLoopLag.snapshot();
    Object.assign(bridgeMetrics, lag);
    const persistenceHealth = writerStats.gpspoints_health || "ok";
    res.json({
      ok: true,
      service: "traccar-bridge",
      traccar_ingress: "http-forward",
      traccar_forward_last_received_at: bridgeMetrics.forward_last_received_at,
      traccar_forward_positions_received_total: bridgeMetrics.forward_positions_received_total || 0,
      traccar_forward_positions_per_sec: bridgeMetrics.forward_positions_per_sec || 0,
      traccar_forward_invalid_total: bridgeMetrics.forward_invalid_total || 0,
      traccar_forward_unauthorized_total: bridgeMetrics.forward_unauthorized_total || 0,
      traccar_forward_queue_depth: forwardQueue.getDepth(),
      traccar_forward_queue_rejected_total: bridgeMetrics.forward_queue_rejected_total || 0,
      traccar_command_responses_total: bridgeMetrics.forward_command_responses_total || 0,
      raw_ingress_received_total: bridgeMetrics.raw_ingress_received_total || 0,
      raw_ingress_accepted_total: bridgeMetrics.raw_ingress_accepted_total || 0,
      raw_ingress_persisted_total: bridgeMetrics.raw_ingress_persisted_total || 0,
      raw_ingress_mongo_attempted_total: bridgeMetrics.raw_ingress_mongo_attempted_total || 0,
      raw_ingress_persist_failures: bridgeMetrics.raw_ingress_persist_failures || 0,
      raw_ingress_queue_depth: bridgeMetrics.raw_ingress_queue_depth || 0,
      raw_ingress_queue_rejected_total: bridgeMetrics.raw_ingress_queue_rejected_total || 0,
      raw_ingress_spooled_total: bridgeMetrics.raw_ingress_spooled_total || 0,
      raw_ingress_spool_depth: bridgeMetrics.raw_ingress_spool_depth || 0,
      raw_ingress_last_received_at: bridgeMetrics.raw_ingress_last_received_at || null,
      raw_ingress_last_persisted_at: bridgeMetrics.raw_ingress_last_persisted_at || null,
      ipc_spooled_batches_total: bridgeMetrics.ipc_spooled_batches_total || 0,
      ipc_spool_depth: bridgeMetrics.ipc_spool_depth || 0,
      ipc_spool_failures_total: bridgeMetrics.ipc_spool_failures_total || 0,
      ipc_send_backpressure_total: bridgeMetrics.ipc_send_backpressure_total || 0,
      ipc_batches_sent_total: bridgeMetrics.ipc_batches_sent_total || bridgeMetrics.ipc_batches_sent || 0,
      ipc_items_sent_total: bridgeMetrics.ipc_items_sent_total || 0,
      ipc_batch_accumulator_depth: bridgeMetrics.ipc_batch_accumulator_depth || 0,
      ipc_batch_avg_size: bridgeMetrics.ipc_batch_avg_size || 0,
      ipc_batch_max_size: bridgeMetrics.ipc_batch_max_size || 0,
      ipc_ack_latency_ms: bridgeMetrics.ipc_ack_latency_ms || 0,
      ipc_ack_latency_max_ms: bridgeMetrics.ipc_ack_latency_max_ms || 0,
      gpspoints_journaled_total: bridgeMetrics.gpspoints_journaled_total || 0,
      gpspoints_mongo_flush_count: bridgeMetrics.gpspoints_mongo_flush_count || 0,
      gpspoints_mongo_docs_per_flush_last: bridgeMetrics.gpspoints_mongo_docs_per_flush_last || 0,
      gpspoints_mongo_docs_per_flush_avg: bridgeMetrics.gpspoints_mongo_docs_per_flush_avg || 0,
      gpspoints_mongo_docs_per_flush_max: bridgeMetrics.gpspoints_mongo_docs_per_flush_max || 0,
      startup_reconciliation_last_run_at: bridgeMetrics.startup_reconciliation_last_run_at || null,
      startup_reconciliation_traccar_devices: bridgeMetrics.startup_reconciliation_traccar_devices || 0,
      startup_reconciliation_mongo_online: bridgeMetrics.startup_reconciliation_mongo_online || 0,
      startup_reconciliation_marked_offline: bridgeMetrics.startup_reconciliation_marked_offline || 0,
      startup_reconciliation_failures: bridgeMetrics.startup_reconciliation_failures || 0,
      traccar_model_sync_requested_total: bridgeMetrics.traccar_model_sync_requested_total || 0,
      traccar_model_sync_success_total: bridgeMetrics.traccar_model_sync_success_total || 0,
      traccar_model_sync_failed_total: bridgeMetrics.traccar_model_sync_failed_total || 0,
      traccar_model_sync_skipped_no_model_total: bridgeMetrics.traccar_model_sync_skipped_no_model_total || 0,
      traccar_model_sync_already_correct_total: bridgeMetrics.traccar_model_sync_already_correct_total || 0,
      traccar_model_sync_pending: bridgeMetrics.traccar_model_sync_pending || 0,
      traccar_model_sync_queue_depth: bridgeMetrics.traccar_model_sync_queue_depth || 0,
      traccar_model_cache_size: bridgeMetrics.traccar_model_cache_size || 0,
      traccar_model_sync_last_success_at: bridgeMetrics.traccar_model_sync_last_success_at || null,
      persistence_health: persistenceHealth,
      gpspoint_spool_dir: writerStats.gpspoint_spool_dir,
      ts: new Date().toISOString(),
      metrics: snapshotMetrics(bridgeMetrics, {
        imei_room_cache_size: imeiToRoomCache.size,
        tenant_room_gps_throttle_enabled: TENANT_ROOM_GPS_THROTTLE_ENABLED,
        tenant_room_gps_window_ms: TENANT_ROOM_GPS_WINDOW_MS,
        tenant_room_gps_max_per_window: TENANT_ROOM_GPS_MAX_PER_WINDOW,
        tenant_room_gps_stationary_min_interval_ms: TENANT_ROOM_GPS_STATIONARY_MIN_INTERVAL_MS,
        tenant_room_gps_stationary_deadband_meters: TENANT_ROOM_GPS_STATIONARY_DEADBAND_METERS,
        tenant_room_gps_throttled: bridgeMetrics.tenant_room_gps_throttled,
        tenant_room_gps_flushed: bridgeMetrics.tenant_room_gps_flushed,
        tenant_room_gps_stationary_suppressed: bridgeMetrics.tenant_room_gps_stationary_suppressed,
        tenant_room_gps_throttle_slots: tenantRoomGpsThrottleByImei.size,
        command_response_channel: COMMAND_RESPONSE_CHANNEL,
        command_response_channel_listeners: counts.command_response_channel_listeners,
        subscriber_unique_sockets: counts.subscriber_unique_sockets,
        subscriber_room_memberships: counts.subscriber_room_memberships,
        tenant_room_listeners: counts.tenant_room_listeners,
        device_subscription_memberships: counts.device_subscription_memberships,
        max_live_fix_age_ms: MAX_LIVE_FIX_AGE_MS,
        live_fix_future_tolerance_ms: LIVE_FIX_FUTURE_TOLERANCE_MS,
        live_device_time_fallback: LIVE_DEVICE_TIME_FALLBACK,
        live_device_time_max_age_ms: LIVE_DEVICE_TIME_MAX_AGE_MS,
        live_device_time_future_tolerance_ms: LIVE_DEVICE_TIME_FUTURE_TOLERANCE_MS,
        live_device_server_max_skew_ms: LIVE_DEVICE_SERVER_MAX_SKEW_MS,
        subscriber_heartbeat_ms: SUBSCRIBER_HEARTBEAT_MS,
        gpspoint_spool_dir: writerStats.gpspoint_spool_dir,
        gpspoints_health: persistenceHealth,
        disk_free_bytes: writerStats.disk_free_bytes,
        event_loop_lag_ms: lag.event_loop_lag_ms,
        event_loop_lag_p50_ms: lag.event_loop_lag_p50_ms,
        event_loop_lag_p99_ms: lag.event_loop_lag_p99_ms,
      }),
    });
  });

  app.post("/traccar/position", (req, res) => {
    if (!isJsonContentType(req)) {
      bridgeMetrics.forward_invalid_total = (bridgeMetrics.forward_invalid_total || 0) + 1;
      return res.status(415).json({ ok: false, error: "json_required" });
    }

    const receivedAt = Date.now();
    bridgeMetrics.raw_ingress_received_total = (bridgeMetrics.raw_ingress_received_total || 0) + 1;
    bridgeMetrics.raw_ingress_last_received_at = new Date(receivedAt).toISOString();
    const rawQueued = rawIngressWriter.enqueue(buildTraccarRawIngressDoc(req.body, receivedAt));
    if (!rawQueued.accepted) {
      return res.status(503).json({ ok: false, error: rawQueued.reason });
    }

    const normalized = normalizeForwardPayload(req.body);
    if (!normalized.ok) {
      bridgeMetrics.forward_invalid_total =
        (bridgeMetrics.forward_invalid_total || 0) + Math.max(1, normalized.invalid.length);
      return res.status(202).json({
        ok: true,
        ingress: "http-forward",
        raw_accepted: true,
        accepted: 0,
        error: "invalid_forward_position",
        invalid: normalized.invalid.slice(0, 5),
      });
    }

    bridgeMetrics.forward_last_received_at = new Date(receivedAt).toISOString();
    bumpForwardPositionsReceived(normalized.items.length);
    bridgeMetrics.forward_invalid_total =
      (bridgeMetrics.forward_invalid_total || 0) + normalized.invalid.length;
    const commandCount = normalized.items.filter((item) => item.hasCommandResponse).length;
    bridgeMetrics.forward_command_responses_total =
      (bridgeMetrics.forward_command_responses_total || 0) + commandCount;

    const queued = forwardQueue.enqueue({ normalized, receivedAt });
    if (!queued.accepted) {
      return res.status(503).json({ ok: false, error: queued.reason });
    }

    return res.status(202).json({
      ok: true,
      ingress: "http-forward",
      accepted: normalized.items.length,
      invalid: normalized.invalid.length,
      queue_depth: queued.depth,
    });
  });

  /**
   * Test FCM push for a specific user_id (GET).
   * Example: http://127.0.0.1:3053/notify-test?user_id=42
   * Optional: title, body
   */
  app.get("/notify-test", async (req, res) => {
    try {
      const userIdRaw = req.query.user_id ?? req.query.userId;
      const userIdNum = Number(userIdRaw);
      if (!Number.isFinite(userIdNum) || userIdNum <= 0) {
        return res.status(400).json({
          ok: false,
          error: "user_id_required",
          usage: "/notify-test?user_id=USER_ID&title=...&body=...",
        });
      }

      const title = String(req.query.title || "إشعار تجريبي").trim() || "إشعار تجريبي";
      const body =
        String(req.query.body || `اختبار إشعار للمستخدم ${userIdNum}`).trim() ||
        `اختبار إشعار للمستخدم ${userIdNum}`;

      const fcmTokensColl = mongoose.connection.collection("fcm_tokens");
      const fcmDocs = await fcmTokensColl
        .find({ user_id: { $in: [userIdNum, String(userIdNum)] } })
        .project({ fcm_token: 1, user_id: 1 })
        .toArray();

      let successCount = 0;
      let failedCount = 0;
      const results = [];

      for (const tokenDoc of fcmDocs) {
        const token = tokenDoc.fcm_token;
        if (!token) continue;
        try {
          const aRe = await sendPushNotification({
            token,
            title,
            body,
            data: {
              type: "notify_test",
              user_id: String(userIdNum),
              title,
              body,
            },
          });
          successCount++;
          results.push({ ok: true, tokenTail: String(token).slice(-8), aRe });
        } catch (err) {
          failedCount++;
          const code = err?.errorInfo?.code || err?.code;
          results.push({
            ok: false,
            tokenTail: String(token).slice(-8),
            code,
            error: err?.message || String(err),
          });
          if (
            code === "messaging/invalid-registration-token" ||
            code === "messaging/registration-token-not-registered"
          ) {
            await fcmTokensColl.deleteOne({ _id: tokenDoc._id });
          }
        }
      }

      return res.json({
        ok: true,
        user_id: userIdNum,
        tokens_found: fcmDocs.length,
        successCount,
        failedCount,
        title,
        body,
        results,
        ts: new Date().toISOString(),
      });
    } catch (err) {
      console.error("[notify-test] error:", err.message);
      return res.status(500).json({ ok: false, error: err.message });
    }
  });

  app.get("/snot", async (req, res) => {
    const fcmTokensColl = mongoose.connection.collection("fcm_tokens");
    const fcmDocs = await fcmTokensColl.find()
      .project({ fcm_token: 1 })
      .toArray();

    let successCount = 0;
    let failedCount = 0;

    let arr = [];
    for (const tokenDoc of fcmDocs) {
      const token = tokenDoc.fcm_token;
      arr.push({
        token: token
      });
      if (!token) continue;
      let aRe   ;
      try {

        let D = {
          token:token  ,
      title: "🚗 تشغيل المركبة",
      body: `تم تشغيل المركبة 1000`,
      data: {
              title: "🚗 تشغيل المركبة",
      body: `تم تشغيل المركبة 1000`,
        type: "acc_on",
        device_id:"1515",
      },
    } ;



     let aRe =    await sendPushNotification({ D });
        successCount++;
        arr.push({
          aRe: aRe,
          D: D,
          successCount: successCount,
        });
      } catch (err) {
        failedCount++;
        const code = err?.errorInfo?.code || err?.code;
        arr.push({
          aRe: aRe,
          err: err,
          code: code,
          failedCount: failedCount,
        });
        if (
          code === "messaging/invalid-registration-token" ||
          code === "messaging/registration-token-not-registered"
        ) {
          arr.push({
            deleteOne: 1,
          });
          await fcmTokensColl.deleteOne({ _id: tokenDoc._id });
        }
      }
    }



    res.json({ ok: true, arr:arr, ts: new Date().toISOString() });
  });

  /**
   * Laravel / داخلي: ضبط المسافة المجمّعة وساعات المحرك في AlFursan Mongo state.
   * JSON: { "deviceId": 189, "hours": 0, "totalDistance": 0 } — أو imei بدل deviceId
   * totalDistance بالمتر؛ أو أرسل totalDistanceKm وسيُحوَّل للمتر إن لم يُرسل totalDistance.
   */
  app.post("/traccar/accumulators", async (req, res) => {
    try {
      const body = req.body || {};
      const { imei, hours } = body;
      const payloadImei = imei ? String(imei).trim() : null;
      if (!payloadImei) {
        return res.status(400).json({ ok: false, error: "imei_required" });
      }

      let totalDistanceM = Number(body.totalDistance);
      if (!Number.isFinite(totalDistanceM) && body.totalDistanceKm != null) {
        totalDistanceM = Number(body.totalDistanceKm) * 1000;
      }
      const h = Number(hours);
      if (!Number.isFinite(h)) {
        return res.status(400).json({ ok: false, error: "invalid_or_missing_hours" });
      }
      if (!Number.isFinite(totalDistanceM)) {
        return res.status(400).json({
          ok: false,
          error: "invalid_or_missing_totalDistance_or_totalDistanceKm",
        });
      }

      const km = totalDistanceM / 1000;
      await mongoose.connection.collection("devicestatuses").updateOne(
        { imei: payloadImei },
        {
          $set: {
            imei: payloadImei,
            engine_hours: h,
            hours: h,
            total_distance_m: totalDistanceM,
            device_total_distance: totalDistanceM,
            km_total: km,
            miles_total: km * 0.621371,
            accumulators_updated_at: new Date(),
            updatedAt: new Date(),
          },
          $setOnInsert: { createdAt: new Date() },
        },
        { upsert: true }
      );

      console.log("[alfursan] accumulators updated", { imei: payloadImei, hours: h, totalDistanceM });
      return res.json({ ok: true, imei: payloadImei, hours: h, totalDistance: totalDistanceM });
    } catch (err) {
      const msg = err?.response?.data?.message || err?.response?.data || err.message || "accumulators_failed";
      console.error("[alfursan] accumulators error:", msg);
      const code = Number(err?.statusCode) || 500;
      return res.status(code).json({ ok: false, error: typeof msg === "string" ? msg : String(msg) });
    }
  });

  // Backward-compatible command endpoint used by existing clients.
  app.post("/send", async (req, res) => {
    try {
      // arTitle/enTitle/arBody/enBody اختيارية لإشعار FCM وتخزينها في command_response
      const body = req.body || {};
      const arTitle = body.arTitle ?? body.cmdArTitle;
      const enTitle = body.enTitle ?? body.cmdEnTitle;
      const arBody = body.arBody ?? body.cmdArBody;
      const enBody = body.enBody ?? body.cmdEnBody;
      const { imei, command, commandId } = body;
      const payloadCommand = String(command || "").trim();
      const payloadImei = imei ? String(imei).trim() : null;

      if (!payloadCommand) {
        return res.status(400).send("âŒ command is required");
      }
      if (!payloadImei) {
        return res.status(400).send("âŒ imei is required");
      }
      if (!traccarHttpClient) {
        return res.status(503).send("âŒ Traccar REST client is not ready");
      }

      const targetDeviceId = await resolveRuntimeDeviceIdByImei(payloadImei);

      await traccarHttpClient.post("/api/commands/send", {
        type: "custom",
        attributes: { data: payloadCommand , noQueue: true },
        deviceId: targetDeviceId,
      });

      const cmdArTitle = String(arTitle || "").trim() || `تم إرسال أمر إلى الجهاز ${payloadImei}`;
      const cmdEnTitle = String(enTitle || "").trim() || `Command sent to device ${payloadImei}`;
      const cmdArBody = String(arBody || "").trim() || payloadCommand;
      const cmdEnBody = String(enBody || "").trim() || payloadCommand;

      setImmediate(async () => {
        try {
          await CommandResponse.create({
            imei: payloadImei,
            command: payloadCommand,
            commandId: commandId || null,
            sentAt: new Date(),
            status: "pending",
            arTitle: cmdArTitle,
            enTitle: cmdEnTitle,
            arBody: cmdArBody,
            enBody: cmdEnBody,
            traccar_runtime_device_id: targetDeviceId,
          });
          await SEND_NOTIFY_TO_CLIENT(payloadImei, cmdArTitle, cmdArBody, {
            type: "command_sent",
            imei: payloadImei,
            command: payloadCommand,
            enTitle: cmdEnTitle,
            enBody: cmdEnBody,
          });
        } catch (err) {
          console.error("Error saving command / notify:", err.message);
        }
      });

      return res.send("✅ تم إرسال الأمر للجهاز عبر Traccar");
    } catch (err) {
      const message = err?.response?.data?.message || err.message || "command_send_failed";
      console.error("Command send error:", message);
      const statusCode = Number(err?.statusCode || err?.response?.status) || 500;
      return res.status(statusCode).send(`âŒ ${message}`);
    }
  });

  const server = http.createServer(app);
  const wss = new WebSocket.Server({
    server,
    origin: ["*"],
  });
  subscribersWss = wss;
  const subscriberHeartbeat = attachSubscriberHeartbeat({
    intervalMs: SUBSCRIBER_HEARTBEAT_MS,
    metrics: bridgeMetrics,
    getClients: () => wss.clients,
    onDead: (ws) => cleanupSubscriberSocket(ws, "heartbeat"),
  });

  wss.on("connection", (ws) => {
    ws.subscriptions = new Set();
    subscriberHeartbeat.initSocket(ws);

    sendWsSafe(ws, {
      type: "welcome",
      mode: "traccar-bridge",
      subscribe_message: { type: "subscribe", imei: "YOUR_IMEI" },
      tenant_room_message: { type: "subscribe_tenant_room", room: "tenant:YOUR_TENANT_ID" },
      command_response_channel_message: {
        type: "subscribe_command_response_channel",
        channel: COMMAND_RESPONSE_CHANNEL,
      },
    });

    ws.on("message", async (msg) => {
      let data = null;
      try {
        data = JSON.parse(msg.toString());
      } catch (e) {
        sendWsSafe(ws, { type: "error", message: "invalid_json" });
        return;
      }

      if (!data || typeof data !== "object") return;

      if (
        data.type === "subscribe_command_response_channel" ||
        (data.type === "subscribe_tenant_room" &&
          String(data.room || "").trim() === COMMAND_RESPONSE_CHANNEL)
      ) {
        joinRoom(ws, COMMAND_RESPONSE_CHANNEL);
        cmdChannelBp("subscribe", {
          listeners: rooms.get(COMMAND_RESPONSE_CHANNEL)?.size || 0,
        });
        sendWsSafe(ws, {
          type: "command_response_channel_subscribed",
          channel: COMMAND_RESPONSE_CHANNEL,
        });
        return;
      }

      if (
        data.type === "unsubscribe_command_response_channel" ||
        (data.type === "unsubscribe_tenant_room" &&
          String(data.room || "").trim() === COMMAND_RESPONSE_CHANNEL)
      ) {
        leaveRoom(ws, COMMAND_RESPONSE_CHANNEL);
        cmdChannelBp("unsubscribe", {
          listeners: rooms.get(COMMAND_RESPONSE_CHANNEL)?.size || 0,
        });
        sendWsSafe(ws, {
          type: "command_response_channel_unsubscribed",
          channel: COMMAND_RESPONSE_CHANNEL,
        });
        return;
      }

      if (data.type === "subscribe_tenant_room") {
        const room = data.room ? String(data.room).trim() : null;
        if (!room || !/^tenant:\d+$/.test(room)) {
          sendWsSafe(ws, { type: "error", message: "invalid_tenant_room" });
          return;
        }
        // TODO: verify room against authenticated tenant session (no WS auth today).
        joinRoom(ws, room);
        sendWsSafe(ws, { type: "tenant_room_subscribed", room });
        return;
      }

      if (data.type === "unsubscribe_tenant_room") {
        const room = data.room ? String(data.room).trim() : null;
        if (!room) {
          sendWsSafe(ws, { type: "error", message: "missing_room" });
          return;
        }
        leaveRoom(ws, room);
        sendWsSafe(ws, { type: "tenant_room_unsubscribed", room });
        return;
      }

      if (data.type === "subscribe" || data.type === "subscribe_device") {
        let imei = data.imei ? String(data.imei) : null;
        if (!imei && data.deviceId != null) {
          imei = await resolveImei(data.deviceId);
        }
        if (!imei) {
          sendWsSafe(ws, { type: "error", message: "missing_imei_or_mapping" });
          return;
        }
        subscribeClient(ws, imei);
        sendWsSafe(ws, { type: "subscribed", imei });
        return;
      }

      if (data.type === "unsubscribe") {
        const imei = data.imei ? String(data.imei) : null;
        if (!imei) {
          sendWsSafe(ws, { type: "error", message: "missing_imei" });
          return;
        }
        unsubscribeClient(ws, imei);
        sendWsSafe(ws, { type: "unsubscribed", imei });
        return;
      }

      if (data.type === "subscriptions") {
        sendWsSafe(ws, {
          type: "subscriptions",
          imeis: Array.from(ws.subscriptions || []),
        });
      }
    });

    ws.on("close", () => {
      cleanupSubscriberSocket(ws, "close");
    });

    ws.on("error", () => {
      cleanupSubscriberSocket(ws, "error");
    });
  });

  server.listen(3053, "127.0.0.1", () => {
    console.log("Bridge HTTP+WS listening on http://127.0.0.1:3053");
  });
}

const ALARM_MESSAGES = {
  general: { en: "General Alarm", ar: "تنبيه عام" },
  sos: { en: "SOS Alarm", ar: "تنبيه استغاثة" },
  vibration: { en: "Vibration Alarm", ar: "تنبيه اهتزاز" },
  movement: { en: "Movement Alarm", ar: "تنبيه حركة" },
  lowspeed: { en: "Low Speed Alarm", ar: "تنبيه انخفاض السرعة" },
  overspeed: { en: "Over Speed Alarm", ar: "تنبيه تجاوز السرعة" },
  falldown: { en: "Fall Down Alarm", ar: "تنبيه سقوط" },
  lowpower: { en: "Low Power Alarm", ar: "تنبيه انخفاض الطاقة" },
  lowbattery: { en: "Low Battery Alarm", ar: "تنبيه انخفاض البطارية" },
  fault: { en: "Device Fault Alarm", ar: "تنبيه عطل بالجهاز" },
  poweroff: { en: "Power Off Alarm", ar: "تنبيه فصل الطاقة" },
  poweron: { en: "Power On Alarm", ar: "تنبيه تشغيل الطاقة" },
  door: { en: "Door Alarm", ar: "تنبيه باب" },
  lock: { en: "Lock Alarm", ar: "تنبيه قفل" },
  unlock: { en: "Unlock Alarm", ar: "تنبيه فتح القفل" },
  geofence: { en: "Geofence Alarm", ar: "تنبيه سياج جغرافي" },
  geofenceenter: { en: "Geofence Enter", ar: "دخول إلى السياج الجغرافي" },
  geofenceexit: { en: "Geofence Exit", ar: "خروج من السياج الجغرافي" },
  gpsantennacut: { en: "GPS Antenna Cut Alarm", ar: "تنبيه فصل هوائي GPS" },
  accident: { en: "Accident Alarm", ar: "تنبيه حادث" },
  tow: { en: "Tow Alarm", ar: "تنبيه سحب" },
  idle: { en: "Idle Alarm", ar: "تنبيه وضع الخمول" },
  highrpm: { en: "High RPM Alarm", ar: "تنبيه ارتفاع RPM" },
  hardacceleration: { en: "Hard Acceleration Alarm", ar: "تنبيه تسارع عنيف" },
  hardbraking: { en: "Hard Braking Alarm", ar: "تنبيه فرملة عنيفة" },
  hardcornering: { en: "Hard Cornering Alarm", ar: "تنبيه انعطاف عنيف" },
  lanechange: { en: "Lane Change Alarm", ar: "تنبيه تغيير مسار" },
  fatiguedriving: { en: "Fatigue Driving Alarm", ar: "تنبيه إجهاد السائق" },
  powercut: { en: "Power Cut Alarm", ar: "تنبيه انقطاع الطاقة" },
  powerrestored: { en: "Power Restored Alarm", ar: "تنبيه عودة الطاقة" },
  jamming: { en: "Jamming Alarm", ar: "تنبيه تشويش" },
  temperature: { en: "Temperature Alarm", ar: "تنبيه درجة الحرارة" },
  parking: { en: "Parking Alarm", ar: "تنبيه اصطفاف" },
  bonnet: { en: "Bonnet Alarm", ar: "تنبيه غطاء المحرك" },
  footbrake: { en: "Foot Brake Alarm", ar: "تنبيه فرامل القدم" },
  fuelleak: { en: "Fuel Leak Alarm", ar: "تنبيه تسرب الوقود" },
  tampering: { en: "Tampering Alarm", ar: "تنبيه عبث بالجهاز" },
  removing: { en: "Removing Alarm", ar: "تنبيه إزالة الجهاز" },
};

const ALARM_TOKEN_ALIASES = {
  lowbat: "lowbattery",
  batterylow: "lowbattery",
  devicebatterylow: "lowbattery",
};

function normalizeAlarmToken(token) {
  const normalized = String(token || "")
    .trim()
    .replace(/[\s_-]+/g, "")
    .toLowerCase();
  return ALARM_TOKEN_ALIASES[normalized] || normalized;
}

function removeIgnoredAlarmCodes(codes) {
  return (codes || []).filter((c) => !IGNORED_ALARM_CODES.has(normalizeAlarmToken(c)));
}

function isIgnoredAlarmOnly(codes) {
  const normalized = (codes || []).map((c) => normalizeAlarmToken(c)).filter(Boolean);
  return normalized.length > 0 && normalized.every((c) => IGNORED_ALARM_CODES.has(c));
}

function pickLegacyAlarmType(alarmCodes) {
  const codes = Array.isArray(alarmCodes) ? alarmCodes : [];
  const map = {
    sos: 0x01,
    poweroff: 0x02,
    powercut: 0x02,
    vibration: 0x03,
    geofenceenter: 0x04,
    geofenceexit: 0x05,
    overspeed: 0x06,
    temperature: 0x07,
    lowbattery: 0x0e,
    tampering: 0x13,
    hardacceleration: 0x26,
    hardbraking: 0x27,
    hardcornering: 0x28,
    accident: 0x29,
  };

  for (const c of codes) {
    if (map[c] != null) return map[c];
  }
  return 0x00;
}

function buildLegacyAlarmCompat({ position, attrs, latitude, longitude, direction, alarmMeta }) {
  const gpsInfoLen = 12;
  const satellites = Number(attrs?.sat) || 0;
  const satellite = ((gpsInfoLen & 0x0f) << 4) | (satellites & 0x0f);

  const alarmType = pickLegacyAlarmType(alarmMeta?.alarmCodes);
  const signalStrength = attrs?.rssi != null ? Number(attrs.rssi) : undefined;
  const voltageLevel = attrs?.batteryLevel != null ? Number(attrs.batteryLevel) : undefined;

  const accOn = typeof attrs?.ignition === "boolean" ? attrs.ignition : undefined;
  const charging = typeof attrs?.charge === "boolean" ? attrs.charge : undefined;
  const oilCut = typeof attrs?.blocked === "boolean" ? attrs.blocked : undefined;

  const statusDecoded = {
    oilCut: !!oilCut,
    gpsPositioned: !!position?.valid,
    alarmCode: 0,
    charging: !!charging,
    accOn: !!accOn,
    defense: false,
  };

  return {
    // legacy numeric-ish protocol used by old parsers (often "type" from attributes)
    protocol: attrs?.type ?? undefined,
    // old schemas had these fields
    serial: position?.id != null ? String(position.id) : undefined,
    satellite,
    gpsInfoLen,
    satellites,
    gps: {
      latitude,
      longitude,
    },
    courseStatus: 0,
    language: "cn",
    direction,
    gpsPositioned: !!position?.valid,
    realTimeGps: true,
    eastLongitude: Number(longitude) >= 0,
    northLatitude: Number(latitude) >= 0,
    voltageLevel,
    signalStrength,
    alarmType,
    alarmText: alarmMeta?.alarmText || undefined,
    alarmTextAr: alarmMeta?.alarmTextAr || undefined,
    statusDecoded,
    // flatten like old parser output
    oilCut: statusDecoded.oilCut,
    alarmCode: statusDecoded.alarmCode,
    charging: statusDecoded.charging,
    accOn: statusDecoded.accOn,
    defense: statusDecoded.defense,
  };
}

function mapAlarmMessage(alarmTextRaw) {
  const raw = String(alarmTextRaw || "").trim();
  if (!raw) {
    return {
      alarmText: "",
      alarmTextAr: "تنبيه من الجهاز",
      alarmCodes: [],
    };
  }

  const parts = raw
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  const uniqueCodes = Array.from(new Set(parts.map((p) => normalizeAlarmToken(p))));
  const enParts = [];
  const arParts = [];

  for (const code of uniqueCodes) {
    const msg = ALARM_MESSAGES[code];
    if (msg) {
      enParts.push(msg.en);
      arParts.push(msg.ar);
    } else {
      enParts.push(code);
      arParts.push(`تنبيه: ${code}`);
    }
  }

  return {
    alarmText: enParts.join(" | "),
    alarmTextAr: arParts.join(" | "),
    alarmCodes: uniqueCodes,
  };
}

function buildAlarmMessageFromCodes(codes) {
  const uniqueCodes = Array.from(new Set((codes || []).map((c) => normalizeAlarmToken(c)).filter(Boolean)));
  if (!uniqueCodes.length) {
    return {
      alarmText: "",
      alarmTextAr: "تنبيه من الجهاز",
      alarmCodes: [],
    };
  }

  const enParts = [];
  const arParts = [];
  for (const code of uniqueCodes) {
    const msg = ALARM_MESSAGES[code];
    if (msg) {
      enParts.push(msg.en);
      arParts.push(msg.ar);
    } else {
      enParts.push(code);
      arParts.push(`تنبيه: ${code}`);
    }
  }

  return {
    alarmText: enParts.join(" | "),
    alarmTextAr: arParts.join(" | "),
    alarmCodes: uniqueCodes,
  };
}

async function fetchTraccarDeviceById(deviceId, reason = "missing_map") {
  if (!traccarHttpClient) {
    console.warn("Traccar client not ready for device fetch", { deviceId, reason });
    return null;
  }
  try {
    const { data } = await traccarHttpClient.get(`/api/devices/${deviceId}`);
    if (!data) {
      console.warn("Traccar device fetch returned empty", { deviceId, reason });
      return null;
    }
    const imei = warmImeiCacheFromTraccarDevice(data);
    if (imei) {
      console.warn("Resolved device by id via Traccar", { deviceId, imei, reason });
      return imei;
    }
  } catch (err) {
    console.warn("Traccar device fetch failed", { deviceId, reason, error: err.message });
  }
  return null;
}
fetchTraccarDeviceByIdImpl = fetchTraccarDeviceById;

async function resolveImei(deviceId) {
  const key = Number(deviceId);
  if (!Number.isFinite(key)) return null;

  const cached = deviceIdToImeiCache.get(key);
  if (cached) return cached;

  // Runtime-only fallback for forwarded packets that omitted uniqueId.
  // This is cached in RAM and is never persisted as an authoritative mapping.
  const fetched = await deviceResolver.resolve(key, "resolve_imei");
  if (fetched) return fetched;

  return null;
}

async function resolveRuntimeDeviceIdByImei(imei) {
  return resolveRuntimeDeviceIdByImeiViaTraccar({
    client: traccarHttpClient,
    imei,
    cache: deviceIdToImeiCache,
  });
}

async function ensureState(imei) {
  if (states.has(imei)) return states.get(imei);
  const openTrip = await Trip.findOne({ imei, is_open: true }).sort({ start_at: -1 }).lean();
  const st = {
    prevSpeed: null,
    lastNonZeroAt: null,
    currentTripId: openTrip?._id || null,
    currentDistKm: openTrip?.distance_km || 0,
    prevPacketAt: null,
  };
  states.set(imei, st);
  return st;
}

function minutesDiff(a, b) {
  return Math.floor((b.getTime() - a.getTime()) / 60000);
}

async function startTrip(imei, startAt, lat, lon) {
  return await Trip.create({
    imei,
    start_at: startAt,
    start_loc: { lat, lon },
    end_at: null,
    end_loc: null,
    distance_km: 0,
    duration_min: 0,
    gap_after_min: null,
    is_open: true,
  });
}

async function appendToTrip(tripId, packetDate, lat, lon, addKm) {
  await Trip.updateOne(
    { _id: tripId },
    {
      $set: { end_at: packetDate, end_loc: { lat, lon } },
      $inc: { distance_km: Math.max(0, Number(addKm) || 0) },
    }
  );
}

async function closeTripAtLastMove(st) {
  if (!st.currentTripId || !st.lastNonZeroAt) return;
  const cur = await Trip.findById(st.currentTripId);
  if (!cur) return;

  const durationMin = minutesDiff(cur.start_at, st.lastNonZeroAt);
  await Trip.updateOne(
    { _id: cur._id },
    {
      $set: {
        end_at: st.lastNonZeroAt,
        duration_min: Math.max(0, durationMin),
        is_open: false,
      },
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

  let shouldClose = false;
  if (st.currentTripId && prevSpeed === 0 && isMoving && prevLastMove) {
    const gap = minutesDiff(prevLastMove, packetDate);
    if (gap >= BASE_GAP_MIN) shouldClose = true;
  }
  if (shouldClose) {
    await closeTripAtLastMove(st);
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
    if ((!title || title === "title") && (!body || body === "body")) return null;

    const safeTitle = title && title !== "title" ? title : body || "تنبيه";
    const safeBody = body && body !== "body" ? body : safeTitle;

    let deviceOwnerId = null;
    let deviceName = "";
    let carnum = "";
    const deviceDetailsColl = mongoose.connection.collection("device_details");
    const deviceDetails = await deviceDetailsColl.findOne({ imei });
    if (deviceDetails) {
      deviceOwnerId = deviceDetails.device_owner_id ?? null;
      deviceName = deviceDetails.name || "";
      carnum = deviceDetails.carnum || "";
    }

    const customTitle =
      `${deviceName}${carnum ? " - " + carnum : ""}`.trim() || safeTitle;

    let successCount = 0;
    let failedCount = 0;
    let tokensCount = 0;

    if (deviceOwnerId) {
      const fcmTokensColl = mongoose.connection.collection("fcm_tokens");
      const fcmDocs = await fcmTokensColl
        .find({ user_id: deviceOwnerId })
        .project({ fcm_token: 1, user_id: 1 })
        .toArray();
      tokensCount = fcmDocs.length;

      for (const tokenDoc of fcmDocs) {
        const token = tokenDoc.fcm_token;
        if (!token) continue;
        try {
          await sendPushNotification({
            token,
            title: customTitle,
            body: safeBody,
            data,
          });
          successCount++;
        } catch (err) {
          failedCount++;
          const code = err?.errorInfo?.code || err?.code;
          if (
            code === "messaging/invalid-registration-token" ||
            code === "messaging/registration-token-not-registered"
          ) {
            await fcmTokensColl.deleteOne({ _id: tokenDoc._id });
          }
        }
      }
    }

    // type=alarm: mirrored from gps_logs post-save hook (mirrorGpsAlarmToNotification)
    const skipDbPersist = data?.type === "alarm";
    if (!skipDbPersist) {
      await persistTenantNotification({
        imei,
        user_id: deviceOwnerId,
        title: customTitle,
        body: safeBody,
        data,
        device_name: deviceName,
        carnum,
        tokens_count: tokensCount,
        success_count: successCount,
        failed_count: failedCount,
      });
    }
  } catch (err) {
    console.error("SEND_NOTIFY_TO_CLIENT error:", err.message);
  }
}

// ===== hooks تجاوز السرعة: إشعارات فقط (لا يغيّر منطق overspeedService الأساسي) =====
async function traccarOverspeedOnStart({ imei, packetDate, lat, lon, speed, speed_limit_kmh }) {
  try {
    await SEND_NOTIFY_TO_CLIENT(
      imei,
      `تنبيه ${imei}`,
      `بدء تجاوز السرعة — السرعة الحالية تقريباً ${round1(speed)} كم/س`,
      {
        type: "alarm",
        subType: "overspeed_start",
        alarmType: "OVERSPEED",
        alarmText: `Overspeed started (limit ${speed_limit_kmh ?? "?"} km/h)`,
        alarmTextAr: `بدء تجاوز السرعة (الحد ${speed_limit_kmh ?? "—"} كم/س)`,
        imei,
        latitude: lat,
        longitude: lon,
        speed: round1(speed),
        speed_limit_kmh: speed_limit_kmh,
      }
    );
  } catch (e) {
    console.error("overspeed start notify error:", e.message);
  }
}

async function traccarOverspeedOnEnd(payload) {
  try {
    await SEND_NOTIFY_TO_CLIENT(
      payload.imei,
      `تنبيه ${payload.imei}`,
      `انتهاء تجاوز السرعة — أقصى سرعة ${payload.speed_kmh} كم/س`,
      {
        type: "alarm",
        subType: "overspeed_end",
        alarmType: "OVERSPEED",
        alarmText: `Overspeed ended: max ${payload.speed_kmh} km/h`,
        alarmTextAr: `انتهاء تجاوز السرعة — أقصى سرعة ${payload.speed_kmh} كم/س`,
        imei: payload.imei,
        latitude: payload.end_lat,
        longitude: payload.end_lon,
        duration_sec: payload.duration_sec,
        distance_km: payload.distance_km,
        speed: payload.speed_kmh,
      }
    );
  } catch (e) {
    console.error("overspeed end notify error:", e.message);
  }
}

const TRACCAR_OVERSPEED_HOOKS = {
  onOverspeedStart: traccarOverspeedOnStart,
  onOverspeedEnd: traccarOverspeedOnEnd,
};

function emitPositionToSocketSubscribers(imei, docType, subscriberData, { immediate = false } = {}) {
  const payload = { type: docType, data: subscriberData };
  if (immediate) broadcastToDeviceSubscribersImmediate(imei, payload);
  else broadcastToDeviceSubscribers(imei, payload);
}

function buildCommandResponseSubscriberPayload(imei, position, commandResponseText, extra = {}) {
  const attrs = position?.attributes || {};
  const packetDate = toDate(position?.fixTime || position?.deviceTime || position?.serverTime);
  const serverDate = toDate(position?.serverTime, packetDate);
  const speedKnots = num(position?.speed, 0);
  const speed = speedKnots * 1.852;
  const speedRounded = round1(speed);
  const direction = num(position?.course, 0);
  const latitude = Number(position?.latitude);
  const longitude = Number(position?.longitude);
  const legacy = parseCommandResponseLegacyFields(commandResponseText);
  const fixIso =
    packetDate && !Number.isNaN(packetDate.getTime()) ? packetDate.toISOString() : null;

  let positionBlock = legacy.position;
  if (!positionBlock && isValidGpsCoord(latitude, longitude)) {
    positionBlock = {
      lat: latitude,
      lon: longitude,
      speed: speedRounded,
      course: direction,
      datetime: fixIso,
    };
  }

  return {
    type: "command_response",
    data: {
      imei,
      type: "command_response",
      response: commandResponseText,
      packet_date: packetDate,
      date: serverDate,
      latitude,
      longitude,
      speed: speedRounded,
      direction,
      deviceId: position?.deviceId ?? null,
      attributes: { ...attrs, result: commandResponseText },
      // traccar_raw: slimTraccarRaw(position),
      position: positionBlock,
      status: legacy.status,
      command_result: legacy.command_result,
      gps: {
        latitude,
        longitude,
        speed: speedRounded,
        direction,
      },
      ...extra,
    },
  };
}

function buildCommandResponseGpsDoc(imei, position, commandResponseText) {
  const attrs = position?.attributes || {};
  const packetDate = toDate(position?.fixTime || position?.deviceTime || position?.serverTime);
  const serverDate = toDate(position?.serverTime, packetDate);
  const speedKnots = num(position?.speed, 0);
  const speed = speedKnots * 1.852;
  const speedRounded = round1(speed);
  const direction = num(position?.course, 0);
  const latitude = Number(position?.latitude);
  const longitude = Number(position?.longitude);

  return {
    imei,
    type: "command_response",
    response: commandResponseText,
    packet_date: packetDate,
    date: serverDate,
    latitude,
    longitude,
    speed,
    course: direction,
    direction,
    valid: !!position?.valid,
    attributes: attrs,
    deviceId: position?.deviceId ?? null,
    traccar_position_id: normalizeTraccarPositionId(position?.id),
    gps: {
      latitude,
      longitude,
      speed: speedRounded,
      direction,
    },
  };
}

/**
 * Command response: broadcast to socket subscribers immediately, persist in background.
 */
function persistCommandResponseFast(imei, position, commandResponseText) {
  // console.warn(' start persistCommandResponseFast' ) ;

  const wirePayload = buildCommandResponseSubscriberPayload(imei, position, commandResponseText);
  const doc = buildCommandResponseGpsDoc(imei, position, commandResponseText);
  cmdChannelBp("persistCommandResponseFast", {
    imei,
    responsePreview: String(commandResponseText || "").slice(0, 80),
  });
  // console.warn(' broadcast persistCommandResponseFast' ) ;
  broadcastToDeviceSubscribersImmediate(imei, wirePayload);
  // console.warn(' end broadcast persistCommandResponseFast' ) ;

  setImmediate(() => {
    persistCommandResponseBackground(imei, position, doc, commandResponseText, wirePayload).catch(
      (err) => {
        console.error("command_response background persist error:", err.message);
      }
    );
  });

  // console.warn(' end persistCommandResponseFast' ) ;
}

/** Returns true when broadcast was sent (cache hit). */
function tryProcessCommandResponseIngressSync(position) {
  // console.warn('start tryProcessCommandResponseIngressSync' ) ;
  const commandResponseText = String(position?.attributes?.result || "").trim();
  if (!commandResponseText) return true;
  const imei = resolveImeiFromCache(position?.deviceId);
  if (!imei) return false;
  // console.warn(' persistCommandResponseFast' ) ;
  persistCommandResponseFast(imei, position, commandResponseText);
  return true;
}

function processCommandResponseIngressDeferred(position) {
  const commandResponseText = String(position?.attributes?.result || "").trim();
  if (!commandResponseText) return Promise.resolve();
  return resolveImei(position?.deviceId).then((imei) => {
    if (!imei) {
      console.warn("command_response skipped (no IMEI)", { deviceId: position?.deviceId });
      return;
    }
    persistCommandResponseFast(imei, position, commandResponseText);
  });
}

function warmImeiCacheFromForwardItem(item) {
  const imei = item?.imei ? String(item.imei).trim() : "";
  const deviceId = Number(item?.runtimeDeviceId ?? item?.position?.deviceId);
  if (!imei || !Number.isFinite(deviceId)) return null;
  deviceIdToImeiCache.set(deviceId, imei);
  return imei;
}

function resolveImeiForForwardItem(item) {
  const direct = item?.imei ? String(item.imei).trim() : "";
  if (direct) return direct;
  return resolveImeiFromCache(item?.runtimeDeviceId ?? item?.position?.deviceId);
}

async function processForwardIngressAsync(normalized, receivedAt) {
  const items = Array.isArray(normalized?.items) ? normalized.items : [];
  if (!items.length) return;

  const grouped = new Map();
  for (const item of items) {
    warmImeiCacheFromForwardItem(item);
    const observedImei = resolveImeiForForwardItem(item);
    if (observedImei) {
      scheduleTraccarModelSyncForDevice({
        imei: observedImei,
        runtimeDeviceId: item?.runtimeDeviceId ?? item?.position?.deviceId,
        currentModel: item?.position?.deviceModel ?? item?.runtimeDevice?.model,
        forwardedDevice: item?.runtimeDevice,
      });
    }
    const position = item.position;
    const commandText = String(position?.attributes?.result || "").trim();
    if (commandText) {
      const imei = observedImei;
      if (imei) {
        persistCommandResponseFast(imei, position, commandText);
      } else if (!tryProcessCommandResponseIngressSync(position)) {
        void processCommandResponseIngressDeferred(position).catch((err) => {
          console.error("forward command_response deferred error:", err.message);
        });
      }
      continue;
    }

    const imei = observedImei;
    if (imei) {
      if (!grouped.has(imei)) grouped.set(imei, []);
      grouped.get(imei).push(position);
    } else {
      persistPosition(position, { source: "traccar_forward", receivedAt });
    }
  }

  for (const [imei, positions] of grouped.entries()) {
    processGpsBurst(imei, positions, { source: "traccar_forward", receivedAt, positions });
  }
}

async function persistCommandResponseBackground(
  imei,
  position,
  doc,
  commandResponseText,
  wirePayload
) {
  try {
    const responseArTitle = "تم استلام الرد على الأمر";
    const responseEnTitle = "Command response received";
    const responseRaw = String(commandResponseText || "").trim() || "—";

    const attrCommandId =
      position?.attributes?.commandId ||
      position?.attributes?.cmdId ||
      doc?.commandId ||
      null;
    const filter = attrCommandId
      ? { imei, commandId: String(attrCommandId), status: "pending" }
      : { imei, status: "pending" };
    let updated = await CommandResponse.findOneAndUpdate(
      filter,
      {
        $set: {
          response: commandResponseText,
          respondedAt: new Date(),
          status: "responded",
          responseArTitle,
          responseEnTitle,
          responseArBody: responseRaw,
          responseEnBody: responseRaw,
        },
      },
      { sort: { sentAt: -1 }, new: true }
    );
    if (!updated && attrCommandId) {
      updated = await CommandResponse.findOneAndUpdate(
        { imei, status: "pending" },
        {
          $set: {
            response: commandResponseText,
            respondedAt: new Date(),
            status: "responded",
            responseArTitle,
            responseEnTitle,
            responseArBody: responseRaw,
            responseEnBody: responseRaw,
          },
        },
        { sort: { sentAt: -1 }, new: true }
      );
    }

    if (updated) {
      const commandRaw = String(updated.command || "").trim() || "—";
      // Clear bilingual bodies: command + device reply (not reply alone like "OK!")
      const responseArBody = `الأمر: ${commandRaw}\nالرد: ${responseRaw}`;
      const responseEnBody = `Command: ${commandRaw}\nResponse: ${responseRaw}`;

      await CommandResponse.updateOne(
        { _id: updated._id },
        { $set: { responseArBody, responseEnBody } }
      );

      await SEND_NOTIFY_TO_CLIENT(imei, responseArTitle, responseArBody, {
        type: "command_response",
        imei,
        response: responseRaw,
        command: commandRaw,
        enTitle: responseEnTitle,
        enBody: responseEnBody,
        alarmText: responseEnBody,
        alarmTextAr: responseArBody,
      });
    }
  } catch (err) {
    console.error("Error updating command response in MongoDB:", err.message);
  }

  if (wirePayload) {
    broadcastToDeviceSubscribersImmediate(imei, {
      ...wirePayload,
      data: { ...wirePayload.data, persisted: true },
    });
  }
}

function persistPosition(position, rawPayload, options = {}) {
  const imei = resolveImeiFromCache(position?.deviceId);
  if (!imei) {
    const deviceId = position?.deviceId;
    deviceResolver.enqueuePending(deviceId, { position, rawPayload, options });
    void deviceResolver.resolve(deviceId, "persist_position").then((resolved) => {
      const pending = deviceResolver.takePending(deviceId);
      if (!resolved) {
        console.warn("Skipping position without IMEI mapping for deviceId:", deviceId);
        return;
      }
      processGpsBurst(resolved, pending.map((row) => row.position), rawPayload);
    }).catch((err) => console.error("resolveImei error:", err.message));
    return;
  }
  persistPositionBody(imei, position, rawPayload, options);
}

function processGpsBurst(imei, positions, rawPayload) {
  const list = Array.isArray(positions) ? positions.slice() : [];
  if (!list.length) return;
  list.sort((a, b) => (resolveFixMs(a) || 0) - (resolveFixMs(b) || 0));
  const picked = pickLatestLiveFromBurst(list, {
    nowMs: Date.now(),
    lastLiveFixMs: liveFixTracker.peekFix(imei),
    lastEffectiveLiveMs: liveFixTracker.peek(imei),
    maxLiveAgeMs: MAX_LIVE_FIX_AGE_MS,
    futureToleranceMs: LIVE_FIX_FUTURE_TOLERANCE_MS,
    allowDeviceTimeFallback: LIVE_DEVICE_TIME_FALLBACK,
    previous: liveFixTracker.peekSeen(imei),
    deviceTimeMaxAgeMs: LIVE_DEVICE_TIME_MAX_AGE_MS,
    deviceTimeFutureToleranceMs: LIVE_DEVICE_TIME_FUTURE_TOLERANCE_MS,
    deviceServerMaxSkewMs: LIVE_DEVICE_SERVER_MAX_SKEW_MS,
  });
  bridgeMetrics.live_scan_finished_at = new Date().toISOString();

  if (picked.liveIndex >= 0) {
    persistPositionBody(imei, list[picked.liveIndex], rawPayload, {
      emitLive: true,
      burstSuperseded: false,
      liveIndex: picked.liveIndex,
      batchSize: list.length,
    });
  }

  for (let i = 0; i < list.length; i++) {
    if (i === picked.liveIndex) continue;
    persistPositionBody(imei, list[i], rawPayload, {
      emitLive: false,
      burstSuperseded: picked.liveIndex >= 0,
      liveIndex: picked.liveIndex,
      batchSize: list.length,
    });
  }
}

function persistPositionBody(imei, position, rawPayload, options = {}) {
  const rawAttrs = position?.attributes || {};
  const attrs = mergeStickyAttributesForImei(imei, rawAttrs);
  if (positionHasCommandResponse(position)) {
    persistCommandResponseFast(imei, position, String(attrs.result).trim());
    return;
  }

  const latitude = Number(position?.latitude);
  const longitude = Number(position?.longitude);
  // Traccar position.speed is in knots -> convert to km/h
  const speedKnots = num(position?.speed, 0);
  const speed = speedKnots * 1.852;
  const speedRounded = round1(speed);
  const direction = num(position?.course, 0);
  const packetDate = toDate(position?.fixTime || position?.deviceTime || position?.serverTime);
  const serverDate = toDate(position?.serverTime, packetDate);
  const alarmTextRaw = attrs?.alarm ? String(attrs.alarm) : "";
  const alarmMetaRaw = mapAlarmMessage(alarmTextRaw);
  // Ignore muted alarms in persisted alarm payload (store other alarms if present).
  const alarmCodesNoIgnored = removeIgnoredAlarmCodes(alarmMetaRaw.alarmCodes || []);
  const ignoredAlarmOnly = isIgnoredAlarmOnly(alarmMetaRaw.alarmCodes || []);
  const alarmMeta = buildAlarmMessageFromCodes(alarmCodesNoIgnored);
  const isAlarm = alarmMeta.alarmCodes.length > 0;
  const hasValidCoords = isValidGpsCoord(latitude, longitude);

  let distanceDiff = 0;
  let jumpMeta = null;
  const lastPoint = lastGpsPointByImei.get(imei);
  if (lastPoint && isFiniteCoord(latitude, longitude)) {
    const res = calcDistanceDiffSafe(
      { lat: lastPoint.lat, lon: lastPoint.lon, date: lastPoint.date },
      { lat: latitude, lon: longitude, date: packetDate }
    );
    distanceDiff = res.distanceKm;
    if (res.isJump) {
      jumpMeta = {
        jump_detected: true,
        jump_reason: res.reason,
        jump_raw_km: res.rawKm,
        jump_dt_min: res.dtMin,
        jump_implied_kmh: res.impliedKmh,
      };
    }
  }
  if (isFiniteCoord(latitude, longitude)) {
    lastGpsPointByImei.set(imei, { lat: latitude, lon: longitude, date: packetDate });
  }

  const doc = {
    imei,
    type: isAlarm ? "alarm" : "gps",
    packet_date: packetDate,
    date: serverDate,
    latitude,
    longitude,
    altitude: num(position?.altitude, 0),
    speed,
    course: direction,
    direction,
    valid: !!position?.valid,
    accuracy: num(position?.accuracy, 0),
    protocol: position?.protocol || null,
    deviceId: position?.deviceId ?? null,
    traccar_position_id: normalizeTraccarPositionId(position?.id),
    traccar_server_time: position?.serverTime || null,
    traccar_device_time: position?.deviceTime || null,
    traccar_fix_time: position?.fixTime || null,
    network: position?.network ?? null,
    geofenceIds: position?.geofenceIds ?? null,
    attributes: attrs,
    // ✅ make idleStatsService.isAccOn() work with Traccar ignition
    accOn: typeof attrs?.ignition === "boolean" ? attrs.ignition : undefined,
    acc_status: typeof attrs?.ignition === "boolean" ? (attrs.ignition ? "on" : "off") : undefined,
    gps: {
      latitude,
      longitude,
      speed: speedRounded,
      direction,
      angle: direction,
      altitude: num(position?.altitude, 0),
      valid: !!position?.valid,
      accuracy: num(position?.accuracy, 0),
    },
    distanceDiff,
    ...(jumpMeta || {}),
  };

  if (isAlarm) {
    doc.alarm = alarmTextRaw;
    doc.alarmCode = alarmTextRaw;
    doc.alarmCodes = alarmMeta.alarmCodes;
    doc.alarmText = alarmMeta.alarmText;
    doc.alarmTextAr = alarmMeta.alarmTextAr;

    Object.assign(
      doc,
      buildLegacyAlarmCompat({
        position,
        attrs,
        latitude,
        longitude,
        direction,
        alarmMeta,
      })
    );
  }

  // If it was ONLY a muted alarm (e.g. tampering), do not store as alarm fields (keep GPS point only).
  if (ignoredAlarmOnly) {
    doc.type = "gps";
    delete doc.alarm;
    delete doc.alarmCode;
    delete doc.alarmCodes;
    delete doc.alarmText;
    delete doc.alarmTextAr;
    delete doc.alarmType;
    delete doc.signalStrength;
    delete doc.voltageLevel;
    delete doc.statusDecoded;
    delete doc.oilCut;
    delete doc.alarmCode;
    delete doc.charging;
    delete doc.accOn;
    delete doc.defense;
    delete doc.gpsInfoLen;
    delete doc.satellites;
    delete doc.satellite;
    delete doc.courseStatus;
    delete doc.language;
    delete doc.gpsPositioned;
    delete doc.realTimeGps;
    delete doc.eastLongitude;
    delete doc.northLatitude;
    delete doc.serial;
    delete doc.gps;
  }

  const subscriberLegacyDoc = {
    ...doc,
    speed: speedRounded,
    gps: { ...(doc.gps || {}), speed: speedRounded },
  };
  const subscriberData = mergeLegacyAndTraccarPayload({
    legacyDoc: subscriberLegacyDoc,
    traccarData: position,
    traccarType: "position",
    source: "traccar_position",
  });

  const receivedAt = Date.now();
  const previousSeen = liveFixTracker.peekSeen(imei);
  const cls = liveFixTracker.classify(imei, position, {
    nowMs: receivedAt,
    maxLiveAgeMs: MAX_LIVE_FIX_AGE_MS,
    futureToleranceMs: LIVE_FIX_FUTURE_TOLERANCE_MS,
    allowDeviceTimeFallback: LIVE_DEVICE_TIME_FALLBACK,
    deviceTimeMaxAgeMs: LIVE_DEVICE_TIME_MAX_AGE_MS,
    deviceTimeFutureToleranceMs: LIVE_DEVICE_TIME_FUTURE_TOLERANCE_MS,
    deviceServerMaxSkewMs: LIVE_DEVICE_SERVER_MAX_SKEW_MS,
  });
  recordLiveClockSplit(bridgeMetrics, cls, position);
  recordPipelineEligibility(cls);
  const emitLive = options.emitLive !== false;
  let liveDecision = cls.decision;
  if (options.burstSuperseded && cls.liveEligible) liveDecision = "superseded";

  const acceptedLive =
    emitLive &&
    cls.liveEligible &&
    cls.effectiveLiveMs != null &&
    liveFixTracker.acceptLive(imei, cls.effectiveLiveMs, {
      source: cls.effectiveLiveSource,
      fixMs: cls.fixMs,
    });
  imeiDebugger.logPosition({
    imei,
    position_id: position?.id,
    protocol: position?.protocol,
    server_received_at: receivedAt,
    fixTime: position?.fixTime,
    deviceTime: position?.deviceTime,
    serverTime: position?.serverTime,
    fix_age_ms: cls.fixAgeMs,
    device_age_ms: cls.deviceAgeMs,
    server_age_ms: cls.serverAgeMs,
    latitude: position?.latitude,
    longitude: position?.longitude,
    speed: position?.speed,
    course: position?.course,
    valid: position?.valid,
    outdated: position?.outdated,
    attributes_motion: attrs?.motion,
    attributes_ignition: attrs?.ignition,
    attributes_type: attrs?.type,
    previous_fixTime: previousSeen?.fixTime ?? null,
    previous_deviceTime: previousSeen?.deviceTime ?? null,
    previous_serverTime: previousSeen?.serverTime ?? null,
    fixTime_changed: String(previousSeen?.fixTime) !== String(position?.fixTime),
    deviceTime_changed: String(previousSeen?.deviceTime) !== String(position?.deviceTime),
    coordinates_changed:
      previousSeen != null &&
      (Number(previousSeen.latitude) !== Number(position?.latitude) ||
        Number(previousSeen.longitude) !== Number(position?.longitude)),
    live_decision: liveDecision,
    liveEligible: cls.liveEligible,
    effective_live_ms: cls.effectiveLiveMs,
    effective_live_source: cls.effectiveLiveSource,
    effectiveLiveMs: cls.effectiveLiveMs,
    effectiveLiveSource: cls.effectiveLiveSource,
    live_emitted: !!acceptedLive,
    last_live_fix_ms: liveFixTracker.peekFix(imei),
    last_effective_live_ms: liveFixTracker.peekEffective(imei),
    device_server_skew_ms: cls.deviceServerSkewMs,
    fallback_reject_reason: cls.fallbackRejectReason,
    tenant_emit: null,
    packet_date: packetDate,
  });
  if (acceptedLive) {
    emitPositionToSocketSubscribers(imei, doc.type, subscriberData, { immediate: true });
    bridgeMetrics.live_emit_at = new Date().toISOString();
    recordLiveDecision(bridgeMetrics, cls.decision);
    maybeLatencyLog(BRIDGE_ENV.BRIDGE_LATENCY_DEBUG, {
      imei,
      traccar_position_id: normalizeTraccarPositionId(position?.id),
      fixTime: position?.fixTime,
      deviceTime: position?.deviceTime,
      serverTime: position?.serverTime,
      bridge_received_at: receivedAt,
      bridge_live_sent_at: Date.now(),
      live_delivery_ms: Date.now() - receivedAt,
      fix_age_ms: cls.fixAgeMs,
      device_age_ms: cls.deviceAgeMs,
      server_age_ms: cls.serverAgeMs,
      live_decision: cls.decision,
      effectiveLiveSource: cls.effectiveLiveSource,
    });
  } else {
    recordLiveDecision(bridgeMetrics, liveDecision);
    maybeLatencyLog(BRIDGE_ENV.BRIDGE_LATENCY_DEBUG, {
      imei,
      traccar_position_id: normalizeTraccarPositionId(position?.id),
      fixTime: position?.fixTime,
      deviceTime: position?.deviceTime,
      serverTime: position?.serverTime,
      bridge_received_at: receivedAt,
      fix_age_ms: cls.fixAgeMs,
      device_age_ms: cls.deviceAgeMs,
      server_age_ms: cls.serverAgeMs,
      live_decision: liveDecision,
      live_drop_reason: liveDecision,
      effectiveLiveSource: cls.effectiveLiveSource,
    });
  }

  bridgeMetrics.persistence_enqueue_at = new Date().toISOString();
  const persistenceAttrs = {
    ignition: typeof attrs?.ignition === "boolean" ? attrs.ignition : null,
    motion: attrs?.motion ?? null,
    charge: attrs?.charge ?? null,
    blocked: attrs?.blocked ?? null,
    power: attrs?.power ?? null,
    battery: attrs?.battery ?? null,
    batteryLevel: attrs?.batteryLevel ?? null,
    rssi: attrs?.rssi ?? null,
    alarm: attrs?.alarm ?? null,
    type: attrs?.type ?? null,
    sat: attrs?.sat ?? null,
    pdop: attrs?.pdop ?? null,
    hdop: attrs?.hdop ?? null,
    status: attrs?.status ?? null,
    hours: attrs?.hours ?? null,
    distance: attrs?.distance ?? null,
    totalDistance: attrs?.totalDistance ?? null,
  };
  const persistenceDoc = {
    imei: doc.imei,
    type: doc.type,
    subType: doc.subType,
    alarmType: doc.alarmType,
    alarmCodes: doc.alarmCodes,
    alarmText: doc.alarmText,
    alarmTextAr: doc.alarmTextAr,
    packet_date: packetDate,
    date: serverDate,
    latitude,
    longitude,
    speed,
    course: direction,
    direction,
    ignition: typeof attrs?.ignition === "boolean" ? attrs.ignition : null,
    distanceDiff,
    traccar_position_id: doc.traccar_position_id,
    protocol: position?.protocol || null,
    deviceId: position?.deviceId ?? null,
    valid: !!position?.valid,
    deviceStatus: position?.deviceStatus ?? null,
    gps: hasValidCoords
      ? {
          latitude,
          longitude,
          speed: speedRounded,
          direction,
        }
      : undefined,
  };
  enqueuePersistForImei(imei, {
    imei,
    doc: persistenceDoc,
    attrs: persistenceAttrs,
    latitude,
    longitude,
    speed,
    speedRounded,
    direction,
    packetDate,
    serverDate: serverDate,
    hasValidCoords,
    ignoredAlarmOnly,
    isHistorical: cls.isHistorical || liveDecision === "superseded" || liveDecision === "out_of_order" || liveDecision === "historical",
    liveDecision,
    alarmCodesForOverspeed: (doc.type === "alarm" && doc.alarmCodes) ? [...doc.alarmCodes] : [],
    alarmCodesForAcc: (doc.type === "alarm" && doc.alarmCodes) ? [...doc.alarmCodes] : [],
    enqueuedAt: Date.now(),
  });
}

async function persistPositionHeavy(ctx) {
  const {
    imei,
    position,
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
    liveDecision,
    alarmCodesForOverspeed,
    alarmCodesForAcc,
  } = ctx;

  // رصد خمول فوري (محرك يعمل + سرعة صفر/منخفضة مدة ≥ 5 دق) — إشعار مرة واحدة حتى الحركة
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
            if (isHistorical) return;
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
          } catch (e) {
            console.error("Idle notify error:", e.message);
          }
        },
      }
    );
  });

  if (hasValidCoords) {
    await handleParkingSample({
      imei,
      timestamp: packetDate,
      lat: latitude,
      lon: longitude,
      speed,
    });
  }

  setImmediate(() => {
    handleOverspeedSample(imei, speed, latitude, longitude, packetDate, {
      alarmCodes: alarmCodesForOverspeed,
      hooks: isHistorical ? {} : TRACCAR_OVERSPEED_HOOKS,
    }).catch((e) => console.warn("Overspeed sample error:", e.message));
  });

  const speedForAcc = speed;
  setImmediate(() => {
    let accOn = undefined;
    let triggeredBy = "ignition";
    for (const code of alarmCodesForAcc) {
      const c = String(code).toLowerCase().replace(/\s+/g, "");
      const onOff = powerEventToAccOn(c);
      if (onOff !== null) {
        accOn = onOff;
        triggeredBy = c === "poweron" ? "poweron" : c === "poweroff" ? "poweroff" : c === "powercut" ? "powercut" : "powerrestored";
        break;
      }
    }
    if (accOn === undefined && typeof attrs?.ignition === "boolean") {
      accOn = attrs.ignition;
    }
    if (accOn !== undefined) {
      if (accOn === false && speedForAcc > 0) accOn = true;
      handleAccSample(imei, accOn, latitude, longitude, packetDate, { triggeredBy });
    }
  });

  const statusDoc = await upsertDeviceStatus({
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
    batteryLevel: attrs?.batteryLevel ?? attrs?.battery ,
    ignition: attrs.ignition ?? null,
    motion: attrs.motion ?? null,
    charge: attrs.charge ?? null,
    blocked: attrs.blocked ?? null,
    rssi: attrs.rssi ?? null,
    alarm: attrs.alarm ?? null,
    deviceStatus: doc?.deviceStatus,




  });

  const totalDistanceM = Number(attrs?.totalDistance);
  const statusSet = {
    last_protocol: position?.protocol || null,
    last_device_id: position?.deviceId ?? null,
    last_valid_fix: !!position?.valid,
    ignition_on: attrs?.ignition ?? null,
    motion: attrs?.motion ?? null,
    is_parked:
      attrs?.motion === false || ((attrs?.ignition === false || attrs?.ignition == null) && speed <= 1),
    blocked: attrs?.blocked ?? null,
    signal_rssi: attrs?.rssi ?? null,
    satellites: attrs?.sat ?? null,
    pdop: attrs?.pdop ?? null,
    hdop: attrs?.hdop ?? null,
    status_code: attrs?.status ?? null,
    io: Object.fromEntries(Object.entries(attrs).filter(([k]) => /^io\d+$/i.test(k))),
    hours: attrs?.hours ?? null,
    device_total_distance: Number.isFinite(totalDistanceM) ? totalDistanceM : null,
    total_distance_m: Number.isFinite(totalDistanceM) ? totalDistanceM : null,
    distance_m: Number.isFinite(Number(attrs?.distance)) ? Number(attrs.distance) : null,
    updatedAt: new Date(),
  };
  if (!ignoredAlarmOnly) {
    statusSet.last_alarm = attrs?.alarm ?? null;
  }

  if (Number.isFinite(totalDistanceM)) {
    const km = totalDistanceM / 1000;
    statusSet.km_total = km;
    statusSet.miles_total = km * 0.621371;
    statusSet.last_mileage_at = serverDate || packetDate;
  }

  const existingStatus =
    statusDoc && typeof statusDoc.toObject === "function"
      ? statusDoc.toObject()
      : statusDoc || {};
  mergeStickyAttributesForImei(imei, attrs, existingStatus);
  applyStickyTelemetryToStatusSet(statusSet, attrs, existingStatus);

  if (!isHistorical) {
    await mongoose.connection.collection("devicestatuses").updateOne(
      { imei },
      { $set: statusSet },
      { upsert: true }
    );
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
        const geofenceSubscriberData = mergeLegacyAndTraccarPayload({
          legacyDoc: evt,
          traccarData: {
            position,
            geofenceEvent: evt,
          },
          traccarType: "geofence_event",
          source: "geofence",
        });
        broadcastToDeviceSubscribers(imei, { type: "alarm", data: geofenceSubscriberData });

        if (isHistorical) continue;
        const title = `تنبيه ${imei}`;
        const body = evt?.alarmTextAr || evt?.alarmText || "تنبيه من الجهاز";
        setImmediate(() => {
          SEND_NOTIFY_TO_CLIENT(imei, title, body, {
            type: "alarm",
            subType: evt?.subType || "geofence",
            alarmType: evt?.alarmType ?? null,
            alarmText: evt?.alarmText,
            alarmTextAr: evt?.alarmTextAr,
            fence_name: evt?.fence_name || evt?.fenceName || null,
            imei,
            latitude,
            longitude,
            speed: speedRounded,
          }).catch((e) => console.error("Geofence notify error:", e.message));
        });
      }
    } catch (e) {
      console.error("Geofence evaluation error:", e.message);
    }
  }

  if (doc.type === "gps" || doc.type === "alarm") {
    await applyTripLogic({ imei, doc, packetDate });
  }

  if (doc.type === "alarm" && !isHistorical) {
    const title = `تنبيه ${imei}`;
    const body = doc.alarmTextAr || "تنبيه من الجهاز";
    setImmediate(() => {
      SEND_NOTIFY_TO_CLIENT(imei, title, body, {
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
      }).catch((e) => console.error("Alarm notify error:", e.message));
    });
  }

}

persistPositionHeavyRef = persistPositionHeavy;

function createTraccarBearerClient() {
  if (!TRACCAR_SERVICE_TOKEN) {
    throw new Error("TRACCAR_SERVICE_TOKEN is required");
  }
  return axios.create({
    baseURL: TRACCAR_BASE,
    timeout: 15000,
    headers: { Authorization: `Bearer ${TRACCAR_SERVICE_TOKEN}` },
  });
}

async function bootBridge() {
  traccarHttpClient = createTraccarBearerClient();
  traccarModelSync = createTraccarModelSync({
    getClient: () => traccarHttpClient,
    metrics: bridgeMetrics,
    log: console,
    concurrency: BRIDGE_ENV.TRACCAR_MODEL_SYNC_CONCURRENCY,
    queueMax: BRIDGE_ENV.TRACCAR_MODEL_SYNC_QUEUE_MAX,
    modelCacheTtlMs: BRIDGE_ENV.TRACCAR_MODEL_CACHE_TTL_MS,
    negativeModelCacheTtlMs: BRIDGE_ENV.TRACCAR_MODEL_NEGATIVE_CACHE_TTL_MS,
    retryBaseMs: BRIDGE_ENV.TRACCAR_MODEL_SYNC_RETRY_BASE_MS,
    retryMaxMs: BRIDGE_ENV.TRACCAR_MODEL_SYNC_RETRY_MAX_MS,
  });
  startSubscribersServer();
  console.log("Traccar REST client configured", {
    baseURL: TRACCAR_BASE,
    auth: "bearer",
  });
  void warmImeiToRoomCacheFromMongo();
  void runStartupConnectivityReconciliation({
    traccarClient: traccarHttpClient,
    metrics: bridgeMetrics,
    log: console,
    onTraccarDevices: (devices) => {
      for (const device of devices || []) {
        const imei = device?.uniqueId ? String(device.uniqueId).trim() : "";
        const runtimeDeviceId = Number(device?.id);
        if (!imei || !Number.isFinite(runtimeDeviceId)) continue;
        deviceIdToImeiCache.set(runtimeDeviceId, imei);
        scheduleTraccarModelSyncForDevice({
          imei,
          runtimeDeviceId,
          currentModel: device?.model,
          forwardedDevice: device,
        });
      }
    },
  });
  startMileageScheduler();
  startTravelStatsScheduler({ stopThresholdsMinutes: [1, 3, 5, 10, 15, 30, 60] });
  startIdleStatsScheduler({ idleSpeedKph: 0, idleMinutes: 5, requireAccOn: true, maxGapSeconds: 10 * 60 });
  startStaticStatsScheduler();
}

async function gracefulShutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.warn("[bridge] graceful shutdown", { signal });
  try {
    await Promise.race([
      Promise.all([
        rawIngressWriter.flushAndStop(BRIDGE_ENV.BRIDGE_SHUTDOWN_TIMEOUT_MS),
        forwardQueue.flushAndStop(BRIDGE_ENV.BRIDGE_SHUTDOWN_TIMEOUT_MS),
        gpsPointWriter.flushAndStop(BRIDGE_ENV.BRIDGE_SHUTDOWN_TIMEOUT_MS),
        persistenceIpc.flushAndStop(BRIDGE_ENV.BRIDGE_SHUTDOWN_TIMEOUT_MS),
      ]),
      new Promise((r) => setTimeout(r, BRIDGE_ENV.BRIDGE_SHUTDOWN_TIMEOUT_MS)),
    ]);
  } catch (err) {
    console.warn("[bridge] shutdown flush error", err.message);
  }
  process.exit(0);
}

process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.on("SIGINT", () => gracefulShutdown("SIGINT"));

bootBridge().catch((e) => {
  console.error("Bridge failed:", e.message);
  process.exit(1);
});
