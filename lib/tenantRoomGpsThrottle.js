function extractSpeedFromPayload(payload) {
  const data = payload?.data;
  if (!data || typeof data !== "object") return null;
  const raw = Number(data.speed ?? data.gps?.speed ?? data.legacy?.speed);
  return Number.isFinite(raw) ? raw : null;
}

function extractLatLngFromPayload(payload) {
  const data = payload?.data;
  if (!data || typeof data !== "object") return null;
  const raw = data.gps || data.legacy || data;
  const lat = Number(raw.latitude ?? raw.lat);
  const lng = Number(raw.longitude ?? raw.lng);
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) return null;
  return { lat, lng };
}

function distanceMeters(a, b) {
  if (!a || !b) return Infinity;
  const toRad = (x) => (x * Math.PI) / 180;
  const dLat = toRad(b.lat - a.lat);
  const dLng = toRad(b.lng - a.lng);
  const lat1 = toRad(a.lat);
  const lat2 = toRad(b.lat);
  const h =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLng / 2) ** 2;
  return 6371000 * 2 * Math.atan2(Math.sqrt(h), Math.sqrt(1 - h));
}

function createTenantRoomGpsThrottle(options = {}) {
  const {
    enabled = true,
    windowMs = 30_000,
    maxPerWindow = 10,
    silenceFlushMs = 30_000,
    stationaryMinIntervalMs = 30_000,
    stationaryDeadbandMeters = 15,
    metrics = {},
    now = () => Date.now(),
    emit,
    setTimeoutFn = setTimeout,
    clearTimeoutFn = clearTimeout,
  } = options;

  if (typeof emit !== "function") throw new Error("createTenantRoomGpsThrottle requires emit");

  const slots = new Map();

  function clearTimer(slot) {
    if (slot?.flushTimer) {
      clearTimeoutFn(slot.flushTimer);
      slot.flushTimer = null;
    }
  }

  function markEmit(slot, payload, at) {
    slot.lastEmitMs = at;
    const pos = extractLatLngFromPayload(payload);
    if (pos) slot.lastEmitPosition = pos;
  }

  function scheduleFlush(key, slot, imei, roomName, flushAtMs = null) {
    clearTimer(slot);
    const targetMs = flushAtMs ?? slot.windowStart + windowMs;
    const delay = Math.max(50, targetMs - now());
    slot.flushTimer = setTimeoutFn(() => {
      slot.flushTimer = null;
      if (!slot.pending) return;
      const pending = slot.pending;
      slot.pending = null;
      const emitAt = now();
      slot.windowStart = emitAt;
      slot.count = 1;
      markEmit(slot, pending, emitAt);
      metrics.tenant_room_gps_flushed = (metrics.tenant_room_gps_flushed || 0) + 1;
      emit(imei, roomName, pending);
    }, delay);
  }

  function push(imei, roomName, payload) {
    if (!enabled) {
      emit(imei, roomName, payload);
      return { emitted: true, reason: "sent" };
    }
    const key = imei ? String(imei).trim() : "";
    if (!key) {
      emit(imei, roomName, payload);
      return { emitted: true, reason: "sent" };
    }
    let slot = slots.get(key);
    if (!slot) {
      slot = {
        windowStart: now(),
        count: 0,
        lastEmitMs: 0,
        lastEmitPosition: null,
        pending: null,
        flushTimer: null,
      };
      slots.set(key, slot);
    }
    const t = now();
    const speed = extractSpeedFromPayload(payload);

    if (speed === 0) {
      const pos = extractLatLngFromPayload(payload);
      const driftMeters = distanceMeters(slot.lastEmitPosition, pos);
      const isSmallStationaryDrift =
        pos &&
        slot.lastEmitPosition &&
        driftMeters <= stationaryDeadbandMeters;
      const recentlyEmitted =
        slot.lastEmitMs > 0 && t - slot.lastEmitMs < stationaryMinIntervalMs;
      if (isSmallStationaryDrift && recentlyEmitted) {
        slot.pending = payload;
        metrics.tenant_room_gps_stationary_suppressed =
          (metrics.tenant_room_gps_stationary_suppressed || 0) + 1;
        scheduleFlush(key, slot, imei, roomName, slot.lastEmitMs + stationaryMinIntervalMs);
        return { emitted: false, reason: "stationary_deadband" };
      }
      clearTimer(slot);
      slot.pending = null;
      slot.windowStart = t;
      slot.count = 1;
      markEmit(slot, payload, t);
      emit(imei, roomName, payload);
      return { emitted: true, reason: "sent" };
    }

    if (speed == null) {
      clearTimer(slot);
      slot.pending = null;
      markEmit(slot, payload, t);
      emit(imei, roomName, payload);
      return { emitted: true, reason: "sent" };
    }

    if (slot.lastEmitMs > 0 && t - slot.lastEmitMs >= silenceFlushMs) {
      clearTimer(slot);
      slot.pending = null;
      slot.windowStart = t;
      slot.count = 1;
      markEmit(slot, payload, t);
      emit(imei, roomName, payload);
      return { emitted: true, reason: "sent" };
    }

    if (t - slot.windowStart >= windowMs) {
      slot.windowStart = t;
      slot.count = 0;
    }

    if (slot.count < maxPerWindow) {
      slot.count += 1;
      markEmit(slot, payload, t);
      slot.pending = null;
      clearTimer(slot);
      emit(imei, roomName, payload);
      return { emitted: true, reason: "sent" };
    }

    slot.pending = payload;
    metrics.tenant_room_gps_throttled = (metrics.tenant_room_gps_throttled || 0) + 1;
    scheduleFlush(key, slot, imei, roomName);
    return { emitted: false, reason: "tenant_throttle" };
  }

  return { push, slots, extractSpeedFromPayload };
}

module.exports = {
  createTenantRoomGpsThrottle,
  extractSpeedFromPayload,
  extractLatLngFromPayload,
  distanceMeters,
};
