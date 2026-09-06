/**
 * Live vs archive eligibility for Traccar positions.
 * Archive eligibility is independent of live eligibility.
 *
 * Live clock (transport):
 *   Primary: fresh fixTime
 *   Fallback: stale fixTime + fresh monotonic deviceTime + fresh serverTime
 *             + current movement/update (speed/motion/coords)
 *   Never: serverTime alone
 *
 * packet_date / historical GPS timestamp remains fixTime (not this clock).
 */

function parseTimeMs(value) {
  if (value == null || value === "") return null;
  if (value instanceof Date) {
    const ms = value.getTime();
    return Number.isNaN(ms) ? null : ms;
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    return value < 1e12 ? value * 1000 : value;
  }
  const d = new Date(value);
  const ms = d.getTime();
  return Number.isNaN(ms) ? null : ms;
}

function resolveFixMs(position) {
  if (!position || typeof position !== "object") return null;
  return parseTimeMs(position.fixTime ?? position.traccar_fix_time);
}

function resolveDeviceMs(position) {
  if (!position || typeof position !== "object") return null;
  return parseTimeMs(position.deviceTime ?? position.traccar_device_time);
}

function resolveServerMs(position) {
  if (!position || typeof position !== "object") return null;
  return parseTimeMs(position.serverTime ?? position.traccar_server_time);
}

function defaultClassifyOpts(opts = {}) {
  const maxLiveAgeMs = opts.maxLiveAgeMs ?? 300_000;
  const futureToleranceMs = opts.futureToleranceMs ?? 120_000;
  return {
    nowMs: opts.nowMs ?? Date.now(),
    lastLiveFixMs: opts.lastLiveFixMs ?? 0,
    lastEffectiveLiveMs: opts.lastEffectiveLiveMs ?? 0,
    maxLiveAgeMs,
    futureToleranceMs,
    allowDeviceTimeFallback: opts.allowDeviceTimeFallback !== false,
    deviceTimeMaxAgeMs: opts.deviceTimeMaxAgeMs ?? 300_000,
    deviceTimeFutureToleranceMs: opts.deviceTimeFutureToleranceMs ?? futureToleranceMs,
    deviceServerMaxSkewMs: opts.deviceServerMaxSkewMs ?? 180_000,
    previous: opts.previous || null,
  };
}

function isFreshAge(ageMs, maxLiveAgeMs) {
  return ageMs != null && ageMs <= maxLiveAgeMs;
}

function extractSpeed(position) {
  const n = Number(position?.speed ?? position?.gps?.speed);
  return Number.isFinite(n) ? n : null;
}

function coordsChanged(position, previous) {
  if (!previous) return false;
  const lat = Number(position?.latitude);
  const lon = Number(position?.longitude);
  const plat = Number(previous.latitude);
  const plon = Number(previous.longitude);
  if (![lat, lon, plat, plon].every(Number.isFinite)) return false;
  if (lat === 0 && lon === 0) return false;
  return lat !== plat || lon !== plon;
}

function representsCurrentUpdate(position, previous) {
  const speed = extractSpeed(position);
  if (speed != null && speed > 1) return true;
  if (position?.attributes?.motion === true) return true;
  return coordsChanged(position, previous);
}

function baseResult({
  fixMs,
  deviceMs,
  serverMs,
  fixAgeMs,
  deviceAgeMs,
  serverAgeMs,
  fixFresh,
  deviceFresh,
  serverFresh,
}) {
  return {
    archiveEligible: true,
    fixMs,
    deviceMs,
    serverMs,
    fixAgeMs,
    deviceAgeMs,
    serverAgeMs,
    fixFresh,
    deviceFresh,
    serverFresh,
    effectiveLiveMs: null,
    effectiveLiveSource: null,
    deviceServerSkewMs: null,
    deviceFallbackRejected: false,
    fallbackRejectReason: null,
  };
}

/**
 * @returns {{
 *   decision: string,
 *   liveEligible: boolean,
 *   archiveEligible: boolean,
 *   isHistorical: boolean,
 *   fixMs: number|null,
 *   deviceMs: number|null,
 *   serverMs: number|null,
 *   fixAgeMs: number|null,
 *   deviceAgeMs: number|null,
 *   serverAgeMs: number|null,
 *   effectiveLiveMs: number|null,
 *   effectiveLiveSource: 'fixTime'|'deviceTime'|null,
 * }}
 */
function classifyLiveFix(position, opts = {}) {
  const {
    nowMs,
    lastLiveFixMs,
    lastEffectiveLiveMs,
    maxLiveAgeMs,
    futureToleranceMs,
    allowDeviceTimeFallback,
    deviceTimeMaxAgeMs,
    deviceTimeFutureToleranceMs,
    deviceServerMaxSkewMs,
    previous,
  } = defaultClassifyOpts(opts);

  const fixMs = resolveFixMs(position);
  const deviceMs = resolveDeviceMs(position);
  const serverMs = resolveServerMs(position);
  const fixAgeMs = fixMs != null ? nowMs - fixMs : null;
  const deviceAgeMs = deviceMs != null ? nowMs - deviceMs : null;
  const serverAgeMs = serverMs != null ? nowMs - serverMs : null;
  const deviceServerSkewMs =
    deviceMs != null && serverMs != null ? Math.abs(deviceMs - serverMs) : null;
  const fixFresh = isFreshAge(fixAgeMs, maxLiveAgeMs);
  const deviceFresh = isFreshAge(deviceAgeMs, deviceTimeMaxAgeMs);
  const serverFresh = isFreshAge(serverAgeMs, maxLiveAgeMs);

  const base = {
    ...baseResult({
      fixMs,
      deviceMs,
      serverMs,
      fixAgeMs,
      deviceAgeMs,
      serverAgeMs,
      fixFresh,
      deviceFresh,
      serverFresh,
    }),
    deviceServerSkewMs,
  };

  if (fixMs == null && deviceMs == null) {
    return {
      ...base,
      decision: "missing_time",
      liveEligible: false,
      isHistorical: true,
    };
  }

  const liveClockMs = fixMs != null ? fixMs : deviceMs;
  if (liveClockMs != null && liveClockMs > nowMs + futureToleranceMs) {
    return {
      ...base,
      decision: "future_invalid",
      liveEligible: false,
      isHistorical: true,
      deviceFallbackRejected: !fixFresh && allowDeviceTimeFallback,
      fallbackRejectReason: "future_invalid",
    };
  }
  if (deviceMs != null && deviceMs > nowMs + deviceTimeFutureToleranceMs) {
    return {
      ...base,
      decision: "future_invalid",
      liveEligible: false,
      isHistorical: true,
      deviceFallbackRejected: !fixFresh && allowDeviceTimeFallback,
      fallbackRejectReason: "device_future",
    };
  }

  if (fixFresh) {
    if (lastLiveFixMs > 0 && fixMs < lastLiveFixMs) {
      return {
        ...base,
        decision: "out_of_order",
        liveEligible: false,
        isHistorical: true,
      };
    }
    return {
      ...base,
      decision: "fresh",
      liveEligible: true,
      isHistorical: false,
      effectiveLiveMs: fixMs,
      effectiveLiveSource: "fixTime",
    };
  }

  const outdated = position?.outdated === true;
  const deviceMonotonic = !(lastEffectiveLiveMs > 0 && deviceMs != null && deviceMs < lastEffectiveLiveMs);
  let fallbackRejectReason = null;
  if (allowDeviceTimeFallback && !fixFresh) {
    if (deviceMs == null) fallbackRejectReason = "missing_device_time";
    else if (outdated) fallbackRejectReason = "outdated";
    else if (!deviceFresh) fallbackRejectReason = "device_stale";
    else if (!deviceMonotonic) fallbackRejectReason = "device_out_of_order";
    else if (!serverFresh) fallbackRejectReason = "server_stale";
    else if (deviceServerSkewMs == null || deviceServerSkewMs > deviceServerMaxSkewMs) {
      fallbackRejectReason = "device_server_skew";
    } else if (!representsCurrentUpdate(position, previous)) fallbackRejectReason = "not_current_update";
  }
  const canFallback = fallbackRejectReason == null && allowDeviceTimeFallback && !fixFresh;

  if (canFallback) {
    return {
      ...base,
      decision: "fresh_device_time",
      liveEligible: true,
      isHistorical: false,
      effectiveLiveMs: deviceMs,
      effectiveLiveSource: "deviceTime",
    };
  }

  if (!deviceMonotonic && deviceFresh && !fixFresh) {
    return {
      ...base,
      decision: "out_of_order",
      liveEligible: false,
      isHistorical: true,
      deviceFallbackRejected: allowDeviceTimeFallback,
      fallbackRejectReason: fallbackRejectReason || "device_out_of_order",
    };
  }

  return {
    ...base,
    decision: "historical",
    liveEligible: false,
    isHistorical: true,
    deviceFallbackRejected: allowDeviceTimeFallback && !fixFresh,
    fallbackRejectReason,
  };
}

function isLiveEligible(position, opts) {
  return classifyLiveFix(position, opts).liveEligible;
}

function isArchiveEligible(position, opts) {
  return classifyLiveFix(position, opts).archiveEligible;
}

function isValidArchiveCoords(lat, lon) {
  const a = Number(lat);
  const b = Number(lon);
  return Number.isFinite(a) && Number.isFinite(b) && !(a === 0 && b === 0);
}

function pickLatestLiveFromBurst(positions, opts = {}) {
  const list = Array.isArray(positions) ? positions : [];
  const {
    nowMs,
    lastLiveFixMs,
    lastEffectiveLiveMs,
    maxLiveAgeMs,
    futureToleranceMs,
    allowDeviceTimeFallback,
    deviceTimeMaxAgeMs,
    deviceTimeFutureToleranceMs,
    deviceServerMaxSkewMs,
  } = defaultClassifyOpts(opts);
  let liveIndex = -1;
  let liveFixMs = lastLiveFixMs || 0;
  let liveEffectiveMs = lastEffectiveLiveMs || 0;
  let previous = opts.previous || null;
  const decisions = [];

  for (let i = 0; i < list.length; i++) {
    const cls = classifyLiveFix(list[i], {
      nowMs,
      lastLiveFixMs: liveFixMs,
      lastEffectiveLiveMs: liveEffectiveMs,
      maxLiveAgeMs,
      futureToleranceMs,
      allowDeviceTimeFallback,
      deviceTimeMaxAgeMs,
      deviceTimeFutureToleranceMs,
      deviceServerMaxSkewMs,
      previous,
    });
    decisions.push(cls);
    if (cls.liveEligible && cls.effectiveLiveMs != null && cls.effectiveLiveMs >= liveEffectiveMs) {
      liveIndex = i;
      liveEffectiveMs = cls.effectiveLiveMs;
      if (cls.effectiveLiveSource === "fixTime" && cls.fixMs != null) liveFixMs = cls.fixMs;
    }
    previous = list[i];
  }

  return {
    liveIndex,
    livePosition: liveIndex >= 0 ? list[liveIndex] : null,
    decisions,
  };
}

function createLiveFixTracker() {
  /** Last accepted GPS fixTime only. Never stores deviceTime. Used for fixTime OOO. */
  const lastLiveFixMsByImei = new Map();
  /** Transport live clock: fixTime or deviceTime fallback. Ordering for acceptLive. */
  const lastEffectiveLiveMsByImei = new Map();
  const lastSeenByImei = new Map();

  function keyOf(imei) {
    return imei != null ? String(imei).trim() : "";
  }

  function peek(imei) {
    const key = keyOf(imei);
    if (!key) return 0;
    return lastEffectiveLiveMsByImei.get(key) || 0;
  }

  function peekEffective(imei) {
    return peek(imei);
  }

  function peekFix(imei) {
    const key = keyOf(imei);
    if (!key) return 0;
    return lastLiveFixMsByImei.get(key) || 0;
  }

  function peekSeen(imei) {
    const key = keyOf(imei);
    if (!key) return null;
    return lastSeenByImei.get(key) || null;
  }

  function rememberSeen(imei, position) {
    const key = keyOf(imei);
    if (!key || !position) return;
    lastSeenByImei.set(key, {
      latitude: position.latitude,
      longitude: position.longitude,
      fixTime: position.fixTime,
      deviceTime: position.deviceTime,
      serverTime: position.serverTime,
    });
  }

  /**
   * @param {number} liveMs effective live clock (fixTime or deviceTime), never serverTime
   */
  function acceptLive(imei, liveMs, extra = {}) {
    const key = keyOf(imei);
    if (!key || liveMs == null) return false;
    const prevEffective = lastEffectiveLiveMsByImei.get(key) || 0;
    if (prevEffective > 0 && liveMs < prevEffective) return false;
    lastEffectiveLiveMsByImei.set(key, liveMs);
    if (extra.source === "deviceTime") return true;
    const fixMs = extra.fixMs != null ? extra.fixMs : liveMs;
    const prevFix = lastLiveFixMsByImei.get(key) || 0;
    if (prevFix > 0 && fixMs < prevFix) {
      lastEffectiveLiveMsByImei.set(key, prevEffective);
      return false;
    }
    lastLiveFixMsByImei.set(key, fixMs);
    return true;
  }

  function classify(imei, position, opts = {}) {
    const previous = opts.previous !== undefined ? opts.previous : peekSeen(imei);
    const cls = classifyLiveFix(position, {
      ...opts,
      lastLiveFixMs: peekFix(imei),
      lastEffectiveLiveMs: peek(imei),
      previous,
    });
    rememberSeen(imei, position);
    return cls;
  }

  return {
    lastLiveFixMsByImei,
    lastEffectiveLiveMsByImei,
    peek,
    peekEffective,
    peekFix,
    peekSeen,
    acceptLive,
    classify,
    rememberSeen,
  };
}

function recordLiveClockSplit(metrics, cls, position) {
  if (!metrics || !cls) return;
  const speed = extractSpeed(position);
  const moving = speed != null && speed > 1;
  if (cls.liveEligible && cls.effectiveLiveSource === "fixTime") {
    metrics.live_effective_source_fix_total = (metrics.live_effective_source_fix_total || 0) + 1;
  }
  if (cls.liveEligible && cls.effectiveLiveSource === "deviceTime") {
    metrics.live_effective_source_device_total = (metrics.live_effective_source_device_total || 0) + 1;
  }
  if (cls.deviceFallbackRejected) {
    metrics.live_device_fallback_rejected_total =
      (metrics.live_device_fallback_rejected_total || 0) + 1;
  }
  if (cls.fixFresh) {
    metrics.live_fix_fresh_total = (metrics.live_fix_fresh_total || 0) + 1;
    return;
  }
  if (cls.deviceFresh) {
    metrics.live_fix_stale_device_fresh_total = (metrics.live_fix_stale_device_fresh_total || 0) + 1;
  } else {
    metrics.live_fix_stale_device_stale_total = (metrics.live_fix_stale_device_stale_total || 0) + 1;
  }
  if (cls.serverFresh) {
    metrics.live_fix_stale_server_fresh_total = (metrics.live_fix_stale_server_fresh_total || 0) + 1;
  }
  if (moving) metrics.live_fix_stale_moving_total = (metrics.live_fix_stale_moving_total || 0) + 1;
  else metrics.live_fix_stale_stationary_total = (metrics.live_fix_stale_stationary_total || 0) + 1;
}

module.exports = {
  parseTimeMs,
  resolveFixMs,
  resolveDeviceMs,
  resolveServerMs,
  classifyLiveFix,
  isLiveEligible,
  isArchiveEligible,
  isValidArchiveCoords,
  pickLatestLiveFromBurst,
  createLiveFixTracker,
  recordLiveClockSplit,
};
