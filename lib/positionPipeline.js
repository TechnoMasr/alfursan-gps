const {
  classifyLiveFix,
  pickLatestLiveFromBurst,
  isValidArchiveCoords,
  createLiveFixTracker,
  recordLiveClockSplit,
} = require("./liveEligibility");
const { recordLiveDecision } = require("./bridgeMetrics");
const { maybeLatencyLog } = require("./latencyLog");

/**
 * Core ingress pipeline used by production bridges and tests.
 * Realtime is never awaited on persistence.
 */
function createPositionPipeline(options = {}) {
  const {
    metrics = {},
    maxLiveAgeMs = 300_000,
    futureToleranceMs = 120_000,
    latencyDebug = false,
    now = () => Date.now(),
    emitLive,
    emitCommand,
    enqueueArchive,
    enqueueAnalytics,
    resolveImeiSync,
    resolveImeiAsync,
    onUnresolvedBatch,
    liveTracker = createLiveFixTracker(),
    allowDeviceTimeFallback = true,
    deviceTimeMaxAgeMs = 300_000,
    deviceTimeFutureToleranceMs = 120_000,
    deviceServerMaxSkewMs = 180_000,
    deferPersistence = false,
    onPersistenceBatch = null,
  } = options;

  function classify(imei, position, nowMs = now()) {
    return liveTracker.classify(imei, position, {
      nowMs,
      maxLiveAgeMs,
      futureToleranceMs,
      allowDeviceTimeFallback,
      deviceTimeMaxAgeMs,
      deviceTimeFutureToleranceMs,
      deviceServerMaxSkewMs,
    });
  }

  function archiveIfEligible(imei, position, extra = {}) {
    const lat = Number(position?.latitude);
    const lon = Number(position?.longitude);
    if (!isValidArchiveCoords(lat, lon)) return false;
    if (typeof enqueueArchive !== "function") return false;
    const archiveEnqueuedAt = now();
    enqueueArchive({
      imei,
      position,
      latitude: lat,
      longitude: lon,
      archive_enqueued_at: archiveEnqueuedAt,
      ...extra,
    });
    return true;
  }

  function emitLiveIfEligible(imei, position, payload, cls, receivedAt) {
    if (!cls.liveEligible) {
      recordLiveDecision(metrics, cls.decision);
      maybeLatencyLog(latencyDebug, {
        imei,
        traccar_position_id: position?.id,
        fixTime: position?.fixTime,
        deviceTime: position?.deviceTime,
        serverTime: position?.serverTime,
        bridge_received_at: receivedAt,
        fix_age_ms: cls.fixAgeMs,
        server_age_ms: cls.serverAgeMs,
        live_decision: cls.decision,
        live_drop_reason: cls.decision,
      });
      return false;
    }
    if (!liveTracker.acceptLive(imei, cls.effectiveLiveMs, {
      source: cls.effectiveLiveSource,
      fixMs: cls.fixMs,
    })) {
      recordLiveDecision(metrics, "out_of_order");
      return false;
    }
    const sentAt = now();
    if (typeof emitLive === "function") emitLive(imei, payload, { position, cls });
    recordLiveDecision(metrics, cls.decision);
    maybeLatencyLog(latencyDebug, {
      imei,
      traccar_position_id: position?.id,
      fixTime: position?.fixTime,
      deviceTime: position?.deviceTime,
      serverTime: position?.serverTime,
      bridge_received_at: receivedAt,
      bridge_live_sent_at: sentAt,
      live_delivery_ms: sentAt - receivedAt,
      fix_age_ms: cls.fixAgeMs,
      server_age_ms: cls.serverAgeMs,
        live_decision: cls.decision,
    });
    return true;
  }

  function handleResolvedGps({ imei, positions, buildPayload, buildAnalytics, receivedAt = now() }) {
    const list = Array.isArray(positions) ? positions : [];
    const picked = pickLatestLiveFromBurst(list, {
      nowMs: receivedAt,
      lastLiveFixMs: liveTracker.peekFix(imei),
      lastEffectiveLiveMs: liveTracker.peek(imei),
      maxLiveAgeMs,
      futureToleranceMs,
      allowDeviceTimeFallback,
      previous: liveTracker.peekSeen(imei),
      deviceTimeMaxAgeMs,
      deviceTimeFutureToleranceMs,
      deviceServerMaxSkewMs,
    });

    if (picked.liveIndex >= 0) {
      const livePos = list[picked.liveIndex];
      const payload =
        typeof buildPayload === "function" ? buildPayload(livePos) : livePos;
      emitLiveIfEligible(imei, livePos, payload, picked.decisions[picked.liveIndex], receivedAt);
      for (let i = 0; i < list.length; i++) {
        if (i === picked.liveIndex) continue;
        const other = picked.decisions[i];
        if (other.liveEligible) recordLiveDecision(metrics, "superseded");
        else recordLiveDecision(metrics, other.decision);
      }
    } else {
      for (const cls of picked.decisions) recordLiveDecision(metrics, cls.decision);
    }

    if (typeof onPersistenceBatch === "function" && list.length) {
      onPersistenceBatch({
        imei,
        positions: list,
        picked,
        receivedAt,
        buildPayload,
        buildAnalytics,
      });
    }

    if (deferPersistence) {
      return;
    }

    for (let i = 0; i < list.length; i++) {
      const position = list[i];
      const cls = picked.decisions[i] || classify(imei, position, receivedAt);
      recordLiveClockSplit(metrics, cls, position);
      archiveIfEligible(imei, position, {
        live_decision: i === picked.liveIndex ? cls.decision : cls.liveEligible ? "superseded" : cls.decision,
        isHistorical: cls.isHistorical || i !== picked.liveIndex,
      });
      if (typeof enqueueAnalytics === "function") {
        const ctx =
          typeof buildAnalytics === "function"
            ? buildAnalytics(position, cls)
            : { imei, position, isHistorical: cls.isHistorical, liveDecision: cls.decision };
        enqueueAnalytics(imei, ctx);
      }
    }
  }

  function handleCommand(imei, position, payload) {
    if (typeof emitCommand === "function") emitCommand(imei, payload, { position });
    metrics.command_response_broadcast_total =
      (metrics.command_response_broadcast_total || 0) + 1;
  }

  return {
    liveTracker,
    classify,
    handleResolvedGps,
    handleCommand,
    archiveIfEligible,
    emitLiveIfEligible,
  };
}

module.exports = { createPositionPipeline };
