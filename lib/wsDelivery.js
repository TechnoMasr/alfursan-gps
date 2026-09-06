const WS_OPEN = 1;

function jsonSizeHint(payload) {
  try {
    return JSON.stringify(payload).length;
  } catch {
    return 0;
  }
}

function createWsDelivery(options = {}) {
  const {
    metrics = {},
    highWatermark = 1_048_576,
    lowWatermark = 262_144,
    criticalWatermark = 8_388_608,
    serialize = (payload) => JSON.stringify(payload),
    onTerminate,
    now = () => Date.now(),
  } = options;

  const flushTimers = new WeakMap();

  function buffered(ws) {
    return Number(ws?.bufferedAmount) || 0;
  }

  function markSlow(ws) {
    if (!ws || ws._slowCounted) return;
    ws._slowCounted = true;
    metrics.slow_subscriber_count = (metrics.slow_subscriber_count || 0) + 1;
  }

  function unmarkSlow(ws) {
    if (!ws || !ws._slowCounted) return;
    ws._slowCounted = false;
    metrics.slow_subscriber_count = Math.max(0, (metrics.slow_subscriber_count || 0) - 1);
  }

  function terminate(ws, reason) {
    unmarkSlow(ws);
    ws._pendingGps = null;
    ws._pendingCommands = null;
    metrics.subscriber_terminated_backpressure =
      (metrics.subscriber_terminated_backpressure || 0) + 1;
    try {
      if (typeof onTerminate === "function") onTerminate(ws, reason);
      else if (typeof ws.terminate === "function") ws.terminate();
    } catch {
      /* ignore */
    }
  }

  function rawSend(ws, payload) {
    if (!ws || ws.readyState !== WS_OPEN) return false;
    try {
      const wire = typeof payload === "string" ? payload : serialize(payload);
      ws.send(wire);
      return true;
    } catch (err) {
      console.error("Subscriber send error:", err.message);
      return false;
    }
  }

  function flushPending(ws) {
    if (!ws || ws.readyState !== WS_OPEN) return;
    const buf = buffered(ws);
    if (buf >= highWatermark) {
      markSlow(ws);
      scheduleFlush(ws);
      return;
    }
    if (buf <= lowWatermark) unmarkSlow(ws);

    const cmds = ws._pendingCommands;
    if (Array.isArray(cmds) && cmds.length) {
      while (cmds.length && buffered(ws) < highWatermark) {
        rawSend(ws, cmds.shift());
      }
    }

    const pendingGps = ws._pendingGps;
    if (pendingGps && pendingGps.size && buffered(ws) < highWatermark) {
      for (const [imei, payload] of pendingGps.entries()) {
        if (buffered(ws) >= highWatermark) break;
        pendingGps.delete(imei);
        rawSend(ws, payload);
      }
    }

    const stillPending =
      (Array.isArray(ws._pendingCommands) && ws._pendingCommands.length > 0) ||
      (ws._pendingGps && ws._pendingGps.size > 0);
    if (stillPending) scheduleFlush(ws);
  }

  function scheduleFlush(ws) {
    if (flushTimers.get(ws)) return;
    const t = setTimeout(() => {
      flushTimers.delete(ws);
      flushPending(ws);
    }, 25);
    if (typeof t.unref === "function") t.unref();
    flushTimers.set(ws, t);
  }

  function send(ws, payload, { kind = "gps", imei = null } = {}) {
    if (!ws || ws.readyState !== WS_OPEN) return { sent: false, reason: "not_open" };
    const buf = buffered(ws);

    if (buf >= criticalWatermark) {
      terminate(ws, "backpressure_critical");
      return { sent: false, reason: "terminated" };
    }

    if (kind === "command") {
      if (buf >= highWatermark) {
        markSlow(ws);
        if (!ws._pendingCommands) ws._pendingCommands = [];
        ws._pendingCommands.push(payload);
        scheduleFlush(ws);
        return { sent: false, queued: true, reason: "command_queued" };
      }
      rawSend(ws, payload);
      return { sent: true };
    }

    if (buf >= highWatermark) {
      markSlow(ws);
      if (!ws._pendingGps) ws._pendingGps = new Map();
      const key = imei != null ? String(imei) : "_";
      if (ws._pendingGps.has(key)) {
        metrics.live_coalesced = (metrics.live_coalesced || 0) + 1;
      }
      ws._pendingGps.set(key, payload);
      metrics.live_backpressure_suppressed = (metrics.live_backpressure_suppressed || 0) + 1;
      scheduleFlush(ws);
      return { sent: false, reason: "backpressure_coalesced" };
    }

    rawSend(ws, payload);
    return { sent: true };
  }

  function clearSocketState(ws) {
    const t = flushTimers.get(ws);
    if (t) clearTimeout(t);
    flushTimers.delete(ws);
    unmarkSlow(ws);
    ws._pendingGps = null;
    ws._pendingCommands = null;
  }

  function pendingGpsCount(ws) {
    return ws?._pendingGps ? ws._pendingGps.size : 0;
  }

  function pendingCommandCount(ws) {
    return Array.isArray(ws?._pendingCommands) ? ws._pendingCommands.length : 0;
  }

  return {
    send,
    flushPending,
    clearSocketState,
    pendingGpsCount,
    pendingCommandCount,
    buffered,
    jsonSizeHint,
    WS_OPEN,
    now,
  };
}

module.exports = { createWsDelivery, WS_OPEN };
