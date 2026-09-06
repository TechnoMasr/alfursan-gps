const { jitter } = require("./gpsPointWriter");

/**
 * Guaranteed-retry Traccar WS reconnect.
 * Failure always schedules the next attempt. Success resets backoff.
 */
function createTraccarReconnect(options = {}) {
  const {
    baseMs = 5_000,
    maxMs = 60_000,
    connect,
    shouldConnect = () => true,
    isSocketHealthy = () => false,
    metrics = {},
    log = console,
    now = () => Date.now(),
    setTimeoutFn = setTimeout,
    clearTimeoutFn = clearTimeout,
  } = options;

  if (typeof connect !== "function") {
    throw new Error("createTraccarReconnect requires connect()");
  }

  let attempt = 0;
  let timer = null;
  let inProgress = false;
  let generation = 0;
  let lastScheduleAt = 0;
  let lastReason = null;
  let stopped = false;

  function delayForAttempt(n) {
    const exp = Math.min(maxMs, baseMs * Math.pow(2, Math.max(0, n - 1)));
    return jitter(exp);
  }

  function clearTimer() {
    if (timer) {
      clearTimeoutFn(timer);
      timer = null;
    }
  }

  function schedule(reason = "unknown", { immediate = false } = {}) {
    if (stopped) return false;
    if (!shouldConnect()) return false;
    if (isSocketHealthy()) return false;
    if (timer) return false;

    lastReason = reason;
    const delay = immediate ? 0 : delayForAttempt(Math.max(1, attempt || 1));
    lastScheduleAt = now() + delay;
    log.warn?.("Scheduling Traccar WS reconnect", { reason, delay_ms: delay, attempt });
    timer = setTimeoutFn(() => {
      timer = null;
      void attemptConnect(reason);
    }, delay);
    if (typeof timer.unref === "function") timer.unref();
    return true;
  }

  async function attemptConnect(reason = "manual") {
    if (stopped) return false;
    if (!shouldConnect()) return false;
    if (inProgress) {
      schedule(reason);
      return false;
    }
    if (isSocketHealthy() && reason !== "periodic_refresh" && reason !== "watchdog") {
      attempt = 0;
      return true;
    }

    inProgress = true;
    const gen = ++generation;
    attempt += 1;
    metrics.reconnect_attempt_total = (metrics.reconnect_attempt_total || 0) + 1;
    log.warn?.("Reconnecting Traccar WS", { reason, attempt, generation: gen });

    try {
      await connect({ reason, attempt, generation: gen });
      if (stopped || gen !== generation) return false;
      attempt = 0;
      lastReason = null;
      return true;
    } catch (err) {
      metrics.reconnect_failure_total = (metrics.reconnect_failure_total || 0) + 1;
      log.warn?.("Reconnect failed", { reason, error: err.message, attempt });
      schedule("connect_failed");
      return false;
    } finally {
      inProgress = false;
      if (!stopped && !isSocketHealthy() && !timer && shouldConnect()) {
        schedule("post_connect_unhealthy");
      }
    }
  }

  function onSocketClose(reason = "ws_close") {
    if (stopped) return;
    if (!shouldConnect()) return;
    schedule(reason);
  }

  function onSocketError() {
    // close handler will schedule; keep as safety net
    if (!timer && !inProgress && !isSocketHealthy()) schedule("ws_error");
  }

  function notifySuccess() {
    attempt = 0;
  }

  function stop() {
    stopped = true;
    clearTimer();
  }

  function snapshot() {
    return {
      attempt,
      inProgress,
      timerPending: !!timer,
      lastReason,
      scheduled_at: lastScheduleAt || null,
      generation,
    };
  }

  return {
    schedule,
    attemptConnect,
    onSocketClose,
    onSocketError,
    notifySuccess,
    clearTimer,
    stop,
    snapshot,
    delayForAttempt,
    get inProgress() {
      return inProgress;
    },
  };
}

/**
 * Watchdog: reconnect if the socket looks open but no messages arrive.
 * Does NOT treat "no GPS" as death when device/heartbeat messages still flow.
 */
function createTraccarSilenceWatchdog(options = {}) {
  const {
    silenceMs = 180_000,
    metrics = {},
    isConnected = () => false,
    reconnect,
    intervalMs = 15_000,
    setIntervalFn = setInterval,
    clearIntervalFn = clearInterval,
    now = () => Date.now(),
    log = console,
  } = options;

  const timer = setIntervalFn(() => {
    if (!isConnected()) return;
    const last = metrics.last_traccar_message_at
      ? new Date(metrics.last_traccar_message_at).getTime()
      : 0;
    if (!last) return;
    const quiet = now() - last;
    if (quiet >= silenceMs) {
      log.warn?.("[traccar] silence watchdog firing", { quiet_ms: quiet, silence_ms: silenceMs });
      reconnect?.("watchdog_silence");
    }
  }, intervalMs);
  if (typeof timer.unref === "function") timer.unref();

  return {
    stop() {
      clearIntervalFn(timer);
    },
  };
}

module.exports = {
  createTraccarReconnect,
  createTraccarSilenceWatchdog,
};
