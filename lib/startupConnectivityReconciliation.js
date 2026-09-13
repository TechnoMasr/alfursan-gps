const mongoose = require("mongoose");
const {
  normalizeImei,
  onlineStatusQuery,
  markDeviceOffline,
} = require("../deviceConnectivityService");
const {
  ENUMERATION_PATH,
  ENUMERATION_PARAMS,
} = require("./traccarDeviceRegistry");

const DEFAULT_MONGO_READY_TIMEOUT_MS = 15000;
const DEFAULT_OFFLINE_CONCURRENCY = 25;
const DEFAULT_STARTUP_GRACE_MS = 120_000;
const DEFAULT_REQUIRED_CONSECUTIVE_EMPTY = 2;
const DEFAULT_RETRY_INTERVAL_MS = 15_000;

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitForMongoReady(timeoutMs = DEFAULT_MONGO_READY_TIMEOUT_MS) {
  const startedAt = Date.now();
  while (mongoose.connection.readyState !== 1) {
    if (Date.now() - startedAt >= timeoutMs) {
      throw new Error("mongo_not_ready");
    }
    await wait(100);
  }
}

async function mapLimit(items, limit, mapper) {
  const concurrency = Math.max(1, Math.min(Number(limit) || 1, items.length || 1));
  let next = 0;
  let completed = 0;
  const results = new Array(items.length);

  async function worker() {
    while (next < items.length) {
      const index = next;
      next += 1;
      results[index] = await mapper(items[index], index);
      completed += 1;
    }
  }

  await Promise.all(Array.from({ length: concurrency }, () => worker()));
  return { results, completed };
}

function setMetric(metrics, key, value) {
  if (metrics) metrics[key] = value;
}

function bump(metrics, key, by = 1) {
  if (!metrics) return;
  metrics[key] = (metrics[key] || 0) + by;
}

/**
 * Authoritative enumeration for memory-mode + service token:
 * GET /api/devices?all=true
 */
async function fetchTraccarDevices(client) {
  if (!client || typeof client.get !== "function") {
    throw new Error("traccar_client_not_ready");
  }
  const { data } = await client.get(ENUMERATION_PATH, { params: { ...ENUMERATION_PARAMS } });
  if (!Array.isArray(data)) {
    throw new Error("traccar_devices_response_not_array");
  }
  return data;
}

async function fetchTraccarDeviceImeis(client) {
  const devices = await fetchTraccarDevices(client);
  const { traccarImeis } = buildTraccarPresenceSets(devices);
  return traccarImeis;
}

/**
 * Build presence sets from a Traccar devices snapshot.
 * Explicit "offline" only — unknown/null/online are NOT offline.
 */
function buildTraccarPresenceSets(devices) {
  const traccarImeis = new Set();
  const traccarOfflineImeis = new Set();
  for (const device of devices || []) {
    const imei = normalizeImei(device?.uniqueId);
    if (!imei) continue;
    traccarImeis.add(imei);
    const status = String(device?.status ?? "").trim().toLowerCase();
    if (status === "offline") {
      traccarOfflineImeis.add(imei);
    }
  }
  return { traccarImeis, traccarOfflineImeis };
}

/**
 * Periodic registry refresh hook: sync Mongo for devices Traccar marks explicitly offline.
 * Does NOT force Mongo online from Traccar online. Does NOT mass-offline on missing IMEIs.
 */
async function syncExplicitTraccarOfflineFromSnapshot({
  devices,
  markOfflineFn = markDeviceOffline,
  at = new Date(),
  reason = "traccar_registry_status_offline",
  source = "traccar_registry_refresh",
  offlineConcurrency = DEFAULT_OFFLINE_CONCURRENCY,
  log = console,
  metrics = null,
} = {}) {
  const { traccarOfflineImeis } = buildTraccarPresenceSets(devices);
  if (traccarOfflineImeis.size === 0) {
    return { offlineCandidates: 0, markedOffline: 0, failures: 0 };
  }

  let markedOffline = 0;
  let failures = 0;
  const offlineList = [...traccarOfflineImeis];
  await mapLimit(offlineList, offlineConcurrency, async (imei) => {
    try {
      const result = await markOfflineFn({
        imei,
        at,
        reason,
        source,
      });
      if (result?.transitioned) markedOffline += 1;
    } catch (err) {
      failures += 1;
      log.warn?.("[traccar-registry-offline-sync] offline transition failed", {
        imei,
        error: err.message,
      });
    }
  });

  bump(metrics, "traccar_registry_explicit_offline_sync_total", 1);
  bump(metrics, "traccar_registry_explicit_offline_marked_total", markedOffline);
  if (failures) bump(metrics, "traccar_registry_explicit_offline_failures_total", failures);

  return {
    offlineCandidates: offlineList.length,
    markedOffline,
    failures,
  };
}

async function getMongoOnlineStatuses() {
  const { DeviceStatus } = require("../mongo");
  return DeviceStatus.find(onlineStatusQuery())
    .select({ imei: 1, status: 1, traccar_device_status: 1, connected: 1, is_connected: 1, connection_state: 1 })
    .lean();
}

/**
 * Shared empty-snapshot confirmation state across reconciliation ticks.
 *
 * During startup grace: valid [] with Mongo online → defer (do not count toward confirmation).
 * After grace: require `requiredConsecutiveEmpty` successful empty snapshots before mass-offline.
 * Non-empty snapshot resets the empty streak.
 */
function createEmptySnapshotGate({
  graceMs = DEFAULT_STARTUP_GRACE_MS,
  requiredConsecutiveEmpty = DEFAULT_REQUIRED_CONSECUTIVE_EMPTY,
  processStartedAtMs = null,
  now = () => Date.now(),
  initialConsecutiveEmpty = 0,
} = {}) {
  const startedAt = processStartedAtMs != null ? Number(processStartedAtMs) : now();
  let consecutiveEmpty = Math.max(0, Number(initialConsecutiveEmpty) || 0);

  function isGraceActive() {
    return now() - startedAt < Math.max(0, Number(graceMs) || 0);
  }

  /**
   * @returns {{
   *   allowMassOfflineFromEmpty: boolean,
   *   deferred: boolean,
   *   graceActive: boolean,
   *   reason: string,
   *   consecutiveEmpty: number,
   * }}
   */
  function evaluateEmpty({ mongoOnlineCount }) {
    const graceActive = isGraceActive();
    const online = Math.max(0, Number(mongoOnlineCount) || 0);

    if (online === 0) {
      // Nothing to mass-offline; do not advance confirmation streak.
      return {
        allowMassOfflineFromEmpty: false,
        deferred: false,
        graceActive,
        reason: "empty_no_mongo_online",
        consecutiveEmpty,
      };
    }

    if (graceActive) {
      consecutiveEmpty = 0;
      return {
        allowMassOfflineFromEmpty: false,
        deferred: true,
        graceActive: true,
        reason: "startup_grace",
        consecutiveEmpty,
      };
    }

    consecutiveEmpty += 1;
    if (consecutiveEmpty < requiredConsecutiveEmpty) {
      return {
        allowMassOfflineFromEmpty: false,
        deferred: true,
        graceActive: false,
        reason: "empty_awaiting_confirmation",
        consecutiveEmpty,
      };
    }

    return {
      allowMassOfflineFromEmpty: true,
      deferred: false,
      graceActive: false,
      reason: "empty_confirmed",
      consecutiveEmpty,
    };
  }

  function noteNonEmpty() {
    consecutiveEmpty = 0;
  }

  function getState() {
    return {
      consecutiveEmpty,
      graceActive: isGraceActive(),
      processStartedAtMs: startedAt,
      graceMs: Math.max(0, Number(graceMs) || 0),
      requiredConsecutiveEmpty,
    };
  }

  return {
    evaluateEmpty,
    noteNonEmpty,
    isGraceActive,
    getState,
  };
}

/**
 * Startup connectivity reconciliation.
 *
 * API failure → do NOT mass-offline.
 * Valid non-empty → normal missing-IMEI offline transitions.
 * Valid [] during startup grace (Mongo online > 0) → defer, no mass-offline.
 * Valid [] after grace → require consecutive empty confirmations before mass-offline.
 */
async function runStartupConnectivityReconciliation({
  registry = null,
  traccarClient,
  metrics,
  log = console,
  mongoReadyTimeoutMs = DEFAULT_MONGO_READY_TIMEOUT_MS,
  offlineConcurrency = DEFAULT_OFFLINE_CONCURRENCY,
  onTraccarDevices,
  getMongoOnline = getMongoOnlineStatuses,
  waitForMongoReadyFn = waitForMongoReady,
  markOfflineFn = markDeviceOffline,
  emptyGate = null,
  graceMs = DEFAULT_STARTUP_GRACE_MS,
  requiredConsecutiveEmpty = DEFAULT_REQUIRED_CONSECUTIVE_EMPTY,
  processStartedAtMs = null,
  now = () => Date.now(),
} = {}) {
  const gate =
    emptyGate ||
    createEmptySnapshotGate({
      graceMs,
      requiredConsecutiveEmpty,
      processStartedAtMs: processStartedAtMs != null ? processStartedAtMs : now(),
      now,
    });

  try {
    await waitForMongoReadyFn(mongoReadyTimeoutMs);
    const runAt = new Date();
    const graceActive = gate.isGraceActive();
    setMetric(metrics, "startup_reconciliation_grace_active", graceActive ? 1 : 0);

    let traccarDevices = [];
    let refreshOk = false;
    let validEmpty = false;

    if (registry && typeof registry.refresh === "function") {
      const refreshResult = await registry.refresh({ force: true });
      if (!refreshResult?.ok) {
        bump(metrics, "startup_reconciliation_traccar_api_failure_total", 1);
        setMetric(metrics, "startup_reconciliation_failures", (metrics?.startup_reconciliation_failures || 0) + 1);
        setMetric(metrics, "startup_reconciliation_last_run_at", runAt.toISOString());
        setMetric(metrics, "startup_reconciliation_mass_offline_skipped", 1);
        setMetric(metrics, "startup_reconciliation_grace_active", graceActive ? 1 : 0);
        log.warn?.(
          `[startup-reconcile] Traccar registry refresh failed — skipping mass-offline: ${refreshResult?.error || "refresh_failed"}`
        );
        return {
          ok: false,
          error: refreshResult?.error || "traccar_registry_refresh_failed",
          massOfflineSkipped: true,
          deferredEmpty: false,
          reason: "traccar_api_failure",
          graceActive,
          shouldRetry: graceActive,
        };
      }
      traccarDevices = registry.listDevices();
      refreshOk = true;
      validEmpty = traccarDevices.length === 0;
    } else {
      try {
        traccarDevices = await fetchTraccarDevices(traccarClient);
        refreshOk = true;
        validEmpty = traccarDevices.length === 0;
      } catch (err) {
        bump(metrics, "startup_reconciliation_traccar_api_failure_total", 1);
        setMetric(metrics, "startup_reconciliation_failures", (metrics?.startup_reconciliation_failures || 0) + 1);
        setMetric(metrics, "startup_reconciliation_mass_offline_skipped", 1);
        setMetric(metrics, "startup_reconciliation_grace_active", graceActive ? 1 : 0);
        log.warn?.(
          `[startup-reconcile] Traccar API failed — skipping mass-offline: ${err.message}`
        );
        return {
          ok: false,
          error: err.message,
          massOfflineSkipped: true,
          deferredEmpty: false,
          reason: "traccar_api_failure",
          graceActive,
          shouldRetry: graceActive,
        };
      }
    }

    if (validEmpty) {
      bump(metrics, "startup_reconciliation_traccar_snapshot_empty_total", 1);
      setMetric(metrics, "startup_reconciliation_traccar_snapshot_empty", 1);
    } else {
      setMetric(metrics, "startup_reconciliation_traccar_snapshot_empty", 0);
      gate.noteNonEmpty();
    }

    if (typeof onTraccarDevices === "function") {
      try {
        onTraccarDevices(traccarDevices);
      } catch (err) {
        log.warn?.("[startup-reconcile] Traccar device callback failed", err.message);
      }
    }

    const { traccarImeis, traccarOfflineImeis } = buildTraccarPresenceSets(traccarDevices);

    const mongoOnline = await getMongoOnline();

    if (validEmpty) {
      const decision = gate.evaluateEmpty({ mongoOnlineCount: mongoOnline.length });
      setMetric(metrics, "startup_reconciliation_grace_active", decision.graceActive ? 1 : 0);

      if (!decision.allowMassOfflineFromEmpty) {
        if (decision.deferred) {
          bump(metrics, "startup_reconciliation_empty_deferred_total", 1);
        }
        setMetric(metrics, "startup_reconciliation_last_run_at", runAt.toISOString());
        setMetric(metrics, "startup_reconciliation_traccar_devices", 0);
        setMetric(metrics, "startup_reconciliation_mongo_online", mongoOnline.length);
        setMetric(metrics, "startup_reconciliation_marked_offline", 0);
        setMetric(metrics, "startup_reconciliation_failures", 0);
        setMetric(metrics, "startup_reconciliation_mass_offline_skipped", 1);

        log.log?.(
          `[startup-reconcile] deferred empty snapshot reason=${decision.reason} mongo_online=${mongoOnline.length} consecutive_empty=${decision.consecutiveEmpty} grace=${decision.graceActive ? 1 : 0}`
        );
        return {
          ok: true,
          refreshOk,
          validEmpty: true,
          deferredEmpty: Boolean(decision.deferred),
          traccarDevices: 0,
          mongoOnline: mongoOnline.length,
          markedOffline: 0,
          failures: 0,
          massOfflineSkipped: true,
          reason: decision.reason,
          graceActive: decision.graceActive,
          consecutiveEmpty: decision.consecutiveEmpty,
          shouldRetry: Boolean(decision.deferred),
        };
      }

      bump(metrics, "startup_reconciliation_empty_confirmed_total", 1);
    }

    // Stale Mongo ONLINE when IMEI is missing from Traccar OR Traccar status is explicitly offline.
    // Unknown/null Traccar status is intentionally NOT treated as offline.
    const toMarkOffline = [];
    for (const status of mongoOnline) {
      const imei = normalizeImei(status?.imei);
      if (!imei) continue;
      if (!traccarImeis.has(imei) || traccarOfflineImeis.has(imei)) {
        toMarkOffline.push(imei);
      }
    }

    let markedOffline = 0;
    let failures = 0;
    await mapLimit(toMarkOffline, offlineConcurrency, async (imei) => {
      try {
        const result = await markOfflineFn({
          imei,
          at: runAt,
          reason: traccarOfflineImeis.has(imei)
            ? "startup_reconciliation_traccar_offline"
            : "startup_reconciliation",
          source: "startup_reconciliation",
        });
        if (result.transitioned) markedOffline += 1;
      } catch (err) {
        failures += 1;
        log.warn?.("[startup-reconcile] offline transition failed", { imei, error: err.message });
      }
    });

    setMetric(metrics, "startup_reconciliation_last_run_at", runAt.toISOString());
    setMetric(metrics, "startup_reconciliation_traccar_devices", traccarImeis.size);
    setMetric(metrics, "startup_reconciliation_traccar_offline_devices", traccarOfflineImeis.size);
    setMetric(metrics, "startup_reconciliation_mongo_online", mongoOnline.length);
    setMetric(metrics, "startup_reconciliation_marked_offline", markedOffline);
    setMetric(metrics, "startup_reconciliation_failures", failures);
    setMetric(metrics, "startup_reconciliation_mass_offline_skipped", 0);
    setMetric(metrics, "startup_reconciliation_grace_active", gate.isGraceActive() ? 1 : 0);

    log.log?.(
      `[startup-reconcile] traccar_devices=${traccarImeis.size} traccar_offline=${traccarOfflineImeis.size} mongo_online=${mongoOnline.length} marked_offline=${markedOffline} unchanged=${mongoOnline.length - toMarkOffline.length} snapshot_empty=${validEmpty ? 1 : 0}`
    );
    return {
      ok: true,
      refreshOk,
      validEmpty,
      deferredEmpty: false,
      traccarDevices: traccarImeis.size,
      traccarOfflineDevices: traccarOfflineImeis.size,
      mongoOnline: mongoOnline.length,
      markedOffline,
      failures,
      massOfflineSkipped: false,
      reason: validEmpty ? "empty_confirmed" : "non_empty",
      graceActive: gate.isGraceActive(),
      shouldRetry: false,
    };
  } catch (err) {
    setMetric(metrics, "startup_reconciliation_failures", (metrics?.startup_reconciliation_failures || 0) + 1);
    setMetric(metrics, "startup_reconciliation_mass_offline_skipped", 1);
    setMetric(metrics, "startup_reconciliation_grace_active", gate.isGraceActive() ? 1 : 0);
    log.warn?.(`[startup-reconcile] skipped: ${err.message}`);
    return {
      ok: false,
      error: err.message,
      massOfflineSkipped: true,
      deferredEmpty: false,
      shouldRetry: gate.isGraceActive(),
      graceActive: gate.isGraceActive(),
    };
  }
}

/**
 * Runs startup reconciliation and retries while empty snapshots are deferred
 * (grace or awaiting consecutive confirmation).
 */
function createStartupConnectivityReconciler(options = {}) {
  const {
    graceMs = DEFAULT_STARTUP_GRACE_MS,
    requiredConsecutiveEmpty = DEFAULT_REQUIRED_CONSECUTIVE_EMPTY,
    retryIntervalMs = DEFAULT_RETRY_INTERVAL_MS,
    now = () => Date.now(),
    log = console,
    metrics = null,
    onComplete = null,
  } = options;

  const processStartedAtMs = now();
  const emptyGate = createEmptySnapshotGate({
    graceMs,
    requiredConsecutiveEmpty,
    processStartedAtMs,
    now,
  });

  let timer = null;
  let stopped = false;
  let running = false;
  let lastResult = null;

  function clearTimer() {
    if (timer) {
      clearTimeout(timer);
      timer = null;
    }
  }

  async function runOnce() {
    lastResult = await runStartupConnectivityReconciliation({
      ...options,
      emptyGate,
      graceMs,
      requiredConsecutiveEmpty,
      processStartedAtMs,
      now,
      log,
      metrics,
    });
    return lastResult;
  }

  function scheduleRetry(delayMs) {
    clearTimer();
    if (stopped) return;
    timer = setTimeout(() => {
      timer = null;
      void tick();
    }, Math.max(1000, Number(delayMs) || DEFAULT_RETRY_INTERVAL_MS));
    if (typeof timer.unref === "function") timer.unref();
  }

  async function tick() {
    if (stopped || running) return lastResult;
    running = true;
    try {
      const result = await runOnce();
      if (!stopped && result?.shouldRetry) {
        scheduleRetry(retryIntervalMs);
      } else if (!stopped && typeof onComplete === "function") {
        try {
          onComplete(result);
        } catch (err) {
          log.warn?.("[startup-reconcile] onComplete failed", err.message);
        }
      }
      return result;
    } finally {
      running = false;
    }
  }

  function start() {
    stopped = false;
    return tick();
  }

  function stop() {
    stopped = true;
    clearTimer();
  }

  return {
    start,
    stop,
    runOnce,
    tick,
    getEmptyGate: () => emptyGate,
    getLastResult: () => lastResult,
  };
}

module.exports = {
  runStartupConnectivityReconciliation,
  createStartupConnectivityReconciler,
  createEmptySnapshotGate,
  buildTraccarPresenceSets,
  syncExplicitTraccarOfflineFromSnapshot,
  fetchTraccarDevices,
  fetchTraccarDeviceImeis,
  waitForMongoReady,
  mapLimit,
  ENUMERATION_PATH,
  ENUMERATION_PARAMS,
  DEFAULT_STARTUP_GRACE_MS,
  DEFAULT_REQUIRED_CONSECUTIVE_EMPTY,
  DEFAULT_RETRY_INTERVAL_MS,
};
