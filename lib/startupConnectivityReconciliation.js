const mongoose = require("mongoose");
const {
  normalizeImei,
  onlineStatusQuery,
  markDeviceOffline,
} = require("../deviceConnectivityService");

const DEFAULT_MONGO_READY_TIMEOUT_MS = 15000;
const DEFAULT_OFFLINE_CONCURRENCY = 25;

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

async function fetchTraccarDeviceImeis(client) {
  if (!client || typeof client.get !== "function") {
    throw new Error("traccar_client_not_ready");
  }
  const { data } = await client.get("/api/devices");
  if (!Array.isArray(data)) {
    throw new Error("traccar_devices_response_not_array");
  }
  const imeis = new Set();
  for (const device of data) {
    const uniqueId = normalizeImei(device?.uniqueId);
    if (uniqueId) imeis.add(uniqueId);
  }
  return imeis;
}

async function getMongoOnlineStatuses() {
  const { DeviceStatus } = require("../mongo");
  return DeviceStatus.find(onlineStatusQuery())
    .select({ imei: 1, status: 1, traccar_device_status: 1, connected: 1, is_connected: 1, connection_state: 1 })
    .lean();
}

async function runStartupConnectivityReconciliation({
  traccarClient,
  metrics,
  log = console,
  mongoReadyTimeoutMs = DEFAULT_MONGO_READY_TIMEOUT_MS,
  offlineConcurrency = DEFAULT_OFFLINE_CONCURRENCY,
} = {}) {
  try {
    await waitForMongoReady(mongoReadyTimeoutMs);
    const runAt = new Date();
    const traccarImeis = await fetchTraccarDeviceImeis(traccarClient);
    const mongoOnline = await getMongoOnlineStatuses();
    const missing = [];

    for (const status of mongoOnline) {
      const imei = normalizeImei(status?.imei);
      if (!imei) continue;
      if (!traccarImeis.has(imei)) missing.push(imei);
    }

    let markedOffline = 0;
    let failures = 0;
    await mapLimit(missing, offlineConcurrency, async (imei) => {
      try {
        const result = await markDeviceOffline({
          imei,
          at: runAt,
          reason: "startup_reconciliation",
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
    setMetric(metrics, "startup_reconciliation_mongo_online", mongoOnline.length);
    setMetric(metrics, "startup_reconciliation_marked_offline", markedOffline);
    setMetric(metrics, "startup_reconciliation_failures", failures);

    log.log?.(
      `[startup-reconcile] traccar_devices=${traccarImeis.size} mongo_online=${mongoOnline.length} marked_offline=${markedOffline} unchanged=${mongoOnline.length - missing.length}`
    );
    return {
      ok: true,
      traccarDevices: traccarImeis.size,
      mongoOnline: mongoOnline.length,
      markedOffline,
      failures,
    };
  } catch (err) {
    setMetric(metrics, "startup_reconciliation_failures", (metrics?.startup_reconciliation_failures || 0) + 1);
    log.warn?.(`[startup-reconcile] skipped: ${err.message}`);
    return { ok: false, error: err.message };
  }
}

module.exports = {
  runStartupConnectivityReconciliation,
  fetchTraccarDeviceImeis,
  waitForMongoReady,
  mapLimit,
};
