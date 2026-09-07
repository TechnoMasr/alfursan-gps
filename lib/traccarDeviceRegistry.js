/**
 * In-process Traccar device registry for memory-mode (database.memory=true).
 *
 * Authoritative fleet enumeration: GET /api/devices?all=true
 * Durable identity: uniqueId / IMEI (string, trimmed — never Number())
 * Ephemeral handle: numeric device.id (changes on Traccar restart)
 *
 * Do NOT use GET /api/devices or ?uniqueId= for service-token fleet/command lookup.
 */

const { normalizeImei } = require("../deviceConnectivityService");

const ENUMERATION_PATH = "/api/devices";
const ENUMERATION_PARAMS = { all: true };

function bump(metrics, key, by = 1) {
  if (!metrics) return;
  metrics[key] = (metrics[key] || 0) + by;
}

function setMetric(metrics, key, value) {
  if (!metrics) return;
  metrics[key] = value;
}

function normalizeUniqueId(value) {
  return normalizeImei(value);
}

function isConcurrentModificationFailure(err) {
  const text = extractErrorText(err);
  return (
    /ConcurrentModificationException/i.test(text) &&
    (/Storage\.getObject/i.test(text) ||
      /CommandsManager\.sendCommand/i.test(text) ||
      /org\.traccar\.storage\.Storage/i.test(text))
  );
}

function extractErrorText(err) {
  const data = err?.response?.data;
  if (typeof data === "string") return data;
  if (data && typeof data === "object") {
    return [data.message, data.error, data.details, JSON.stringify(data)].filter(Boolean).join(" ");
  }
  return String(err?.message || err || "");
}

/**
 * Stale/missing runtime deviceId after Traccar memory rebuild — not CME.
 */
function isStaleRuntimeDeviceError(err) {
  if (isConcurrentModificationFailure(err)) return false;
  const status = Number(err?.response?.status || err?.statusCode || 0);
  const text = extractErrorText(err).toLowerCase();
  if (status === 404) return true;
  if (status === 400 || status === 404) {
    return /unknown device|device not found|invalid device|no such device|device.*not.*(exist|found|available)|not found/i.test(
      text
    );
  }
  return false;
}

function annotateCmeError(err) {
  const annotated = Object.assign(
    new Error(
      "traccar_core_concurrent_modification: Traccar 6.13.x memory-mode command path is not approved; pin 6.12.2"
    ),
    {
      statusCode: Number(err?.response?.status || err?.statusCode) || 502,
      cause: err,
      code: "TRACCAR_CME",
    }
  );
  return annotated;
}

function createTraccarDeviceRegistry(options = {}) {
  const {
    getClient,
    metrics = null,
    log = console,
    now = () => Date.now(),
    pollIntervalMs = 900_000,
    cooldownMs = 60_000,
    onSnapshot = null,
  } = options;

  if (typeof getClient !== "function") {
    throw new Error("createTraccarDeviceRegistry requires getClient");
  }

  let byUniqueId = new Map();
  let byRuntimeId = new Map();
  let lastSuccessAtMs = null;
  let lastFailureAtMs = null;
  let lastError = null;
  let lastRefreshOk = false;
  let refreshInflight = null;
  let pollTimer = null;

  function listDevices() {
    return [...byUniqueId.values()];
  }

  function getByUniqueId(uniqueId, { count = true } = {}) {
    const key = normalizeUniqueId(uniqueId);
    if (!key) return null;
    const device = byUniqueId.get(key) || null;
    if (count) {
      if (device) bump(metrics, "traccar_registry_cache_hit_total", 1);
      else bump(metrics, "traccar_registry_cache_miss_total", 1);
    }
    return device;
  }

  function getByRuntimeId(runtimeId) {
    const id = Number(runtimeId);
    if (!Number.isFinite(id)) return null;
    return byRuntimeId.get(id) || null;
  }

  function getRuntimeId(uniqueId) {
    const device = getByUniqueId(uniqueId);
    if (!device) return null;
    const id = Number(device.id);
    return Number.isFinite(id) ? id : null;
  }

  function applySnapshot(devices) {
    const nextByUniqueId = new Map();
    const nextByRuntimeId = new Map();
    const runtimeIdChanges = [];

    for (const device of devices) {
      const uniqueId = normalizeUniqueId(device?.uniqueId);
      const runtimeId = Number(device?.id);
      if (!uniqueId || !Number.isFinite(runtimeId)) continue;

      const prev = byUniqueId.get(uniqueId);
      const prevId = prev != null ? Number(prev.id) : null;
      if (prevId != null && Number.isFinite(prevId) && prevId !== runtimeId) {
        bump(metrics, "traccar_registry_runtime_id_change_total", 1);
        runtimeIdChanges.push({ uniqueId, previousId: prevId, runtimeId, device });
      }

      // Deterministic: last wins if duplicate uniqueIds appear (should be rare).
      nextByUniqueId.set(uniqueId, device);
      nextByRuntimeId.set(runtimeId, device);
    }

    byUniqueId = nextByUniqueId;
    byRuntimeId = nextByRuntimeId;
    lastSuccessAtMs = now();
    lastFailureAtMs = null;
    lastError = null;
    lastRefreshOk = true;

    setMetric(metrics, "traccar_registry_devices", byUniqueId.size);
    setMetric(
      metrics,
      "traccar_registry_last_refresh_at",
      new Date(lastSuccessAtMs).toISOString()
    );
    setMetric(metrics, "traccar_registry_last_snapshot_empty", byUniqueId.size === 0);
    setMetric(metrics, "traccar_registry_last_refresh_ok", true);

    if (typeof onSnapshot === "function") {
      try {
        onSnapshot(listDevices(), { runtimeIdChanges });
      } catch (err) {
        log.warn?.("[traccar-registry] onSnapshot failed", err.message);
      }
    }

    return { runtimeIdChanges };
  }

  async function doRefresh({ force = false } = {}) {
    bump(metrics, "traccar_registry_refresh_attempt_total", 1);
    if (force) bump(metrics, "traccar_registry_forced_refresh_total", 1);

    const client = getClient();
    if (!client || typeof client.get !== "function") {
      throw Object.assign(new Error("traccar_client_not_ready"), { statusCode: 503 });
    }

    const { data } = await client.get(ENUMERATION_PATH, { params: { ...ENUMERATION_PARAMS } });
    if (!Array.isArray(data)) {
      throw new Error("traccar_devices_response_not_array");
    }

    const { runtimeIdChanges } = applySnapshot(data);
    bump(metrics, "traccar_registry_refresh_success_total", 1);
    return {
      ok: true,
      devices: byUniqueId.size,
      empty: byUniqueId.size === 0,
      runtimeIdChanges,
      forced: force,
    };
  }

  /**
   * Refresh from GET /api/devices?all=true.
   * Failures preserve the previous good snapshot.
   * Concurrent callers share one in-flight promise.
   */
  async function refresh({ force = false } = {}) {
    if (refreshInflight) return refreshInflight;

    if (
      !force &&
      lastFailureAtMs != null &&
      now() - lastFailureAtMs < cooldownMs
    ) {
      return {
        ok: false,
        skipped: true,
        reason: "cooldown_after_failure",
        preserved: lastSuccessAtMs != null,
        devices: byUniqueId.size,
      };
    }

    refreshInflight = (async () => {
      try {
        return await doRefresh({ force });
      } catch (err) {
        lastFailureAtMs = now();
        lastError = String(err?.message || err);
        lastRefreshOk = false;
        bump(metrics, "traccar_registry_refresh_failure_total", 1);
        setMetric(metrics, "traccar_registry_last_refresh_ok", false);
        setMetric(metrics, "traccar_registry_last_error", lastError);
        log.warn?.("[traccar-registry] refresh failed; preserving snapshot", lastError);
        return {
          ok: false,
          error: lastError,
          preserved: lastSuccessAtMs != null,
          devices: byUniqueId.size,
          empty: false,
          validEmpty: false,
        };
      } finally {
        refreshInflight = null;
        if (lastSuccessAtMs != null) {
          setMetric(metrics, "traccar_registry_refresh_age_ms", now() - lastSuccessAtMs);
        }
      }
    })();

    return refreshInflight;
  }

  /**
   * Resolve current ephemeral Traccar device.id for an IMEI.
   * Cache miss → one forced refresh → lookup again.
   */
  async function resolveRuntimeId(uniqueId, { allowForcedRefresh = true } = {}) {
    const key = normalizeUniqueId(uniqueId);
    if (!key) {
      throw Object.assign(new Error("imei_required"), { statusCode: 400 });
    }

    let device = getByUniqueId(key);
    if (device) {
      const id = Number(device.id);
      if (!Number.isFinite(id)) {
        throw Object.assign(new Error("invalid_runtime_device_id"), { statusCode: 502 });
      }
      return id;
    }

    if (!allowForcedRefresh) {
      throw Object.assign(new Error("device_not_registered_in_traccar_runtime"), {
        statusCode: 404,
      });
    }

    await refresh({ force: true });
    device = getByUniqueId(key, { count: true });
    if (!device) {
      throw Object.assign(new Error("device_not_registered_in_traccar_runtime"), {
        statusCode: 404,
      });
    }
    const id = Number(device.id);
    if (!Number.isFinite(id)) {
      throw Object.assign(new Error("invalid_runtime_device_id"), { statusCode: 502 });
    }
    return id;
  }

  function getStatus() {
    const age =
      lastSuccessAtMs != null ? Math.max(0, now() - lastSuccessAtMs) : null;
    return {
      devices: byUniqueId.size,
      last_refresh_at:
        lastSuccessAtMs != null ? new Date(lastSuccessAtMs).toISOString() : null,
      refresh_age_ms: age,
      last_refresh_ok: lastRefreshOk,
      last_error: lastError,
      last_snapshot_empty: lastSuccessAtMs != null && byUniqueId.size === 0,
      has_snapshot: lastSuccessAtMs != null,
      inflight: Boolean(refreshInflight),
    };
  }

  function startPolling() {
    stopPolling();
    const interval = Math.max(30_000, Number(pollIntervalMs) || 900_000);
    pollTimer = setInterval(() => {
      void refresh({ force: false });
    }, interval);
    if (typeof pollTimer.unref === "function") pollTimer.unref();
    return interval;
  }

  function stopPolling() {
    if (pollTimer) {
      clearInterval(pollTimer);
      pollTimer = null;
    }
  }

  /** Test/helper: seed without HTTP. */
  function replaceSnapshotForTests(devices) {
    return applySnapshot(Array.isArray(devices) ? devices : []);
  }

  return {
    refresh,
    resolveRuntimeId,
    getByUniqueId,
    getByRuntimeId,
    getRuntimeId,
    listDevices,
    getStatus,
    startPolling,
    stopPolling,
    replaceSnapshotForTests,
    ENUMERATION_PATH,
    ENUMERATION_PARAMS,
  };
}

/**
 * Send custom command using registry-resolved runtime id.
 * On stale-id failure: force refresh once; retry only if runtime id changed.
 * Never retry CME / success.
 */
async function sendTraccarCustomCommand({
  registry,
  client,
  metrics = null,
  imei,
  command,
  type = "custom",
  attributes = null,
  textChannel,
} = {}) {
  if (!registry) throw Object.assign(new Error("registry_required"), { statusCode: 503 });
  if (!client || typeof client.post !== "function") {
    throw Object.assign(new Error("traccar_client_not_ready"), { statusCode: 503 });
  }
  const payloadCommand = String(command || "").trim();
  const payloadImei = normalizeUniqueId(imei);
  if (!payloadCommand) {
    throw Object.assign(new Error("command_required"), { statusCode: 400 });
  }
  if (!payloadImei) {
    throw Object.assign(new Error("imei_required"), { statusCode: 400 });
  }

  bump(metrics, "traccar_command_send_total", 1);
  let runtimeId = await registry.resolveRuntimeId(payloadImei);

  const buildBody = (deviceId) => {
    const body = {
      type,
      attributes: attributes
        ? { ...attributes }
        : { data: payloadCommand, noQueue: true },
      deviceId,
    };
    // Only include textChannel when caller opts in (preserve legacy payload shape).
    if (textChannel === true || textChannel === false) {
      body.textChannel = Boolean(textChannel);
    }
    return body;
  };

  async function postOnce(deviceId) {
    await client.post("/api/commands/send", buildBody(deviceId));
  }

  try {
    await postOnce(runtimeId);
    bump(metrics, "traccar_command_send_success_total", 1);
    return { ok: true, imei: payloadImei, runtimeId, retried: false };
  } catch (err) {
    if (isConcurrentModificationFailure(err)) {
      bump(metrics, "traccar_command_send_failure_total", 1);
      bump(metrics, "traccar_command_cme_total", 1);
      logCme(metrics, err);
      throw annotateCmeError(err);
    }

    if (!isStaleRuntimeDeviceError(err)) {
      bump(metrics, "traccar_command_send_failure_total", 1);
      throw err;
    }

    bump(metrics, "traccar_command_stale_id_retry_total", 1);
    await registry.refresh({ force: true });
    let newId;
    try {
      newId = await registry.resolveRuntimeId(payloadImei, { allowForcedRefresh: false });
    } catch (resolveErr) {
      bump(metrics, "traccar_command_send_failure_total", 1);
      throw err;
    }

    if (newId === runtimeId) {
      bump(metrics, "traccar_command_send_failure_total", 1);
      throw err;
    }

    try {
      await postOnce(newId);
      bump(metrics, "traccar_command_stale_id_retry_success_total", 1);
      bump(metrics, "traccar_command_send_success_total", 1);
      return { ok: true, imei: payloadImei, runtimeId: newId, retried: true, previousRuntimeId: runtimeId };
    } catch (retryErr) {
      bump(metrics, "traccar_command_send_failure_total", 1);
      if (isConcurrentModificationFailure(retryErr)) {
        bump(metrics, "traccar_command_cme_total", 1);
        throw annotateCmeError(retryErr);
      }
      throw retryErr;
    }
  }
}

function logCme(_metrics, err) {
  // No tokens; message only.
  console.error(
    "[traccar-command] ConcurrentModificationException from Traccar core — pin Traccar 6.12.2 for memory-mode commands",
    extractErrorText(err).slice(0, 300)
  );
}

module.exports = {
  createTraccarDeviceRegistry,
  sendTraccarCustomCommand,
  normalizeUniqueId,
  isStaleRuntimeDeviceError,
  isConcurrentModificationFailure,
  extractErrorText,
  ENUMERATION_PATH,
  ENUMERATION_PARAMS,
};
