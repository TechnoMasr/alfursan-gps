const DEFAULT_MODEL_CACHE_TTL_MS = 300_000;
const DEFAULT_NEGATIVE_MODEL_CACHE_TTL_MS = 60_000;
const DEFAULT_CONCURRENCY = 16;
const DEFAULT_QUEUE_MAX = 20_000;
const DEFAULT_RETRY_BASE_MS = 30_000;
const DEFAULT_RETRY_MAX_MS = 300_000;

function normalizeImei(value) {
  return String(value ?? "").trim();
}

function normalizeModel(value) {
  const model = String(value ?? "").trim();
  return model || null;
}

function normalizeRuntimeDeviceId(value) {
  const id = Number(value);
  return Number.isFinite(id) && id > 0 ? id : null;
}

function statusCode(err) {
  return Number(err?.response?.status || err?.statusCode || 0) || null;
}

function cloneDevice(device) {
  if (!device || typeof device !== "object") return null;
  return { ...device };
}

function createTraccarModelSync({
  getClient,
  metrics = {},
  log = console,
  concurrency = DEFAULT_CONCURRENCY,
  queueMax = DEFAULT_QUEUE_MAX,
  modelCacheTtlMs = DEFAULT_MODEL_CACHE_TTL_MS,
  negativeModelCacheTtlMs = DEFAULT_NEGATIVE_MODEL_CACHE_TTL_MS,
  retryBaseMs = DEFAULT_RETRY_BASE_MS,
  retryMaxMs = DEFAULT_RETRY_MAX_MS,
  lookupModel = null,
  now = () => Date.now(),
} = {}) {
  const modelCache = new Map();
  const syncedRuntime = new Map();
  const pending = new Map();
  const queuedKeys = [];
  const running = new Set();
  const retryState = new Map();
  const maxConcurrency = Math.max(1, Number(concurrency) || DEFAULT_CONCURRENCY);
  const maxQueueDepth = Math.max(1, Number(queueMax) || DEFAULT_QUEUE_MAX);
  let active = 0;
  let pumpScheduled = false;

  metrics.traccar_model_sync_requested_total = metrics.traccar_model_sync_requested_total || 0;
  metrics.traccar_model_sync_success_total = metrics.traccar_model_sync_success_total || 0;
  metrics.traccar_model_sync_failed_total = metrics.traccar_model_sync_failed_total || 0;
  metrics.traccar_model_sync_skipped_no_model_total =
    metrics.traccar_model_sync_skipped_no_model_total || 0;
  metrics.traccar_model_sync_already_correct_total =
    metrics.traccar_model_sync_already_correct_total || 0;
  metrics.traccar_model_sync_attempt_total = metrics.traccar_model_sync_attempt_total || 0;
  metrics.traccar_model_sync_verified_total = metrics.traccar_model_sync_verified_total || 0;
  metrics.traccar_model_sync_mismatch_total = metrics.traccar_model_sync_mismatch_total || 0;
  metrics.traccar_model_sync_pending = metrics.traccar_model_sync_pending || 0;
  metrics.traccar_model_sync_queue_depth = metrics.traccar_model_sync_queue_depth || 0;
  metrics.traccar_model_cache_size = metrics.traccar_model_cache_size || 0;
  metrics.traccar_model_sync_last_success_at = metrics.traccar_model_sync_last_success_at || null;
  const debugImei = normalizeImei(process.env.TRACCAR_MODEL_SYNC_DEBUG_IMEI);

  function bump(name, amount = 1) {
    metrics[name] = (metrics[name] || 0) + amount;
  }

  function debug(imei, message, fields = {}) {
    if (!debugImei || debugImei !== imei) return;
    log.log?.(`[traccar-model-sync-debug] imei=${imei} ${message}`, fields);
  }

  function refreshMetrics() {
    metrics.traccar_model_sync_pending = pending.size + running.size;
    metrics.traccar_model_sync_queue_depth = queuedKeys.length;
    metrics.traccar_model_cache_size = modelCache.size;
  }

  function cacheDesiredModel(imei, trModel, ttlMs) {
    modelCache.set(imei, {
      trModel,
      expiresAt: now() + Math.max(1000, Number(ttlMs) || 1000),
    });
    refreshMetrics();
  }

  function getCachedModel(imei) {
    const cached = modelCache.get(imei);
    if (!cached) return { hit: false };
    if (cached.expiresAt <= now()) {
      modelCache.delete(imei);
      refreshMetrics();
      return { hit: false };
    }
    return { hit: true, trModel: cached.trModel };
  }

  async function lookupDesiredModel(imei) {
    const cached = getCachedModel(imei);
    if (cached.hit) return cached.trModel;

    if (typeof lookupModel === "function") {
      const trModel = normalizeModel(await lookupModel(imei));
      cacheDesiredModel(imei, trModel, trModel ? modelCacheTtlMs : negativeModelCacheTtlMs);
      return trModel;
    }

    const { DeviceStatus } = require("../mongo");
    const mongoose = require("mongoose");
    const projection = { tr_model: 1 };
    const details = await mongoose.connection
      .collection("device_details")
      .findOne({ imei }, { projection });
    const detailsModel = normalizeModel(details?.tr_model);
    if (detailsModel) {
      cacheDesiredModel(imei, detailsModel, modelCacheTtlMs);
      return detailsModel;
    }

    const status = await DeviceStatus.findOne({ imei }).select({ tr_model: 1 }).lean();
    const statusModel = normalizeModel(status?.tr_model);
    if (statusModel) {
      cacheDesiredModel(imei, statusModel, modelCacheTtlMs);
      return statusModel;
    }

    cacheDesiredModel(imei, null, negativeModelCacheTtlMs);
    return null;
  }

  function runtimeKey(imei, runtimeDeviceId) {
    return `${imei}|${runtimeDeviceId}`;
  }

  function canUseSyncedState(imei, runtimeDeviceId, currentModel, currentModelProvided) {
    const cached = getCachedModel(imei);
    if (!cached.hit || !cached.trModel) return false;
    const state = syncedRuntime.get(runtimeKey(imei, runtimeDeviceId));
    if (!state || state.trModel !== cached.trModel) return false;
    if (currentModelProvided && !currentModel) return false;
    if (currentModel && currentModel !== cached.trModel) return false;
    return true;
  }

  function retryDelayFor(key) {
    const state = retryState.get(key);
    if (!state) return 0;
    return Math.max(0, state.nextAttemptAt - now());
  }

  function recordRetry(key, err) {
    const previous = retryState.get(key) || { failures: 0 };
    const failures = previous.failures + 1;
    const delay = Math.min(retryMaxMs, retryBaseMs * 2 ** Math.min(failures - 1, 6));
    retryState.set(key, { failures, nextAttemptAt: now() + delay, lastError: err?.message || String(err) });
  }

  function clearRetry(key) {
    retryState.delete(key);
  }

  function schedulePump() {
    if (pumpScheduled) return;
    pumpScheduled = true;
    setImmediate(() => {
      pumpScheduled = false;
      void pump();
    });
  }

  function schedule(task) {
    const imei = normalizeImei(task?.imei);
    const runtimeDeviceId = normalizeRuntimeDeviceId(task?.runtimeDeviceId);
    if (!imei || !runtimeDeviceId) return { accepted: false, reason: "missing_identity" };

    const currentModelProvided = Object.prototype.hasOwnProperty.call(task || {}, "currentModel");
    const currentModel = normalizeModel(task?.currentModel);
    const cached = getCachedModel(imei);
    if (cached.hit && !cached.trModel) {
      return { accepted: true, skipped: "no_model_cached" };
    }
    if (cached.hit && cached.trModel && currentModel === cached.trModel) {
      syncedRuntime.set(runtimeKey(imei, runtimeDeviceId), {
        trModel: cached.trModel,
        syncedAt: now(),
      });
      bump("traccar_model_sync_already_correct_total");
      refreshMetrics();
      return { accepted: true, skipped: "already_correct" };
    }
    if (canUseSyncedState(imei, runtimeDeviceId, currentModel, currentModelProvided)) {
      return { accepted: true, skipped: "already_synced" };
    }

    const key = runtimeKey(imei, runtimeDeviceId);
    const retryDelay = retryDelayFor(key);
    if (retryDelay > 0) return { accepted: true, skipped: "backoff" };
    if (pending.has(key) || running.has(key)) return { accepted: true, skipped: "pending" };
    if (queuedKeys.length >= maxQueueDepth) {
      bump("traccar_model_sync_failed_total");
      refreshMetrics();
      return { accepted: false, reason: "queue_full" };
    }

    pending.set(key, {
      imei,
      runtimeDeviceId,
      currentModel,
      currentModelProvided,
      forwardedDevice: cloneDevice(task.forwardedDevice),
    });
    queuedKeys.push(key);
    bump("traccar_model_sync_requested_total");
    refreshMetrics();
    schedulePump();
    return { accepted: true, key };
  }

  async function getCurrentDevice(client, task) {
    const { data } = await client.get(`/api/devices/${task.runtimeDeviceId}`);
    if (!data || typeof data !== "object") throw new Error("traccar_device_empty");
    const id = normalizeRuntimeDeviceId(data.id);
    const uniqueId = normalizeImei(data.uniqueId);
    if (id !== task.runtimeDeviceId || uniqueId !== task.imei) {
      throw new Error("traccar_device_identity_mismatch");
    }
    return data;
  }

  async function runTask(key, task) {
    try {
      const desiredModel = await lookupDesiredModel(task.imei);
      debug(task.imei, "desired_model", {
        runtime_id: task.runtimeDeviceId,
        desired_model: desiredModel,
        forwarded_model: task.currentModel,
        forwarded_model_provided: task.currentModelProvided,
      });
      if (!desiredModel) {
        bump("traccar_model_sync_skipped_no_model_total");
        log.warn?.(`[traccar-model-sync] imei=${task.imei} skipped: no tr_model`);
        return;
      }

      if (normalizeModel(task.currentModel) === desiredModel) {
        syncedRuntime.set(runtimeKey(task.imei, task.runtimeDeviceId), {
          trModel: desiredModel,
          syncedAt: now(),
        });
        clearRetry(key);
        bump("traccar_model_sync_already_correct_total");
        return;
      }

      const client = typeof getClient === "function" ? getClient() : null;
      if (!client || typeof client.get !== "function" || typeof client.put !== "function") {
        throw new Error("traccar_client_not_ready");
      }
      const currentDevice = await getCurrentDevice(client, task);
      debug(task.imei, "current_device", {
        runtime_id: task.runtimeDeviceId,
        traccar_model: currentDevice.model ?? null,
      });
      if (normalizeModel(currentDevice.model) === desiredModel) {
        syncedRuntime.set(runtimeKey(task.imei, task.runtimeDeviceId), {
          trModel: desiredModel,
          syncedAt: now(),
        });
        clearRetry(key);
        bump("traccar_model_sync_already_correct_total");
        bump("traccar_model_sync_verified_total");
        return;
      }

      const updatedDevice = { ...currentDevice, model: desiredModel };
      bump("traccar_model_sync_attempt_total");
      const putResponse = await client.put(`/api/devices/${task.runtimeDeviceId}`, updatedDevice);
      const verifiedDevice = await getCurrentDevice(client, task);
      const verifiedModel = normalizeModel(verifiedDevice.model);
      debug(task.imei, "put_verified", {
        runtime_id: task.runtimeDeviceId,
        put_status: putResponse?.status ?? null,
        expected_model: desiredModel,
        verified_model: verifiedModel,
      });
      if (verifiedModel !== desiredModel) {
        bump("traccar_model_sync_mismatch_total");
        throw new Error(`traccar_model_verify_mismatch expected=${desiredModel} actual=${verifiedModel || "null"}`);
      }
      syncedRuntime.set(runtimeKey(task.imei, task.runtimeDeviceId), {
        trModel: desiredModel,
        syncedAt: now(),
      });
      clearRetry(key);
      bump("traccar_model_sync_success_total");
      bump("traccar_model_sync_verified_total");
      metrics.traccar_model_sync_last_success_at = new Date(now()).toISOString();
      log.log?.(
        `[traccar-model-sync] imei=${task.imei} runtime_id=${task.runtimeDeviceId} model=${desiredModel} synced`
      );
    } catch (err) {
      const code = statusCode(err);
      if (code === 404) syncedRuntime.delete(runtimeKey(task.imei, task.runtimeDeviceId));
      recordRetry(key, err);
      bump("traccar_model_sync_failed_total");
      log.warn?.(
        `[traccar-model-sync] imei=${task.imei} runtime_id=${task.runtimeDeviceId} failed status=${code || "n/a"} error=${err.message}`
      );
    } finally {
      running.delete(key);
      active -= 1;
      refreshMetrics();
      schedulePump();
    }
  }

  async function pump() {
    while (active < maxConcurrency && queuedKeys.length) {
      const key = queuedKeys.shift();
      const task = pending.get(key);
      pending.delete(key);
      if (!task) continue;
      running.add(key);
      active += 1;
      refreshMetrics();
      void runTask(key, task);
    }
    refreshMetrics();
  }

  return {
    schedule,
    getStats: () => ({
      pending: pending.size + running.size,
      queueDepth: queuedKeys.length,
      cacheSize: modelCache.size,
    }),
    _modelCache: modelCache,
    _syncedRuntime: syncedRuntime,
    _pending: pending,
    _queuedKeys: queuedKeys,
  };
}

module.exports = {
  createTraccarModelSync,
  normalizeModel,
  normalizeImei,
  normalizeRuntimeDeviceId,
};
