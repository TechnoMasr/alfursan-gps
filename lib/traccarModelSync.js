const DEFAULT_MODEL_CACHE_TTL_MS = 300_000;
const DEFAULT_NEGATIVE_MODEL_CACHE_TTL_MS = 600_000;
const DEFAULT_CONCURRENCY = 16;
const DEFAULT_QUEUE_MAX = 20_000;
const DEFAULT_RETRY_BASE_MS = 30_000;
const DEFAULT_RETRY_MAX_MS = 300_000;
/** Effective Traccar model when Mongo tr_model is missing/null/blank. Not written back to Mongo. */
const DEFAULT_DEVICE_MODEL = "Seeworld";

/**
 * Traccar model sync is DEVICE-LIFECYCLE based, never packet-triggered.
 *
 * Hot path after resolution: Map lookup only (no Mongo, no Traccar HTTP, no log).
 *
 * Concepts:
 *   A) desired model metadata cache  — IMEI -> effective tr_model (TTL)
 *   B) runtime sync state            — IMEI|runtimeId -> synced model
 * Traccar restart (forwarded model null) invalidates B, not necessarily A.
 *
 * Effective desired model:
 *   non-empty Mongo tr_model → that model
 *   missing/null/blank Mongo  → "Seeworld" (runtime fallback; do not mutate Mongo)
 */

function normalizeImei(value) {
  return String(value ?? "").trim();
}

function normalizeModel(value) {
  const model = String(value ?? "").trim();
  return model || null;
}

/**
 * Resolve effective desired Traccar model from a raw Mongo/lookup value.
 * Does not read or write Mongo.
 */
function resolveEffectiveDesiredModel(rawModel) {
  const explicit = normalizeModel(rawModel);
  if (explicit) {
    return { model: explicit, usedDefault: false, source: "explicit" };
  }
  return {
    model: DEFAULT_DEVICE_MODEL,
    usedDefault: true,
    source: "default_seeworld",
  };
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
  defaultDeviceModel = DEFAULT_DEVICE_MODEL,
  lookupModel = null,
  now = () => Date.now(),
} = {}) {
  /** @type {Map<string, { trModel: string|null, expiresAt: number, usedDefault?: boolean, announcedNoModel?: boolean }>} */
  const modelCache = new Map();
  /** @type {Map<string, { trModel: string, syncedAt: number }>} */
  const syncedRuntime = new Map();
  /**
   * Compact lifecycle view (derived + explicit for hot-path decisions).
   * state: unknown | resolving | syncing | synced | retry_wait
   * (no_model retained only as defensive; missing Mongo model → Seeworld)
   * @type {Map<string, { runtimeDeviceId: number|null, desiredModel: string|null, state: string, lastCheckedAt: number, lastObservedTraccarModel: string|null }>}
   */
  const lifecycle = new Map();
  const pending = new Map();
  const queuedKeys = [];
  const running = new Set();
  const retryState = new Map();
  const maxConcurrency = Math.max(1, Number(concurrency) || DEFAULT_CONCURRENCY);
  const maxQueueDepth = Math.max(1, Number(queueMax) || DEFAULT_QUEUE_MAX);
  const fallbackModel = normalizeModel(defaultDeviceModel) || DEFAULT_DEVICE_MODEL;
  let active = 0;
  let pumpScheduled = false;

  // Keep unused negative TTL param for env/API compatibility (no longer used for blank Mongo).
  void negativeModelCacheTtlMs;

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
  metrics.traccar_model_no_model_cache_size = metrics.traccar_model_no_model_cache_size || 0;
  metrics.traccar_model_sync_last_success_at = metrics.traccar_model_sync_last_success_at || null;
  metrics.traccar_model_sync_explicit_total = metrics.traccar_model_sync_explicit_total || 0;
  metrics.traccar_model_sync_default_seeworld_total =
    metrics.traccar_model_sync_default_seeworld_total || 0;
  const debugImei = normalizeImei(process.env.TRACCAR_MODEL_SYNC_DEBUG_IMEI);

  function bump(name, amount = 1) {
    metrics[name] = (metrics[name] || 0) + amount;
  }

  function resolveDesired(rawModel) {
    const explicit = normalizeModel(rawModel);
    if (explicit) {
      return { model: explicit, usedDefault: false, source: "explicit" };
    }
    return {
      model: fallbackModel,
      usedDefault: true,
      source: "default_seeworld",
    };
  }

  function debug(imei, message, fields = {}) {
    if (!debugImei || debugImei !== imei) return;
    log.log?.(`[traccar-model-sync-debug] imei=${imei} ${message}`, fields);
  }

  function countNoModelCache() {
    let n = 0;
    const ts = now();
    for (const entry of modelCache.values()) {
      if (entry.trModel == null && entry.expiresAt > ts) n += 1;
    }
    return n;
  }

  function refreshMetrics() {
    metrics.traccar_model_sync_pending = pending.size + running.size;
    metrics.traccar_model_sync_queue_depth = queuedKeys.length;
    metrics.traccar_model_cache_size = modelCache.size;
    metrics.traccar_model_no_model_cache_size = countNoModelCache();
  }

  function setLifecycle(imei, patch) {
    const prev = lifecycle.get(imei) || {
      runtimeDeviceId: null,
      desiredModel: null,
      state: "unknown",
      lastCheckedAt: 0,
      lastObservedTraccarModel: null,
    };
    lifecycle.set(imei, { ...prev, ...patch, lastCheckedAt: now() });
  }

  function cacheDesiredModel(imei, trModel, ttlMs, { usedDefault = false } = {}) {
    modelCache.set(imei, {
      trModel,
      expiresAt: now() + Math.max(1000, Number(ttlMs) || 1000),
      usedDefault: Boolean(usedDefault),
      announcedNoModel: false,
    });
    refreshMetrics();
  }

  function getCachedModel(imei) {
    const cached = modelCache.get(imei);
    if (!cached) return { hit: false };
    if (cached.expiresAt <= now()) {
      modelCache.delete(imei);
      refreshMetrics();
      return {
        hit: false,
        expired: true,
        previousTrModel: cached.trModel,
        usedDefault: cached.usedDefault,
      };
    }
    return {
      hit: true,
      trModel: cached.trModel,
      usedDefault: cached.usedDefault,
    };
  }

  async function lookupRawMongoModel(imei) {
    if (typeof lookupModel === "function") {
      return lookupModel(imei);
    }

    const { DeviceStatus } = require("../mongo");
    const mongoose = require("mongoose");
    const projection = { tr_model: 1 };
    const details = await mongoose.connection
      .collection("device_details")
      .findOne({ imei }, { projection });
    const detailsModel = normalizeModel(details?.tr_model);
    if (detailsModel) return detailsModel;

    const status = await DeviceStatus.findOne({ imei }).select({ tr_model: 1 }).lean();
    return normalizeModel(status?.tr_model);
  }

  /**
   * Resolve + cache effective desired model (explicit Mongo or Seeworld fallback).
   * Never writes to Mongo.
   */
  async function lookupDesiredModel(imei) {
    const cached = getCachedModel(imei);
    if (cached.hit && cached.trModel) return cached.trModel;

    // Legacy negative null cache entry → treat as miss and apply Seeworld.
    if (cached.hit && !cached.trModel) {
      modelCache.delete(imei);
    }

    const raw = await lookupRawMongoModel(imei);
    const resolved = resolveDesired(raw);
    cacheDesiredModel(imei, resolved.model, modelCacheTtlMs, {
      usedDefault: resolved.usedDefault,
    });
    if (resolved.usedDefault) bump("traccar_model_sync_default_seeworld_total");
    else bump("traccar_model_sync_explicit_total");
    return resolved.model;
  }

  function runtimeKey(imei, runtimeDeviceId) {
    return `${imei}|${runtimeDeviceId}`;
  }

  function markRuntimeSynced(imei, runtimeDeviceId, trModel) {
    syncedRuntime.set(runtimeKey(imei, runtimeDeviceId), {
      trModel,
      syncedAt: now(),
    });
    setLifecycle(imei, {
      runtimeDeviceId,
      desiredModel: trModel,
      state: "synced",
      lastObservedTraccarModel: trModel,
    });
  }

  function canUseSyncedState(imei, runtimeDeviceId, currentModel, currentModelProvided) {
    const cached = getCachedModel(imei);
    if (!cached.hit || !cached.trModel) return false;
    const state = syncedRuntime.get(runtimeKey(imei, runtimeDeviceId));
    if (!state || state.trModel !== cached.trModel) return false;
    // Traccar memory restart: forwarded model became empty → must re-sync once.
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
    retryState.set(key, {
      failures,
      nextAttemptAt: now() + delay,
      lastError: err?.message || String(err),
    });
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

  /**
   * Hot-path entry. After lifecycle resolution, almost always a silent Map hit.
   */
  function schedule(task) {
    const imei = normalizeImei(task?.imei);
    const runtimeDeviceId = normalizeRuntimeDeviceId(task?.runtimeDeviceId);
    if (!imei || !runtimeDeviceId) return { accepted: false, reason: "missing_identity" };

    const currentModelProvided = Object.prototype.hasOwnProperty.call(task || {}, "currentModel");
    const currentModel = normalizeModel(task?.currentModel);
    const life = lifecycle.get(imei);

    let cached = getCachedModel(imei);
    // Drop obsolete null/no_model cache entries so Seeworld fallback can apply.
    if (cached.hit && !cached.trModel) {
      modelCache.delete(imei);
      refreshMetrics();
      cached = { hit: false };
    }

    // --- silent synced / already-correct (same runtime lifecycle) ---
    if (cached.hit && cached.trModel) {
      if (canUseSyncedState(imei, runtimeDeviceId, currentModel, currentModelProvided)) {
        debug(imei, "synced_cache_hit", { runtime_id: runtimeDeviceId, model: cached.trModel });
        return { accepted: true, skipped: "already_synced" };
      }
      if (currentModel === cached.trModel) {
        const key = runtimeKey(imei, runtimeDeviceId);
        const prev = syncedRuntime.get(key);
        if (!prev || prev.trModel !== cached.trModel) {
          markRuntimeSynced(imei, runtimeDeviceId, cached.trModel);
          bump("traccar_model_sync_already_correct_total");
          log.log?.(
            `[traccar-model-sync] imei=${imei} runtime_id=${runtimeDeviceId} model=${cached.trModel} already_correct`
          );
        }
        return { accepted: true, skipped: "already_correct" };
      }
    }

    // Retry backoff — silent
    const key = runtimeKey(imei, runtimeDeviceId);
    const retryDelay = retryDelayFor(key);
    if (retryDelay > 0) {
      setLifecycle(imei, {
        runtimeDeviceId,
        desiredModel: cached.hit ? cached.trModel : life?.desiredModel ?? null,
        state: "retry_wait",
        lastObservedTraccarModel: currentModel,
      });
      return { accepted: true, skipped: "backoff" };
    }
    if (pending.has(key) || running.has(key)) {
      return { accepted: true, skipped: "pending" };
    }
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
      hadDesiredCached: Boolean(cached.hit && cached.trModel),
    });
    queuedKeys.push(key);
    bump("traccar_model_sync_requested_total");
    setLifecycle(imei, {
      runtimeDeviceId,
      desiredModel: cached.hit ? cached.trModel : life?.desiredModel ?? null,
      state: cached.hit && cached.trModel ? "syncing" : "resolving",
      lastObservedTraccarModel: currentModel,
    });
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

      // Defensive only — resolveDesired always yields Seeworld when Mongo blank.
      if (!desiredModel) {
        bump("traccar_model_sync_skipped_no_model_total");
        setLifecycle(task.imei, {
          runtimeDeviceId: task.runtimeDeviceId,
          desiredModel: null,
          state: "no_model",
          lastObservedTraccarModel: task.currentModel,
        });
        return;
      }

      if (normalizeModel(task.currentModel) === desiredModel) {
        markRuntimeSynced(task.imei, task.runtimeDeviceId, desiredModel);
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
        markRuntimeSynced(task.imei, task.runtimeDeviceId, desiredModel);
        clearRetry(key);
        bump("traccar_model_sync_already_correct_total");
        bump("traccar_model_sync_verified_total");
        return;
      }

      const updatedDevice = { ...currentDevice, model: desiredModel };
      bump("traccar_model_sync_attempt_total");
      setLifecycle(task.imei, {
        runtimeDeviceId: task.runtimeDeviceId,
        desiredModel,
        state: "syncing",
        lastObservedTraccarModel: normalizeModel(currentDevice.model),
      });
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
        throw new Error(
          `traccar_model_verify_mismatch expected=${desiredModel} actual=${verifiedModel || "null"}`
        );
      }
      markRuntimeSynced(task.imei, task.runtimeDeviceId, desiredModel);
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
      setLifecycle(task.imei, {
        runtimeDeviceId: task.runtimeDeviceId,
        state: "retry_wait",
        lastObservedTraccarModel: task.currentModel,
      });
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

  /** External invalidation when another process updates tr_model. */
  function invalidate(imeiRaw) {
    const imei = normalizeImei(imeiRaw);
    if (!imei) return;
    modelCache.delete(imei);
    for (const key of [...syncedRuntime.keys()]) {
      if (key.startsWith(`${imei}|`)) syncedRuntime.delete(key);
    }
    setLifecycle(imei, {
      desiredModel: null,
      state: "unknown",
      lastObservedTraccarModel: null,
    });
    refreshMetrics();
  }

  return {
    schedule,
    invalidate,
    getStats: () => ({
      pending: pending.size + running.size,
      queueDepth: queuedKeys.length,
      cacheSize: modelCache.size,
      noModelCacheSize: countNoModelCache(),
      lifecycleSize: lifecycle.size,
    }),
    _modelCache: modelCache,
    _syncedRuntime: syncedRuntime,
    _lifecycle: lifecycle,
    _pending: pending,
    _queuedKeys: queuedKeys,
  };
}

module.exports = {
  createTraccarModelSync,
  normalizeModel,
  normalizeImei,
  normalizeRuntimeDeviceId,
  resolveEffectiveDesiredModel,
  DEFAULT_DEVICE_MODEL,
};
