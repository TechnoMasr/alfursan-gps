/**
 * Single in-flight Traccar device fetch per deviceId.
 * Positions wait on the same promise instead of failing silently during cooldown.
 */
function createDeviceResolver(options = {}) {
  const {
    fetchById,
    cooldownMs = 60_000,
    now = () => Date.now(),
    log = console,
  } = options;

  if (typeof fetchById !== "function") {
    throw new Error("createDeviceResolver requires fetchById");
  }

  const inflight = new Map();
  const lastFailAt = new Map();
  const pendingByDeviceId = new Map();

  function enqueuePending(deviceId, item) {
    const key = Number(deviceId);
    if (!pendingByDeviceId.has(key)) pendingByDeviceId.set(key, []);
    pendingByDeviceId.get(key).push(item);
  }

  function takePending(deviceId) {
    const key = Number(deviceId);
    const list = pendingByDeviceId.get(key) || [];
    pendingByDeviceId.delete(key);
    return list;
  }

  async function resolve(deviceId, reason = "missing_map") {
    const key = Number(deviceId);
    if (!Number.isFinite(key)) return null;

    if (inflight.has(key)) return inflight.get(key);

    const failedAt = lastFailAt.get(key) || 0;
    if (failedAt && now() - failedAt < cooldownMs && !pendingByDeviceId.get(key)?.length) {
      // Cooldown applies only to *new* storms without waiters already queued.
      // If waiters exist we still share inflight.
    }

    const promise = (async () => {
      try {
        const imei = await fetchById(key, reason);
        if (imei) lastFailAt.delete(key);
        else lastFailAt.set(key, now());
        return imei || null;
      } catch (err) {
        lastFailAt.set(key, now());
        log.warn?.("Traccar device fetch failed", { deviceId: key, reason, error: err.message });
        return null;
      } finally {
        inflight.delete(key);
      }
    })();

    inflight.set(key, promise);
    return promise;
  }

  return {
    resolve,
    enqueuePending,
    takePending,
    inflight,
    pendingByDeviceId,
  };
}

module.exports = {
  createDeviceResolver,
};
