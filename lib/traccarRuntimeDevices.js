/**
 * Runtime Traccar device id resolution (IMEI → ephemeral device.id).
 *
 * Prefer createTraccarDeviceRegistry + resolveRuntimeId.
 * This helper remains for tests/callers that pass an explicit registry or client.
 *
 * NEVER use GET /api/devices?uniqueId= for service-token memory-mode lookup.
 */

const {
  createTraccarDeviceRegistry,
  sendTraccarCustomCommand,
  normalizeUniqueId,
} = require("./traccarDeviceRegistry");

/**
 * Resolve current Traccar runtime device.id for an IMEI.
 *
 * Preferred: pass `registry` (in-process snapshot from ?all=true).
 * Fallback: one-shot temporary registry refresh via `client` (still uses ?all=true).
 */
async function resolveRuntimeDeviceIdByImei({
  registry = null,
  client = null,
  imei,
  cache = null,
  metrics = null,
} = {}) {
  const normalized = normalizeUniqueId(imei);
  if (!normalized) {
    throw Object.assign(new Error("imei_required"), { statusCode: 400 });
  }

  let active = registry;
  let temporary = null;
  if (!active) {
    if (!client || typeof client.get !== "function") {
      throw Object.assign(new Error("traccar_client_not_ready"), { statusCode: 503 });
    }
    temporary = createTraccarDeviceRegistry({
      getClient: () => client,
      metrics,
    });
    active = temporary;
  }

  const runtimeId = await active.resolveRuntimeId(normalized);
  if (cache && typeof cache.set === "function") {
    cache.set(runtimeId, normalized);
  }
  return runtimeId;
}

module.exports = {
  resolveRuntimeDeviceIdByImei,
  createTraccarDeviceRegistry,
  sendTraccarCustomCommand,
  normalizeUniqueId,
};
