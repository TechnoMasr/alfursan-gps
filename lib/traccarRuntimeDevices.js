async function resolveRuntimeDeviceIdByImei({ client, imei, cache } = {}) {
  const normalized = String(imei || "").trim();
  if (!normalized) throw Object.assign(new Error("imei_required"), { statusCode: 400 });
  if (!client || typeof client.get !== "function") {
    throw Object.assign(new Error("traccar_client_not_ready"), { statusCode: 503 });
  }

  const { data } = await client.get("/api/devices", {
    params: { uniqueId: normalized },
  });
  const list = Array.isArray(data) ? data : data ? [data] : [];
  const matches = list.filter((dev) => String(dev?.uniqueId || "").trim() === normalized);

  if (matches.length !== 1) {
    throw Object.assign(
      new Error(
        matches.length === 0
          ? "device_not_registered_in_traccar_runtime"
          : "multiple_runtime_devices_for_imei"
      ),
      { statusCode: matches.length === 0 ? 404 : 409 }
    );
  }

  const runtimeId = Number(matches[0].id);
  if (!Number.isFinite(runtimeId)) {
    throw Object.assign(new Error("invalid_runtime_device_id"), { statusCode: 502 });
  }

  if (cache && typeof cache.set === "function") cache.set(runtimeId, normalized);
  return runtimeId;
}

module.exports = { resolveRuntimeDeviceIdByImei };
