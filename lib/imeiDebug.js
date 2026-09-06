function parseDebugImeis(raw) {
  if (raw == null || String(raw).trim() === "") return new Set();
  return new Set(
    String(raw)
      .split(/[,\s]+/)
      .map((s) => s.trim())
      .filter(Boolean)
  );
}

function createImeiDebugger({ envValue = process.env.BRIDGE_DEBUG_IMEI, log = console } = {}) {
  const imeis = parseDebugImeis(envValue);
  function matches(imei) {
    if (!imeis.size) return false;
    const key = imei != null ? String(imei).trim() : "";
    return key && imeis.has(key);
  }
  function logPosition(fields) {
    if (!matches(fields?.imei)) return;
    log.info?.("[BRIDGE_DEBUG_IMEI]", {
      imei: fields.imei,
      position_id: fields.position_id ?? fields.traccar_position_id ?? null,
      protocol: fields.protocol ?? null,
      server_received_at: fields.server_received_at ?? fields.bridge_received_at ?? null,
      fixTime: fields.fixTime ?? null,
      deviceTime: fields.deviceTime ?? null,
      serverTime: fields.serverTime ?? null,
      fix_age_ms: fields.fix_age_ms ?? null,
      device_age_ms: fields.device_age_ms ?? null,
      server_age_ms: fields.server_age_ms ?? null,
      latitude: fields.latitude ?? null,
      longitude: fields.longitude ?? null,
      speed: fields.speed ?? null,
      course: fields.course ?? null,
      valid: fields.valid ?? null,
      outdated: fields.outdated ?? null,
      "attributes.motion": fields.attributes_motion ?? null,
      "attributes.ignition": fields.attributes_ignition ?? null,
      "attributes.type": fields.attributes_type ?? fields["attributes.type"] ?? null,
      previous_fixTime: fields.previous_fixTime ?? null,
      previous_deviceTime: fields.previous_deviceTime ?? null,
      previous_serverTime: fields.previous_serverTime ?? null,
      fixTime_changed: fields.fixTime_changed ?? null,
      deviceTime_changed: fields.deviceTime_changed ?? null,
      coordinates_changed: fields.coordinates_changed ?? null,
      live_decision: fields.live_decision ?? null,
      liveEligible: fields.liveEligible ?? null,
      effectiveLiveMs: fields.effectiveLiveMs ?? fields.effective_live_ms ?? null,
      effectiveLiveSource: fields.effectiveLiveSource ?? fields.effective_live_source ?? null,
      effective_live_ms: fields.effective_live_ms ?? fields.effectiveLiveMs ?? null,
      effective_live_source: fields.effective_live_source ?? fields.effectiveLiveSource ?? null,
      live_emitted: fields.live_emitted ?? null,
      last_live_fix_ms: fields.last_live_fix_ms ?? null,
      last_effective_live_ms: fields.last_effective_live_ms ?? null,
      device_server_skew_ms: fields.device_server_skew_ms ?? null,
      fallback_reject_reason: fields.fallback_reject_reason ?? null,
      tenant_room: fields.tenant_room ?? null,
      tenant_emit: fields.tenant_emit ?? null,
      tenant_suppression_reason: fields.tenant_suppression_reason ?? null,
      packet_date: fields.packet_date ?? null,
    });
  }
  return { matches, logPosition, imeis };
}

module.exports = { parseDebugImeis, createImeiDebugger };
