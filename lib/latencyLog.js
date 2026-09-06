function maybeLatencyLog(enabled, fields) {
  if (!enabled) return;
  const line = [
    "[bridge-latency]",
    `imei=${fields.imei ?? ""}`,
    `traccar_position_id=${fields.traccar_position_id ?? ""}`,
    `fixTime=${fields.fixTime ?? ""}`,
    `deviceTime=${fields.deviceTime ?? ""}`,
    `serverTime=${fields.serverTime ?? ""}`,
    `bridge_received_at=${fields.bridge_received_at ?? ""}`,
    `bridge_live_sent_at=${fields.bridge_live_sent_at ?? ""}`,
    `archive_enqueued_at=${fields.archive_enqueued_at ?? ""}`,
    `archive_persisted_at=${fields.archive_persisted_at ?? ""}`,
    `fix_age_ms=${fields.fix_age_ms ?? ""}`,
    `server_age_ms=${fields.server_age_ms ?? ""}`,
    `live_delivery_ms=${fields.live_delivery_ms ?? ""}`,
    `persist_lag_ms=${fields.persist_lag_ms ?? ""}`,
    `ws_buffered_amount=${fields.ws_buffered_amount ?? ""}`,
    `live_decision=${fields.live_decision ?? ""}`,
    `live_drop_reason=${fields.live_drop_reason ?? ""}`,
  ].join(" ");
  console.log(line);
}

module.exports = { maybeLatencyLog };
