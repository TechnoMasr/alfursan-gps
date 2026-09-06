function createBridgeMetrics() {
  return {
    live_broadcast_total: 0,
    live_stale_suppressed: 0,
    live_out_of_order_suppressed: 0,
    live_coalesced: 0,
    live_backpressure_suppressed: 0,
    live_missing_time_suppressed: 0,
    live_future_invalid_suppressed: 0,

    subscriber_count: 0,
    slow_subscriber_count: 0,
    subscriber_terminated_backpressure: 0,
    subscriber_terminated_heartbeat: 0,

    gpspoints_queue_depth: 0,
    gpspoints_received_total: 0,
    gpspoints_batch_completed: 0,
    gpspoints_persisted_total: 0,
    gpspoints_retry_total: 0,
    gpspoints_spooled_total: 0,
    gpspoints_spool_depth: 0,
    gpspoints_spool_files: 0,
    gpspoints_spool_bytes: 0,
    gpspoints_spool_oldest_age_ms: 0,
    gpspoints_spool_write_failures: 0,
    gpspoints_mongo_attempted_total: 0,
    gpspoints_mongo_acknowledged_total: 0,
    gpspoints_duplicate_already_persisted_total: 0,
    gpspoints_unexpected_duplicate_total: 0,
    gpspoints_persist_failures: 0,
    gpspoints_duplicates_ignored: 0,
    gpspoints_health: "ok",
    disk_free_bytes: null,
    event_loop_lag_ms: null,
    persistence_dropped: 0,

    analytics_queue_depth: 0,
    analytics_spooled_total: 0,
    analytics_completed: 0,
    analytics_failures: 0,

    command_response_broadcast_total: 0,

    traccar_ingress: "http-forward",

    gpslogs_write_enabled: false,
    gpslogs_writes_attempted: 0,
    gpslogs_writes_skipped: 0,

    broadcast_per_sec: 0,
    _broadcast_window: 0,
    _broadcast_window_ts: Date.now(),

    tenant_room_gps_throttled: 0,
    tenant_room_gps_flushed: 0,
    tenant_room_gps_stationary_suppressed: 0,

    live_fix_fresh_total: 0,
    live_fix_stale_device_fresh_total: 0,
    live_fix_stale_device_stale_total: 0,
    live_fix_stale_server_fresh_total: 0,
    live_fix_stale_moving_total: 0,
    live_fix_stale_stationary_total: 0,

    traccar_positions_received_total: 0,
    traccar_positions_per_sec: 0,
    forward_positions_received_total: 0,
    forward_invalid_total: 0,
    forward_unauthorized_total: 0,
    forward_command_responses_total: 0,
    forward_last_received_at: null,
    forward_positions_per_sec: 0,
    forward_queue_depth: 0,
    forward_queue_rejected_total: 0,
    forward_queue_process_failures_total: 0,
    live_positions_eligible_total: 0,
    live_positions_suppressed_total: 0,
    live_eligibility_ratio: 0,
    live_eligible_total: 0,
    tenant_gps_emitted_total: 0,
    device_gps_emitted_total: 0,
    live_broadcast_device_time_total: 0,
    live_effective_source_fix_total: 0,
    live_effective_source_device_total: 0,
    live_device_fallback_rejected_total: 0,

    positions_received: 0,
    positions_live_eligible: 0,
    positions_live_historical: 0,
    positions_live_out_of_order: 0,
    tenant_room_resolved: 0,
    tenant_room_unresolved: 0,
    tenant_room_has_listener: 0,
    tenant_room_no_listener: 0,
    tenant_gps_sent: 0,

    _positions_window: 0,
    _positions_window_ts: Date.now(),
  };
}

function recordLiveDecision(metrics, decision) {
  if (!metrics) return;
  switch (decision) {
    case "fresh":
      metrics.live_broadcast_total += 1;
      break;
    case "fresh_device_time":
      metrics.live_broadcast_total += 1;
      metrics.live_broadcast_device_time_total =
        (metrics.live_broadcast_device_time_total || 0) + 1;
      break;
    case "historical":
      metrics.live_stale_suppressed += 1;
      break;
    case "out_of_order":
      metrics.live_out_of_order_suppressed += 1;
      break;
    case "missing_time":
      metrics.live_missing_time_suppressed += 1;
      break;
    case "future_invalid":
      metrics.live_future_invalid_suppressed += 1;
      break;
    case "superseded":
    case "backpressure_coalesced":
      metrics.live_coalesced += 1;
      break;
    default:
      break;
  }
}

function snapshotMetrics(metrics, extra = {}) {
  return {
    gpslogs_write_enabled: !!metrics.gpslogs_write_enabled,
    traccar_ingress: metrics.traccar_ingress || "http-forward",
    live_broadcast_total: metrics.live_broadcast_total,
    live_stale_suppressed: metrics.live_stale_suppressed,
    live_out_of_order_suppressed: metrics.live_out_of_order_suppressed,
    live_coalesced: metrics.live_coalesced,
    live_backpressure_suppressed: metrics.live_backpressure_suppressed,
    live_missing_time_suppressed: metrics.live_missing_time_suppressed,
    live_future_invalid_suppressed: metrics.live_future_invalid_suppressed,
    subscriber_count: metrics.subscriber_count,
    slow_subscriber_count: metrics.slow_subscriber_count,
    subscriber_terminated_backpressure: metrics.subscriber_terminated_backpressure,
    subscriber_terminated_heartbeat: metrics.subscriber_terminated_heartbeat,
    gpspoints_queue_depth: metrics.gpspoints_queue_depth,
    gpspoints_received_total: metrics.gpspoints_received_total,
    gpspoints_batch_completed: metrics.gpspoints_batch_completed,
    gpspoints_persisted_total: metrics.gpspoints_persisted_total,
    gpspoints_retry_total: metrics.gpspoints_retry_total,
    gpspoints_spooled_total: metrics.gpspoints_spooled_total,
    gpspoints_spool_depth: metrics.gpspoints_spool_depth,
    gpspoints_spool_files: metrics.gpspoints_spool_files,
    gpspoints_spool_bytes: metrics.gpspoints_spool_bytes,
    gpspoints_spool_oldest_age_ms: metrics.gpspoints_spool_oldest_age_ms,
    gpspoints_spool_write_failures: metrics.gpspoints_spool_write_failures,
    gpspoints_mongo_attempted_total: metrics.gpspoints_mongo_attempted_total,
    gpspoints_mongo_acknowledged_total: metrics.gpspoints_mongo_acknowledged_total,
    gpspoints_duplicate_already_persisted_total: metrics.gpspoints_duplicate_already_persisted_total,
    gpspoints_unexpected_duplicate_total: metrics.gpspoints_unexpected_duplicate_total,
    gpspoints_persist_failures: metrics.gpspoints_persist_failures,
    gpspoints_duplicates_ignored: metrics.gpspoints_duplicates_ignored,
    gpspoints_health: metrics.gpspoints_health || "ok",
    disk_free_bytes: metrics.disk_free_bytes,
    event_loop_lag_ms: metrics.event_loop_lag_ms,
    persistence_dropped: metrics.persistence_dropped,
    analytics_queue_depth: metrics.analytics_queue_depth,
    analytics_spooled_total: metrics.analytics_spooled_total,
    analytics_completed: metrics.analytics_completed,
    command_response_broadcast_total: metrics.command_response_broadcast_total,
    broadcast_per_sec: metrics.broadcast_per_sec,
    live_fix_fresh_total: metrics.live_fix_fresh_total || 0,
    live_fix_stale_device_fresh_total: metrics.live_fix_stale_device_fresh_total || 0,
    live_fix_stale_device_stale_total: metrics.live_fix_stale_device_stale_total || 0,
    live_fix_stale_server_fresh_total: metrics.live_fix_stale_server_fresh_total || 0,
    live_fix_stale_moving_total: metrics.live_fix_stale_moving_total || 0,
    live_fix_stale_stationary_total: metrics.live_fix_stale_stationary_total || 0,
    traccar_positions_received_total: metrics.traccar_positions_received_total || 0,
    traccar_positions_per_sec: metrics.traccar_positions_per_sec || 0,
    forward_positions_received_total: metrics.forward_positions_received_total || 0,
    forward_invalid_total: metrics.forward_invalid_total || 0,
    forward_unauthorized_total: metrics.forward_unauthorized_total || 0,
    forward_command_responses_total: metrics.forward_command_responses_total || 0,
    forward_last_received_at: metrics.forward_last_received_at || null,
    forward_positions_per_sec: metrics.forward_positions_per_sec || 0,
    forward_queue_depth: metrics.forward_queue_depth || 0,
    forward_queue_rejected_total: metrics.forward_queue_rejected_total || 0,
    forward_queue_process_failures_total: metrics.forward_queue_process_failures_total || 0,
    live_positions_eligible_total: metrics.live_positions_eligible_total || 0,
    live_positions_suppressed_total: metrics.live_positions_suppressed_total || 0,
    live_eligibility_ratio: metrics.live_eligibility_ratio || 0,
    live_eligible_total: metrics.live_eligible_total || 0,
    tenant_gps_emitted_total: metrics.tenant_gps_emitted_total || 0,
    device_gps_emitted_total: metrics.device_gps_emitted_total || 0,
    live_broadcast_device_time_total: metrics.live_broadcast_device_time_total || 0,
    live_effective_source_fix_total: metrics.live_effective_source_fix_total || 0,
    live_effective_source_device_total: metrics.live_effective_source_device_total || 0,
    live_device_fallback_rejected_total: metrics.live_device_fallback_rejected_total || 0,
    positions_received: metrics.positions_received || 0,
    positions_live_eligible: metrics.positions_live_eligible || 0,
    positions_live_historical: metrics.positions_live_historical || 0,
    positions_live_out_of_order: metrics.positions_live_out_of_order || 0,
    tenant_room_resolved: metrics.tenant_room_resolved || 0,
    tenant_room_unresolved: metrics.tenant_room_unresolved || 0,
    tenant_room_has_listener: metrics.tenant_room_has_listener || 0,
    tenant_room_no_listener: metrics.tenant_room_no_listener || 0,
    tenant_gps_sent: metrics.tenant_gps_sent || 0,
    ...extra,
  };
}

module.exports = {
  createBridgeMetrics,
  recordLiveDecision,
  snapshotMetrics,
};
