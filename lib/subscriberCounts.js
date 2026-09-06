const COMMAND_RESPONSE_CHANNEL = "command_response_chanel";

function countSubscriberMetrics({
  deviceSubscribers = new Map(),
  rooms = new Map(),
  commandChannel = COMMAND_RESPONSE_CHANNEL,
} = {}) {
  const unique = new Set();
  let deviceMemberships = 0;
  for (const clients of deviceSubscribers.values()) {
    deviceMemberships += clients.size;
    for (const ws of clients) unique.add(ws);
  }
  let roomMemberships = 0;
  let tenantRoomListeners = 0;
  for (const [name, clients] of rooms.entries()) {
    roomMemberships += clients.size;
    for (const ws of clients) unique.add(ws);
    if (String(name).startsWith("tenant:")) tenantRoomListeners += clients.size;
  }
  return {
    subscriber_count: unique.size,
    subscriber_unique_sockets: unique.size,
    subscriber_room_memberships: roomMemberships,
    tenant_room_listeners: tenantRoomListeners,
    command_response_channel_listeners: rooms.get(commandChannel)?.size || 0,
    device_subscription_memberships: deviceMemberships,
  };
}

module.exports = { countSubscriberMetrics, COMMAND_RESPONSE_CHANNEL };
