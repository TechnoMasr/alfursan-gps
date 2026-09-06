const { partitionPositions } = require("./ingressPartition");

function createRealtimeIngress({
  metrics,
  groupByDeviceId,
  resolveImeiFromCache,
  bumpTraccarPositionsReceived,
  tryProcessCommandResponseIngressSync,
  processCommandResponseIngressDeferred,
  processGpsBurst,
  persistPosition,
  warmImeiCacheFromTraccarDevice,
  persistTraccarDeviceStatus,
  persistEvent,
  hasLegacySubscribers = () => true,
  setImmediateImpl = setImmediate,
}) {
  if (typeof groupByDeviceId !== "function") {
    throw new Error("createRealtimeIngress requires groupByDeviceId");
  }

  async function handleMessage(data) {
    if (!data || typeof data !== "object") return;
    if (!hasLegacySubscribers()) return;

    try {
      if (Array.isArray(data.positions) && data.positions.length) {
        metrics.last_traccar_positions_message_at = metrics.last_traccar_message_at;
        bumpTraccarPositionsReceived?.(data.positions.length);
        const { commands, alarms, gps } = partitionPositions(data.positions);
        for (const position of commands) {
          if (!tryProcessCommandResponseIngressSync(position)) {
            void processCommandResponseIngressDeferred(position).catch((err) => {
              console.error("command_response deferred error:", err.message);
            });
          }
        }
        const otherPositions = [...alarms, ...gps];
        if (otherPositions.length) {
          const grouped = groupByDeviceId(otherPositions);
          for (const [deviceId, list] of grouped.entries()) {
            const imei = resolveImeiFromCache(deviceId);
            if (imei) processGpsBurst(imei, list, data);
            else {
              for (const position of list) persistPosition(position, data);
            }
          }
        }
      }

      if (Array.isArray(data.devices) && data.devices.length) {
        metrics.last_traccar_devices_message_at = metrics.last_traccar_message_at;
        setImmediateImpl(() => {
          for (const dev of data.devices) warmImeiCacheFromTraccarDevice(dev);
          for (const dev of data.devices) {
            void persistTraccarDeviceStatus(dev).catch((err) => {
              console.error("persistTraccarDeviceStatus error:", err.message);
            });
          }
        });
      }

      if (Array.isArray(data.events) && data.events.length) {
        setImmediateImpl(() => {
          for (const eventObj of data.events) {
            void persistEvent(eventObj).catch((err) => {
              console.error("persistEvent error:", err.message);
            });
          }
        });
      }
    } catch (err) {
      console.error("Traccar bridge processing error:", err.message);
    }
  }

  return { handleMessage };
}

module.exports = { createRealtimeIngress };
