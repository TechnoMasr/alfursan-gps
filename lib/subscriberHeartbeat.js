/**
 * Subscriber WebSocket heartbeat. readyState===OPEN is not enough.
 */
function attachSubscriberHeartbeat(options = {}) {
  const {
    intervalMs = 30_000,
    metrics = {},
    onDead,
    getClients,
    setIntervalFn = setInterval,
    clearIntervalFn = clearInterval,
  } = options;

  if (typeof getClients !== "function") {
    throw new Error("attachSubscriberHeartbeat requires getClients");
  }

  const timer = setIntervalFn(() => {
    const clients = getClients() || [];
    for (const ws of clients) {
      if (!ws) continue;
      if (ws.isAlive === false) {
        metrics.subscriber_terminated_heartbeat =
          (metrics.subscriber_terminated_heartbeat || 0) + 1;
        try {
          if (typeof onDead === "function") onDead(ws, "heartbeat");
          else if (typeof ws.terminate === "function") ws.terminate();
        } catch {
          /* ignore */
        }
        continue;
      }
      ws.isAlive = false;
      try {
        if (typeof ws.ping === "function") ws.ping();
      } catch {
        ws.isAlive = false;
      }
    }
  }, intervalMs);

  if (typeof timer.unref === "function") timer.unref();

  function markAlive(ws) {
    if (ws) ws.isAlive = true;
  }

  function initSocket(ws) {
    if (!ws) return;
    ws.isAlive = true;
    if (typeof ws.on === "function") {
      ws.on("pong", () => {
        ws.isAlive = true;
      });
    }
  }

  function stop() {
    clearIntervalFn(timer);
  }

  return { timer, initSocket, markAlive, stop };
}

module.exports = { attachSubscriberHeartbeat };
