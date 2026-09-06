function createTraccarForwardQueue(options = {}) {
  const {
    maxDepth = 5000,
    processFn,
    metrics = {},
    setImmediateFn = setImmediate,
    log = console,
  } = options;

  if (typeof processFn !== "function") {
    throw new Error("createTraccarForwardQueue requires processFn");
  }

  const queue = [];
  let running = false;
  let closed = false;

  function updateDepth() {
    metrics.forward_queue_depth = queue.length;
    metrics.forward_queue_high_watermark = Math.max(
      metrics.forward_queue_high_watermark || 0,
      queue.length
    );
  }

  function pump() {
    if (running || closed) return;
    const next = queue.shift();
    updateDepth();
    if (!next) return;
    running = true;
    Promise.resolve()
      .then(() => processFn(next))
      .catch((err) => {
        metrics.forward_queue_process_failures_total =
          (metrics.forward_queue_process_failures_total || 0) + 1;
        log.error?.("forward queue process error:", err.message);
      })
      .finally(() => {
        running = false;
        if (queue.length) setImmediateFn(pump);
      });
  }

  function enqueue(item) {
    if (closed) {
      metrics.forward_queue_rejected_total =
        (metrics.forward_queue_rejected_total || 0) + 1;
      return { accepted: false, reason: "closed" };
    }
    if (queue.length >= maxDepth) {
      metrics.forward_queue_rejected_total =
        (metrics.forward_queue_rejected_total || 0) + 1;
      return { accepted: false, reason: "queue_full" };
    }
    queue.push(item);
    updateDepth();
    setImmediateFn(pump);
    return { accepted: true, depth: queue.length };
  }

  async function flushAndStop(timeoutMs = 8000) {
    closed = true;
    const started = Date.now();
    while ((queue.length || running) && Date.now() - started < timeoutMs) {
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
    updateDepth();
    return { remaining: queue.length + (running ? 1 : 0) };
  }

  return {
    enqueue,
    flushAndStop,
    getDepth: () => queue.length,
    isRunning: () => running,
    _queue: queue,
  };
}

module.exports = { createTraccarForwardQueue };
