const { fork } = require("child_process");
const path = require("path");

function createPersistenceIpc({
  workerModulePath,
  metrics = {},
  log = console,
  maxQueueBatches = 512,
  maxBatchSize = 2000,
  restartBaseMs = 250,
  restartMaxMs = 5000,
} = {}) {
  if (!workerModulePath) {
    throw new Error("createPersistenceIpc requires workerModulePath");
  }

  const absWorkerPath = path.resolve(workerModulePath);
  const queue = [];
  let inflight = null;
  let worker = null;
  let ready = false;
  let closing = false;
  let restartDelayMs = restartBaseMs;
  let restartTimer = null;
  let batchSeq = 0;

  metrics.persistence_worker_connected = false;
  metrics.persistence_worker_pid = null;
  metrics.ipc_queue_depth = metrics.ipc_queue_depth || 0;
  metrics.ipc_queue_high_watermark = metrics.ipc_queue_high_watermark || 0;
  metrics.ipc_batches_sent = metrics.ipc_batches_sent || 0;
  metrics.ipc_send_backpressure_total = metrics.ipc_send_backpressure_total || 0;
  metrics.worker_restart_total = metrics.worker_restart_total || 0;
  metrics.worker_failure_total = metrics.worker_failure_total || 0;
  metrics.positions_worker_pending = metrics.positions_worker_pending || 0;
  metrics.positions_worker_acked_total = metrics.positions_worker_acked_total || 0;
  metrics.positions_worker_enqueued_total = metrics.positions_worker_enqueued_total || 0;

  function updateDepth() {
    const queued = queue.reduce((sum, batch) => sum + (batch.items?.length || 0), 0);
    const inflightCount = inflight?.items?.length || 0;
    const depth = queued + inflightCount;
    metrics.ipc_queue_depth = depth;
    metrics.positions_worker_pending = depth;
    metrics.ipc_queue_high_watermark = Math.max(metrics.ipc_queue_high_watermark || 0, depth);
  }

  function scheduleRestart(reason) {
    if (closing || restartTimer) return;
    metrics.worker_restart_total += 1;
    restartTimer = setTimeout(() => {
      restartTimer = null;
      startWorker(reason);
    }, restartDelayMs);
    if (typeof restartTimer.unref === "function") restartTimer.unref();
    restartDelayMs = Math.min(restartMaxMs, Math.max(restartBaseMs, restartDelayMs * 2));
  }

  function cleanupWorker() {
    if (!worker) return;
    worker.removeAllListeners("message");
    worker.removeAllListeners("exit");
    worker.removeAllListeners("error");
    if (worker.connected) {
      try {
        worker.disconnect();
      } catch {
        /* ignore */
      }
    }
    try {
      worker.kill();
    } catch {
      /* ignore */
    }
    worker = null;
    ready = false;
    metrics.persistence_worker_connected = false;
    metrics.persistence_worker_pid = null;
  }

  function startWorker(reason = "start") {
    if (closing) return;
    cleanupWorker();
    worker = fork(absWorkerPath, [], {
      stdio: ["inherit", "inherit", "inherit", "ipc"],
    });
    metrics.persistence_worker_pid = worker.pid || null;
    metrics.persistence_worker_connected = false;
    ready = false;

    worker.on("message", (msg) => {
      if (!msg || typeof msg !== "object") return;
      if (msg.type === "ready") {
        ready = true;
        metrics.persistence_worker_connected = true;
        restartDelayMs = restartBaseMs;
        pump();
        return;
      }
      if (msg.type === "ack") {
        metrics.positions_worker_acked_total += Number(msg.count || 0);
        if (inflight && msg.batchId === inflight.batchId) {
          inflight = null;
          pump();
        }
        updateDepth();
        return;
      }
      if (msg.type === "worker_metric" && msg.key) {
        metrics[msg.key] =
          typeof msg.value === "number" ||
          typeof msg.value === "string" ||
          typeof msg.value === "boolean" ||
          msg.value === null
            ? msg.value
            : Number(msg.value || 0);
      }
    });

    worker.on("exit", (code, signal) => {
      metrics.persistence_worker_connected = false;
      metrics.persistence_worker_pid = null;
      ready = false;
      worker = null;
      if (inflight) {
        queue.unshift(inflight);
        inflight = null;
      }
      updateDepth();
      if (!closing) {
        metrics.worker_failure_total += 1;
        scheduleRestart(`exit:${code || signal || "unknown"}`);
      }
    });

    worker.on("error", (err) => {
      metrics.worker_failure_total += 1;
      log.error?.("[persistence-ipc] worker error", err.message);
    });
  }

  function normalizeBatch(batch) {
    const items = Array.isArray(batch?.items) ? batch.items.slice(0, maxBatchSize) : [];
    return {
      batchId: batch?.batchId || `${Date.now()}-${batchSeq++}`,
      createdAt: batch?.createdAt || new Date().toISOString(),
      imei: batch?.imei || null,
      kind: batch?.kind || "position_batch",
      items,
      receivedAt: batch?.receivedAt || null,
      liveIndex: Number.isInteger(batch?.liveIndex) ? batch.liveIndex : -1,
      liveDecision: batch?.liveDecision || null,
    };
  }

  function pump() {
    if (!ready || inflight || !worker || closing) return;
    const next = queue.shift();
    if (!next) {
      updateDepth();
      return;
    }
    inflight = next;
    updateDepth();
    metrics.ipc_batches_sent += 1;
    const ok = worker.send({ type: "batch", batch: next });
    if (!ok) metrics.ipc_send_backpressure_total += 1;
  }

  function enqueue(batch) {
    const normalized = normalizeBatch(batch);
    if (!normalized.items.length) return { accepted: false, reason: "empty" };

    queue.push(normalized);
    metrics.positions_worker_enqueued_total += normalized.items.length;
    updateDepth();
    if (queue.length > maxQueueBatches) {
      metrics.ipc_send_backpressure_total += 1;
    }
    pump();
    return { accepted: true, batchId: normalized.batchId };
  }

  async function flushAndStop(timeoutMs = 8000) {
    closing = true;
    if (restartTimer) {
      clearTimeout(restartTimer);
      restartTimer = null;
    }
    const started = Date.now();
    while ((queue.length || inflight) && Date.now() - started < timeoutMs) {
      await new Promise((r) => setTimeout(r, 25));
    }
    cleanupWorker();
    return { remaining: queue.length + (inflight ? 1 : 0) };
  }

  startWorker("start");

  return {
    enqueue,
    flushAndStop,
    getDepth: () => metrics.ipc_queue_depth || 0,
    getWorkerPid: () => metrics.persistence_worker_pid || null,
    isWorkerAlive: () => !!worker && ready,
    _queue: queue,
  };
}

module.exports = { createPersistenceIpc };
