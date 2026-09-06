const { fork } = require("child_process");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

function createPersistenceIpc({
  workerModulePath,
  metrics = {},
  log = console,
  maxQueueBatches = 512,
  maxBatchSize = 2000,
  batchMaxItems = 100,
  batchMaxWaitMs = 5,
  restartBaseMs = 250,
  restartMaxMs = 5000,
  spoolDir = null,
} = {}) {
  if (!workerModulePath) {
    throw new Error("createPersistenceIpc requires workerModulePath");
  }

  const absWorkerPath = path.resolve(workerModulePath);
  const resolvedSpoolDir = path.resolve(
    spoolDir || path.join(path.dirname(absWorkerPath), "..", "data", "persistence-ipc-spool")
  );
  const queue = [];
  let inflight = null;
  let worker = null;
  let ready = false;
  let closing = false;
  let restartDelayMs = restartBaseMs;
  let restartTimer = null;
  let batchSeq = 0;
  let pendingItems = [];
  let pendingMeta = null;
  let pendingFlushTimer = null;
  const accumulatorMaxItems = Math.max(1, Math.min(Number(batchMaxItems) || 100, maxBatchSize));
  const accumulatorMaxWaitMs = Math.max(0, Number(batchMaxWaitMs) || 0);

  metrics.persistence_worker_connected = false;
  metrics.persistence_worker_pid = null;
  metrics.ipc_queue_depth = metrics.ipc_queue_depth || 0;
  metrics.ipc_queue_high_watermark = metrics.ipc_queue_high_watermark || 0;
  metrics.ipc_batches_sent = metrics.ipc_batches_sent || 0;
  metrics.ipc_batches_sent_total = metrics.ipc_batches_sent_total || metrics.ipc_batches_sent || 0;
  metrics.ipc_items_sent_total = metrics.ipc_items_sent_total || 0;
  metrics.ipc_batch_accumulator_depth = metrics.ipc_batch_accumulator_depth || 0;
  metrics.ipc_batch_avg_size = metrics.ipc_batch_avg_size || 0;
  metrics.ipc_batch_max_size = metrics.ipc_batch_max_size || 0;
  metrics.ipc_ack_latency_ms = metrics.ipc_ack_latency_ms || 0;
  metrics.ipc_ack_latency_max_ms = metrics.ipc_ack_latency_max_ms || 0;
  metrics.ipc_send_backpressure_total = metrics.ipc_send_backpressure_total || 0;
  metrics.ipc_spooled_batches_total = metrics.ipc_spooled_batches_total || 0;
  metrics.ipc_spool_depth = metrics.ipc_spool_depth || 0;
  metrics.ipc_spool_failures_total = metrics.ipc_spool_failures_total || 0;
  metrics.ipc_spool_dir = resolvedSpoolDir;
  metrics.worker_restart_total = metrics.worker_restart_total || 0;
  metrics.worker_failure_total = metrics.worker_failure_total || 0;
  metrics.positions_worker_pending = metrics.positions_worker_pending || 0;
  metrics.positions_worker_acked_total = metrics.positions_worker_acked_total || 0;
  metrics.positions_worker_enqueued_total = metrics.positions_worker_enqueued_total || 0;

  function updateDepth() {
    const queued = queue.reduce((sum, batch) => sum + (batch.items?.length || 0), 0);
    const inflightCount = inflight?.items?.length || 0;
    const pendingCount = pendingItems.length;
    const depth = queued + inflightCount + pendingCount;
    metrics.ipc_queue_depth = depth;
    metrics.ipc_batch_accumulator_depth = pendingCount;
    metrics.positions_worker_pending = depth;
    metrics.ipc_queue_high_watermark = Math.max(metrics.ipc_queue_high_watermark || 0, depth);
  }

  function ensureSpoolDir() {
    fs.mkdirSync(resolvedSpoolDir, { recursive: true });
  }

  function spoolFileTs(filePath) {
    const m = path.basename(filePath).match(/^ipc-(\d+)-/);
    return m ? Number(m[1]) : 0;
  }

  function listSpoolFiles() {
    try {
      ensureSpoolDir();
      return fs
        .readdirSync(resolvedSpoolDir)
        .filter((name) => name.endsWith(".json") && !name.endsWith(".tmp"))
        .map((name) => path.join(resolvedSpoolDir, name))
        .sort((a, b) => {
          const d = spoolFileTs(a) - spoolFileTs(b);
          return d !== 0 ? d : a.localeCompare(b);
        });
    } catch (err) {
      metrics.ipc_spool_failures_total += 1;
      log.error?.("[persistence-ipc] spool list failed", err.message);
      return [];
    }
  }

  function refreshSpoolDepth() {
    metrics.ipc_spool_depth = listSpoolFiles().length;
  }

  function spoolBatch(batch) {
    try {
      ensureSpoolDir();
      const filePath = path.join(
        resolvedSpoolDir,
        `ipc-${String(Date.now()).padStart(13, "0")}-${crypto.randomBytes(6).toString("hex")}.json`
      );
      const tmpPath = `${filePath}.tmp`;
      fs.writeFileSync(tmpPath, JSON.stringify(batch), "utf8");
      fs.renameSync(tmpPath, filePath);
      metrics.ipc_spooled_batches_total += 1;
      refreshSpoolDepth();
      return true;
    } catch (err) {
      metrics.ipc_spool_failures_total += 1;
      log.error?.("[persistence-ipc] spool write failed", err.message);
      return false;
    }
  }

  function loadNextSpoolBatch() {
    const filePath = listSpoolFiles()[0];
    if (!filePath) return null;
    try {
      const batch = JSON.parse(fs.readFileSync(filePath, "utf8"));
      return normalizeBatch({ ...batch, _spoolFile: filePath });
    } catch (err) {
      metrics.ipc_spool_failures_total += 1;
      log.error?.("[persistence-ipc] spool read failed", err.message);
      try {
        fs.renameSync(filePath, `${filePath}.bad`);
      } catch {
        /* ignore */
      }
      refreshSpoolDepth();
      return null;
    }
  }

  function removeSpoolFile(filePath) {
    if (!filePath) return;
    try {
      fs.unlinkSync(filePath);
      refreshSpoolDepth();
    } catch (err) {
      if (err?.code === "ENOENT") return;
      metrics.ipc_spool_failures_total += 1;
      log.error?.("[persistence-ipc] spool unlink failed", err.message);
    }
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
        refreshSpoolDepth();
        pump();
        return;
      }
      if (msg.type === "ack") {
        metrics.positions_worker_acked_total += Number(msg.count || 0);
        if (inflight && msg.batchId === inflight.batchId) {
          const ackLatencyMs = inflight._sentAt ? Math.max(0, Date.now() - inflight._sentAt) : 0;
          metrics.ipc_ack_latency_ms = ackLatencyMs;
          metrics.ipc_ack_latency_max_ms = Math.max(metrics.ipc_ack_latency_max_ms || 0, ackLatencyMs);
          if (msg.error) {
            const failed = inflight;
            metrics.worker_failure_total += 1;
            if (!failed._spoolFile && !spoolBatch(failed)) queue.unshift(failed);
            inflight = null;
            cleanupWorker();
            scheduleRestart(`ack_error:${msg.error}`);
            updateDepth();
            return;
          }
          if (inflight._spoolFile) removeSpoolFile(inflight._spoolFile);
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
        if (inflight._spoolFile) {
          refreshSpoolDepth();
        } else if (queue.length >= maxQueueBatches) {
          spoolBatch(inflight);
        } else {
          queue.unshift(inflight);
        }
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
      _spoolFile: batch?._spoolFile || null,
      _sentAt: batch?._sentAt || null,
    };
  }

  function batchForWorker(batch) {
    const { _spoolFile, _sentAt, ...wireBatch } = batch || {};
    return wireBatch;
  }

  function clearPendingFlushTimer() {
    if (!pendingFlushTimer) return;
    clearTimeout(pendingFlushTimer);
    pendingFlushTimer = null;
  }

  function schedulePendingFlush() {
    if (pendingFlushTimer || !pendingItems.length) return;
    pendingFlushTimer = setTimeout(() => {
      pendingFlushTimer = null;
      flushPendingBatch();
    }, accumulatorMaxWaitMs);
    if (typeof pendingFlushTimer.unref === "function") pendingFlushTimer.unref();
  }

  function makePendingBatch() {
    if (!pendingItems.length) return null;
    const first = pendingMeta || {};
    const items = pendingItems;
    pendingItems = [];
    pendingMeta = null;
    clearPendingFlushTimer();
    metrics.ipc_batch_accumulator_depth = 0;
    return normalizeBatch({
      batchId: `ipc-${Date.now()}-${batchSeq++}`,
      kind: first.kind || "position_batch",
      imei: first.imei || null,
      createdAt: first.createdAt || new Date().toISOString(),
      receivedAt: first.receivedAt || null,
      liveIndex: Number.isInteger(first.liveIndex) ? first.liveIndex : -1,
      liveDecision: first.liveDecision || null,
      items,
    });
  }

  function restorePendingBatch(batch) {
    if (!batch?.items?.length) return;
    pendingItems = batch.items.concat(pendingItems);
    if (!pendingMeta) {
      pendingMeta = {
        kind: batch.kind,
        imei: batch.imei,
        createdAt: batch.createdAt,
        receivedAt: batch.receivedAt,
        liveIndex: batch.liveIndex,
        liveDecision: batch.liveDecision,
      };
    }
    metrics.ipc_batch_accumulator_depth = pendingItems.length;
    if (!closing) schedulePendingFlush();
  }

  function queueBatch(batch) {
    const normalized = normalizeBatch(batch);
    if (!normalized.items.length) return { accepted: false, reason: "empty" };

    if (queue.length >= maxQueueBatches) {
      metrics.ipc_send_backpressure_total += 1;
      if (!spoolBatch(normalized)) {
        updateDepth();
        return { accepted: false, reason: "ipc_queue_full" };
      }
      metrics.positions_worker_enqueued_total += normalized.items.length;
      updateDepth();
      pump();
      return { accepted: true, batchId: normalized.batchId, spooled: true };
    }

    metrics.positions_worker_enqueued_total += normalized.items.length;
    queue.push(normalized);
    updateDepth();
    pump();
    return { accepted: true, batchId: normalized.batchId };
  }

  function flushPendingBatch() {
    const batch = makePendingBatch();
    if (!batch) {
      updateDepth();
      return { accepted: false, reason: "empty" };
    }
    const result = queueBatch(batch);
    if (!result.accepted) restorePendingBatch(batch);
    return result;
  }

  function pump() {
    if (!ready || inflight || !worker || closing) return;
    if (!queue.length && (metrics.ipc_spool_depth || 0) > 0) {
      const spooled = loadNextSpoolBatch();
      if (spooled?.items?.length) queue.push(spooled);
    }
    const next = queue.shift();
    if (!next) {
      updateDepth();
      return;
    }
    inflight = next;
    inflight._sentAt = Date.now();
    updateDepth();
    metrics.ipc_batches_sent += 1;
    metrics.ipc_batches_sent_total = metrics.ipc_batches_sent;
    metrics.ipc_items_sent_total += next.items?.length || 0;
    metrics.ipc_batch_max_size = Math.max(metrics.ipc_batch_max_size || 0, next.items?.length || 0);
    metrics.ipc_batch_avg_size = metrics.ipc_batches_sent
      ? Number((metrics.ipc_items_sent_total / metrics.ipc_batches_sent).toFixed(2))
      : 0;
    const ok = worker.send({ type: "batch", batch: batchForWorker(next) });
    if (!ok) metrics.ipc_send_backpressure_total += 1;
  }

  function enqueue(batch) {
    const normalized = normalizeBatch(batch);
    if (!normalized.items.length) return { accepted: false, reason: "empty" };

    let lastResult = { accepted: true, batchId: normalized.batchId };
    for (const item of normalized.items) {
      if (!pendingMeta) {
        pendingMeta = {
          kind: normalized.kind,
          imei: normalized.imei,
          createdAt: normalized.createdAt,
          receivedAt: normalized.receivedAt,
          liveIndex: normalized.liveIndex,
          liveDecision: normalized.liveDecision,
        };
      }
      pendingItems.push(item);
      metrics.ipc_batch_accumulator_depth = pendingItems.length;
      if (pendingItems.length >= accumulatorMaxItems) {
        lastResult = flushPendingBatch();
        if (!lastResult.accepted) break;
      }
    }

    updateDepth();
    if (pendingItems.length) schedulePendingFlush();
    return lastResult;
  }

  async function flushAndStop(timeoutMs = 8000) {
    if (restartTimer) {
      clearTimeout(restartTimer);
      restartTimer = null;
    }
    flushPendingBatch();
    const started = Date.now();
    while ((queue.length || inflight) && Date.now() - started < timeoutMs) {
      pump();
      await new Promise((r) => setTimeout(r, 25));
    }
    closing = true;
    flushPendingBatch();
    if (inflight) {
      if (!inflight._spoolFile) spoolBatch(inflight);
      inflight = null;
    }
    while (queue.length) {
      const batch = queue.shift();
      if (batch && !batch._spoolFile) spoolBatch(batch);
    }
    cleanupWorker();
    refreshSpoolDepth();
    return { remaining: queue.length + (inflight ? 1 : 0), spool_depth: metrics.ipc_spool_depth || 0 };
  }

  startWorker("start");
  refreshSpoolDepth();

  return {
    enqueue,
    flushPendingBatch,
    flushAndStop,
    getDepth: () => metrics.ipc_queue_depth || 0,
    getWorkerPid: () => metrics.persistence_worker_pid || null,
    isWorkerAlive: () => !!worker && ready,
    _queue: queue,
  };
}

module.exports = { createPersistenceIpc };
