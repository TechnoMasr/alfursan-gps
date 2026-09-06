const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

function toDateOrRaw(value) {
  if (value == null || value === "") return null;
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? value : d;
}

function toNumberOrNull(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function firstArrayItem(value) {
  return Array.isArray(value) && value.length ? value[0] : null;
}

function pickRawPosition(rawPayload) {
  if (!rawPayload || typeof rawPayload !== "object") return null;
  return (
    rawPayload.position ||
    rawPayload.data?.position ||
    firstArrayItem(rawPayload.positions) ||
    firstArrayItem(rawPayload.data?.positions) ||
    (rawPayload.latitude != null || rawPayload.longitude != null ? rawPayload : null)
  );
}

function pickRawDevice(rawPayload, position) {
  if (!rawPayload || typeof rawPayload !== "object") return null;
  return rawPayload.device || rawPayload.data?.device || position?.device || null;
}

function buildTraccarRawIngressDoc(rawPayload, receivedAt = Date.now()) {
  const position = pickRawPosition(rawPayload);
  const device = pickRawDevice(rawPayload, position);
  const attrs = position?.attributes || rawPayload?.attributes || {};
  const imei =
    device?.uniqueId ??
    position?.uniqueId ??
    position?.imei ??
    rawPayload?.uniqueId ??
    rawPayload?.imei ??
    null;
  const runtimeDeviceId = position?.deviceId ?? device?.id ?? rawPayload?.deviceId ?? null;

  return {
    received_at: new Date(receivedAt),
    source: "traccar_http_forward",
    imei: imei == null || imei === "" ? null : String(imei),
    runtime_device_id: toNumberOrNull(runtimeDeviceId),
    protocol: position?.protocol ?? device?.protocol ?? rawPayload?.protocol ?? null,
    position_id: position?.id ?? rawPayload?.positionId ?? null,
    fix_time: toDateOrRaw(position?.fixTime ?? rawPayload?.fixTime),
    device_time: toDateOrRaw(position?.deviceTime ?? rawPayload?.deviceTime),
    server_time: toDateOrRaw(position?.serverTime ?? rawPayload?.serverTime),
    latitude: toNumberOrNull(position?.latitude ?? rawPayload?.latitude),
    longitude: toNumberOrNull(position?.longitude ?? rawPayload?.longitude),
    has_command_response: String(attrs?.result || "").trim().length > 0,
    raw_payload: rawPayload,
  };
}

function createTraccarRawIngressWriter(options = {}) {
  const {
    insertMany,
    spoolDir = path.join(__dirname, "..", "data", "traccar-raw-ingress-spool"),
    batchSize = 250,
    flushMs = 100,
    maxQueueDepth = 10_000,
    retryBaseMs = 250,
    retryMaxMs = 30_000,
    maxMongoBatchesPerCycle = 4,
    metrics = {},
    fsImpl = fs,
    now = () => Date.now(),
    log = console,
  } = options;

  if (typeof insertMany !== "function") {
    throw new Error("createTraccarRawIngressWriter requires insertMany");
  }

  const resolvedSpoolDir = path.resolve(spoolDir);
  let activePath = null;
  let activeDocs = 0;
  const activeWriteQueue = [];
  let writingActive = false;
  /** True only during rotateActive rename — blocks new appends, not Mongo drain. */
  let rotating = false;
  let pendingDocs = 0;
  let pendingFiles = 0;
  /** Single owner for rotate + drainFile + persistBatch chains. */
  let flushing = false;
  let closed = false;
  let flushTimer = null;
  let retryTimer = null;
  let retryDelayMs = retryBaseMs;
  let ensureDirPromise = null;
  let startupPromise = null;

  metrics.raw_ingress_queue_depth = metrics.raw_ingress_queue_depth || 0;
  metrics.raw_ingress_spool_depth = metrics.raw_ingress_spool_depth || 0;
  metrics.raw_ingress_spool_dir = resolvedSpoolDir;
  const fsAsync = fsImpl.promises || fs.promises;

  function bump(name, n = 1) {
    metrics[name] = (metrics[name] || 0) + n;
  }

  function ensureDir() {
    if (!ensureDirPromise) {
      ensureDirPromise = fsAsync.mkdir(resolvedSpoolDir, { recursive: true }).catch((err) => {
        ensureDirPromise = null;
        throw err;
      });
    }
    return ensureDirPromise;
  }

  function activeDepth() {
    return activeDocs + activeWriteQueue.length;
  }

  function updateDepths() {
    const depth = activeDepth();
    metrics.raw_ingress_queue_depth = depth;
    metrics.raw_ingress_memory_queue_depth = activeWriteQueue.length;
    metrics.raw_ingress_active_journal_docs = activeDocs;
    metrics.raw_ingress_pending_spool_docs = pendingDocs;
    metrics.raw_ingress_spool_depth = depth + pendingDocs;
    metrics.raw_ingress_spool_files = pendingFiles + (activeDocs || activePath ? 1 : 0);
  }

  function wakePump() {
    if (closed && !activeWriteQueue.length) return;
    if (!activeWriteQueue.length) return;
    if (writingActive || rotating) return;
    void pumpActiveWrites();
  }

  function newActivePath() {
    return path.join(
      resolvedSpoolDir,
      `raw-${String(now()).padStart(13, "0")}-${crypto.randomBytes(6).toString("hex")}.jsonl.active`
    );
  }

  function spoolFileTs(filePath) {
    const m = path.basename(filePath).match(/^raw-(\d+)-/);
    return m ? Number(m[1]) : 0;
  }

  function stablePathFor(filePath) {
    return filePath.replace(/\.active$/, "");
  }

  async function listPendingFiles() {
    await ensureDir();
    return (await fsAsync.readdir(resolvedSpoolDir))
      .filter((name) => name.endsWith(".jsonl") && !name.endsWith(".tmp") && !name.endsWith(".active"))
      .map((name) => path.join(resolvedSpoolDir, name))
      .sort((a, b) => {
        const d = spoolFileTs(a) - spoolFileTs(b);
        return d !== 0 ? d : a.localeCompare(b);
      });
  }

  async function countDocsInFile(filePath) {
    try {
      return (await fsAsync.readFile(filePath, "utf8")).split("\n").filter((line) => line.trim()).length;
    } catch {
      return 0;
    }
  }

  async function recoverActiveFiles() {
    await ensureDir();
    for (const name of await fsAsync.readdir(resolvedSpoolDir)) {
      if (!name.endsWith(".jsonl.active")) continue;
      const source = path.join(resolvedSpoolDir, name);
      const dest = stablePathFor(source);
      try {
        await fsAsync.rename(source, dest);
      } catch (err) {
        bump("raw_ingress_persist_failures");
        log.error?.("[raw-ingress] failed to recover active spool", err.message);
      }
    }
  }

  async function refreshSpoolStats() {
    await recoverActiveFiles();
    const files = await listPendingFiles();
    pendingFiles = files.length;
    pendingDocs = 0;
    for (const filePath of files) {
      pendingDocs += await countDocsInFile(filePath);
    }
    updateDepths();
  }

  /**
   * Rename current .active journal to stable .jsonl.
   * MUST only be called when writingActive === false and rotating === true (flush owner).
   * Counters reset only after successful rename.
   */
  async function rotateActive() {
    if (!activePath || activeDocs <= 0) return null;
    const previousPath = activePath;
    const previousDocs = activeDocs;
    const stablePath = stablePathFor(previousPath);
    await fsAsync.rename(previousPath, stablePath);
    activePath = null;
    activeDocs = 0;
    pendingDocs += previousDocs;
    pendingFiles += 1;
    updateDepths();
    return stablePath;
  }

  async function appendToActive(doc) {
    await ensureDir();
    if (!activePath) activePath = newActivePath();
    await fsAsync.appendFile(activePath, `${JSON.stringify(doc)}\n`, "utf8");
    activeDocs += 1;
    bump("raw_ingress_spooled_total");
    updateDepths();
  }

  function resolveEntryDurable(entry, result) {
    if (!entry || typeof entry.resolveDurable !== "function") return;
    try {
      entry.resolveDurable(result);
    } catch {
      // ignore double-resolve
    }
    entry.resolveDurable = null;
  }

  async function pumpActiveWrites() {
    if (writingActive || rotating) return;
    writingActive = true;
    try {
      if (rotating) return;
      while (activeWriteQueue.length) {
        if (rotating) break;
        const entry = activeWriteQueue[0];
        const doc = entry && Object.prototype.hasOwnProperty.call(entry, "doc") ? entry.doc : entry;
        await appendToActive(doc);
        activeWriteQueue.shift();
        resolveEntryDurable(entry, { ok: true });
        // Bound active file size; flush rotates even if RAM queue still has work.
        if (!closed && activeDocs >= batchSize) break;
      }
    } catch (err) {
      bump("raw_ingress_persist_failures");
      log.error?.("[raw-ingress] async spool write failed", err.message);
      const failed = activeWriteQueue[0];
      if (failed) {
        activeWriteQueue.shift();
        resolveEntryDurable(failed, { ok: false, reason: "raw_ingress_spool_write_failed" });
      }
    } finally {
      writingActive = false;
      updateDepths();
      if (!closed && activeDocs >= batchSize) scheduleFlush(0);
      else if (!closed && activeWriteQueue.length && !rotating) void pumpActiveWrites();
    }
  }

  function scheduleFlush(delay = flushMs) {
    if (closed || flushTimer) return;
    flushTimer = setTimeout(() => {
      flushTimer = null;
      void flushCycle();
    }, Math.max(0, delay));
    if (typeof flushTimer.unref === "function") flushTimer.unref();
  }

  function scheduleRetry() {
    if (closed || retryTimer) return;
    retryTimer = setTimeout(() => {
      retryTimer = null;
      void flushCycle();
    }, retryDelayMs);
    if (typeof retryTimer.unref === "function") retryTimer.unref();
  }

  async function parseSpoolFile(filePath) {
    const raw = await fsAsync.readFile(filePath, "utf8");
    const docs = [];
    for (const line of raw.split("\n")) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      docs.push(JSON.parse(trimmed));
    }
    return docs;
  }

  async function persistBatch(docs) {
    if (!docs.length) return;
    bump("raw_ingress_mongo_attempted_total", docs.length);
    await insertMany(docs);
    bump("raw_ingress_persisted_total", docs.length);
    metrics.raw_ingress_last_persisted_at = new Date(now()).toISOString();
    retryDelayMs = retryBaseMs;
  }

  /**
   * Drain one stable spool file with a bounded number of insertMany batches.
   * Returns { batches, fullyDrained }. Partial drain keeps remaining lines on disk.
   */
  async function drainFile(filePath, maxBatches) {
    const docs = await parseSpoolFile(filePath);
    if (!docs.length) {
      await fsAsync.unlink(filePath);
      pendingFiles = Math.max(0, pendingFiles - 1);
      updateDepths();
      return { batches: 0, fullyDrained: true };
    }

    let batches = 0;
    let offset = 0;
    while (offset < docs.length && batches < maxBatches) {
      const slice = docs.slice(offset, offset + batchSize);
      await persistBatch(slice);
      offset += slice.length;
      batches += 1;
      // Fairness: let new ingress journal while large historical files drain.
      wakePump();
    }

    if (offset >= docs.length) {
      await fsAsync.unlink(filePath);
      pendingDocs = Math.max(0, pendingDocs - docs.length);
      pendingFiles = Math.max(0, pendingFiles - 1);
      updateDepths();
      return { batches, fullyDrained: true };
    }

    // Persist succeeded for [0, offset); keep remainder recoverable on disk.
    const remaining = docs.slice(offset);
    const tmpPath = `${filePath}.tmp`;
    await fsAsync.writeFile(tmpPath, `${remaining.map((d) => JSON.stringify(d)).join("\n")}\n`, "utf8");
    await fsAsync.rename(tmpPath, filePath);
    pendingDocs = Math.max(0, pendingDocs - offset);
    updateDepths();
    return { batches, fullyDrained: false };
  }

  async function flushCycle(opts = {}) {
    if (flushing) return;
    if (closed && !opts.force) return;
    flushing = true;
    try {
      // Never rename while an append is in flight.
      if (writingActive) {
        scheduleFlush(10);
        return;
      }

      rotating = true;
      try {
        if (writingActive) {
          scheduleFlush(10);
          return;
        }
        await rotateActive();
      } finally {
        rotating = false;
      }

      // New packets may keep journaling while historical spool drains to Mongo.
      wakePump();

      const files = await listPendingFiles();
      let batches = 0;
      for (const filePath of files) {
        if (batches >= maxMongoBatchesPerCycle) break;
        if (closed && !opts.force && batches > 0) break;
        const result = await drainFile(filePath, maxMongoBatchesPerCycle - batches);
        batches += result.batches;
        wakePump();
        if (!result.fullyDrained) break;
      }

      if (!closed && (activeDocs || pendingDocs || activeWriteQueue.length)) {
        scheduleFlush(flushMs);
      }
    } catch (err) {
      bump("raw_ingress_persist_failures");
      retryDelayMs = Math.min(retryMaxMs, retryDelayMs * 2);
      log.warn?.("[raw-ingress] flush retry scheduled", err.message);
      scheduleRetry();
    } finally {
      flushing = false;
      rotating = false;
      updateDepths();
      wakePump();
    }
  }

  /**
   * Accept into bounded RAM queue and wake async journal pump.
   * `accepted: true` = queued for local journal (NOT Mongo).
   * `durable` resolves when appended to local .jsonl.active (or fails).
   * Never waits for Mongo.
   */
  function enqueue(doc) {
    if (closed) {
      bump("raw_ingress_queue_rejected_total");
      return {
        accepted: false,
        reason: "raw_ingress_closed",
        durable: Promise.resolve({ ok: false, reason: "raw_ingress_closed" }),
      };
    }
    if (activeDepth() >= maxQueueDepth) {
      bump("raw_ingress_queue_rejected_total");
      return {
        accepted: false,
        reason: "raw_ingress_queue_full",
        durable: Promise.resolve({ ok: false, reason: "raw_ingress_queue_full" }),
      };
    }
    let resolveDurable;
    const durable = new Promise((resolve) => {
      resolveDurable = resolve;
    });
    try {
      activeWriteQueue.push({ doc, resolveDurable });
      updateDepths();
      wakePump();
    } catch (err) {
      bump("raw_ingress_queue_rejected_total");
      bump("raw_ingress_persist_failures");
      log.error?.("[raw-ingress] durable accept failed", err.message);
      resolveDurable({ ok: false, reason: "raw_ingress_queue_full" });
      return { accepted: false, reason: "raw_ingress_queue_full", durable };
    }
    bump("raw_ingress_accepted_total");
    if (activeDepth() >= batchSize) scheduleFlush(0);
    else scheduleFlush(flushMs);
    return { accepted: true, depth: activeDepth(), durable };
  }

  async function flushAndStop(timeoutMs = 8000) {
    closed = true;
    if (flushTimer) {
      clearTimeout(flushTimer);
      flushTimer = null;
    }
    if (retryTimer) {
      clearTimeout(retryTimer);
      retryTimer = null;
    }
    const started = now();

    // Finish journaling accepted RAM docs (when closed, pump does not stop at batchSize).
    while (activeWriteQueue.length || writingActive || rotating) {
      if (now() - started >= timeoutMs) break;
      wakePump();
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
    while (activeWriteQueue.length) {
      const entry = activeWriteQueue.shift();
      resolveEntryDurable(entry, { ok: false, reason: "raw_ingress_shutdown_timeout" });
    }
    updateDepths();

    while (now() - started < timeoutMs) {
      if (!activeDocs && !pendingDocs && !activeWriteQueue.length && !writingActive && !flushing) {
        break;
      }
      try {
        await flushCycle({ force: true });
      } catch {
        // persist_failures already bumped inside flushCycle
      }
      if (!activeDocs && !pendingDocs && !activeWriteQueue.length) break;
      await new Promise((resolve) => setTimeout(resolve, 25));
    }

    return {
      elapsed_ms: now() - started,
      queue_depth: activeDepth(),
      spool_depth: activeDepth() + pendingDocs,
    };
  }

  startupPromise = refreshSpoolStats()
    .then(() => {
      if (pendingDocs) scheduleFlush(0);
      wakePump();
    })
    .catch((err) => {
      bump("raw_ingress_persist_failures");
      log.warn?.("[raw-ingress] failed to refresh spool stats", err.message);
    });

  return {
    enqueue,
    flushCycle,
    flushAndStop,
    whenReady: () => startupPromise,
    getStats: () => {
      updateDepths();
      return {
        raw_ingress_queue_depth: activeDepth(),
        raw_ingress_memory_queue_depth: activeWriteQueue.length,
        raw_ingress_active_journal_docs: activeDocs,
        raw_ingress_pending_spool_docs: pendingDocs,
        raw_ingress_spool_depth: activeDepth() + pendingDocs,
        raw_ingress_spool_files: pendingFiles + (activeDocs || activePath ? 1 : 0),
        raw_ingress_spool_dir: resolvedSpoolDir,
      };
    },
  };
}

module.exports = {
  createTraccarRawIngressWriter,
  buildTraccarRawIngressDoc,
};
