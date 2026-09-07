const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const RETRYABLE_MONGO_CODES = new Set([
  6, 7, 50, 89, 91, 189, 10107, 11600, 11602, 13435, 13436,
]);

/**
 * Durability contract (honest):
 * - ACK after local disk write()/append + successful path visibility (rename for seal).
 * - Guarantees process-crash recovery while the OS/page cache survives.
 * - Does NOT fsync; power-loss / kernel crash may lose the last unflushed appends.
 * - Mongo is never required before IPC ACK.
 */

function jitter(ms) {
  const base = Math.max(0, Number(ms) || 0);
  return Math.floor(base * (0.8 + Math.random() * 0.4));
}

function errCode(err) {
  return err?.code || err?.cause?.code || null;
}

function errText(err) {
  return String(err?.errmsg || err?.message || err || "");
}

function isRetryableMongoError(err) {
  if (!err) return false;
  const code = errCode(err);
  if (RETRYABLE_MONGO_CODES.has(code)) return true;
  const msg = errText(err);
  return /ECONNRESET|ETIMEDOUT|ENOTFOUND|EAI_AGAIN|EPIPE|network|timed out|timeout|not primary|NotWritablePrimary|Interrupted|step down|stepdown|write concern|PrimarySteppedDown|ShutdownInProgress|connection/i.test(
    msg
  );
}

function isDuplicateKeyError(err) {
  if (!err) return false;
  if (errCode(err) === 11000) return true;
  return /E11000|duplicate key/i.test(errText(err));
}

function collectWriteErrors(err) {
  if (!err) return [];
  const raw =
    err.writeErrors ||
    err.result?.writeErrors ||
    (typeof err.result?.getWriteErrors === "function" ? err.result.getWriteErrors() : null) ||
    [];
  return Array.isArray(raw) ? raw : [];
}

function isIdempotencyDuplicate(errPart) {
  if (!errPart) return false;
  const pattern = errPart.keyPattern || errPart.errInfo?.keyPattern || errPart.err?.keyPattern;
  if (pattern && Object.prototype.hasOwnProperty.call(pattern, "traccar_position_id")) {
    return true;
  }
  const name = String(errPart.indexName || errPart.err?.index || "");
  if (/traccar_position_id/i.test(name)) return true;
  return /traccar_position_id/i.test(errText(errPart));
}

function isDiskError(err) {
  const code = errCode(err);
  return code === "EACCES" || code === "ENOENT" || code === "ENOSPC" || code === "EROFS";
}

function isPermanentMongoError(err) {
  const code = errCode(err);
  if (code === 121 || code === 2 || code === 14 || code === 9 || code === 22) return true;
  const msg = errText(err);
  return /DocumentValidationFailure|validation failed|must be/i.test(msg);
}

function classifyInsertManyError(docs, err) {
  const list = Array.isArray(docs) ? docs : [];
  const result = {
    acked: [],
    duplicates: [],
    unexpectedDuplicates: [],
    retry: [],
    invalid: [],
    retryableFailure: false,
  };
  if (!err) {
    result.acked = list.slice();
    return result;
  }

  const writeErrors = collectWriteErrors(err);
  if (!writeErrors.length) {
    if (isDuplicateKeyError(err)) {
      if (isIdempotencyDuplicate(err)) {
        result.duplicates = list.slice();
        result.acked = list.slice();
      } else {
        result.unexpectedDuplicates = list.slice();
      }
      return result;
    }
    if (isPermanentMongoError(err)) {
      result.invalid = list.slice();
      return result;
    }
    result.retry = list.slice();
    result.retryableFailure = true;
    return result;
  }

  const failed = new Map();
  for (const we of writeErrors) {
    const idx = Number(we.index);
    if (Number.isInteger(idx)) failed.set(idx, we);
  }

  for (let i = 0; i < list.length; i++) {
    const doc = list[i];
    const we = failed.get(i);
    if (!we) {
      result.acked.push(doc);
      continue;
    }
    if (isDuplicateKeyError(we) || we.code === 11000) {
      if (isIdempotencyDuplicate(we) || isIdempotencyDuplicate(err)) {
        result.duplicates.push(doc);
        result.acked.push(doc);
      } else {
        result.unexpectedDuplicates.push(doc);
      }
      continue;
    }
    if (isPermanentMongoError(we)) {
      result.invalid.push(doc);
      continue;
    }
    result.retry.push(doc);
    result.retryableFailure = true;
  }
  return result;
}

function duplicateCount(err) {
  const errors = collectWriteErrors(err);
  if (errors.length) return errors.filter((e) => e.code === 11000).length;
  return isDuplicateKeyError(err) ? 1 : 0;
}

function hasNonDuplicateWriteError(err) {
  const errors = collectWriteErrors(err);
  if (errors.length) return errors.some((e) => e.code !== 11000);
  return !isDuplicateKeyError(err);
}

function mergeClassified(a, b) {
  return {
    acked: [...a.acked, ...b.acked],
    duplicates: [...a.duplicates, ...b.duplicates],
    unexpectedDuplicates: [...a.unexpectedDuplicates, ...b.unexpectedDuplicates],
    retry: [...a.retry, ...b.retry],
    invalid: [...a.invalid, ...b.invalid],
    retryableFailure: Boolean(a.retryableFailure || b.retryableFailure),
  };
}

function emptyClassified() {
  return {
    acked: [],
    duplicates: [],
    unexpectedDuplicates: [],
    retry: [],
    invalid: [],
    retryableFailure: false,
  };
}

function isLegacySpoolName(name) {
  return /^(hot|pending)-\d+-.*\.jsonl$/i.test(name);
}

function isReadySegmentName(name) {
  return /\.jsonl\.ready$/i.test(name);
}

function isActiveSegmentName(name) {
  return /\.jsonl\.active$/i.test(name);
}

function isDrainingSegmentName(name) {
  return /\.jsonl\.draining\.\d+$/i.test(name);
}

function isDrainableName(name) {
  return isLegacySpoolName(name) || isReadySegmentName(name) || isDrainingSegmentName(name);
}

function spoolFileTs(filePath) {
  const name = path.basename(filePath);
  const m =
    name.match(/^(?:hot|pending|gps)-(\d+)-/) ||
    name.match(/^gps-(\d+)-/);
  return m ? Number(m[1]) : 0;
}

function pidAlive(pid) {
  const n = Number(pid);
  if (!Number.isInteger(n) || n <= 0) return false;
  try {
    process.kill(n, 0);
    return true;
  } catch {
    return false;
  }
}

function createGpsPointWriter(options = {}) {
  const {
    insertMany,
    spoolDir = path.join(__dirname, "..", "data", "gpspoints-spool"),
    batchSize = 250,
    flushMs = 100,
    memHigh = 10_000,
    retryBaseMs = 250,
    retryMaxMs = 30_000,
    spoolWarnBytes = 512 * 1024 * 1024,
    spoolCriticalBytes = 2 * 1024 * 1024 * 1024,
    minDiskFreeBytes = 1024 * 1024 * 1024,
    drainNewFilesPerCycle = 2,
    drainOldFilesPerCycle = 1,
    maxMongoBatchesPerCycle = 16,
    maxFilesPerCycle = 500,
    drainOldDocRatio = 0.5,
    journalCoalesceMs = 0,
    spoolReconcileMs = 30_000,
    /** E2: docs before seal (500–2000 typical). */
    segmentMaxDocs = 1000,
    /** E2: bytes before seal. */
    segmentMaxBytes = 2 * 1024 * 1024,
    /** E2: time-based seal so low traffic does not leave active forever. */
    segmentSealMs = 200,
    /**
     * full = journal + drain (E2 default / rollback)
     * producer = journal/seal only (E3 persistence worker)
     * drain = Mongo drain only (E3 gpspoints-writer)
     */
    mode = "full",
    drainOwnerId = process.pid,
    heartbeatMs = 2000,
    metrics = {},
    fsImpl = fs,
    now = () => Date.now(),
    log = console,
  } = options;

  const resolvedMode = String(mode || "full").toLowerCase();
  if (!["full", "producer", "drain"].includes(resolvedMode)) {
    throw new Error(`createGpsPointWriter invalid mode=${mode}`);
  }
  const enableJournal = resolvedMode === "full" || resolvedMode === "producer";
  const enableMongoDrain = resolvedMode === "full" || resolvedMode === "drain";

  if (enableMongoDrain && typeof insertMany !== "function") {
    throw new Error("createGpsPointWriter requires insertMany when drain is enabled");
  }
  if (enableJournal && typeof insertMany !== "function" && resolvedMode === "full") {
    // full needs insertMany; producer may pass a noop
  }
  if (resolvedMode === "producer" && typeof insertMany !== "function") {
    // allow missing insertMany for producer-only
  }

  const resolvedSpoolDir = path.resolve(spoolDir);
  const quarantineDir = path.join(resolvedSpoolDir, "quarantine");
  const drainLockPath = path.join(resolvedSpoolDir, "gpspoints-drain.lock");
  const heartbeatPath = path.join(resolvedSpoolDir, "gpspoints-writer.heartbeat.json");
  const unjournaled = [];
  let flushing = false;
  let closed = false;
  let retryDelayMs = retryBaseMs;
  let retryTimer = null;
  let flushTimer = null;
  let journalTimer = null;
  let journalTimeout = null;
  let reconcileTimer = null;
  let sealTimer = null;
  let heartbeatTimer = null;
  let spoolFiles = 0;
  let spoolBytes = 0;
  let spoolOldestAgeMs = 0;
  let spoolOldestMtime = null;
  const spoolMeta = new Map();
  let readdirCount = 0;
  let diskFreeBytes = null;
  let lastDiskError = null;
  let lastDiskErrorAt = null;
  let mongoBatchesThisCycle = 0;
  let drainLockHeld = false;

  // Active segment (producer side)
  let activePath = null;
  let activeDocs = 0;
  let activeBytes = 0;
  let activeOpenedAt = 0;
  let sealing = false;

  metrics.persistence_dropped = metrics.persistence_dropped || 0;
  metrics.gpspoints_journaled_total = metrics.gpspoints_journaled_total || 0;
  metrics.gpspoints_mongo_flush_count = metrics.gpspoints_mongo_flush_count || 0;
  metrics.gpspoints_mongo_docs_per_flush_last = metrics.gpspoints_mongo_docs_per_flush_last || 0;
  metrics.gpspoints_mongo_docs_per_flush_avg = metrics.gpspoints_mongo_docs_per_flush_avg || 0;
  metrics.gpspoints_mongo_docs_per_flush_max = metrics.gpspoints_mongo_docs_per_flush_max || 0;
  metrics.gpspoints_writer_mode = resolvedMode;
  metrics.gpspoints_spool_dir = resolvedSpoolDir;
  metrics.gpspoints_durability =
    "process_crash_safe_no_fsync; power_loss_may_lose_last_appends";
  log.info?.(
    `[gpspoints] mode=${resolvedMode} spool=${resolvedSpoolDir} durability=${metrics.gpspoints_durability}`
  );

  function bump(name, n = 1) {
    metrics[name] = (metrics[name] || 0) + n;
  }

  function ensureDir(dir) {
    fsImpl.mkdirSync(dir, { recursive: true });
  }

  function recordWriteFailure(err, op) {
    const code = errCode(err) || "UNKNOWN";
    lastDiskError = `${code}:${errText(err)}`;
    lastDiskErrorAt = now();
    bump("gpspoints_spool_write_failures");
    bump("gpspoints_persist_failures");
    log.error?.("[gpspoints] durable storage failure", {
      op,
      code,
      message: errText(err),
      spool_dir: resolvedSpoolDir,
    });
  }

  function statDiskFree() {
    try {
      const statfs = fsImpl.statfsSync || fs.statfsSync;
      if (typeof statfs !== "function") {
        diskFreeBytes = null;
        return null;
      }
      const s = statfs.call(fsImpl.statfsSync ? fsImpl : fs, resolvedSpoolDir);
      const avail = Number(s.bavail ?? s.bAvailable ?? s.bfree);
      const size = Number(s.bsize ?? s.blockSize ?? 4096);
      if (Number.isFinite(avail) && Number.isFinite(size)) {
        diskFreeBytes = avail * size;
        metrics.disk_free_bytes = diskFreeBytes;
        return diskFreeBytes;
      }
    } catch {
      diskFreeBytes = null;
    }
    metrics.disk_free_bytes = diskFreeBytes;
    return diskFreeBytes;
  }

  function listNames() {
    readdirCount += 1;
    try {
      ensureDir(resolvedSpoolDir);
      return fsImpl.readdirSync(resolvedSpoolDir);
    } catch (err) {
      recordWriteFailure(err, "readdir");
      return [];
    }
  }

  function listDrainableFiles() {
    return listNames()
      .filter((name) => isDrainableName(name))
      .map((name) => path.join(resolvedSpoolDir, name))
      .sort((a, b) => {
        const d = spoolFileTs(a) - spoolFileTs(b);
        return d !== 0 ? d : a.localeCompare(b);
      });
  }

  function applyMetaToMetrics() {
    spoolFiles = spoolMeta.size + (activePath && activeDocs > 0 ? 1 : 0);
    spoolBytes = activeBytes;
    spoolOldestMtime = activeOpenedAt || null;
    let spoolDocs = activeDocs;
    for (const meta of spoolMeta.values()) {
      spoolBytes += Number(meta.size) || 0;
      spoolDocs += Number(meta.docs) || 0;
      const mt = Number(meta.mtime) || 0;
      if (!spoolOldestMtime || (mt && mt < spoolOldestMtime)) spoolOldestMtime = mt;
    }
    spoolOldestAgeMs = spoolOldestMtime ? Math.max(0, now() - spoolOldestMtime) : 0;
    metrics.gpspoints_spool_files = spoolFiles;
    metrics.gpspoints_spool_bytes = spoolBytes;
    metrics.gpspoints_spool_oldest_age_ms = spoolOldestAgeMs;
    metrics.gpspoints_spool_docs = spoolDocs;
    metrics.gpspoints_backlog_docs = spoolDocs;
    metrics.gpspoints_backlog_files = spoolFiles;
    metrics.gpspoints_backlog_bytes = spoolBytes;
    metrics.gpspoints_backlog_oldest_age = spoolOldestAgeMs;
    metrics.gpspoints_spool_depth = spoolDocs > 0 ? spoolDocs : spoolFiles;
    metrics.gpspoints_queue_depth = unjournaled.length;
    metrics.gpspoints_retry_pending_total = unjournaled.length + spoolDocs;
    metrics.gpspoints_active_segment_docs = activeDocs;
    metrics.gpspoints_active_segment_bytes = activeBytes;
  }

  function trackSpoolFile(filePath, size, mtime, docs = 0) {
    spoolMeta.set(filePath, {
      size: Number(size) || 0,
      mtime: Number(mtime) || now(),
      docs: Math.max(0, Number(docs) || 0),
    });
    applyMetaToMetrics();
  }

  function untrackSpoolFile(filePath) {
    spoolMeta.delete(filePath);
    applyMetaToMetrics();
  }

  function countLinesSync(filePath) {
    try {
      const raw = fsImpl.readFileSync(filePath, "utf8");
      let n = 0;
      for (const line of raw.split("\n")) {
        if (line.trim()) n += 1;
      }
      return n;
    } catch {
      return 0;
    }
  }

  function refreshSpoolStats() {
    const files = listDrainableFiles();
    spoolMeta.clear();
    for (const filePath of files) {
      try {
        const st = fsImpl.statSync(filePath);
        spoolMeta.set(filePath, {
          size: Number(st.size) || 0,
          mtime: Number(st.mtimeMs) || now(),
          docs: 0,
        });
      } catch {
        /* skip */
      }
    }
    if (activePath) {
      try {
        const st = fsImpl.statSync(activePath);
        activeBytes = Number(st.size) || activeBytes;
      } catch {
        /* keep */
      }
    }
    applyMetaToMetrics();
    statDiskFree();
    return spoolFiles;
  }

  function scheduleReconcile() {
    if (reconcileTimer || !spoolReconcileMs) return;
    reconcileTimer = setTimeout(() => {
      reconcileTimer = null;
      try {
        refreshSpoolStats();
      } catch {
        /* ignore */
      }
      if (!closed) scheduleReconcile();
    }, spoolReconcileMs);
    if (typeof reconcileTimer.unref === "function") reconcileTimer.unref();
  }

  function persistenceHealth() {
    const free = diskFreeBytes;
    if (
      lastDiskError &&
      /ENOSPC|EROFS|EACCES/.test(lastDiskError) &&
      now() - (lastDiskErrorAt || 0) < 60_000
    ) {
      return "critical";
    }
    if (spoolBytes >= spoolCriticalBytes) return "critical";
    if (free != null && free < minDiskFreeBytes) return "critical";
    if (spoolBytes >= spoolWarnBytes) return "warning";
    if (free != null && free < minDiskFreeBytes * 2) return "warning";
    if ((metrics.gpspoints_spool_write_failures || 0) > 0 && unjournaled.length > 0) {
      return "warning";
    }
    return "ok";
  }

  function cleanupTmpFiles() {
    try {
      ensureDir(resolvedSpoolDir);
      for (const name of listNames()) {
        if (!name.endsWith(".tmp")) continue;
        const tmpPath = path.join(resolvedSpoolDir, name);
        try {
          fsImpl.unlinkSync(tmpPath);
          bump("gpspoints_spool_tmp_cleaned");
          log.warn?.("[gpspoints] removed incomplete spool tmp", { file: name });
        } catch (err) {
          recordWriteFailure(err, "unlink_tmp");
        }
      }
    } catch (err) {
      recordWriteFailure(err, "cleanup_tmp");
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
    }, jitter(retryDelayMs));
    if (typeof retryTimer.unref === "function") retryTimer.unref();
  }

  function scheduleJournal() {
    if (!enableJournal) return;
    if (journalTimer || journalTimeout) return;
    if (!journalCoalesceMs) {
      journalTimer = setImmediate(() => {
        journalTimer = null;
        journalNow();
      });
      return;
    }
    journalTimeout = setTimeout(() => {
      journalTimeout = null;
      journalNow();
    }, journalCoalesceMs);
    if (typeof journalTimeout.unref === "function") journalTimeout.unref();
  }

  function scheduleSealDeadline() {
    if (!enableJournal || sealTimer || !segmentSealMs) return;
    if (!activePath || activeDocs <= 0) return;
    const age = now() - activeOpenedAt;
    const delay = Math.max(0, segmentSealMs - age);
    sealTimer = setTimeout(() => {
      sealTimer = null;
      try {
        sealActiveSegment("deadline");
      } catch (err) {
        recordWriteFailure(err, "seal_deadline");
      }
      if (!closed && enableMongoDrain) scheduleFlush(0);
      else if (!closed) scheduleFlush(flushMs);
    }, delay);
    if (typeof sealTimer.unref === "function") sealTimer.unref();
  }

  function readyPathForActive(activeFilePath) {
    return String(activeFilePath).replace(/\.jsonl\.active$/i, ".jsonl.ready");
  }

  function drainingPathForReady(readyFilePath) {
    return `${String(readyFilePath).replace(/\.jsonl\.ready$/i, ".jsonl.draining")}.${drainOwnerId}`;
  }

  function readyPathFromDraining(drainingFilePath) {
    return String(drainingFilePath).replace(/\.jsonl\.draining\.\d+$/i, ".jsonl.ready");
  }

  function newActivePath() {
    const name = `gps-${String(now()).padStart(13, "0")}-${crypto.randomBytes(6).toString("hex")}.jsonl.active`;
    return path.join(resolvedSpoolDir, name);
  }

  function openActiveIfNeeded() {
    if (activePath) return;
    ensureDir(resolvedSpoolDir);
    activePath = newActivePath();
    activeDocs = 0;
    activeBytes = 0;
    activeOpenedAt = now();
  }

  function shouldSealActive() {
    if (!activePath || activeDocs <= 0) return false;
    if (activeDocs >= segmentMaxDocs) return true;
    if (activeBytes >= segmentMaxBytes) return true;
    if (segmentSealMs > 0 && now() - activeOpenedAt >= segmentSealMs) return true;
    return false;
  }

  function sealActiveSegment(reason = "threshold") {
    if (!enableJournal || sealing) return null;
    if (!activePath || activeDocs <= 0) return null;
    sealing = true;
    try {
      const from = activePath;
      const docs = activeDocs;
      const bytes = activeBytes;
      const mtime = activeOpenedAt || now();
      const dest = readyPathForActive(from);
      fsImpl.renameSync(from, dest);
      activePath = null;
      activeDocs = 0;
      activeBytes = 0;
      activeOpenedAt = 0;
      if (sealTimer) {
        clearTimeout(sealTimer);
        sealTimer = null;
      }
      trackSpoolFile(dest, bytes, mtime, docs);
      bump("gpspoints_segment_sealed_total");
      metrics.gpspoints_last_seal_reason = reason;
      return dest;
    } finally {
      sealing = false;
    }
  }

  function appendDocsToActive(docs) {
    if (!docs.length) return;
    openActiveIfNeeded();
    const body = docs.map((doc) => JSON.stringify(doc)).join("\n") + "\n";
    fsImpl.appendFileSync(activePath, body, "utf8");
    const nbytes = Buffer.byteLength(body);
    activeDocs += docs.length;
    activeBytes += nbytes;
    bump("gpspoints_spooled_total", docs.length);
    bump("durably_spooled_total", docs.length);
    applyMetaToMetrics();
    scheduleSealDeadline();
    if (shouldSealActive()) {
      sealActiveSegment(activeDocs >= segmentMaxDocs ? "docs" : activeBytes >= segmentMaxBytes ? "bytes" : "deadline");
    }
  }

  /** Legacy helper still used by rewrite-on-partial paths. */
  function writeSpoolFileSync(docs, prefix = "pending") {
    if (!docs.length) return null;
    ensureDir(resolvedSpoolDir);
    const name = `${prefix}-${String(now()).padStart(13, "0")}-${crypto.randomBytes(6).toString("hex")}.jsonl`;
    const filePath = path.join(resolvedSpoolDir, name);
    const tmpPath = `${filePath}.tmp`;
    const body = docs.map((doc) => JSON.stringify(doc)).join("\n") + "\n";
    fsImpl.writeFileSync(tmpPath, body, "utf8");
    fsImpl.renameSync(tmpPath, filePath);
    let size = Buffer.byteLength(body);
    try {
      size = fsImpl.statSync(filePath).size;
    } catch {
      /* use byteLength */
    }
    trackSpoolFile(filePath, size, now(), docs.length);
    bump("gpspoints_spooled_total", docs.length);
    bump("durably_spooled_total", docs.length);
    return filePath;
  }

  function quarantineDocs(docs, reason) {
    if (!docs.length) return;
    try {
      ensureDir(quarantineDir);
      const name = `quarantine-${now()}-${crypto.randomBytes(4).toString("hex")}.jsonl`;
      const filePath = path.join(quarantineDir, name);
      const tmpPath = `${filePath}.tmp`;
      const body =
        docs.map((doc) => JSON.stringify({ reason, doc, at: now() })).join("\n") + "\n";
      fsImpl.writeFileSync(tmpPath, body, "utf8");
      fsImpl.renameSync(tmpPath, filePath);
      bump("gpspoints_quarantined_total", docs.length);
      bump("gpspoints_writer_quarantined", docs.length);
      log.error?.("[gpspoints] quarantined documents", { reason, count: docs.length, file: name });
    } catch (err) {
      unjournaled.push(...docs);
      recordWriteFailure(err, "quarantine");
    }
  }

  function quarantineFile(filePath, reason) {
    try {
      ensureDir(quarantineDir);
      const dest = path.join(quarantineDir, `${path.basename(filePath)}.${reason || "corrupt"}`);
      fsImpl.renameSync(filePath, dest);
      bump("gpspoints_spool_corrupt_files");
      log.error?.("[gpspoints] quarantined spool file", { file: filePath, reason });
    } catch (err) {
      recordWriteFailure(err, "quarantine_file");
    }
  }

  function journalNow() {
    if (!enableJournal) return true;
    if (!unjournaled.length) return true;
    const docs = unjournaled.splice(0, unjournaled.length);
    try {
      appendDocsToActive(docs);
      bump("gpspoints_journaled_total", docs.length);
      metrics.gpspoints_queue_depth = unjournaled.length;
      return true;
    } catch (err) {
      unjournaled.unshift(...docs);
      metrics.gpspoints_queue_depth = unjournaled.length;
      recordWriteFailure(err, "journal");
      return false;
    }
  }

  async function flushJournal() {
    return new Promise((resolve) => {
      setImmediate(() => {
        journalNow();
        resolve();
      });
    });
  }

  function enqueue(doc) {
    if (!enableJournal || !doc) return;
    bump("gpspoints_received_total");
    bump("archive_enqueued_total");
    unjournaled.push(doc);
    metrics.gpspoints_queue_depth = unjournaled.length;
    scheduleJournal();
    if (unjournaled.length >= batchSize || unjournaled.length >= memHigh) scheduleFlush(0);
    else scheduleFlush(flushMs);
  }

  function applyClassifiedMetrics(classified) {
    bump("gpspoints_mongo_acknowledged_total", classified.acked.length);
    bump("gpspoints_persisted_total", classified.acked.length - classified.duplicates.length);
    bump("gpspoints_duplicate_already_persisted_total", classified.duplicates.length);
    bump("gpspoints_duplicates_ignored", classified.duplicates.length);
    bump("gpspoints_unexpected_duplicate_total", classified.unexpectedDuplicates.length);
    if (classified.acked.length) bump("gpspoints_batch_completed");
    if (classified.acked.length) {
      metrics.gpspoints_writer_last_persisted_at = new Date(now()).toISOString();
    }
  }

  function recordMongoLatency(startedAt) {
    const ms = Math.max(0, now() - startedAt);
    const ring = metrics._gpspoints_mongo_latency_ring || (metrics._gpspoints_mongo_latency_ring = []);
    ring.push(ms);
    if (ring.length > 128) ring.shift();
    const sorted = ring.slice().sort((a, b) => a - b);
    const pct = (p) => {
      if (!sorted.length) return 0;
      const idx = Math.min(sorted.length - 1, Math.max(0, Math.ceil((p / 100) * sorted.length) - 1));
      return sorted[idx];
    };
    metrics.gpspoints_mongo_latency_ms = ms;
    metrics.gpspoints_mongo_latency_p50 = pct(50);
    metrics.gpspoints_mongo_latency_p95 = pct(95);
    metrics.gpspoints_mongo_latency_p99 = pct(99);
  }

  async function persistBatchClassified(docs) {
    if (!docs.length) return emptyClassified();
    mongoBatchesThisCycle += 1;
    const attemptedBefore = metrics.gpspoints_mongo_attempted_total || 0;
    bump("gpspoints_mongo_flush_count");
    metrics.gpspoints_mongo_docs_per_flush_last = docs.length;
    metrics.gpspoints_batch_docs_avg = metrics.gpspoints_mongo_docs_per_flush_avg;
    metrics.gpspoints_mongo_docs_per_flush_max = Math.max(
      metrics.gpspoints_mongo_docs_per_flush_max || 0,
      docs.length
    );
    metrics.gpspoints_batch_docs_max = metrics.gpspoints_mongo_docs_per_flush_max;
    metrics.gpspoints_mongo_docs_per_flush_avg = metrics.gpspoints_mongo_flush_count
      ? Number(((attemptedBefore + docs.length) / metrics.gpspoints_mongo_flush_count).toFixed(2))
      : docs.length;
    metrics.gpspoints_batch_docs_avg = metrics.gpspoints_mongo_docs_per_flush_avg;
    bump("gpspoints_mongo_attempted_total", docs.length);
    const started = now();
    try {
      await insertMany(docs);
      recordMongoLatency(started);
      retryDelayMs = retryBaseMs;
      const classified = classifyInsertManyError(docs, null);
      applyClassifiedMetrics(classified);
      return classified;
    } catch (err) {
      recordMongoLatency(started);
      const classified = classifyInsertManyError(docs, err);
      if (classified.acked.length || classified.unexpectedDuplicates.length || classified.invalid.length) {
        retryDelayMs = retryBaseMs;
        applyClassifiedMetrics(classified);
        if (classified.unexpectedDuplicates.length) {
          quarantineDocs(classified.unexpectedDuplicates, "unexpected_duplicate_key");
        }
        if (classified.invalid.length) {
          quarantineDocs(classified.invalid, "permanent_invalid");
        }
        if (classified.retry.length) {
          bump("gpspoints_retry_total", classified.retry.length);
          bump("gpspoints_writer_retries", classified.retry.length);
          retryDelayMs = Math.min(retryMaxMs, retryDelayMs * 2);
        }
        return classified;
      }
      // Ambiguous permanent failure on large batch: bounded binary isolation.
      if (isPermanentMongoError(err) && docs.length > 1) {
        const mid = Math.floor(docs.length / 2);
        const left = await persistBatchWithPoisonIsolation(docs.slice(0, mid));
        const right = await persistBatchWithPoisonIsolation(docs.slice(mid));
        return mergeClassified(left, right);
      }
      bump("gpspoints_retry_total", docs.length);
      bump("gpspoints_writer_retries", docs.length);
      bump("gpspoints_persist_failures");
      retryDelayMs = Math.min(retryMaxMs, retryDelayMs * 2);
      throw err;
    }
  }

  async function persistBatchWithPoisonIsolation(docs) {
    if (!docs.length) return emptyClassified();
    if (docs.length === 1) {
      try {
        return await persistBatchClassified(docs);
      } catch (err) {
        if (isPermanentMongoError(err)) {
          quarantineDocs(docs, "permanent_invalid_isolated");
          return {
            ...emptyClassified(),
            invalid: docs.slice(),
          };
        }
        throw err;
      }
    }
    try {
      return await persistBatchClassified(docs);
    } catch (err) {
      if (!isPermanentMongoError(err)) throw err;
      const mid = Math.floor(docs.length / 2);
      const left = await persistBatchWithPoisonIsolation(docs.slice(0, mid));
      const right = await persistBatchWithPoisonIsolation(docs.slice(mid));
      return mergeClassified(left, right);
    }
  }

  function parseSpoolFile(filePath) {
    let raw;
    try {
      raw = fsImpl.readFileSync(filePath, "utf8");
    } catch (err) {
      recordWriteFailure(err, "read_spool");
      throw err;
    }
    const docs = [];
    const badLines = [];
    for (const line of raw.split("\n")) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      try {
        docs.push(JSON.parse(trimmed));
      } catch {
        badLines.push(trimmed);
      }
    }
    if (badLines.length) {
      quarantineDocs(
        badLines.map((line) => ({ _raw: line })),
        "corrupt_jsonl_line"
      );
      bump("gpspoints_spool_corrupt_lines", badLines.length);
    }
    return docs;
  }

  function pickFairFilesByDocs(files) {
    if (!files.length) return [];
    const ratio = Math.min(1, Math.max(0, Number(drainOldDocRatio) || 0.5));
    const docBudget = Math.max(batchSize, batchSize * Math.max(1, maxMongoBatchesPerCycle));
    const oldBudget = Math.max(batchSize, Math.floor(docBudget * ratio));
    const newBudget = Math.max(batchSize, docBudget - oldBudget);
    const minOldFiles = Math.max(1, Number(drainOldFilesPerCycle) || 1);
    const minNewFiles = Math.max(1, Number(drainNewFilesPerCycle) || 1);
    const maxFiles = Math.max(minOldFiles + minNewFiles, Number(maxFilesPerCycle) || 500);

    const selected = [];
    const seen = new Set();
    let oldDocs = 0;
    let newDocs = 0;
    let iOld = 0;
    let iNew = files.length - 1;
    let oldFiles = 0;
    let newFiles = 0;

    function take(filePath, side) {
      if (!filePath || seen.has(filePath)) return false;
      seen.add(filePath);
      selected.push({ path: filePath, side });
      if (side === "old") oldFiles += 1;
      else newFiles += 1;
      return true;
    }

    function estDocs(filePath) {
      const meta = spoolMeta.get(filePath);
      const n = Number(meta?.docs);
      return Number.isFinite(n) && n > 0 ? n : 1;
    }

    while (selected.length < maxFiles && iOld <= iNew) {
      const needOld = oldDocs < oldBudget || oldFiles < minOldFiles;
      const needNew = newDocs < newBudget || newFiles < minNewFiles;
      if (!needOld && !needNew && oldDocs + newDocs >= Math.min(docBudget, oldBudget + newBudget)) {
        break;
      }
      if (!needOld && !needNew) break;

      let side;
      if (needOld && needNew) side = oldDocs <= newDocs ? "old" : "new";
      else if (needOld) side = "old";
      else side = "new";

      if (side === "old") {
        while (iOld <= iNew && seen.has(files[iOld])) iOld += 1;
        if (iOld > iNew) break;
        const filePath = files[iOld++];
        if (take(filePath, "old")) oldDocs += estDocs(filePath);
      } else {
        while (iNew >= iOld && seen.has(files[iNew])) iNew -= 1;
        if (iNew < iOld) break;
        const filePath = files[iNew--];
        if (take(filePath, "new")) newDocs += estDocs(filePath);
      }
    }
    return selected;
  }

  function claimFile(filePath) {
    const base = path.basename(filePath);
    if (isDrainingSegmentName(base)) return filePath;
    if (isReadySegmentName(base)) {
      const dest = drainingPathForReady(filePath);
      fsImpl.renameSync(filePath, dest);
      if (spoolMeta.has(filePath)) {
        const meta = spoolMeta.get(filePath);
        spoolMeta.delete(filePath);
        spoolMeta.set(dest, meta);
      }
      return dest;
    }
    // Legacy hot/pending: claim by renaming to draining sibling for crash safety.
    if (isLegacySpoolName(base)) {
      const dest = path.join(
        resolvedSpoolDir,
        `${base.replace(/\.jsonl$/i, "")}.jsonl.draining.${drainOwnerId}`
      );
      try {
        fsImpl.renameSync(filePath, dest);
        if (spoolMeta.has(filePath)) {
          const meta = spoolMeta.get(filePath);
          spoolMeta.delete(filePath);
          spoolMeta.set(dest, meta);
        }
        return dest;
      } catch {
        return filePath;
      }
    }
    return filePath;
  }

  function releaseClaimToReady(filePath) {
    const base = path.basename(filePath);
    if (!isDrainingSegmentName(base)) return filePath;
    if (isLegacySpoolName(base.replace(/\.draining\.\d+$/i, ""))) {
      // legacy draining name: restore pending-
      const restored = path.join(
        resolvedSpoolDir,
        base.replace(/\.jsonl\.draining\.\d+$/i, ".jsonl")
      );
      try {
        fsImpl.renameSync(filePath, restored);
        return restored;
      } catch {
        return filePath;
      }
    }
    const ready = readyPathFromDraining(filePath);
    try {
      fsImpl.renameSync(filePath, ready);
      return ready;
    } catch {
      return filePath;
    }
  }

  function finalizeFileState(fileState) {
    const remaining = [];
    for (let i = 0; i < fileState.docs.length; i++) {
      if (fileState.status[i] === "acked") continue;
      remaining.push(fileState.docs[i]);
    }
    try {
      fsImpl.unlinkSync(fileState.path);
    } catch (err) {
      if (errCode(err) !== "ENOENT") {
        recordWriteFailure(err, "unlink_acked_spool");
        throw err;
      }
    }
    untrackSpoolFile(fileState.path);
    if (remaining.length) {
      writeSpoolFileSync(remaining, "pending");
    }
  }

  async function drainSpoolFair() {
    if (!enableMongoDrain) return;
    // Always scan disk — meta paths go stale across claim/release renames.
    const files = listDrainableFiles();
    if (!files.length) return;

    const picked = pickFairFilesByDocs(files);
    const fileStates = [];
    for (const { path: pickedPath } of picked) {
      let claimedPath = pickedPath;
      try {
        claimedPath = claimFile(pickedPath);
      } catch (err) {
        // Another writer claimed it — skip.
        if (errCode(err) === "ENOENT") continue;
        recordWriteFailure(err, "claim");
        continue;
      }
      let docs;
      try {
        docs = parseSpoolFile(claimedPath);
      } catch (err) {
        if (errCode(err) === "ENOENT") {
          untrackSpoolFile(claimedPath);
          untrackSpoolFile(pickedPath);
          continue;
        }
        quarantineFile(claimedPath, "unreadable");
        untrackSpoolFile(claimedPath);
        continue;
      }
      if (!docs.length) {
        try {
          fsImpl.unlinkSync(claimedPath);
        } catch (err) {
          recordWriteFailure(err, "unlink_empty");
        }
        untrackSpoolFile(claimedPath);
        continue;
      }
      const meta = spoolMeta.get(claimedPath) || spoolMeta.get(pickedPath);
      if (meta) meta.docs = docs.length;
      else trackSpoolFile(claimedPath, 0, now(), docs.length);
      fileStates.push({
        path: claimedPath,
        docs,
        status: docs.map(() => "pending"),
        cursor: 0,
      });
    }
    if (!fileStates.length) return;

    let hardFail = null;
    try {
      while (mongoBatchesThisCycle < maxMongoBatchesPerCycle) {
        const batch = [];
        const refs = [];
        let progressed = true;
        while (batch.length < batchSize && progressed) {
          progressed = false;
          for (const state of fileStates) {
            if (batch.length >= batchSize) break;
            while (
              state.cursor < state.docs.length &&
              state.status[state.cursor] !== "pending"
            ) {
              state.cursor += 1;
            }
            if (state.cursor >= state.docs.length) continue;
            const idx = state.cursor;
            batch.push(state.docs[idx]);
            refs.push({ state, index: idx });
            state.cursor += 1;
            progressed = true;
          }
        }
        if (!batch.length) break;

        let classified;
        try {
          classified = await persistBatchWithPoisonIsolation(batch);
        } catch (err) {
          hardFail = err;
          break;
        }

        const acked = new Set(classified.acked);
        const retry = new Set(classified.retry);
        for (const ref of refs) {
          const doc = ref.state.docs[ref.index];
          if (acked.has(doc)) ref.state.status[ref.index] = "acked";
          else if (retry.has(doc)) ref.state.status[ref.index] = "pending";
          else ref.state.status[ref.index] = "acked";
        }

        if (classified.retry.length && !classified.acked.length) break;
      }
    } finally {
      for (const state of fileStates) {
        const hasPending = state.status.some((s) => s === "pending");
        const hasAcked = state.status.some((s) => s === "acked");
        if (!hasPending && hasAcked) {
          try {
            fsImpl.unlinkSync(state.path);
          } catch (err) {
            if (errCode(err) !== "ENOENT") {
              recordWriteFailure(err, "unlink_acked_spool");
              throw err;
            }
          }
          untrackSpoolFile(state.path);
        } else if (hasPending && hasAcked) {
          finalizeFileState(state);
        } else if (hasPending && !hasAcked) {
          const restored = releaseClaimToReady(state.path);
          untrackSpoolFile(state.path);
          if (restored) {
            try {
              const st = fsImpl.statSync(restored);
              trackSpoolFile(restored, st.size, now(), state.docs.length);
            } catch {
              trackSpoolFile(restored, 0, now(), state.docs.length);
            }
          }
        }
      }
      applyMetaToMetrics();
    }
    if (hardFail) throw hardFail;
  }

  async function flushUnjournaledToMongo() {
    if (!enableMongoDrain) return;
    while (unjournaled.length && mongoBatchesThisCycle < maxMongoBatchesPerCycle) {
      const batch = unjournaled.splice(0, batchSize);
      try {
        const classified = await persistBatchWithPoisonIsolation(batch);
        if (classified.retry.length) {
          unjournaled.unshift(...classified.retry);
          throw new Error("retryable_mongo_batch");
        }
      } catch (err) {
        if (!String(err.message).includes("retryable_mongo_batch")) {
          unjournaled.unshift(...batch);
        }
        throw err;
      } finally {
        metrics.gpspoints_queue_depth = unjournaled.length;
      }
    }
  }

  function acquireDrainLock() {
    if (!enableMongoDrain) return true;
    ensureDir(resolvedSpoolDir);
    const payload = JSON.stringify({
      pid: drainOwnerId,
      mode: resolvedMode,
      started_at: new Date(now()).toISOString(),
    });
    try {
      fsImpl.writeFileSync(drainLockPath, payload, { flag: "wx" });
      drainLockHeld = true;
      return true;
    } catch (err) {
      if (errCode(err) !== "EEXIST") {
        recordWriteFailure(err, "drain_lock");
        throw err;
      }
      let existing = null;
      try {
        existing = JSON.parse(fsImpl.readFileSync(drainLockPath, "utf8"));
      } catch {
        existing = null;
      }
      const otherPid = Number(existing?.pid);
      if (otherPid === Number(drainOwnerId)) {
        drainLockHeld = true;
        return true;
      }
      if (otherPid && !pidAlive(otherPid)) {
        try {
          fsImpl.unlinkSync(drainLockPath);
        } catch {
          /* ignore */
        }
        try {
          fsImpl.writeFileSync(drainLockPath, payload, { flag: "wx" });
          drainLockHeld = true;
          log.warn?.("[gpspoints] reclaimed stale drain lock", { stale_pid: otherPid });
          return true;
        } catch (err2) {
          if (errCode(err2) === "EEXIST") {
            const msg =
              "gpspoints dual-consumer forbidden: another drain lock holder exists";
            log.error?.(msg, { lock: drainLockPath });
            throw new Error(msg);
          }
          throw err2;
        }
      }
      const msg =
        "gpspoints dual-consumer forbidden: drain already owned (set GPSPOINT_EXTERNAL_WRITER consistently)";
      log.error?.(msg, { lock: drainLockPath, holder: existing });
      throw new Error(msg);
    }
  }

  function releaseDrainLock() {
    if (!drainLockHeld) return;
    try {
      const raw = fsImpl.readFileSync(drainLockPath, "utf8");
      const existing = JSON.parse(raw);
      if (Number(existing?.pid) === Number(drainOwnerId)) {
        fsImpl.unlinkSync(drainLockPath);
      }
    } catch {
      /* ignore */
    }
    drainLockHeld = false;
  }

  function writeHeartbeat() {
    if (!enableMongoDrain) return;
    try {
      ensureDir(resolvedSpoolDir);
      const body = JSON.stringify({
        alive: true,
        pid: drainOwnerId,
        mode: resolvedMode,
        ts: new Date(now()).toISOString(),
        last_persisted_at: metrics.gpspoints_writer_last_persisted_at || null,
        backlog_docs: metrics.gpspoints_backlog_docs || 0,
        backlog_files: metrics.gpspoints_backlog_files || 0,
      });
      const tmp = `${heartbeatPath}.tmp`;
      fsImpl.writeFileSync(tmp, body, "utf8");
      fsImpl.renameSync(tmp, heartbeatPath);
      metrics.gpspoints_writer_alive = true;
      metrics.gpspoints_writer_last_heartbeat = new Date(now()).toISOString();
    } catch (err) {
      recordWriteFailure(err, "heartbeat");
    }
  }

  function scheduleHeartbeat() {
    if (!enableMongoDrain || heartbeatTimer || !heartbeatMs) return;
    writeHeartbeat();
    heartbeatTimer = setInterval(() => {
      writeHeartbeat();
    }, heartbeatMs);
    if (typeof heartbeatTimer.unref === "function") heartbeatTimer.unref();
  }

  function reclaimDrainingAndOrphanActives() {
    ensureDir(resolvedSpoolDir);
    const names = listNames();
    const actives = names.filter((n) => isActiveSegmentName(n));

    // Stale draining → ready (at-least-once replay OK).
    for (const name of names) {
      if (!isDrainingSegmentName(name)) continue;
      const m = name.match(/\.draining\.(\d+)$/);
      const pid = m ? Number(m[1]) : 0;
      if (pid && pidAlive(pid) && pid !== Number(drainOwnerId)) continue;
      const from = path.join(resolvedSpoolDir, name);
      let dest;
      if (/^(hot|pending)-/i.test(name)) {
        dest = path.join(resolvedSpoolDir, name.replace(/\.jsonl\.draining\.\d+$/i, ".jsonl"));
      } else {
        dest = readyPathFromDraining(from);
      }
      try {
        fsImpl.renameSync(from, dest);
        bump("gpspoints_draining_reclaimed_total");
      } catch (err) {
        recordWriteFailure(err, "reclaim_draining");
      }
    }

    // Orphan actives: if we are producer/full and have no active yet, adopt the newest;
    // seal the rest to ready. Drain-only mode seals all actives to ready.
    actives.sort();
    if (enableJournal && !activePath && actives.length) {
      const adopt = actives.pop();
      activePath = path.join(resolvedSpoolDir, adopt);
      activeDocs = countLinesSync(activePath);
      try {
        activeBytes = fsImpl.statSync(activePath).size;
      } catch {
        activeBytes = 0;
      }
      activeOpenedAt = now();
      scheduleSealDeadline();
    }
    for (const name of actives) {
      const from = path.join(resolvedSpoolDir, name);
      const dest = readyPathForActive(from);
      try {
        fsImpl.renameSync(from, dest);
        bump("gpspoints_orphan_active_sealed_total");
      } catch (err) {
        recordWriteFailure(err, "seal_orphan_active");
      }
    }
  }

  async function flushCycle(opts = {}) {
    if (flushing) return;
    if (closed && !opts.force) return;
    flushing = true;
    mongoBatchesThisCycle = 0;
    try {
      if (enableJournal) {
        journalNow();
        // Full mode: seal before drain so Mongo sees work (not left stuck in .active).
        // Producer: seal only on threshold/deadline/force (avoid tiny segments every tick).
        const sealForDrain = enableMongoDrain && activeDocs > 0;
        if (sealForDrain || shouldSealActive() || opts.force) {
          try {
            sealActiveSegment(
              opts.force ? "shutdown" : sealForDrain ? "pre_drain" : "threshold"
            );
          } catch (err) {
            recordWriteFailure(err, "seal");
          }
        }
      }
      if (enableMongoDrain) {
        await drainSpoolFair();
        if (unjournaled.length) {
          await flushUnjournaledToMongo();
        }
      }
    } catch (err) {
      log.warn?.("[gpspoints] flush retry scheduled", err.message);
      scheduleRetry();
    } finally {
      flushing = false;
      applyMetaToMetrics();
      metrics.gpspoints_health = persistenceHealth();
      const pendingWork =
        unjournaled.length ||
        spoolFiles ||
        (activePath && activeDocs > 0);
      if (!closed && pendingWork) {
        if (!retryTimer) scheduleFlush(flushMs);
      }
    }
  }

  function recoverPendingSpool() {
    cleanupTmpFiles();
    reclaimDrainingAndOrphanActives();
    if (enableMongoDrain) {
      try {
        acquireDrainLock();
        scheduleHeartbeat();
      } catch (err) {
        recordWriteFailure(err, "drain_lock_recover");
        // Dual-consumer is a hard configuration error — never start a second drain.
        if (/dual-consumer|drain already owned/i.test(errText(err))) {
          throw err;
        }
      }
    }
    refreshSpoolStats();
    scheduleReconcile();
    if (enableMongoDrain && spoolFiles > 0) scheduleFlush(0);
    else if (enableJournal && activeDocs > 0) scheduleSealDeadline();
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
    if (journalTimer) {
      clearImmediate(journalTimer);
      journalTimer = null;
    }
    if (journalTimeout) {
      clearTimeout(journalTimeout);
      journalTimeout = null;
    }
    if (sealTimer) {
      clearTimeout(sealTimer);
      sealTimer = null;
    }
    if (reconcileTimer) {
      clearTimeout(reconcileTimer);
      reconcileTimer = null;
    }
    if (heartbeatTimer) {
      clearInterval(heartbeatTimer);
      heartbeatTimer = null;
    }
    const started = now();
    if (enableJournal) {
      journalNow();
      try {
        sealActiveSegment("shutdown");
      } catch (err) {
        recordWriteFailure(err, "shutdown_seal");
      }
    }
    try {
      if (enableMongoDrain) {
        await Promise.race([
          flushCycle({ force: true }).catch(() => {}),
          new Promise((resolve) => setTimeout(resolve, timeoutMs)),
        ]);
      }
    } finally {
      if (unjournaled.length && enableJournal) {
        const leftover = unjournaled.splice(0, unjournaled.length);
        try {
          appendDocsToActive(leftover);
          sealActiveSegment("shutdown_leftover");
        } catch (err) {
          unjournaled.unshift(...leftover);
          recordWriteFailure(err, "shutdown_journal");
        }
      }
      releaseDrainLock();
    }
    return { elapsed_ms: now() - started, spool_depth: refreshSpoolStats() };
  }

  function simulateCrash() {
    closed = true;
    if (flushTimer) {
      clearTimeout(flushTimer);
      flushTimer = null;
    }
    if (retryTimer) {
      clearTimeout(retryTimer);
      retryTimer = null;
    }
    if (journalTimer) {
      clearImmediate(journalTimer);
      journalTimer = null;
    }
    if (journalTimeout) {
      clearTimeout(journalTimeout);
      journalTimeout = null;
    }
    if (sealTimer) {
      clearTimeout(sealTimer);
      sealTimer = null;
    }
    if (heartbeatTimer) {
      clearInterval(heartbeatTimer);
      heartbeatTimer = null;
    }
    // Leave active/ready files on disk. Drop in-process lock so a successor in the
    // same test PID can recover (real multi-process crash uses pidAlive reclaim).
    try {
      fsImpl.unlinkSync(drainLockPath);
    } catch {
      /* ignore */
    }
    unjournaled.length = 0;
    flushing = false;
    activePath = null;
    activeDocs = 0;
    activeBytes = 0;
    drainLockHeld = false;
  }

  function getStats() {
    applyMetaToMetrics();
    return {
      gpspoint_spool_dir: resolvedSpoolDir,
      gpspoints_writer_mode: resolvedMode,
      gpspoints_durability: metrics.gpspoints_durability,
      gpspoints_queue_depth: unjournaled.length,
      gpspoints_spool_files: spoolFiles,
      gpspoints_spool_bytes: spoolBytes,
      gpspoints_spool_oldest_age_ms: spoolOldestAgeMs,
      gpspoints_spool_docs: metrics.gpspoints_spool_docs || 0,
      gpspoints_spool_depth: metrics.gpspoints_spool_depth || spoolFiles,
      gpspoints_backlog_docs: metrics.gpspoints_backlog_docs || 0,
      gpspoints_backlog_files: metrics.gpspoints_backlog_files || 0,
      gpspoints_backlog_bytes: metrics.gpspoints_backlog_bytes || 0,
      gpspoints_backlog_oldest_age: metrics.gpspoints_backlog_oldest_age || 0,
      gpspoints_active_segment_docs: activeDocs,
      gpspoints_active_segment_bytes: activeBytes,
      gpspoints_spool_write_failures: metrics.gpspoints_spool_write_failures || 0,
      disk_free_bytes: diskFreeBytes,
      gpspoints_journaled_total: metrics.gpspoints_journaled_total || 0,
      gpspoints_mongo_flush_count: metrics.gpspoints_mongo_flush_count || 0,
      gpspoints_mongo_docs_per_flush_last: metrics.gpspoints_mongo_docs_per_flush_last || 0,
      gpspoints_mongo_docs_per_flush_avg: metrics.gpspoints_mongo_docs_per_flush_avg || 0,
      gpspoints_mongo_docs_per_flush_max: metrics.gpspoints_mongo_docs_per_flush_max || 0,
      gpspoints_batch_docs_avg: metrics.gpspoints_batch_docs_avg || 0,
      gpspoints_batch_docs_max: metrics.gpspoints_batch_docs_max || 0,
      gpspoints_mongo_latency_p50: metrics.gpspoints_mongo_latency_p50 || 0,
      gpspoints_mongo_latency_p95: metrics.gpspoints_mongo_latency_p95 || 0,
      gpspoints_mongo_latency_p99: metrics.gpspoints_mongo_latency_p99 || 0,
      gpspoints_persisted_total: metrics.gpspoints_persisted_total || 0,
      gpspoints_retry_total: metrics.gpspoints_retry_total || 0,
      gpspoints_writer_retries: metrics.gpspoints_writer_retries || 0,
      gpspoints_writer_quarantined: metrics.gpspoints_writer_quarantined || 0,
      gpspoints_writer_alive: metrics.gpspoints_writer_alive || false,
      gpspoints_writer_last_heartbeat: metrics.gpspoints_writer_last_heartbeat || null,
      gpspoints_writer_last_persisted_at: metrics.gpspoints_writer_last_persisted_at || null,
      gpspoints_persist_failures: metrics.gpspoints_persist_failures || 0,
      persistence_dropped: metrics.persistence_dropped || 0,
      gpspoints_health: persistenceHealth(),
      last_disk_error: lastDiskError,
    };
  }

  recoverPendingSpool();

  return {
    enqueue,
    flushCycle,
    flushJournal,
    journalNow,
    sealActiveSegment,
    recoverPendingSpool,
    flushAndStop,
    simulateCrash,
    getMemoryDepth: () => unjournaled.length,
    getSpoolDepth: () => {
      applyMetaToMetrics();
      return spoolFiles;
    },
    getReaddirCount: () => readdirCount,
    getStats,
    getSpoolDir: () => resolvedSpoolDir,
    getMode: () => resolvedMode,
    getActivePath: () => activePath,
    _memoryQueue: unjournaled,
    isDuplicateKeyError,
  };
}

function readGpspointsWriterHeartbeat(spoolDir, fsImpl = fs) {
  const heartbeatPath = path.join(path.resolve(spoolDir), "gpspoints-writer.heartbeat.json");
  try {
    const raw = fsImpl.readFileSync(heartbeatPath, "utf8");
    const data = JSON.parse(raw);
    const ts = Date.parse(data.ts || "");
    const ageMs = Number.isFinite(ts) ? Math.max(0, Date.now() - ts) : null;
    return {
      gpspoints_writer_alive: ageMs != null && ageMs < 15_000,
      gpspoints_writer_last_heartbeat: data.ts || null,
      gpspoints_writer_last_persisted_at: data.last_persisted_at || null,
      gpspoints_writer_heartbeat_age_ms: ageMs,
      gpspoints_writer_pid: data.pid || null,
    };
  } catch {
    return {
      gpspoints_writer_alive: false,
      gpspoints_writer_last_heartbeat: null,
      gpspoints_writer_last_persisted_at: null,
      gpspoints_writer_heartbeat_age_ms: null,
      gpspoints_writer_pid: null,
    };
  }
}

module.exports = {
  createGpsPointWriter,
  readGpspointsWriterHeartbeat,
  isDuplicateKeyError,
  duplicateCount,
  hasNonDuplicateWriteError,
  classifyInsertManyError,
  isIdempotencyDuplicate,
  isRetryableMongoError,
  isPermanentMongoError,
  isLegacySpoolName,
  isReadySegmentName,
  isActiveSegmentName,
  isDrainingSegmentName,
  jitter,
};
