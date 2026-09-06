const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const RETRYABLE_MONGO_CODES = new Set([
  6, 7, 50, 89, 91, 189, 10107, 11600, 11602, 13435, 13436,
]);

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
    maxMongoBatchesPerCycle = 4,
    journalCoalesceMs = 0,
    spoolReconcileMs = 30_000,
    metrics = {},
    fsImpl = fs,
    now = () => Date.now(),
    log = console,
  } = options;

  if (typeof insertMany !== "function") {
    throw new Error("createGpsPointWriter requires insertMany");
  }

  const resolvedSpoolDir = path.resolve(spoolDir);
  const quarantineDir = path.join(resolvedSpoolDir, "quarantine");
  const unjournaled = [];
  let flushing = false;
  let closed = false;
  let retryDelayMs = retryBaseMs;
  let retryTimer = null;
  let flushTimer = null;
  let journalTimer = null;
  let journalTimeout = null;
  let reconcileTimer = null;
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

  metrics.persistence_dropped = metrics.persistence_dropped || 0;
  metrics.gpspoints_journaled_total = metrics.gpspoints_journaled_total || 0;
  metrics.gpspoints_mongo_flush_count = metrics.gpspoints_mongo_flush_count || 0;
  metrics.gpspoints_mongo_docs_per_flush_last = metrics.gpspoints_mongo_docs_per_flush_last || 0;
  metrics.gpspoints_mongo_docs_per_flush_avg = metrics.gpspoints_mongo_docs_per_flush_avg || 0;
  metrics.gpspoints_mongo_docs_per_flush_max = metrics.gpspoints_mongo_docs_per_flush_max || 0;
  metrics.gpspoints_spool_dir = resolvedSpoolDir;
  log.info?.(`[gpspoints] gpspoint_spool_dir=${resolvedSpoolDir}`);

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

  function spoolFileTs(filePath) {
    const name = path.basename(filePath);
    const m = name.match(/^(?:hot|pending)-(\d+)-/);
    return m ? Number(m[1]) : 0;
  }

  function listSpoolFiles() {
    readdirCount += 1;
    try {
      ensureDir(resolvedSpoolDir);
      return fsImpl
        .readdirSync(resolvedSpoolDir)
        .filter((name) => name.endsWith(".jsonl") && !name.endsWith(".tmp"))
        .map((name) => path.join(resolvedSpoolDir, name))
        .sort((a, b) => {
          const d = spoolFileTs(a) - spoolFileTs(b);
          return d !== 0 ? d : a.localeCompare(b);
        });
    } catch (err) {
      recordWriteFailure(err, "readdir");
      return [];
    }
  }

  function applyMetaToMetrics() {
    spoolFiles = spoolMeta.size;
    spoolBytes = 0;
    spoolOldestMtime = null;
    for (const meta of spoolMeta.values()) {
      spoolBytes += Number(meta.size) || 0;
      const mt = Number(meta.mtime) || 0;
      if (!spoolOldestMtime || (mt && mt < spoolOldestMtime)) spoolOldestMtime = mt;
    }
    spoolOldestAgeMs = spoolOldestMtime ? Math.max(0, now() - spoolOldestMtime) : 0;
    metrics.gpspoints_spool_files = spoolFiles;
    metrics.gpspoints_spool_bytes = spoolBytes;
    metrics.gpspoints_spool_oldest_age_ms = spoolOldestAgeMs;
    metrics.gpspoints_spool_depth = spoolFiles;
    metrics.gpspoints_queue_depth = unjournaled.length;
    metrics.gpspoints_retry_pending_total = unjournaled.length + (metrics.gpspoints_spool_docs || 0);
  }

  function trackSpoolFile(filePath, size, mtime) {
    spoolMeta.set(filePath, { size: Number(size) || 0, mtime: Number(mtime) || now() });
    applyMetaToMetrics();
  }

  function untrackSpoolFile(filePath) {
    spoolMeta.delete(filePath);
    applyMetaToMetrics();
  }

  function refreshSpoolStats() {
    const files = listSpoolFiles();
    spoolMeta.clear();
    for (const filePath of files) {
      try {
        const st = fsImpl.statSync(filePath);
        spoolMeta.set(filePath, { size: Number(st.size) || 0, mtime: Number(st.mtimeMs) || now() });
      } catch {
        /* skip */
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
      const names = fsImpl.readdirSync(resolvedSpoolDir);
      for (const name of names) {
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

  function writeSpoolFileSync(docs, prefix = "hot") {
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
    trackSpoolFile(filePath, size, now());
    bump("gpspoints_spooled_total", docs.length);
    bump("gpspoints_spool_docs", docs.length);
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
        docs.map((doc) => JSON.stringify({ reason, doc })).join("\n") + "\n";
      fsImpl.writeFileSync(tmpPath, body, "utf8");
      fsImpl.renameSync(tmpPath, filePath);
      bump("gpspoints_quarantined_total", docs.length);
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
    if (!unjournaled.length) return true;
    const docs = unjournaled.splice(0, unjournaled.length);
    try {
      writeSpoolFileSync(docs, "hot");
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
    if (!doc) return;
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
  }

  async function persistBatchClassified(docs) {
    if (!docs.length) {
      return { acked: [], duplicates: [], unexpectedDuplicates: [], retry: [], invalid: [], retryableFailure: false };
    }
    mongoBatchesThisCycle += 1;
    const attemptedBefore = metrics.gpspoints_mongo_attempted_total || 0;
    bump("gpspoints_mongo_flush_count");
    metrics.gpspoints_mongo_docs_per_flush_last = docs.length;
    metrics.gpspoints_mongo_docs_per_flush_max = Math.max(
      metrics.gpspoints_mongo_docs_per_flush_max || 0,
      docs.length
    );
    metrics.gpspoints_mongo_docs_per_flush_avg = metrics.gpspoints_mongo_flush_count
      ? Number(((attemptedBefore + docs.length) / metrics.gpspoints_mongo_flush_count).toFixed(2))
      : docs.length;
    bump("gpspoints_mongo_attempted_total", docs.length);
    try {
      await insertMany(docs);
      retryDelayMs = retryBaseMs;
      const classified = classifyInsertManyError(docs, null);
      applyClassifiedMetrics(classified);
      return classified;
    } catch (err) {
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
          retryDelayMs = Math.min(retryMaxMs, retryDelayMs * 2);
        }
        return classified;
      }
      bump("gpspoints_retry_total", docs.length);
      bump("gpspoints_persist_failures");
      retryDelayMs = Math.min(retryMaxMs, retryDelayMs * 2);
      throw err;
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

  async function drainSpoolFile(filePath) {
    let docs;
    try {
      docs = parseSpoolFile(filePath);
    } catch {
      quarantineFile(filePath, "unreadable");
      untrackSpoolFile(filePath);
      return;
    }
    if (!docs.length) {
      try {
        fsImpl.unlinkSync(filePath);
      } catch (err) {
        recordWriteFailure(err, "unlink_empty");
      }
      untrackSpoolFile(filePath);
      return;
    }

    const remaining = [];
    let offset = 0;
    while (offset < docs.length && mongoBatchesThisCycle < maxMongoBatchesPerCycle) {
      const batch = docs.slice(offset, offset + batchSize);
      offset += batch.length;
      let classified;
      try {
        classified = await persistBatchClassified(batch);
      } catch (err) {
        remaining.push(...batch, ...docs.slice(offset));
        try {
          writeSpoolFileSync(remaining, "pending");
          fsImpl.unlinkSync(filePath);
          untrackSpoolFile(filePath);
        } catch (rewriteErr) {
          recordWriteFailure(rewriteErr, "rewrite_retry_spool");
        }
        throw err;
      }
      remaining.push(...classified.retry);
    }
    remaining.push(...docs.slice(offset));

    if (remaining.length) {
      try {
        writeSpoolFileSync(remaining, "pending");
      } catch (err) {
        recordWriteFailure(err, "rewrite_remaining_spool");
        return;
      }
    }
    try {
      fsImpl.unlinkSync(filePath);
    } catch (err) {
      if (errCode(err) !== "ENOENT") {
        recordWriteFailure(err, "unlink_acked_spool");
        throw err;
      }
    }
    untrackSpoolFile(filePath);
  }

  function pickFairFiles(files) {
    if (!files.length) return [];
    const newest = files.slice().reverse();
    const oldest = files;
    const selected = [];
    const seen = new Set();
    for (const filePath of newest.slice(0, Math.max(1, drainNewFilesPerCycle))) {
      selected.push(filePath);
      seen.add(filePath);
    }
    for (const filePath of oldest) {
      if (selected.length >= drainNewFilesPerCycle + drainOldFilesPerCycle) break;
      if (seen.has(filePath)) continue;
      selected.push(filePath);
      seen.add(filePath);
    }
    return selected;
  }

  async function drainSpoolFair() {
    const files = spoolMeta.size
      ? Array.from(spoolMeta.keys()).sort((a, b) => {
          const d = spoolFileTs(a) - spoolFileTs(b);
          return d !== 0 ? d : a.localeCompare(b);
        })
      : listSpoolFiles();
    const selected = pickFairFiles(files);
    for (const filePath of selected) {
      if (mongoBatchesThisCycle >= maxMongoBatchesPerCycle) break;
      await drainSpoolFile(filePath);
    }
  }

  async function flushUnjournaledToMongo() {
    while (unjournaled.length && mongoBatchesThisCycle < maxMongoBatchesPerCycle) {
      const batch = unjournaled.splice(0, batchSize);
      try {
        const classified = await persistBatchClassified(batch);
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

  async function flushCycle(opts = {}) {
    if (flushing) return;
    if (closed && !opts.force) return;
    flushing = true;
    mongoBatchesThisCycle = 0;
    try {
      const journaled = journalNow();
      await drainSpoolFair();
      if (!journaled && unjournaled.length) {
        await flushUnjournaledToMongo();
      }
    } catch (err) {
      log.warn?.("[gpspoints] flush retry scheduled", err.message);
      scheduleRetry();
    } finally {
      flushing = false;
      applyMetaToMetrics();
      metrics.gpspoints_health = persistenceHealth();
      if (!closed && (unjournaled.length || spoolFiles)) {
        if (!retryTimer) scheduleFlush(flushMs);
      }
    }
  }

  function recoverPendingSpool() {
    cleanupTmpFiles();
    refreshSpoolStats();
    scheduleReconcile();
    if (spoolFiles > 0) scheduleFlush(0);
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
    if (reconcileTimer) {
      clearTimeout(reconcileTimer);
      reconcileTimer = null;
    }
    const started = now();
    journalNow();
    try {
      await Promise.race([
        flushCycle({ force: true }).catch(() => {}),
        new Promise((resolve) => setTimeout(resolve, timeoutMs)),
      ]);
    } finally {
      if (unjournaled.length) {
        const leftover = unjournaled.splice(0, unjournaled.length);
        try {
          writeSpoolFileSync(leftover, "pending");
        } catch (err) {
          unjournaled.unshift(...leftover);
          recordWriteFailure(err, "shutdown_journal");
        }
      }
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
    unjournaled.length = 0;
    flushing = false;
  }

  function getStats() {
    applyMetaToMetrics();
    return {
      gpspoint_spool_dir: resolvedSpoolDir,
      gpspoints_queue_depth: unjournaled.length,
      gpspoints_spool_files: spoolFiles,
      gpspoints_spool_bytes: spoolBytes,
      gpspoints_spool_oldest_age_ms: spoolOldestAgeMs,
      gpspoints_spool_write_failures: metrics.gpspoints_spool_write_failures || 0,
      disk_free_bytes: diskFreeBytes,
      gpspoints_journaled_total: metrics.gpspoints_journaled_total || 0,
      gpspoints_mongo_flush_count: metrics.gpspoints_mongo_flush_count || 0,
      gpspoints_mongo_docs_per_flush_last: metrics.gpspoints_mongo_docs_per_flush_last || 0,
      gpspoints_mongo_docs_per_flush_avg: metrics.gpspoints_mongo_docs_per_flush_avg || 0,
      gpspoints_mongo_docs_per_flush_max: metrics.gpspoints_mongo_docs_per_flush_max || 0,
      gpspoints_persisted_total: metrics.gpspoints_persisted_total || 0,
      gpspoints_retry_total: metrics.gpspoints_retry_total || 0,
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
    _memoryQueue: unjournaled,
    isDuplicateKeyError,
  };
}

module.exports = {
  createGpsPointWriter,
  isDuplicateKeyError,
  duplicateCount,
  hasNonDuplicateWriteError,
  classifyInsertManyError,
  isIdempotencyDuplicate,
  isRetryableMongoError,
  isPermanentMongoError,
  jitter,
};
