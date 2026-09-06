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

function cloneJson(value) {
  if (value == null || typeof value !== "object") return value;
  return JSON.parse(JSON.stringify(value));
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
    raw_payload: cloneJson(rawPayload),
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
  let pendingDocs = 0;
  let pendingFiles = 0;
  let flushing = false;
  let closed = false;
  let flushTimer = null;
  let retryTimer = null;
  let retryDelayMs = retryBaseMs;

  metrics.raw_ingress_queue_depth = metrics.raw_ingress_queue_depth || 0;
  metrics.raw_ingress_spool_depth = metrics.raw_ingress_spool_depth || 0;
  metrics.raw_ingress_spool_dir = resolvedSpoolDir;

  function bump(name, n = 1) {
    metrics[name] = (metrics[name] || 0) + n;
  }

  function ensureDir() {
    fsImpl.mkdirSync(resolvedSpoolDir, { recursive: true });
  }

  function updateDepths() {
    metrics.raw_ingress_queue_depth = activeDocs;
    metrics.raw_ingress_spool_depth = activeDocs + pendingDocs;
    metrics.raw_ingress_spool_files = pendingFiles + (activeDocs ? 1 : 0);
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

  function listPendingFiles() {
    ensureDir();
    return fsImpl
      .readdirSync(resolvedSpoolDir)
      .filter((name) => name.endsWith(".jsonl") && !name.endsWith(".tmp"))
      .map((name) => path.join(resolvedSpoolDir, name))
      .sort((a, b) => {
        const d = spoolFileTs(a) - spoolFileTs(b);
        return d !== 0 ? d : a.localeCompare(b);
      });
  }

  function countDocsInFile(filePath) {
    try {
      return fsImpl
        .readFileSync(filePath, "utf8")
        .split("\n")
        .filter((line) => line.trim()).length;
    } catch {
      return 0;
    }
  }

  function recoverActiveFiles() {
    ensureDir();
    for (const name of fsImpl.readdirSync(resolvedSpoolDir)) {
      if (!name.endsWith(".jsonl.active")) continue;
      const source = path.join(resolvedSpoolDir, name);
      const dest = stablePathFor(source);
      try {
        fsImpl.renameSync(source, dest);
      } catch (err) {
        bump("raw_ingress_persist_failures");
        log.error?.("[raw-ingress] failed to recover active spool", err.message);
      }
    }
  }

  function refreshSpoolStats() {
    recoverActiveFiles();
    const files = listPendingFiles();
    pendingFiles = files.length;
    pendingDocs = files.reduce((sum, filePath) => sum + countDocsInFile(filePath), 0);
    updateDepths();
  }

  function rotateActive() {
    if (!activePath || activeDocs <= 0) return null;
    const stablePath = stablePathFor(activePath);
    fsImpl.renameSync(activePath, stablePath);
    pendingDocs += activeDocs;
    pendingFiles += 1;
    const rotated = stablePath;
    activePath = null;
    activeDocs = 0;
    updateDepths();
    return rotated;
  }

  function appendToActive(doc) {
    ensureDir();
    if (!activePath) activePath = newActivePath();
    fsImpl.appendFileSync(activePath, `${JSON.stringify(doc)}\n`, "utf8");
    activeDocs += 1;
    bump("raw_ingress_spooled_total");
    updateDepths();
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

  function parseSpoolFile(filePath) {
    const raw = fsImpl.readFileSync(filePath, "utf8");
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

  async function drainFile(filePath) {
    const docs = parseSpoolFile(filePath);
    for (let offset = 0; offset < docs.length; offset += batchSize) {
      await persistBatch(docs.slice(offset, offset + batchSize));
    }
    fsImpl.unlinkSync(filePath);
    pendingDocs = Math.max(0, pendingDocs - docs.length);
    pendingFiles = Math.max(0, pendingFiles - 1);
    updateDepths();
  }

  async function flushCycle(opts = {}) {
    if (flushing) return;
    if (closed && !opts.force) return;
    flushing = true;
    try {
      rotateActive();
      const files = listPendingFiles();
      let drained = 0;
      for (const filePath of files) {
        if (drained >= maxMongoBatchesPerCycle) break;
        await drainFile(filePath);
        drained += 1;
      }
      if (!closed && (activeDocs || pendingDocs)) scheduleFlush(flushMs);
    } catch (err) {
      bump("raw_ingress_persist_failures");
      retryDelayMs = Math.min(retryMaxMs, retryDelayMs * 2);
      log.warn?.("[raw-ingress] flush retry scheduled", err.message);
      scheduleRetry();
    } finally {
      flushing = false;
      updateDepths();
    }
  }

  function enqueue(doc) {
    if (closed) {
      bump("raw_ingress_queue_rejected_total");
      return { accepted: false, reason: "raw_ingress_closed" };
    }
    if (activeDocs >= maxQueueDepth) {
      bump("raw_ingress_queue_rejected_total");
      return { accepted: false, reason: "raw_ingress_queue_full" };
    }
    try {
      appendToActive(doc);
    } catch (err) {
      bump("raw_ingress_queue_rejected_total");
      bump("raw_ingress_persist_failures");
      log.error?.("[raw-ingress] durable accept failed", err.message);
      return { accepted: false, reason: "raw_ingress_queue_full" };
    }
    bump("raw_ingress_accepted_total");
    if (activeDocs >= batchSize) scheduleFlush(0);
    else scheduleFlush(flushMs);
    return { accepted: true, depth: activeDocs };
  }

  async function flushAndStop(timeoutMs = 8000) {
    closed = true;
    if (flushTimer) clearTimeout(flushTimer);
    if (retryTimer) clearTimeout(retryTimer);
    const started = now();
    await Promise.race([
      flushCycle({ force: true }).catch(() => {}),
      new Promise((resolve) => setTimeout(resolve, timeoutMs)),
    ]);
    return {
      elapsed_ms: now() - started,
      queue_depth: activeDocs,
      spool_depth: activeDocs + pendingDocs,
    };
  }

  refreshSpoolStats();
  if (pendingDocs) scheduleFlush(0);

  return {
    enqueue,
    flushCycle,
    flushAndStop,
    getStats: () => {
      updateDepths();
      return {
        raw_ingress_queue_depth: activeDocs,
        raw_ingress_spool_depth: activeDocs + pendingDocs,
        raw_ingress_spool_files: pendingFiles + (activeDocs ? 1 : 0),
        raw_ingress_spool_dir: resolvedSpoolDir,
      };
    },
  };
}

module.exports = {
  createTraccarRawIngressWriter,
  buildTraccarRawIngressDoc,
};
