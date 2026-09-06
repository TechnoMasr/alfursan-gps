const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

/**
 * Per-IMEI FIFO + global concurrency. Never coalesces. Never drops silently.
 * Saturation spills to a durable JSONL spool.
 *
 * Durability note vs gpspoints WAL:
 * - Live RAM items that fail processFn are currently counted as analytics_failures
 *   and are not automatically re-queued (must not stall realtime).
 * - Spool files are deleted only after processFn succeeds for every row (ACK-before-unlink).
 */
function createAnalyticsQueue(options = {}) {
  const {
    processFn,
    concurrency = 8,
    memHigh = 20_000,
    spoolDir = "data/analytics-spool",
    metrics = {},
    fsImpl = fs,
    log = console,
    now = () => Date.now(),
  } = options;

  if (typeof processFn !== "function") {
    throw new Error("createAnalyticsQueue requires processFn");
  }

  const queues = new Map();
  const ready = [];
  let globalInflight = 0;
  let closed = false;
  let depth = 0;
  let drainTimer = null;

  function bumpDepth(delta) {
    depth = Math.max(0, depth + delta);
    metrics.analytics_queue_depth = depth;
  }

  function ensureSpoolDir() {
    fsImpl.mkdirSync(spoolDir, { recursive: true });
  }

  function listSpoolFiles() {
    try {
      ensureSpoolDir();
      return fsImpl
        .readdirSync(spoolDir)
        .filter((name) => name.endsWith(".jsonl"))
        .sort()
        .map((name) => path.join(spoolDir, name));
    } catch {
      return [];
    }
  }

  function spillAllToDisk(reason = "mem_high") {
    const items = [];
    for (const [imei, q] of queues.entries()) {
      while (q.items.length) {
        items.push({ imei, ctx: q.items.shift() });
      }
    }
    if (!items.length) return;
    ensureSpoolDir();
    const filePath = path.join(
      spoolDir,
      `pending-${now()}-${crypto.randomBytes(6).toString("hex")}.jsonl`
    );
    const tmp = `${filePath}.tmp`;
    fsImpl.writeFileSync(tmp, items.map((row) => JSON.stringify(row)).join("\n") + "\n");
    fsImpl.renameSync(tmp, filePath);
    bumpDepth(-items.length);
    metrics.analytics_spooled_total = (metrics.analytics_spooled_total || 0) + items.length;
    log.warn?.("[analytics] spilled to durable spool", { reason, count: items.length });
  }

  async function recoverSpool() {
    const files = listSpoolFiles();
    for (const filePath of files) {
      try {
        const raw = fsImpl.readFileSync(filePath, "utf8");
        const rows = raw
          .split("\n")
          .map((l) => l.trim())
          .filter(Boolean)
          .map((l) => JSON.parse(l));
        for (const row of rows) {
          await Promise.resolve(processFn(row.ctx));
          metrics.analytics_completed = (metrics.analytics_completed || 0) + 1;
        }
        fsImpl.unlinkSync(filePath);
      } catch (err) {
        metrics.analytics_failures = (metrics.analytics_failures || 0) + 1;
        log.warn?.("[analytics] spool recover failed; leaving file", err.message);
      }
    }
  }

  recoverSpool();

  function kick(imei) {
    if (closed) return;
    const q = queues.get(imei);
    if (!q || q.running || !q.items.length) return;
    if (globalInflight >= concurrency) {
      if (!ready.includes(imei)) ready.push(imei);
      return;
    }
    const ctx = q.items.shift();
    bumpDepth(-1);
    q.running = true;
    globalInflight += 1;

    Promise.resolve()
      .then(() => processFn(ctx))
      .then(() => {
        metrics.analytics_completed = (metrics.analytics_completed || 0) + 1;
      })
      .catch((err) => {
        metrics.analytics_failures = (metrics.analytics_failures || 0) + 1;
        log.error?.("[analytics] item failed", err.message);
      })
      .finally(() => {
        q.running = false;
        globalInflight -= 1;
        if (!q.items.length && !q.running) queues.delete(imei);
        const next = ready.shift();
        if (q.items.length) {
          if (!ready.includes(imei)) ready.push(imei);
        }
        if (next) kick(next);
        else if (q.items.length) kick(imei);
        else {
          for (const otherImei of queues.keys()) {
            kick(otherImei);
            break;
          }
        }
      });
  }

  function enqueue(imei, ctx) {
    const key = imei != null ? String(imei).trim() : "_unknown";
    let q = queues.get(key);
    if (!q) {
      q = { items: [], running: false };
      queues.set(key, q);
    }
    q.items.push(ctx);
    bumpDepth(1);
    if (depth >= memHigh) {
      setImmediate(() => spillAllToDisk("mem_high"));
    }
    kick(key);
  }

  function getDepth() {
    return depth;
  }

  async function flushAndStop(timeoutMs = 8000) {
    closed = true;
    const started = Date.now();
    while (depth > 0 && Date.now() - started < timeoutMs) {
      await new Promise((r) => setTimeout(r, 25));
    }
    if (depth > 0) spillAllToDisk("shutdown");
    return { remaining: depth };
  }

  return {
    enqueue,
    getDepth,
    flushAndStop,
    recoverSpool,
    _queues: queues,
  };
}

module.exports = { createAnalyticsQueue };
