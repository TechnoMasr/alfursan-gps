/**
 * Exact Traccar HTTP-forward retry suppression (NOT general GPS dedupe).
 * Fingerprint uses stable Position fields that remain identical on Traccar retry.
 * Never uses position.id (often 0 with database.memory=true).
 */

function normalizeCoord(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "";
  // Fixed precision avoids float noise while keeping distinct nearby samples.
  return n.toFixed(6);
}

function buildForwardRetryFingerprint(item) {
  const position = item?.position && typeof item.position === "object" ? item.position : {};
  const attrs = position.attributes && typeof position.attributes === "object" ? position.attributes : {};
  const result = String(attrs.result || "").trim().slice(0, 64);
  return [
    String(item?.imei || ""),
    String(position.protocol || ""),
    String(position.fixTime || ""),
    String(position.deviceTime || ""),
    String(position.serverTime || ""),
    normalizeCoord(position.latitude),
    normalizeCoord(position.longitude),
    attrs.type == null ? "" : String(attrs.type),
    attrs.alarm == null ? "" : String(attrs.alarm),
    result,
  ].join("|");
}

/**
 * Bounded TTL map: Map insertion order used as rough LRU (delete+re-set on touch).
 * Memory estimate: ~50k entries × ~200B key+meta ≈ ~10MB — acceptable.
 */
function createForwardRetryDedupe(options = {}) {
  const {
    ttlMs = 120_000,
    maxEntries = 50_000,
    now = () => Date.now(),
    metrics = {},
  } = options;

  const entries = new Map();

  function bump(name, n = 1) {
    metrics[name] = (metrics[name] || 0) + n;
  }

  function updateSizeMetric() {
    metrics.forward_retry_cache_size = entries.size;
  }

  function isExpired(entry, ts) {
    return !entry || ts - entry.at > ttlMs;
  }

  function evictExpired(ts) {
    for (const [key, entry] of entries) {
      if (!isExpired(entry, ts)) break;
      entries.delete(key);
    }
  }

  function evictOverflow() {
    while (entries.size > maxEntries) {
      const oldest = entries.keys().next().value;
      if (oldest == null) break;
      entries.delete(oldest);
    }
  }

  /**
   * @returns {{ action: 'process'|'suppress', fingerprint: string, commit?: Function, abandon?: Function }}
   */
  function begin(item) {
    const fingerprint = buildForwardRetryFingerprint(item);
    const ts = now();
    evictExpired(ts);

    const existing = entries.get(fingerprint);
    if (existing && !isExpired(existing, ts)) {
      // Refresh TTL order
      entries.delete(fingerprint);
      entries.set(fingerprint, { ...existing, at: ts });
      updateSizeMetric();
      return { action: "suppress", fingerprint };
    }

    entries.set(fingerprint, { state: "processing", at: ts });
    evictOverflow();
    updateSizeMetric();

    return {
      action: "process",
      fingerprint,
      commit() {
        const cur = entries.get(fingerprint);
        if (!cur || cur.state !== "processing") return;
        entries.delete(fingerprint);
        entries.set(fingerprint, { state: "processed", at: now() });
        evictOverflow();
        updateSizeMetric();
      },
      abandon() {
        const cur = entries.get(fingerprint);
        if (!cur || cur.state !== "processing") return;
        entries.delete(fingerprint);
        updateSizeMetric();
      },
    };
  }

  function getSize() {
    evictExpired(now());
    updateSizeMetric();
    return entries.size;
  }

  return {
    begin,
    buildFingerprint: buildForwardRetryFingerprint,
    getSize,
    // test helpers
    _entries: entries,
  };
}

module.exports = {
  buildForwardRetryFingerprint,
  createForwardRetryDedupe,
};
