/**
 * Exact Traccar HTTP-forward retry suppression (NOT general GPS dedupe).
 * Fingerprint uses stable Position fields that remain identical on Traccar retry.
 * Never uses position.id (often 0 with database.memory=true).
 *
 * Memory (order-of-magnitude, V8 one-byte ASCII strings):
 *   fingerprint ≈ 120–180 chars ≈ 120–180 B
 *   Map entry + {state,at} overhead ≈ 200–350 B
 *   ⇒ ≈ 350–500 B / entry (use 500 B for planning)
 *   50k  ≈ 25 MB
 *   100k ≈ 50 MB
 *   250k ≈ 125 MB
 *
 * Cleanup: expired eviction walks Map insertion order and stops at first
 * non-expired entry (O(expired_prefix)), capped per call to avoid event-loop
 * spikes. Capacity eviction prefers non-processing entries.
 */

function normalizeCoord(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "";
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

/** Estimate bytes for planning (not exact heap). */
function estimateFingerprintEntryBytes(fingerprint) {
  const keyBytes = Buffer.byteLength(String(fingerprint || ""), "utf8");
  // Map slot + object {state,at} + string header overhead (conservative).
  return keyBytes + 320;
}

/**
 * Bounded TTL map (insertion order ≈ LRU for expired prefix).
 *
 * Recommended defaults (31 GB server):
 *   CURRENT / near-term: TTL 120s, MAX 100_000 (~50 MB) — covers ~800 pkt/s×120s
 *   Future 1–2k pkt/s:   MAX 250_000 (~125 MB) OR TTL 60s with MAX 150_000
 * Do not set MAX below sustained_rate × TTL_seconds without accepting capacity eviction.
 */
function createForwardRetryDedupe(options = {}) {
  const {
    ttlMs = 120_000,
    maxEntries = 100_000,
    maxExpirePerCall = 64,
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

  /**
   * Expire from oldest while entries are past TTL.
   * Skip/stop on in-flight `processing` so abandon/commit remain authoritative.
   * Cap deletions per call to avoid multi-ms spikes after idle gaps.
   */
  function evictExpired(ts) {
    let removed = 0;
    for (const [key, entry] of entries) {
      if (removed >= maxExpirePerCall) break;
      if (entry.state === "processing") break;
      if (!isExpired(entry, ts)) break;
      entries.delete(key);
      removed += 1;
    }
    if (removed) bump("forward_retry_cache_expired_total", removed);
  }

  function evictOverflow() {
    while (entries.size > maxEntries) {
      let victimKey = null;
      for (const [key, entry] of entries) {
        if (entry.state !== "processing") {
          victimKey = key;
          break;
        }
      }
      if (!victimKey) {
        // All in-flight — still bound memory (pathological).
        victimKey = entries.keys().next().value;
      }
      if (victimKey == null) break;
      entries.delete(victimKey);
      bump("forward_retry_cache_evicted_capacity_total");
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
      // Touch for LRU-ish order; keep state (processing|processed).
      entries.delete(fingerprint);
      entries.set(fingerprint, { state: existing.state, at: ts });
      updateSizeMetric();
      return { action: "suppress", fingerprint, state: existing.state };
    }

    entries.set(fingerprint, { state: "processing", at: ts });
    evictOverflow();
    updateSizeMetric();

    return {
      action: "process",
      fingerprint,
      state: "processing",
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

  function getMemoryEstimateBytes() {
    let bytes = 0;
    for (const key of entries.keys()) bytes += estimateFingerprintEntryBytes(key);
    return bytes;
  }

  return {
    begin,
    buildFingerprint: buildForwardRetryFingerprint,
    getSize,
    getMemoryEstimateBytes,
    estimateFingerprintEntryBytes,
    // test helpers
    _entries: entries,
    _ttlMs: ttlMs,
    _maxEntries: maxEntries,
  };
}

module.exports = {
  buildForwardRetryFingerprint,
  createForwardRetryDedupe,
  estimateFingerprintEntryBytes,
};
