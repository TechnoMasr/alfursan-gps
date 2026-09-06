/**
 * P0-B Traccar HTTP forward handler orchestration.
 * Realtime/persistence dispatch must not be blocked by raw Mongo.
 * HTTP success waits only for local raw journal durable acceptance (not Mongo).
 */

const LATENCY_RING = 128;

function recordRawDurableLatency(metrics, ms) {
  metrics.raw_durable_accept_latency_ms = ms;
  if (!metrics._raw_durable_latency_ring) {
    metrics._raw_durable_latency_ring = new Float64Array(LATENCY_RING);
    metrics._raw_durable_latency_idx = 0;
    metrics._raw_durable_latency_filled = 0;
  }
  const ring = metrics._raw_durable_latency_ring;
  const idx = metrics._raw_durable_latency_idx % LATENCY_RING;
  ring[idx] = ms;
  metrics._raw_durable_latency_idx = idx + 1;
  metrics._raw_durable_latency_filled = Math.min(
    LATENCY_RING,
    (metrics._raw_durable_latency_filled || 0) + 1
  );

  // Cheap percentile refresh: sort a copy of ≤128 samples (µs-scale).
  const n = metrics._raw_durable_latency_filled;
  const copy = Array.from(ring.subarray(0, n)).sort((a, b) => a - b);
  metrics.raw_durable_accept_latency_p50_ms = copy[Math.floor((n - 1) * 0.5)] ?? ms;
  metrics.raw_durable_accept_latency_p95_ms = copy[Math.floor((n - 1) * 0.95)] ?? ms;
  metrics.raw_durable_accept_latency_p99_ms = copy[Math.floor((n - 1) * 0.99)] ?? ms;
}

async function handleTraccarForwardPosition(ctx) {
  const {
    body,
    receivedAt = Date.now(),
    metrics,
    rawIngressWriter,
    buildRawDoc,
    normalizeForwardPayload,
    forwardQueue,
    retryDedupe,
    bumpForwardPositionsReceived,
    imeiDebugger = null,
  } = ctx;

  metrics.raw_ingress_received_total = (metrics.raw_ingress_received_total || 0) + 1;
  metrics.raw_ingress_last_received_at = new Date(receivedAt).toISOString();

  // Initiate raw local archival (async journal). Do NOT gate realtime on this.
  const rawQueued = rawIngressWriter.enqueue(buildRawDoc(body, receivedAt));

  const normalized = normalizeForwardPayload(body);
  if (!normalized.ok) {
    metrics.forward_invalid_total =
      (metrics.forward_invalid_total || 0) + Math.max(1, normalized.invalid.length);
    const durable = await settleRawDurable(rawQueued, metrics);
    if (!durable.ok) {
      return {
        status: 503,
        body: { ok: false, error: durable.reason || rawQueued.reason || "raw_ingress_durable_failed" },
      };
    }
    return {
      status: 202,
      body: {
        ok: true,
        ingress: "http-forward",
        raw_accepted: true,
        accepted: 0,
        error: "invalid_forward_position",
        invalid: normalized.invalid.slice(0, 5),
      },
    };
  }

  const processItems = [];
  const tickets = [];
  let suppressed = 0;

  for (const item of normalized.items) {
    const decision = retryDedupe.begin(item);
    if (imeiDebugger?.matches?.(item.imei)) {
      imeiDebugger.logPosition?.({
        imei: item.imei,
        protocol: item.position?.protocol,
        fixTime: item.position?.fixTime,
        deviceTime: item.position?.deviceTime,
        serverTime: item.position?.serverTime,
        latitude: item.position?.latitude,
        longitude: item.position?.longitude,
        "attributes.type": item.position?.attributes?.type,
        live_decision: decision.action === "suppress" ? "exact_retry_suppress" : "exact_retry_process",
        forward_retry_fingerprint: decision.fingerprint,
        forward_retry_state: decision.state || null,
        position_id: item.position?.id ?? null,
      });
    }
    if (decision.action === "suppress") {
      suppressed += 1;
      continue;
    }
    processItems.push(item);
    tickets.push(decision);
  }

  if (suppressed > 0) {
    metrics.forward_exact_retry_total = (metrics.forward_exact_retry_total || 0) + suppressed;
    metrics.forward_retry_suppressed_live_total =
      (metrics.forward_retry_suppressed_live_total || 0) + suppressed;
    metrics.forward_retry_suppressed_persistence_total =
      (metrics.forward_retry_suppressed_persistence_total || 0) + suppressed;
  }

  let forwardAccepted = true;
  let forwardDepth = 0;

  if (processItems.length > 0) {
    metrics.forward_last_received_at = new Date(receivedAt).toISOString();
    if (typeof bumpForwardPositionsReceived === "function") {
      bumpForwardPositionsReceived(processItems.length);
    } else {
      metrics.forward_positions_received_total =
        (metrics.forward_positions_received_total || 0) + processItems.length;
      metrics.positions_received = (metrics.positions_received || 0) + processItems.length;
    }
    metrics.forward_invalid_total =
      (metrics.forward_invalid_total || 0) + normalized.invalid.length;
    const commandCount = processItems.filter((item) => item.hasCommandResponse).length;
    metrics.forward_command_responses_total =
      (metrics.forward_command_responses_total || 0) + commandCount;

    const queued = forwardQueue.enqueue({
      normalized: {
        ok: true,
        items: processItems,
        invalid: normalized.invalid,
      },
      receivedAt,
    });
    if (!queued.accepted) {
      // Critical: abandon processing → retry must be allowed to dispatch again.
      forwardAccepted = false;
      for (const ticket of tickets) ticket.abandon?.();
      metrics.forward_queue_rejected_total = (metrics.forward_queue_rejected_total || 0) + 1;
    } else {
      forwardDepth = queued.depth;
      for (const ticket of tickets) ticket.commit?.();
    }
  }

  if (!rawQueued.accepted && processItems.length > 0 && forwardAccepted) {
    metrics.raw_failure_realtime_continued_total =
      (metrics.raw_failure_realtime_continued_total || 0) + 1;
  }

  const durable = await settleRawDurable(rawQueued, metrics);

  if (!forwardAccepted) {
    return {
      status: 503,
      body: { ok: false, error: "forward_queue_full" },
    };
  }

  if (!durable.ok) {
    return {
      status: 503,
      body: {
        ok: false,
        error: durable.reason || rawQueued.reason || "raw_ingress_durable_failed",
        realtime_continued: processItems.length > 0,
        retry_suppressed: suppressed,
      },
    };
  }

  return {
    status: 202,
    body: {
      ok: true,
      ingress: "http-forward",
      accepted: processItems.length,
      invalid: normalized.invalid.length,
      queue_depth: forwardDepth,
      retry_suppressed: suppressed,
      raw_durable: true,
    },
  };
}

async function settleRawDurable(rawQueued, metrics) {
  const started = Date.now();
  if (!rawQueued?.accepted) {
    metrics.raw_durable_accept_failure_total =
      (metrics.raw_durable_accept_failure_total || 0) + 1;
    recordRawDurableLatency(metrics, Date.now() - started);
    return { ok: false, reason: rawQueued?.reason || "raw_ingress_queue_full" };
  }
  try {
    const durable = await (rawQueued.durable || Promise.resolve({ ok: true }));
    recordRawDurableLatency(metrics, Date.now() - started);
    if (durable && durable.ok) {
      metrics.raw_durable_accept_success_total =
        (metrics.raw_durable_accept_success_total || 0) + 1;
      return { ok: true };
    }
    metrics.raw_durable_accept_failure_total =
      (metrics.raw_durable_accept_failure_total || 0) + 1;
    return { ok: false, reason: durable?.reason || "raw_ingress_durable_failed" };
  } catch (err) {
    recordRawDurableLatency(metrics, Date.now() - started);
    metrics.raw_durable_accept_failure_total =
      (metrics.raw_durable_accept_failure_total || 0) + 1;
    return { ok: false, reason: err?.message || "raw_ingress_durable_failed" };
  }
}

module.exports = {
  handleTraccarForwardPosition,
  recordRawDurableLatency,
};
