const { describe, it, beforeEach, afterEach } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const os = require("os");
const path = require("path");
const {
  buildForwardRetryFingerprint,
  createForwardRetryDedupe,
  estimateFingerprintEntryBytes,
} = require("../lib/forwardRetryDedupe");
const { handleTraccarForwardPosition } = require("../lib/handleTraccarForwardPosition");
const { normalizeForwardPayload } = require("../lib/traccarForwardIngress");
const {
  createTraccarRawIngressWriter,
  buildTraccarRawIngressDoc,
} = require("../lib/traccarRawIngressWriter");

function tmpDir(prefix) {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function sampleBody(overrides = {}) {
  return {
    device: { id: 7, uniqueId: "359339080000001", model: "SEEWORLD" },
    position: {
      id: 0,
      protocol: "gt06",
      serverTime: "2026-09-06T01:00:00.000Z",
      deviceTime: "2026-09-06T00:59:59.000Z",
      fixTime: "2026-09-06T00:59:58.000Z",
      latitude: 24.7136,
      longitude: 46.6753,
      speed: 10,
      attributes: { ignition: true, type: 19 },
      ...overrides.position,
    },
    ...overrides,
  };
}

function makeForwardQueue({ failOnce = false } = {}) {
  const items = [];
  let failed = false;
  return {
    items,
    enqueue(payload) {
      if (failOnce && !failed) {
        failed = true;
        return { accepted: false, reason: "forward_queue_full" };
      }
      items.push(payload);
      return { accepted: true, depth: items.length };
    },
  };
}

describe("P0-B hardening — retry cache", () => {
  it("documents memory estimate scale and stays bounded under capacity eviction", () => {
    const sampleFp = buildForwardRetryFingerprint(normalizeForwardPayload(sampleBody()).items[0]);
    const perEntry = estimateFingerprintEntryBytes(sampleFp);
    assert.ok(perEntry >= 200 && perEntry <= 800, `unexpected entry estimate ${perEntry}`);
    // Planning numbers used in docs (500 B/entry):
    assert.ok(50_000 * 500 < 30 * 1024 * 1024);
    assert.ok(100_000 * 500 < 60 * 1024 * 1024);
    assert.ok(250_000 * 500 < 140 * 1024 * 1024);

    const metrics = {};
    const dedupe = createForwardRetryDedupe({
      ttlMs: 60_000,
      maxEntries: 5,
      maxExpirePerCall: 64,
      metrics,
    });
    for (let i = 0; i < 20; i++) {
      const item = normalizeForwardPayload(
        sampleBody({
          position: {
            id: 0,
            protocol: "gt06",
            serverTime: `2026-09-06T03:00:${String(i).padStart(2, "0")}.000Z`,
            deviceTime: `2026-09-06T03:00:${String(i).padStart(2, "0")}.000Z`,
            fixTime: `2026-09-06T03:00:${String(i).padStart(2, "0")}.000Z`,
            latitude: 24.7 + i / 1000,
            longitude: 46.6,
            attributes: { type: 19 },
          },
        })
      ).items[0];
      dedupe.begin(item).commit?.();
    }
    assert.ok(dedupe.getSize() <= 5);
    assert.ok((metrics.forward_retry_cache_evicted_capacity_total || 0) >= 15);
    assert.equal(metrics.forward_retry_cache_size, dedupe.getSize());
  });

  it("TTL expiry increments expired metric and allows reprocess", () => {
    let now = 1000;
    const metrics = {};
    const dedupe = createForwardRetryDedupe({
      ttlMs: 50,
      maxEntries: 100,
      now: () => now,
      metrics,
    });
    const item = normalizeForwardPayload(sampleBody()).items[0];
    dedupe.begin(item).commit();
    assert.equal(dedupe.begin(item).action, "suppress");
    now += 100;
    assert.equal(dedupe.begin(item).action, "process");
    assert.ok((metrics.forward_retry_cache_expired_total || 0) >= 1);
  });

  it("simultaneous identical attempts: only first dispatches (second sees processing)", () => {
    const dedupe = createForwardRetryDedupe({ ttlMs: 60_000, maxEntries: 100, metrics: {} });
    const item = normalizeForwardPayload(sampleBody()).items[0];
    const a = dedupe.begin(item);
    const b = dedupe.begin(item);
    assert.equal(a.action, "process");
    assert.equal(b.action, "suppress");
    assert.equal(b.state, "processing");
    a.commit();
    assert.equal(dedupe.begin(item).action, "suppress");
  });

  it("processed only after commit; abandon allows retry after forward failure", () => {
    const dedupe = createForwardRetryDedupe({ ttlMs: 60_000, maxEntries: 100, metrics: {} });
    const item = normalizeForwardPayload(sampleBody()).items[0];
    const first = dedupe.begin(item);
    assert.equal(first.action, "process");
    first.abandon();
    const retry = dedupe.begin(item);
    assert.equal(retry.action, "process");
    retry.commit();
    assert.equal(dedupe.begin(item).action, "suppress");
  });
});

describe("P0-B hardening — handler handoff", () => {
  let spoolDir;
  beforeEach(() => {
    spoolDir = tmpDir("p0b-hard-");
  });
  afterEach(() => {
    fs.rmSync(spoolDir, { recursive: true, force: true });
  });

  it("downstream enqueue failure does not poison retry cache", async () => {
    const metrics = {};
    const writer = createTraccarRawIngressWriter({
      spoolDir,
      batchSize: 50,
      flushMs: 5,
      metrics,
      insertMany: async () => {},
    });
    await writer.whenReady();
    const retryDedupe = createForwardRetryDedupe({ ttlMs: 60_000, maxEntries: 1000, metrics });
    const forwardQueue = makeForwardQueue({ failOnce: true });
    const body = sampleBody();

    const first = await handleTraccarForwardPosition({
      body,
      metrics,
      rawIngressWriter: writer,
      buildRawDoc: buildTraccarRawIngressDoc,
      normalizeForwardPayload,
      forwardQueue,
      retryDedupe,
    });
    assert.equal(first.status, 503);
    assert.equal(forwardQueue.items.length, 0);

    const second = await handleTraccarForwardPosition({
      body,
      metrics,
      rawIngressWriter: writer,
      buildRawDoc: buildTraccarRawIngressDoc,
      normalizeForwardPayload,
      forwardQueue,
      retryDedupe,
    });
    assert.equal(second.status, 202);
    assert.equal(forwardQueue.items.length, 1);
    await writer.flushAndStop(1000);
  });

  it("raw durable failure still permits first realtime; exact retry skips downstream", async () => {
    const metrics = {};
    const retryDedupe = createForwardRetryDedupe({ ttlMs: 60_000, maxEntries: 1000, metrics });
    const forwardQueue = makeForwardQueue();
    let rawOk = false;
    const flakyRaw = {
      enqueue() {
        if (!rawOk) {
          return {
            accepted: false,
            reason: "raw_ingress_queue_full",
            durable: Promise.resolve({ ok: false, reason: "raw_ingress_queue_full" }),
          };
        }
        return { accepted: true, durable: Promise.resolve({ ok: true }) };
      },
    };
    const body = sampleBody();
    const first = await handleTraccarForwardPosition({
      body,
      metrics,
      rawIngressWriter: flakyRaw,
      buildRawDoc: buildTraccarRawIngressDoc,
      normalizeForwardPayload,
      forwardQueue,
      retryDedupe,
    });
    assert.equal(first.status, 503);
    assert.equal(forwardQueue.items.length, 1);
    assert.equal(metrics.raw_failure_realtime_continued_total, 1);
    assert.ok((metrics.raw_durable_accept_latency_ms ?? 0) >= 0);

    rawOk = true;
    const second = await handleTraccarForwardPosition({
      body,
      metrics,
      rawIngressWriter: flakyRaw,
      buildRawDoc: buildTraccarRawIngressDoc,
      normalizeForwardPayload,
      forwardQueue,
      retryDedupe,
    });
    assert.equal(second.status, 202);
    assert.equal(forwardQueue.items.length, 1);
    assert.equal(metrics.forward_exact_retry_total, 1);
  });

  it("records raw durable accept latency aggregates", async () => {
    const metrics = {};
    const writer = createTraccarRawIngressWriter({
      spoolDir,
      batchSize: 50,
      flushMs: 5,
      metrics,
      insertMany: async () => {},
    });
    await writer.whenReady();
    const retryDedupe = createForwardRetryDedupe({ metrics });
    const forwardQueue = makeForwardQueue();
    await handleTraccarForwardPosition({
      body: sampleBody(),
      metrics,
      rawIngressWriter: writer,
      buildRawDoc: buildTraccarRawIngressDoc,
      normalizeForwardPayload,
      forwardQueue,
      retryDedupe,
    });
    assert.ok(typeof metrics.raw_durable_accept_latency_ms === "number");
    assert.ok(typeof metrics.raw_durable_accept_latency_p95_ms === "number");
    assert.ok(typeof metrics.raw_durable_accept_latency_p99_ms === "number");
    await writer.flushAndStop(1000);
  });
});
