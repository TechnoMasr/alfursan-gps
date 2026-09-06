const { describe, it, beforeEach, afterEach } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const os = require("os");
const path = require("path");
const {
  buildForwardRetryFingerprint,
  createForwardRetryDedupe,
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

function makeForwardQueue() {
  const items = [];
  return {
    items,
    enqueue(payload) {
      items.push(payload);
      return { accepted: true, depth: items.length };
    },
  };
}

describe("P0-B forward retry dedupe", () => {
  it("builds fingerprints without position.id and distinguishes stationary samples by time", () => {
    const a = normalizeForwardPayload(sampleBody()).items[0];
    const b = normalizeForwardPayload(
      sampleBody({
        position: {
          id: 0,
          protocol: "gt06",
          serverTime: "2026-09-06T01:00:10.000Z",
          deviceTime: "2026-09-06T01:00:09.000Z",
          fixTime: "2026-09-06T01:00:08.000Z",
          latitude: 24.7136,
          longitude: 46.6753,
          attributes: { ignition: true, type: 19 },
        },
      })
    ).items[0];
    assert.notEqual(buildForwardRetryFingerprint(a), buildForwardRetryFingerprint(b));
    assert.equal(buildForwardRetryFingerprint(a).includes("|0|"), false);
  });

  it("suppresses exact retries and bounds cache + TTL", () => {
    let now = 1_000;
    const metrics = {};
    const dedupe = createForwardRetryDedupe({
      ttlMs: 50,
      maxEntries: 3,
      now: () => now,
      metrics,
    });
    const item = normalizeForwardPayload(sampleBody()).items[0];
    const first = dedupe.begin(item);
    assert.equal(first.action, "process");
    first.commit();
    assert.equal(dedupe.begin(item).action, "suppress");

    // Bounded eviction
    for (let i = 0; i < 5; i++) {
      const other = normalizeForwardPayload(
        sampleBody({
          position: {
            id: 0,
            protocol: "gt06",
            serverTime: `2026-09-06T02:00:0${i}.000Z`,
            deviceTime: `2026-09-06T02:00:0${i}.000Z`,
            fixTime: `2026-09-06T02:00:0${i}.000Z`,
            latitude: 24.7 + i / 1000,
            longitude: 46.6,
            attributes: { type: 19 },
          },
        })
      ).items[0];
      dedupe.begin(other).commit?.();
    }
    assert.ok(dedupe.getSize() <= 3);

    // TTL expiry
    now += 100;
    assert.equal(dedupe.begin(item).action, "process");
  });

  it("does not use Mongo for retry detection", () => {
    const src = fs.readFileSync(path.join(__dirname, "../lib/forwardRetryDedupe.js"), "utf8");
    assert.equal(/mongoose|findOne|insertMany|Mongo/.test(src), false);
  });
});

describe("P0-B handleTraccarForwardPosition", () => {
  let spoolDir;
  beforeEach(() => {
    spoolDir = tmpDir("p0b-raw-");
  });
  afterEach(() => {
    fs.rmSync(spoolDir, { recursive: true, force: true });
  });

  async function setup(opts = {}) {
    const metrics = {};
    let mongoCalls = 0;
    const rawIngressWriter =
      opts.rawIngressWriter ||
      createTraccarRawIngressWriter({
        spoolDir,
        batchSize: opts.batchSize || 50,
        flushMs: 5,
        maxQueueDepth: opts.maxQueueDepth || 1000,
        metrics,
        insertMany: async (docs) => {
          mongoCalls += 1;
          if (opts.mongoDelayMs) await sleep(opts.mongoDelayMs);
          if (opts.mongoFail) throw new Error("mongo down");
        },
      });
    if (rawIngressWriter.whenReady) await rawIngressWriter.whenReady();
    const forwardQueue = makeForwardQueue();
    const retryDedupe = createForwardRetryDedupe({
      ttlMs: opts.ttlMs || 60_000,
      maxEntries: opts.maxEntries || 1000,
      metrics,
    });
    return { metrics, rawIngressWriter, forwardQueue, retryDedupe, mongoCalls: () => mongoCalls };
  }

  it("raw healthy: realtime+persistence once and 202", async () => {
    const ctx = await setup({ mongoDelayMs: 30 });
    const body = sampleBody();
    const result = await handleTraccarForwardPosition({
      body,
      metrics: ctx.metrics,
      rawIngressWriter: ctx.rawIngressWriter,
      buildRawDoc: buildTraccarRawIngressDoc,
      normalizeForwardPayload,
      forwardQueue: ctx.forwardQueue,
      retryDedupe: ctx.retryDedupe,
    });
    assert.equal(result.status, 202);
    assert.equal(ctx.forwardQueue.items.length, 1);
    assert.equal(ctx.metrics.raw_durable_accept_success_total, 1);
    // HTTP must not wait on Mongo: durable success can complete before Mongo flush.
    assert.ok(ctx.metrics.raw_durable_accept_success_total >= 1);
    await ctx.rawIngressWriter.flushAndStop(1000);
  });

  it("raw durable accept fails: realtime still runs on first attempt", async () => {
    const ctx = await setup();
    const rawIngressWriter = {
      enqueue() {
        return {
          accepted: false,
          reason: "raw_ingress_queue_full",
          durable: Promise.resolve({ ok: false, reason: "raw_ingress_queue_full" }),
        };
      },
    };
    const result = await handleTraccarForwardPosition({
      body: sampleBody(),
      metrics: ctx.metrics,
      rawIngressWriter,
      buildRawDoc: buildTraccarRawIngressDoc,
      normalizeForwardPayload,
      forwardQueue: ctx.forwardQueue,
      retryDedupe: ctx.retryDedupe,
    });
    assert.equal(result.status, 503);
    assert.equal(ctx.forwardQueue.items.length, 1);
    assert.equal(ctx.metrics.raw_failure_realtime_continued_total, 1);
    assert.equal(result.body.realtime_continued, true);
  });

  it("exact retry suppresses realtime and persistence but still archives raw", async () => {
    const ctx = await setup();
    const body = sampleBody();
    const first = await handleTraccarForwardPosition({
      body,
      metrics: ctx.metrics,
      rawIngressWriter: ctx.rawIngressWriter,
      buildRawDoc: buildTraccarRawIngressDoc,
      normalizeForwardPayload,
      forwardQueue: ctx.forwardQueue,
      retryDedupe: ctx.retryDedupe,
    });
    assert.equal(first.status, 202);
    assert.equal(ctx.forwardQueue.items.length, 1);

    // Simulate raw failure path then Traccar retry of same payload.
    const failingRaw = {
      enqueue() {
        ctx.metrics.raw_attempts = (ctx.metrics.raw_attempts || 0) + 1;
        return {
          accepted: true,
          durable: Promise.resolve({ ok: true }),
        };
      },
    };
    const second = await handleTraccarForwardPosition({
      body,
      metrics: ctx.metrics,
      rawIngressWriter: failingRaw,
      buildRawDoc: buildTraccarRawIngressDoc,
      normalizeForwardPayload,
      forwardQueue: ctx.forwardQueue,
      retryDedupe: ctx.retryDedupe,
    });
    assert.equal(second.status, 202);
    assert.equal(ctx.forwardQueue.items.length, 1, "persistence not dispatched twice");
    assert.equal(ctx.metrics.forward_exact_retry_total, 1);
    assert.equal(ctx.metrics.forward_retry_suppressed_live_total, 1);
    assert.equal(ctx.metrics.forward_retry_suppressed_persistence_total, 1);
    assert.equal(ctx.metrics.raw_attempts, 1);
    await ctx.rawIngressWriter.flushAndStop(1000);
  });

  it("raw can fail then succeed on retry after realtime already happened", async () => {
    const ctx = await setup();
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
        return {
          accepted: true,
          durable: Promise.resolve({ ok: true }),
        };
      },
    };
    const body = sampleBody();
    const first = await handleTraccarForwardPosition({
      body,
      metrics: ctx.metrics,
      rawIngressWriter: flakyRaw,
      buildRawDoc: buildTraccarRawIngressDoc,
      normalizeForwardPayload,
      forwardQueue: ctx.forwardQueue,
      retryDedupe: ctx.retryDedupe,
    });
    assert.equal(first.status, 503);
    assert.equal(ctx.forwardQueue.items.length, 1);

    rawOk = true;
    const second = await handleTraccarForwardPosition({
      body,
      metrics: ctx.metrics,
      rawIngressWriter: flakyRaw,
      buildRawDoc: buildTraccarRawIngressDoc,
      normalizeForwardPayload,
      forwardQueue: ctx.forwardQueue,
      retryDedupe: ctx.retryDedupe,
    });
    assert.equal(second.status, 202);
    assert.equal(ctx.forwardQueue.items.length, 1);
    assert.equal(ctx.metrics.raw_durable_accept_success_total, 1);
  });

  it("two legitimate stationary packets are not incorrectly deduped", async () => {
    const ctx = await setup();
    const a = sampleBody();
    const b = sampleBody({
      position: {
        id: 0,
        protocol: "gt06",
        serverTime: "2026-09-06T01:01:00.000Z",
        deviceTime: "2026-09-06T01:00:59.000Z",
        fixTime: "2026-09-06T01:00:58.000Z",
        latitude: 24.7136,
        longitude: 46.6753,
        speed: 0,
        attributes: { ignition: true, type: 19 },
      },
    });
    await handleTraccarForwardPosition({
      body: a,
      metrics: ctx.metrics,
      rawIngressWriter: ctx.rawIngressWriter,
      buildRawDoc: buildTraccarRawIngressDoc,
      normalizeForwardPayload,
      forwardQueue: ctx.forwardQueue,
      retryDedupe: ctx.retryDedupe,
    });
    await handleTraccarForwardPosition({
      body: b,
      metrics: ctx.metrics,
      rawIngressWriter: ctx.rawIngressWriter,
      buildRawDoc: buildTraccarRawIngressDoc,
      normalizeForwardPayload,
      forwardQueue: ctx.forwardQueue,
      retryDedupe: ctx.retryDedupe,
    });
    assert.equal(ctx.forwardQueue.items.length, 2);
    assert.equal(ctx.metrics.forward_exact_retry_total || 0, 0);
    await ctx.rawIngressWriter.flushAndStop(1000);
  });

  it("enqueue durable resolves on journal without waiting for Mongo", async () => {
    const metrics = {};
    let mongoStarted = false;
    const writer = createTraccarRawIngressWriter({
      spoolDir,
      batchSize: 100,
      flushMs: 10_000,
      metrics,
      insertMany: async () => {
        mongoStarted = true;
        await sleep(200);
      },
    });
    await writer.whenReady();
    const queued = writer.enqueue(buildTraccarRawIngressDoc(sampleBody(), Date.now()));
    assert.equal(queued.accepted, true);
    const durable = await queued.durable;
    assert.equal(durable.ok, true);
    assert.equal(mongoStarted, false, "Mongo must not be required for durable accept");
    await writer.flushAndStop(1000);
  });
});
