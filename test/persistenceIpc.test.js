const { describe, it } = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const fs = require("node:fs");
const os = require("node:os");
const { createPersistenceIpc } = require("../lib/persistenceIpc");

function tmpDir(prefix) {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

function readLoggedBatches(filePath) {
  if (!fs.existsSync(filePath)) return [];
  return fs
    .readFileSync(filePath, "utf8")
    .split("\n")
    .filter(Boolean)
    .map((line) => JSON.parse(line));
}

async function waitForMetric(predicate, timeoutMs = 2000) {
  const deadline = Date.now() + timeoutMs;
  while (!predicate() && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}

describe("persistence IPC", () => {
  it("acks batches from a worker process and drains the queue", async () => {
    const metrics = {};
    const ipc = createPersistenceIpc({
      workerModulePath: path.join(__dirname, "fixtures", "persistence-worker-fixture.js"),
      metrics,
      maxQueueBatches: 4,
      maxBatchSize: 100,
    });

    try {
      ipc.enqueue({
        batchId: "batch-1",
        items: [{ id: 1 }, { id: 2 }],
      });

      const deadline = Date.now() + 2000;
      while ((metrics.positions_worker_acked_total || 0) < 2 && Date.now() < deadline) {
        await new Promise((resolve) => setTimeout(resolve, 20));
      }

      assert.equal(metrics.positions_worker_acked_total, 2);
      assert.equal(ipc.getDepth(), 0);
      assert.ok(ipc.getWorkerPid());
    } finally {
      await ipc.flushAndStop(500);
    }
  });

  it("flushes one item after batch max wait", async () => {
    const metrics = {};
    const dir = tmpDir("ipc-batch-wait-");
    const logFile = path.join(dir, "batches.jsonl");
    process.env.IPC_FIXTURE_BATCH_LOG = logFile;
    const ipc = createPersistenceIpc({
      workerModulePath: path.join(__dirname, "fixtures", "persistence-worker-fixture.js"),
      metrics,
      maxQueueBatches: 4,
      maxBatchSize: 100,
      batchMaxItems: 100,
      batchMaxWaitMs: 25,
      spoolDir: path.join(dir, "spool"),
    });

    try {
      ipc.enqueue({ batchId: "one", items: [{ id: 1 }] });
      await waitForMetric(() => (metrics.positions_worker_acked_total || 0) >= 1);

      const batches = readLoggedBatches(logFile);
      assert.equal(batches.length, 1);
      assert.deepEqual(batches[0].map((item) => item.id), [1]);
      assert.equal(metrics.ipc_batch_max_size, 1);
    } finally {
      await ipc.flushAndStop(500);
      delete process.env.IPC_FIXTURE_BATCH_LOG;
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("flushes immediately at max items and preserves order", async () => {
    const metrics = {};
    const dir = tmpDir("ipc-batch-max-");
    const logFile = path.join(dir, "batches.jsonl");
    process.env.IPC_FIXTURE_BATCH_LOG = logFile;
    const ipc = createPersistenceIpc({
      workerModulePath: path.join(__dirname, "fixtures", "persistence-worker-fixture.js"),
      metrics,
      maxQueueBatches: 8,
      maxBatchSize: 100,
      batchMaxItems: 3,
      batchMaxWaitMs: 1000,
      spoolDir: path.join(dir, "spool"),
    });

    try {
      for (let i = 0; i < 3; i++) {
        ipc.enqueue({ batchId: `item-${i}`, items: [{ id: i }] });
      }
      await waitForMetric(() => (metrics.positions_worker_acked_total || 0) >= 3);

      const batches = readLoggedBatches(logFile);
      assert.equal(batches.length, 1);
      assert.deepEqual(batches[0].map((item) => item.id), [0, 1, 2]);
      assert.equal(metrics.ipc_batch_max_size, 3);
    } finally {
      await ipc.flushAndStop(500);
      delete process.env.IPC_FIXTURE_BATCH_LOG;
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("batches 100 single-item enqueues into substantially fewer IPC messages", async () => {
    const metrics = {};
    const dir = tmpDir("ipc-batch-100-");
    const logFile = path.join(dir, "batches.jsonl");
    process.env.IPC_FIXTURE_BATCH_LOG = logFile;
    const ipc = createPersistenceIpc({
      workerModulePath: path.join(__dirname, "fixtures", "persistence-worker-fixture.js"),
      metrics,
      maxQueueBatches: 16,
      maxBatchSize: 100,
      batchMaxItems: 25,
      batchMaxWaitMs: 1000,
      spoolDir: path.join(dir, "spool"),
    });

    try {
      for (let i = 0; i < 100; i++) {
        ipc.enqueue({ batchId: `item-${i}`, items: [{ id: i }] });
      }
      await waitForMetric(() => (metrics.positions_worker_acked_total || 0) >= 100);

      const batches = readLoggedBatches(logFile);
      const ids = batches.flat().map((item) => item.id);
      assert.ok(batches.length <= 4, `expected <= 4 batches, got ${batches.length}`);
      assert.deepEqual(ids, Array.from({ length: 100 }, (_, i) => i));
      assert.equal(metrics.ipc_items_sent_total, 100);
      assert.equal(metrics.ipc_batch_max_size, 25);
    } finally {
      await ipc.flushAndStop(500);
      delete process.env.IPC_FIXTURE_BATCH_LOG;
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});
