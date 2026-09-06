const { describe, it } = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { createPersistenceIpc } = require("../lib/persistenceIpc");

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
});
