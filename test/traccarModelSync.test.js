const { describe, it } = require("node:test");
const assert = require("node:assert/strict");
const { createTraccarModelSync } = require("../lib/traccarModelSync");

async function waitFor(predicate, timeoutMs = 1000) {
  const deadline = Date.now() + timeoutMs;
  while (!predicate() && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

describe("Traccar model sync", () => {
  it("skips without a configured tr_model", async () => {
    let lookups = 0;
    const metrics = {};
    const sync = createTraccarModelSync({
      metrics,
      lookupModel: async () => {
        lookups += 1;
        return null;
      },
      getClient: () => ({
        get: async () => {
          throw new Error("should_not_get");
        },
        put: async () => {
          throw new Error("should_not_put");
        },
      }),
    });

    sync.schedule({ imei: "A", runtimeDeviceId: 1, currentModel: "" });
    await waitFor(() => metrics.traccar_model_sync_skipped_no_model_total === 1);

    assert.equal(lookups, 1);
    assert.equal(metrics.traccar_model_sync_skipped_no_model_total, 1);
    assert.equal(metrics.traccar_model_sync_success_total || 0, 0);
  });

  it("does not PUT when forwarded model is already correct", async () => {
    let puts = 0;
    const metrics = {};
    const sync = createTraccarModelSync({
      metrics,
      lookupModel: async () => "SEEWORLD",
      getClient: () => ({
        get: async () => {
          throw new Error("should_not_get");
        },
        put: async () => {
          puts += 1;
        },
      }),
    });

    sync.schedule({ imei: "A", runtimeDeviceId: 1, currentModel: "SEEWORLD" });
    await waitFor(() => metrics.traccar_model_sync_already_correct_total === 1);

    assert.equal(puts, 0);
    assert.equal(metrics.traccar_model_sync_already_correct_total, 1);
  });

  it("deduplicates pending sync work for repeated packets", async () => {
    let lookups = 0;
    let puts = 0;
    let apiModel = null;
    const metrics = {};
    const sync = createTraccarModelSync({
      metrics,
      concurrency: 1,
      lookupModel: async () => {
        lookups += 1;
        return "SEEWORLD";
      },
      getClient: () => ({
        get: async () => ({ data: { id: 7, uniqueId: "A", name: "A", model: apiModel } }),
        put: async () => {
          puts += 1;
          apiModel = "SEEWORLD";
        },
      }),
    });

    for (let i = 0; i < 100; i++) {
      sync.schedule({ imei: "A", runtimeDeviceId: 7, currentModel: "" });
    }
    await waitFor(() => metrics.traccar_model_sync_success_total === 1);

    assert.equal(lookups, 1);
    assert.equal(puts, 1);
    assert.equal(metrics.traccar_model_sync_requested_total, 1);
    assert.equal(metrics.traccar_model_sync_success_total, 1);
  });

  it("uses GET before PUT when forwarded device is incomplete", async () => {
    const putBodies = [];
    let getCalls = 0;
    const sync = createTraccarModelSync({
      lookupModel: async () => "SEEWORLD",
      getClient: () => ({
        get: async (path) => {
          assert.equal(path, "/api/devices/9");
          getCalls += 1;
          return {
            data: {
              id: 9,
              uniqueId: "A",
              name: "Device A",
              category: "car",
              model: getCalls > 1 ? "SEEWORLD" : null,
            },
          };
        },
        put: async (path, body) => {
          assert.equal(path, "/api/devices/9");
          putBodies.push(body);
        },
      }),
    });

    sync.schedule({ imei: "A", runtimeDeviceId: 9, currentModel: "", forwardedDevice: { id: 9 } });
    await waitFor(() => putBodies.length === 1);

    assert.equal(putBodies[0].uniqueId, "A");
    assert.equal(putBodies[0].name, "Device A");
    assert.equal(putBodies[0].category, "car");
    assert.equal(putBodies[0].model, "SEEWORLD");
    assert.equal(getCalls, 2);
  });

  it("runtime id change schedules another sync", async () => {
    const puts = [];
    const apiModels = new Map();
    const metrics = {};
    const sync = createTraccarModelSync({
      metrics,
      lookupModel: async () => "SEEWORLD",
      getClient: () => ({
        get: async (path) => {
          const id = Number(path.split("/").pop());
          return { data: { id, uniqueId: "A", name: "A", model: apiModels.get(id) || null } };
        },
        put: async (path) => {
          const id = Number(path.split("/").pop());
          apiModels.set(id, "SEEWORLD");
          puts.push(path);
        },
      }),
    });

    sync.schedule({ imei: "A", runtimeDeviceId: 1, currentModel: "" });
    await waitFor(() => puts.length === 1);
    sync.schedule({ imei: "A", runtimeDeviceId: 2, currentModel: "" });
    await waitFor(() => puts.length === 2);

    assert.deepEqual(puts, ["/api/devices/1", "/api/devices/2"]);
  });

  it("records API failure without throwing from schedule", async () => {
    const metrics = {};
    const sync = createTraccarModelSync({
      metrics,
      lookupModel: async () => "SEEWORLD",
      getClient: () => ({
        get: async () => ({ data: { id: 5, uniqueId: "A", name: "A", model: null } }),
        put: async () => {
          throw Object.assign(new Error("temporary"), { response: { status: 500 } });
        },
      }),
    });

    const result = sync.schedule({ imei: "A", runtimeDeviceId: 5, currentModel: "" });
    await waitFor(() => metrics.traccar_model_sync_failed_total === 1);

    assert.equal(result.accepted, true);
    assert.equal(metrics.traccar_model_sync_failed_total, 1);
  });

  it("does not trust cached runtime sync when forwarded model is explicitly empty again", async () => {
    const puts = [];
    let apiModel = null;
    const metrics = {};
    const sync = createTraccarModelSync({
      metrics,
      lookupModel: async () => "SEEWORLD",
      getClient: () => ({
        get: async () => ({ data: { id: 10, uniqueId: "A", name: "A", model: apiModel } }),
        put: async () => {
          puts.push(Date.now());
          apiModel = "SEEWORLD";
          return { status: 200 };
        },
      }),
    });

    sync.schedule({ imei: "A", runtimeDeviceId: 10, currentModel: null });
    await waitFor(() => metrics.traccar_model_sync_success_total === 1);

    apiModel = null;
    sync.schedule({ imei: "A", runtimeDeviceId: 10, currentModel: null });
    await waitFor(() => metrics.traccar_model_sync_success_total === 2);

    assert.equal(puts.length, 2);
    assert.equal(metrics.traccar_model_sync_verified_total, 2);
  });

  it("retries when GET after PUT does not verify the requested model", async () => {
    const metrics = {};
    const sync = createTraccarModelSync({
      metrics,
      retryBaseMs: 10_000,
      lookupModel: async () => "SEEWORLD",
      getClient: () => ({
        get: async () => ({ data: { id: 11, uniqueId: "A", name: "A", model: null } }),
        put: async () => ({ status: 200 }),
      }),
    });

    const result = sync.schedule({ imei: "A", runtimeDeviceId: 11, currentModel: null });
    await waitFor(() => metrics.traccar_model_sync_failed_total === 1);

    assert.equal(result.accepted, true);
    assert.equal(metrics.traccar_model_sync_attempt_total, 1);
    assert.equal(metrics.traccar_model_sync_mismatch_total, 1);
    assert.equal(metrics.traccar_model_sync_success_total || 0, 0);
  });
});
