const { describe, it } = require("node:test");
const assert = require("node:assert/strict");
const {
  createTraccarModelSync,
  resolveEffectiveDesiredModel,
  DEFAULT_DEVICE_MODEL,
} = require("../lib/traccarModelSync");

async function waitFor(predicate, timeoutMs = 1000) {
  const deadline = Date.now() + timeoutMs;
  while (!predicate() && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

describe("effective desired model resolution", () => {
  it("defaults missing/null/blank to Seeworld; keeps explicit models", () => {
    assert.equal(DEFAULT_DEVICE_MODEL, "Seeworld");
    assert.deepEqual(resolveEffectiveDesiredModel(undefined), {
      model: "Seeworld",
      usedDefault: true,
      source: "default_seeworld",
    });
    assert.deepEqual(resolveEffectiveDesiredModel(null), {
      model: "Seeworld",
      usedDefault: true,
      source: "default_seeworld",
    });
    assert.deepEqual(resolveEffectiveDesiredModel(""), {
      model: "Seeworld",
      usedDefault: true,
      source: "default_seeworld",
    });
    assert.deepEqual(resolveEffectiveDesiredModel("   "), {
      model: "Seeworld",
      usedDefault: true,
      source: "default_seeworld",
    });
    assert.deepEqual(resolveEffectiveDesiredModel("R12L"), {
      model: "R12L",
      usedDefault: false,
      source: "explicit",
    });
  });
});

describe("Traccar model sync Seeworld fallback", () => {
  for (const raw of [undefined, null, "", "   "]) {
    it(`Mongo model ${JSON.stringify(raw)} → desired Seeworld (not no_model)`, async () => {
      let lookups = 0;
      let puts = [];
      let apiModel = null;
      const metrics = {};
      const sync = createTraccarModelSync({
        metrics,
        lookupModel: async () => {
          lookups += 1;
          return raw;
        },
        getClient: () => ({
          get: async () => ({
            data: { id: 3235, uniqueId: "A", name: "A", model: apiModel },
          }),
          put: async (path, body) => {
            assert.equal(path, "/api/devices/3235");
            puts.push(body.model);
            apiModel = body.model;
            return { status: 200 };
          },
        }),
      });

      sync.schedule({ imei: "A", runtimeDeviceId: 3235, currentModel: null });
      await waitFor(() => metrics.traccar_model_sync_success_total === 1);

      assert.equal(lookups, 1);
      assert.deepEqual(puts, ["Seeworld"]);
      assert.equal(sync._syncedRuntime.get("A|3235").trModel, "Seeworld");
      assert.notEqual(sync._lifecycle.get("A")?.state, "no_model");
      assert.equal(metrics.traccar_model_sync_default_seeworld_total, 1);
      assert.equal(metrics.traccar_model_sync_skipped_no_model_total || 0, 0);

      for (let i = 0; i < 50; i++) {
        const r = sync.schedule({
          imei: "A",
          runtimeDeviceId: 3235,
          currentModel: "Seeworld",
        });
        assert.ok(r.skipped === "already_synced" || r.skipped === "already_correct");
      }
      assert.equal(lookups, 1);
    });
  }

  it("Mongo model R12L remains explicit", async () => {
    let puts = [];
    let apiModel = null;
    const metrics = {};
    const sync = createTraccarModelSync({
      metrics,
      lookupModel: async () => "R12L",
      getClient: () => ({
        get: async () => ({ data: { id: 1, uniqueId: "B", name: "B", model: apiModel } }),
        put: async (_p, body) => {
          puts.push(body.model);
          apiModel = body.model;
          return { status: 200 };
        },
      }),
    });
    sync.schedule({ imei: "B", runtimeDeviceId: 1, currentModel: null });
    await waitFor(() => metrics.traccar_model_sync_success_total === 1);
    assert.deepEqual(puts, ["R12L"]);
    assert.equal(metrics.traccar_model_sync_explicit_total, 1);
    assert.equal(metrics.traccar_model_sync_default_seeworld_total || 0, 0);
  });

  it("fallback does not write Seeworld back to Mongo", async () => {
    const mongoMutations = [];
    let apiModel = null;
    const sync = createTraccarModelSync({
      metrics: {},
      lookupModel: async () => {
        mongoMutations.push("read");
        return null;
      },
      getClient: () => ({
        get: async () => ({ data: { id: 9, uniqueId: "C", name: "C", model: apiModel } }),
        put: async (_p, body) => {
          apiModel = body.model;
          return { status: 200 };
        },
      }),
    });
    sync.schedule({ imei: "C", runtimeDeviceId: 9, currentModel: "" });
    await waitFor(() => sync._syncedRuntime.has("C|9"));
    assert.deepEqual(mongoMutations, ["read"]);
    assert.equal(sync._syncedRuntime.get("C|9").trModel, "Seeworld");
  });

  it("runtime id change after Traccar restart re-syncs Seeworld to new id", async () => {
    const puts = [];
    const apiModels = new Map([
      [3235, null],
      [4102, null],
    ]);
    const metrics = {};
    const sync = createTraccarModelSync({
      metrics,
      lookupModel: async () => null,
      getClient: () => ({
        get: async (path) => {
          const id = Number(String(path).split("/").pop());
          return {
            data: { id, uniqueId: "D", name: "D", model: apiModels.get(id) },
          };
        },
        put: async (path, body) => {
          const id = Number(String(path).split("/").pop());
          puts.push({ id, model: body.model });
          apiModels.set(id, body.model);
          return { status: 200 };
        },
      }),
    });

    sync.schedule({ imei: "D", runtimeDeviceId: 3235, currentModel: null });
    await waitFor(() => sync._syncedRuntime.has("D|3235"));
    assert.equal(sync._syncedRuntime.get("D|3235").trModel, "Seeworld");

    sync.schedule({ imei: "D", runtimeDeviceId: 4102, currentModel: null });
    await waitFor(() => sync._syncedRuntime.has("D|4102"));
    assert.equal(sync._syncedRuntime.get("D|4102").trModel, "Seeworld");
    assert.deepEqual(puts, [
      { id: 3235, model: "Seeworld" },
      { id: 4102, model: "Seeworld" },
    ]);
  });
});

describe("Traccar model sync", () => {
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
              model: getCalls > 1 ? "SEEWORLD" : null,
            },
          };
        },
        put: async (path, body) => {
          assert.equal(path, "/api/devices/9");
          putBodies.push(body);
          return { status: 200 };
        },
      }),
    });

    sync.schedule({ imei: "A", runtimeDeviceId: 9, currentModel: "", forwardedDevice: { id: 9 } });
    await waitFor(() => putBodies.length === 1);
    assert.equal(putBodies[0].model, "SEEWORLD");
    assert.ok(getCalls >= 2);
  });

  it("runtime id change schedules another sync", async () => {
    const puts = [];
    const apiModels = new Map([
      [1, null],
      [2, null],
    ]);
    const sync = createTraccarModelSync({
      lookupModel: async () => "SEEWORLD",
      getClient: () => ({
        get: async (path) => {
          const id = Number(String(path).split("/").pop());
          return { data: { id, uniqueId: "A", name: "A", model: apiModels.get(id) || null } };
        },
        put: async (path, body) => {
          const id = Number(String(path).split("/").pop());
          puts.push(path);
          apiModels.set(id, body.model);
          return { status: 200 };
        },
      }),
    });

    sync.schedule({ imei: "A", runtimeDeviceId: 1, currentModel: "" });
    await waitFor(() => puts.includes("/api/devices/1"));
    sync.schedule({ imei: "A", runtimeDeviceId: 2, currentModel: "" });
    await waitFor(() => puts.includes("/api/devices/2"));
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
          const err = new Error("temporary");
          err.response = { status: 500 };
          throw err;
        },
      }),
    });
    const result = sync.schedule({ imei: "A", runtimeDeviceId: 5, currentModel: "" });
    assert.equal(result.accepted, true);
    await waitFor(() => metrics.traccar_model_sync_failed_total === 1);
    assert.equal(metrics.traccar_model_sync_failed_total, 1);
  });

  it("does not trust cached runtime sync when forwarded model is explicitly empty again", async () => {
    let apiModel = null;
    const metrics = {};
    const sync = createTraccarModelSync({
      metrics,
      lookupModel: async () => "SEEWORLD",
      getClient: () => ({
        get: async () => ({ data: { id: 10, uniqueId: "A", name: "A", model: apiModel } }),
        put: async () => {
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
    assert.equal(metrics.traccar_model_sync_verified_total, 2);
  });

  it("retries when GET after PUT does not verify the requested model", async () => {
    const metrics = {};
    const sync = createTraccarModelSync({
      metrics,
      lookupModel: async () => "SEEWORLD",
      getClient: () => ({
        get: async () => ({ data: { id: 11, uniqueId: "A", name: "A", model: null } }),
        put: async () => ({ status: 200 }),
      }),
    });
    const result = sync.schedule({ imei: "A", runtimeDeviceId: 11, currentModel: null });
    assert.equal(result.accepted, true);
    await waitFor(() => metrics.traccar_model_sync_failed_total === 1);
    assert.equal(metrics.traccar_model_sync_attempt_total, 1);
    assert.equal(metrics.traccar_model_sync_mismatch_total, 1);
    assert.equal(metrics.traccar_model_sync_success_total || 0, 0);
  });
});

describe("Traccar model sync lifecycle (no packet spam)", () => {
  it("blank Mongo model: one lookup syncs Seeworld then silent cache hits", async () => {
    let lookups = 0;
    let puts = 0;
    let apiModel = null;
    const metrics = {};
    const sync = createTraccarModelSync({
      metrics,
      lookupModel: async () => {
        lookups += 1;
        return null;
      },
      getClient: () => ({
        get: async () => ({ data: { id: 1, uniqueId: "N1", name: "N1", model: apiModel } }),
        put: async (_p, body) => {
          puts += 1;
          apiModel = body.model;
          return { status: 200 };
        },
      }),
    });

    sync.schedule({ imei: "N1", runtimeDeviceId: 1, currentModel: null });
    await waitFor(() => metrics.traccar_model_sync_success_total === 1);
    assert.equal(lookups, 1);
    assert.equal(puts, 1);
    assert.equal(apiModel, "Seeworld");
    assert.equal(metrics.traccar_model_sync_default_seeworld_total, 1);
    assert.equal(metrics.traccar_model_sync_skipped_no_model_total || 0, 0);

    for (let i = 0; i < 100; i++) {
      const r = sync.schedule({ imei: "N1", runtimeDeviceId: 1, currentModel: "Seeworld" });
      assert.ok(r.skipped === "already_synced" || r.skipped === "already_correct");
    }
    assert.equal(lookups, 1);
    assert.equal(puts, 1);
    assert.equal(metrics.traccar_model_sync_requested_total, 1);
  });

  it("synced device: thousands of packets do no model work", async () => {
    let lookups = 0;
    let gets = 0;
    let puts = 0;
    const metrics = {};
    const sync = createTraccarModelSync({
      metrics,
      lookupModel: async () => {
        lookups += 1;
        return "SEEWORLD";
      },
      getClient: () => ({
        get: async () => {
          gets += 1;
          return { data: { id: 4, uniqueId: "S1", name: "S1", model: "SEEWORLD" } };
        },
        put: async () => {
          puts += 1;
        },
      }),
    });

    sync.schedule({ imei: "S1", runtimeDeviceId: 4, currentModel: "SEEWORLD" });
    await waitFor(() => metrics.traccar_model_sync_already_correct_total >= 1);

    const requestedAfterFirst = metrics.traccar_model_sync_requested_total || 0;
    for (let i = 0; i < 2000; i++) {
      sync.schedule({ imei: "S1", runtimeDeviceId: 4, currentModel: "SEEWORLD" });
    }
    assert.equal(lookups, 1);
    assert.equal(gets, 0);
    assert.equal(puts, 0);
    assert.equal(metrics.traccar_model_sync_requested_total, requestedAfterFirst);
    assert.equal(metrics.traccar_model_sync_already_correct_total, 1);
  });

  it("Model A -> Model B eventually refreshes after positive TTL", async () => {
    let now = 5_000;
    let model = "MODEL_A";
    let puts = [];
    let apiModel = null;
    const metrics = {};
    const sync = createTraccarModelSync({
      metrics,
      now: () => now,
      modelCacheTtlMs: 1_000,
      lookupModel: async () => model,
      getClient: () => ({
        get: async () => ({ data: { id: 8, uniqueId: "M1", name: "M1", model: apiModel } }),
        put: async (_p, body) => {
          apiModel = body.model;
          puts.push(body.model);
          return { status: 200 };
        },
      }),
    });

    sync.schedule({ imei: "M1", runtimeDeviceId: 8, currentModel: null });
    await waitFor(() => puts[0] === "MODEL_A");

    model = "MODEL_B";
    now += 2_000;
    sync.schedule({ imei: "M1", runtimeDeviceId: 8, currentModel: "MODEL_A" });
    await waitFor(() => puts.includes("MODEL_B"));
    assert.ok(puts.includes("MODEL_B"));
  });
});
