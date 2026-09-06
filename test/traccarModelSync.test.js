const { describe, it } = require("node:test");
const assert = require("node:assert/strict");
const { createTraccarModelSync } = require("../lib/traccarModelSync");

async function waitFor(predicate, timeoutMs = 1000) {
  const deadline = Date.now() + timeoutMs;
  while (!predicate() && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

function captureLogs() {
  const lines = [];
  return {
    lines,
    log: {
      log: (...args) => lines.push(args.map(String).join(" ")),
      warn: (...args) => lines.push(`WARN ${args.map(String).join(" ")}`),
      error: (...args) => lines.push(`ERROR ${args.map(String).join(" ")}`),
    },
  };
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
    let lookups = 0;
    const metrics = {};
    const sync = createTraccarModelSync({
      metrics,
      lookupModel: async () => {
        lookups += 1;
        return "SEEWORLD";
      },
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
    // Positive desired-model cache reused — no second Mongo lookup.
    assert.equal(lookups, 1);
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
    let lookups = 0;
    const metrics = {};
    const sync = createTraccarModelSync({
      metrics,
      lookupModel: async () => {
        lookups += 1;
        return "SEEWORLD";
      },
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
    assert.equal(lookups, 1, "desired model cache survives Traccar restart resync");
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

describe("Traccar model sync lifecycle (no packet spam)", () => {
  it("no_model: one Mongo lookup then silent cache hits with no repeated logs", async () => {
    let lookups = 0;
    let puts = 0;
    const metrics = {};
    const { lines, log } = captureLogs();
    const sync = createTraccarModelSync({
      metrics,
      log,
      negativeModelCacheTtlMs: 600_000,
      lookupModel: async () => {
        lookups += 1;
        return null;
      },
      getClient: () => ({
        get: async () => {
          throw new Error("no_get");
        },
        put: async () => {
          puts += 1;
        },
      }),
    });

    sync.schedule({ imei: "N1", runtimeDeviceId: 1, currentModel: null });
    await waitFor(() => metrics.traccar_model_sync_skipped_no_model_total === 1);
    assert.equal(lookups, 1);
    assert.equal(metrics.traccar_model_sync_requested_total, 1);
    const logsAfterFirst = lines.filter((l) => l.includes("no tr_model")).length;
    assert.equal(logsAfterFirst, 1);

    for (let i = 0; i < 100; i++) {
      const r = sync.schedule({ imei: "N1", runtimeDeviceId: 1, currentModel: null });
      assert.equal(r.skipped, "no_model_cached");
    }
    assert.equal(lookups, 1);
    assert.equal(puts, 0);
    assert.equal(metrics.traccar_model_sync_requested_total, 1);
    assert.equal(metrics.traccar_model_sync_skipped_no_model_total, 1);
    assert.equal(lines.filter((l) => l.includes("no tr_model")).length, 1);
  });

  it("negative TTL expiry triggers exactly one new lookup; still-null stays quiet", async () => {
    let now = 1_000;
    let lookups = 0;
    let model = null;
    const metrics = {};
    const { lines, log } = captureLogs();
    const sync = createTraccarModelSync({
      metrics,
      log,
      now: () => now,
      negativeModelCacheTtlMs: 1_000,
      lookupModel: async () => {
        lookups += 1;
        return model;
      },
      getClient: () => ({
        get: async () => ({ data: { id: 2, uniqueId: "N2", name: "N2", model: null } }),
        put: async (_p, body) => {
          model = body.model;
          return { status: 200 };
        },
      }),
    });

    sync.schedule({ imei: "N2", runtimeDeviceId: 2, currentModel: null });
    await waitFor(() => lookups === 1);
    assert.equal(metrics.traccar_model_sync_skipped_no_model_total, 1);

    now += 2_000; // expire negative cache
    sync.schedule({ imei: "N2", runtimeDeviceId: 2, currentModel: null });
    await waitFor(() => lookups === 2);
    assert.equal(metrics.traccar_model_sync_requested_total, 2);
    // Still no model — do not re-bump skipped / re-log.
    assert.equal(metrics.traccar_model_sync_skipped_no_model_total, 1);
    assert.equal(lines.filter((l) => l.includes("no tr_model")).length, 1);

    model = "SEEWORLD";
    // Force metadata refresh: expire again then discover.
    now += 2_000;
    // Put path needs GET to see null then verify SEEWORLD after put — reset api via closure
    let apiModel = null;
    const sync2 = createTraccarModelSync({
      metrics: {},
      log,
      now: () => now,
      negativeModelCacheTtlMs: 1_000,
      lookupModel: async () => {
        lookups += 1;
        return model;
      },
      getClient: () => ({
        get: async () => ({
          data: { id: 3, uniqueId: "N3", name: "N3", model: apiModel },
        }),
        put: async () => {
          apiModel = "SEEWORLD";
          return { status: 200 };
        },
      }),
    });
    // Seed no_model then expire and discover on N3
    model = null;
    sync2.schedule({ imei: "N3", runtimeDeviceId: 3, currentModel: null });
    await waitFor(() => sync2._lifecycle.get("N3")?.state === "no_model");
    now += 2_000;
    model = "SEEWORLD";
    sync2.schedule({ imei: "N3", runtimeDeviceId: 3, currentModel: null });
    await waitFor(() => sync2._syncedRuntime.has("N3|3"));
    assert.equal(sync2._syncedRuntime.get("N3|3").trModel, "SEEWORLD");
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
    now += 2_000; // expire positive metadata
    // Forwarded model still A → mismatch with new desired after refresh
    sync.schedule({ imei: "M1", runtimeDeviceId: 8, currentModel: "MODEL_A" });
    await waitFor(() => puts.includes("MODEL_B"));
    assert.ok(puts.includes("MODEL_B"));
  });
});
