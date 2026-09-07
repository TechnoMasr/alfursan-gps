const { describe, it } = require("node:test");
const assert = require("node:assert/strict");
const {
  createTraccarDeviceRegistry,
  sendTraccarCustomCommand,
  isStaleRuntimeDeviceError,
  isConcurrentModificationFailure,
  ENUMERATION_PATH,
  ENUMERATION_PARAMS,
} = require("../lib/traccarDeviceRegistry");
const { resolveRuntimeDeviceIdByImei } = require("../lib/traccarRuntimeDevices");
const {
  runStartupConnectivityReconciliation,
  createEmptySnapshotGate,
  fetchTraccarDevices,
} = require("../lib/startupConnectivityReconciliation");

function fakeClient(handler) {
  const calls = [];
  return {
    calls,
    get: async (path, opts) => {
      calls.push({ method: "GET", path, opts });
      return handler({ method: "GET", path, opts });
    },
    post: async (path, body) => {
      calls.push({ method: "POST", path, body });
      return handler({ method: "POST", path, body });
    },
  };
}

describe("traccar device registry enumeration", () => {
  it("uses GET /api/devices?all=true only for fleet snapshot", async () => {
    const client = fakeClient(({ path, opts }) => {
      assert.equal(path, "/api/devices");
      assert.deepEqual(opts.params, { all: true });
      return {
        data: [
          { id: 10, uniqueId: "353994714422091" },
          { id: 11, uniqueId: " 222 " },
        ],
      };
    });
    const metrics = {};
    const registry = createTraccarDeviceRegistry({
      getClient: () => client,
      metrics,
    });
    const result = await registry.refresh({ force: true });
    assert.equal(result.ok, true);
    assert.equal(result.devices, 2);
    assert.equal(registry.getRuntimeId("353994714422091"), 10);
    assert.equal(registry.getRuntimeId("222"), 11);
    assert.equal(client.calls.length, 1);
    assert.equal(ENUMERATION_PATH, "/api/devices");
    assert.deepEqual(ENUMERATION_PARAMS, { all: true });
  });

  it("does not use plain /api/devices or uniqueId query for enumeration", async () => {
    const client = fakeClient(({ path, opts }) => {
      assert.equal(path, "/api/devices");
      assert.ok(opts?.params?.all === true);
      assert.equal(opts?.params?.uniqueId, undefined);
      return { data: [{ id: 1, uniqueId: "A" }] };
    });
    const registry = createTraccarDeviceRegistry({ getClient: () => client });
    await registry.refresh({ force: true });
    const fetched = await fetchTraccarDevices(client);
    assert.equal(fetched.length, 1);
    for (const call of client.calls) {
      assert.notEqual(call.opts?.params?.uniqueId, "A");
      assert.equal(call.opts?.params?.all, true);
    }
  });

  it("cache hit does not refetch; miss forces one refresh", async () => {
    let devices = [{ id: 5, uniqueId: "IMEI1" }];
    const client = fakeClient(() => ({ data: devices }));
    const metrics = {};
    const registry = createTraccarDeviceRegistry({
      getClient: () => client,
      metrics,
    });
    await registry.refresh({ force: true });
    assert.equal(client.calls.length, 1);
    assert.equal(await registry.resolveRuntimeId("IMEI1"), 5);
    assert.equal(client.calls.length, 1);
    assert.ok(metrics.traccar_registry_cache_hit_total >= 1);

    devices = [
      { id: 5, uniqueId: "IMEI1" },
      { id: 9, uniqueId: "IMEI2" },
    ];
    assert.equal(await registry.resolveRuntimeId("IMEI2"), 9);
    assert.equal(client.calls.length, 2);
    assert.ok(metrics.traccar_registry_forced_refresh_total >= 1);
    assert.ok(metrics.traccar_registry_cache_miss_total >= 1);
  });

  it("concurrent refreshes share one in-flight request", async () => {
    let inflight = 0;
    let maxInflight = 0;
    const client = fakeClient(async () => {
      inflight += 1;
      maxInflight = Math.max(maxInflight, inflight);
      await new Promise((r) => setTimeout(r, 30));
      inflight -= 1;
      return { data: [{ id: 1, uniqueId: "X" }] };
    });
    const registry = createTraccarDeviceRegistry({ getClient: () => client });
    const results = await Promise.all([
      registry.refresh({ force: true }),
      registry.refresh({ force: true }),
      registry.refresh({ force: true }),
    ]);
    assert.equal(client.calls.length, 1);
    assert.equal(maxInflight, 1);
    assert.ok(results.every((r) => r.ok));
  });

  it("failed refresh preserves last known good snapshot", async () => {
    let mode = "ok";
    const client = fakeClient(() => {
      if (mode === "ok") return { data: [{ id: 7, uniqueId: "KEEP" }] };
      throw new Error("timeout");
    });
    const metrics = {};
    const registry = createTraccarDeviceRegistry({
      getClient: () => client,
      metrics,
      cooldownMs: 0,
    });
    await registry.refresh({ force: true });
    assert.equal(registry.getRuntimeId("KEEP"), 7);
    mode = "fail";
    const failed = await registry.refresh({ force: true });
    assert.equal(failed.ok, false);
    assert.equal(failed.preserved, true);
    assert.equal(registry.getRuntimeId("KEEP"), 7);
    assert.ok(metrics.traccar_registry_refresh_failure_total >= 1);
  });

  it("valid [] is authoritative empty and distinguishable from failure", async () => {
    const client = fakeClient(() => ({ data: [] }));
    const metrics = {};
    const registry = createTraccarDeviceRegistry({
      getClient: () => client,
      metrics,
    });
    const result = await registry.refresh({ force: true });
    assert.equal(result.ok, true);
    assert.equal(result.empty, true);
    assert.equal(registry.getStatus().last_snapshot_empty, true);
    assert.equal(registry.getStatus().has_snapshot, true);

    const failClient = fakeClient(() => {
      throw new Error("boom");
    });
    const reg2 = createTraccarDeviceRegistry({
      getClient: () => failClient,
      metrics: {},
    });
    const fail = await reg2.refresh({ force: true });
    assert.equal(fail.ok, false);
    assert.equal(fail.validEmpty, false);
    assert.equal(reg2.getStatus().has_snapshot, false);
  });

  it("Traccar restart updates uniqueId -> runtime id mapping", async () => {
    let devices = [{ id: 3973, uniqueId: "353994714422091" }];
    const client = fakeClient(() => ({ data: devices }));
    const metrics = {};
    const registry = createTraccarDeviceRegistry({
      getClient: () => client,
      metrics,
      cooldownMs: 0,
    });
    await registry.refresh({ force: true });
    assert.equal(registry.getRuntimeId("353994714422091"), 3973);
    devices = [{ id: 3235, uniqueId: "353994714422091" }];
    await registry.refresh({ force: true });
    assert.equal(registry.getRuntimeId("353994714422091"), 3235);
    assert.ok(metrics.traccar_registry_runtime_id_change_total >= 1);
  });

  it("normalizeUniqueId does not coerce IMEI to Number", () => {
    const registry = createTraccarDeviceRegistry({
      getClient: () => fakeClient(() => ({ data: [] })),
    });
    registry.replaceSnapshotForTests([{ id: 1, uniqueId: "012345678901234" }]);
    assert.equal(registry.getRuntimeId("012345678901234"), 1);
    assert.equal(registry.getRuntimeId(12345678901234), null);
  });
});

describe("traccar command send + stale id retry", () => {
  it("command uses runtime id from cache and does not list devices on hit", async () => {
    const client = fakeClient(({ method }) => {
      if (method === "GET") return { data: [{ id: 3235, uniqueId: "353994714422091" }] };
      return { data: { ok: true } };
    });
    const metrics = {};
    const registry = createTraccarDeviceRegistry({ getClient: () => client, metrics });
    await registry.refresh({ force: true });
    const before = client.calls.length;
    const result = await sendTraccarCustomCommand({
      registry,
      client,
      metrics,
      imei: "353994714422091",
      command: "STATUS#",
    });
    assert.equal(result.runtimeId, 3235);
    assert.equal(result.retried, false);
    const posts = client.calls.filter((c) => c.method === "POST");
    assert.equal(posts.length, 1);
    assert.equal(posts[0].path, "/api/commands/send");
    assert.equal(posts[0].body.deviceId, 3235);
    assert.equal(posts[0].body.type, "custom");
    assert.equal(posts[0].body.attributes.data, "STATUS#");
    assert.equal(posts[0].body.textChannel, undefined);
    assert.equal(client.calls.filter((c) => c.method === "GET").length, before);
  });

  it("missing after forced refresh returns clear failure", async () => {
    const client = fakeClient(() => ({ data: [] }));
    const registry = createTraccarDeviceRegistry({ getClient: () => client });
    await assert.rejects(
      () =>
        sendTraccarCustomCommand({
          registry,
          client,
          imei: "missing",
          command: "STATUS#",
        }),
      /device_not_registered_in_traccar_runtime/
    );
  });

  it("stale id retries once only when runtime id changed", async () => {
    let devices = [{ id: 100, uniqueId: "IMEI9" }];
    let postCount = 0;
    const client = fakeClient(({ method, body }) => {
      if (method === "GET") return { data: devices };
      postCount += 1;
      if (body.deviceId === 100) {
        const err = new Error("Unknown device");
        err.response = { status: 400, data: { message: "Unknown device" } };
        throw err;
      }
      return { data: { ok: true } };
    });
    const metrics = {};
    const registry = createTraccarDeviceRegistry({
      getClient: () => client,
      metrics,
      cooldownMs: 0,
    });
    await registry.refresh({ force: true });
    devices = [{ id: 200, uniqueId: "IMEI9" }];
    const result = await sendTraccarCustomCommand({
      registry,
      client,
      metrics,
      imei: "IMEI9",
      command: "ENGINE STOP",
    });
    assert.equal(result.retried, true);
    assert.equal(result.runtimeId, 200);
    assert.equal(postCount, 2);
    assert.equal(metrics.traccar_command_stale_id_retry_total, 1);
    assert.equal(metrics.traccar_command_stale_id_retry_success_total, 1);
  });

  it("unchanged runtime id after refresh does not blind-retry", async () => {
    const devices = [{ id: 55, uniqueId: "IMEI8" }];
    let postCount = 0;
    const client = fakeClient(({ method }) => {
      if (method === "GET") return { data: devices };
      postCount += 1;
      const err = new Error("Unknown device");
      err.response = { status: 400, data: { message: "Unknown device" } };
      throw err;
    });
    const metrics = {};
    const registry = createTraccarDeviceRegistry({
      getClient: () => client,
      metrics,
      cooldownMs: 0,
    });
    await registry.refresh({ force: true });
    await assert.rejects(
      () =>
        sendTraccarCustomCommand({
          registry,
          client,
          metrics,
          imei: "IMEI8",
          command: "STATUS#",
        }),
      /Unknown device/
    );
    assert.equal(postCount, 1);
    assert.equal(metrics.traccar_command_stale_id_retry_total, 1);
    assert.equal(metrics.traccar_command_stale_id_retry_success_total || 0, 0);
  });

  it("successful command is never resent", async () => {
    let postCount = 0;
    const client = fakeClient(({ method }) => {
      if (method === "GET") return { data: [{ id: 1, uniqueId: "A" }] };
      postCount += 1;
      return { data: {} };
    });
    const registry = createTraccarDeviceRegistry({ getClient: () => client });
    await registry.refresh({ force: true });
    await sendTraccarCustomCommand({
      registry,
      client,
      imei: "A",
      command: "STATUS#",
    });
    assert.equal(postCount, 1);
  });

  it("ConcurrentModificationException is surfaced and not retry-looped", async () => {
    let postCount = 0;
    const client = fakeClient(({ method }) => {
      if (method === "GET") return { data: [{ id: 1, uniqueId: "A" }] };
      postCount += 1;
      const err = new Error("CME");
      err.response = {
        status: 400,
        data: {
          message:
            "java.util.ConcurrentModificationException\nat org.traccar.storage.Storage.getObject\nat org.traccar.database.CommandsManager.sendCommand",
        },
      };
      throw err;
    });
    const metrics = {};
    const registry = createTraccarDeviceRegistry({ getClient: () => client, metrics });
    await registry.refresh({ force: true });
    await assert.rejects(
      () =>
        sendTraccarCustomCommand({
          registry,
          client,
          metrics,
          imei: "A",
          command: "STATUS#",
        }),
      /traccar_core_concurrent_modification/
    );
    assert.equal(postCount, 1);
    assert.equal(metrics.traccar_command_cme_total, 1);
    assert.equal(isConcurrentModificationFailure({
      response: {
        data: "java.util.ConcurrentModificationException Storage.getObject CommandsManager.sendCommand",
      },
    }), true);
    assert.equal(
      isStaleRuntimeDeviceError({
        response: {
          status: 400,
          data: "java.util.ConcurrentModificationException Storage.getObject",
        },
      }),
      false
    );
  });

  it("resolveRuntimeDeviceIdByImei uses registry all=true path not uniqueId=", async () => {
    const client = fakeClient(({ path, opts }) => {
      assert.equal(path, "/api/devices");
      assert.deepEqual(opts.params, { all: true });
      return { data: [{ id: 42, uniqueId: "123" }] };
    });
    const id = await resolveRuntimeDeviceIdByImei({ imei: "123", client });
    assert.equal(id, 42);
    assert.equal(client.calls[0].opts.params.uniqueId, undefined);
  });
});

describe("startup connectivity reconciliation", () => {
  const quietLog = { warn() {}, log() {} };

  function mongoOnline(n) {
    return Array.from({ length: n }, (_, i) => ({ imei: `IMEI${i}`, status: "online" }));
  }

  it("matching all=true devices does not false-offline", async () => {
    const marked = [];
    const registry = createTraccarDeviceRegistry({
      getClient: () =>
        fakeClient(() => ({
          data: Array.from({ length: 3 }, (_, i) => ({
            id: i + 1,
            uniqueId: `IMEI${i}`,
          })),
        })),
    });
    const metrics = {};
    const result = await runStartupConnectivityReconciliation({
      registry,
      metrics,
      log: quietLog,
      waitForMongoReadyFn: async () => {},
      markOfflineFn: async ({ imei }) => {
        marked.push(imei);
        return { transitioned: true };
      },
      getMongoOnline: async () => [
        { imei: "IMEI0", status: "online" },
        { imei: "IMEI1", status: "online" },
        { imei: "IMEI2", status: "online" },
      ],
    });
    assert.equal(result.ok, true);
    assert.equal(result.markedOffline, 0);
    assert.equal(result.traccarDevices, 3);
    assert.equal(result.reason, "non_empty");
    assert.equal(marked.length, 0);
  });

  it("API failure skips mass offline", async () => {
    const marked = [];
    const registry = createTraccarDeviceRegistry({
      getClient: () =>
        fakeClient(() => {
          throw new Error("ECONNREFUSED");
        }),
    });
    const metrics = {};
    const result = await runStartupConnectivityReconciliation({
      registry,
      metrics,
      log: quietLog,
      waitForMongoReadyFn: async () => {},
      markOfflineFn: async ({ imei }) => {
        marked.push(imei);
        return { transitioned: true };
      },
      getMongoOnline: async () => [{ imei: "IMEI0", status: "online" }],
    });
    assert.equal(result.ok, false);
    assert.equal(result.massOfflineSkipped, true);
    assert.equal(result.reason, "traccar_api_failure");
    assert.equal(marked.length, 0);
    assert.ok(metrics.startup_reconciliation_traccar_api_failure_total >= 1);
  });

  it("startup valid [] with many Mongo online does NOT mass-offline during grace", async () => {
    const marked = [];
    const metrics = {};
    const registry = createTraccarDeviceRegistry({
      getClient: () => fakeClient(() => ({ data: [] })),
    });
    const result = await runStartupConnectivityReconciliation({
      registry,
      metrics,
      log: quietLog,
      waitForMongoReadyFn: async () => {},
      graceMs: 120_000,
      processStartedAtMs: Date.now(),
      markOfflineFn: async ({ imei }) => {
        marked.push(imei);
        return { transitioned: true };
      },
      getMongoOnline: async () => mongoOnline(857),
    });
    assert.equal(result.ok, true);
    assert.equal(result.validEmpty, true);
    assert.equal(result.deferredEmpty, true);
    assert.equal(result.massOfflineSkipped, true);
    assert.equal(result.reason, "startup_grace");
    assert.equal(result.markedOffline, 0);
    assert.equal(marked.length, 0);
    assert.ok(metrics.startup_reconciliation_empty_deferred_total >= 1);
    assert.equal(metrics.startup_reconciliation_grace_active, 1);
  });

  it("valid [] during grace repeatedly still does NOT mass-offline", async () => {
    const marked = [];
    const started = Date.now();
    let t = started;
    const gate = createEmptySnapshotGate({
      graceMs: 60_000,
      processStartedAtMs: started,
      now: () => t,
    });
    const registry = createTraccarDeviceRegistry({
      getClient: () => fakeClient(() => ({ data: [] })),
      cooldownMs: 0,
    });
    const metrics = {};
    for (let i = 0; i < 3; i++) {
      t = started + i * 5_000;
      const result = await runStartupConnectivityReconciliation({
        registry,
        emptyGate: gate,
        metrics,
        log: quietLog,
        waitForMongoReadyFn: async () => {},
        markOfflineFn: async ({ imei }) => {
          marked.push(imei);
          return { transitioned: true };
        },
        getMongoOnline: async () => mongoOnline(10),
        now: () => t,
      });
      assert.equal(result.deferredEmpty, true);
      assert.equal(result.markedOffline, 0);
      assert.equal(result.reason, "startup_grace");
    }
    assert.equal(marked.length, 0);
  });

  it("later non-empty snapshot runs normal reconciliation", async () => {
    const marked = [];
    const started = Date.now();
    let t = started;
    let devices = [];
    const gate = createEmptySnapshotGate({
      graceMs: 60_000,
      processStartedAtMs: started,
      now: () => t,
    });
    const registry = createTraccarDeviceRegistry({
      getClient: () => fakeClient(() => ({ data: devices })),
      cooldownMs: 0,
    });
    // First: empty during grace → deferred
    let result = await runStartupConnectivityReconciliation({
      registry,
      emptyGate: gate,
      metrics: {},
      log: quietLog,
      waitForMongoReadyFn: async () => {},
      markOfflineFn: async ({ imei }) => {
        marked.push(imei);
        return { transitioned: true };
      },
      getMongoOnline: async () => [
        { imei: "KEEP", status: "online" },
        { imei: "GONE", status: "online" },
      ],
      now: () => t,
    });
    assert.equal(result.deferredEmpty, true);
    assert.equal(marked.length, 0);

    // Later: non-empty (still in grace) → normal missing offline
    devices = [{ id: 1, uniqueId: "KEEP" }];
    result = await runStartupConnectivityReconciliation({
      registry,
      emptyGate: gate,
      metrics: {},
      log: quietLog,
      waitForMongoReadyFn: async () => {},
      markOfflineFn: async ({ imei }) => {
        marked.push(imei);
        return { transitioned: true };
      },
      getMongoOnline: async () => [
        { imei: "KEEP", status: "online" },
        { imei: "GONE", status: "online" },
      ],
      now: () => t,
    });
    assert.equal(result.ok, true);
    assert.equal(result.reason, "non_empty");
    assert.equal(result.markedOffline, 1);
    assert.deepEqual(marked, ["GONE"]);
  });

  it("after grace first empty confirmation still does NOT mass-offline", async () => {
    const marked = [];
    const started = Date.now();
    let t = started + 130_000; // past default-like grace
    const gate = createEmptySnapshotGate({
      graceMs: 120_000,
      processStartedAtMs: started,
      now: () => t,
      requiredConsecutiveEmpty: 2,
    });
    const registry = createTraccarDeviceRegistry({
      getClient: () => fakeClient(() => ({ data: [] })),
      cooldownMs: 0,
    });
    const metrics = {};
    const result = await runStartupConnectivityReconciliation({
      registry,
      emptyGate: gate,
      metrics,
      log: quietLog,
      waitForMongoReadyFn: async () => {},
      markOfflineFn: async ({ imei }) => {
        marked.push(imei);
        return { transitioned: true };
      },
      getMongoOnline: async () => mongoOnline(5),
      now: () => t,
    });
    assert.equal(result.deferredEmpty, true);
    assert.equal(result.reason, "empty_awaiting_confirmation");
    assert.equal(result.consecutiveEmpty, 1);
    assert.equal(result.markedOffline, 0);
    assert.equal(marked.length, 0);
    assert.ok(metrics.startup_reconciliation_empty_deferred_total >= 1);
  });

  it("after grace second confirmed empty snapshot allows CURRENT offline behavior", async () => {
    const marked = [];
    const started = Date.now();
    let t = started + 130_000;
    const gate = createEmptySnapshotGate({
      graceMs: 120_000,
      processStartedAtMs: started,
      now: () => t,
      requiredConsecutiveEmpty: 2,
    });
    const registry = createTraccarDeviceRegistry({
      getClient: () => fakeClient(() => ({ data: [] })),
      cooldownMs: 0,
    });
    const metrics = {};
    const common = {
      registry,
      emptyGate: gate,
      metrics,
      log: quietLog,
      waitForMongoReadyFn: async () => {},
      markOfflineFn: async ({ imei }) => {
        marked.push(imei);
        return { transitioned: true };
      },
      getMongoOnline: async () => [
        { imei: "ONLY1", status: "online" },
        { imei: "ONLY2", status: "online" },
      ],
      now: () => t,
    };
    const first = await runStartupConnectivityReconciliation(common);
    assert.equal(first.reason, "empty_awaiting_confirmation");
    assert.equal(marked.length, 0);

    t = started + 145_000;
    const second = await runStartupConnectivityReconciliation(common);
    assert.equal(second.ok, true);
    assert.equal(second.reason, "empty_confirmed");
    assert.equal(second.deferredEmpty, false);
    assert.equal(second.markedOffline, 2);
    assert.deepEqual(marked.sort(), ["ONLY1", "ONLY2"]);
    assert.ok(metrics.startup_reconciliation_empty_confirmed_total >= 1);
  });

  it("Mongo online absent from valid non-empty snapshot is marked offline", async () => {
    const marked = [];
    const registry = createTraccarDeviceRegistry({
      getClient: () =>
        fakeClient(() => ({
          data: [{ id: 1, uniqueId: "KEEP" }],
        })),
    });
    const result = await runStartupConnectivityReconciliation({
      registry,
      metrics: {},
      log: quietLog,
      waitForMongoReadyFn: async () => {},
      markOfflineFn: async ({ imei }) => {
        marked.push(imei);
        return { transitioned: true };
      },
      getMongoOnline: async () => [
        { imei: "KEEP", status: "online" },
        { imei: "GONE", status: "online" },
      ],
    });
    assert.equal(result.ok, true);
    assert.equal(result.markedOffline, 1);
    assert.deepEqual(marked, ["GONE"]);
  });
});

describe("model sync uses registry runtime ids", () => {
  it("runtime id change after restart schedules new runtime handle", async () => {
    const scheduled = [];
    let devices = [{ id: 1, uniqueId: "M1", model: "SEEWORLD" }];
    const registry = createTraccarDeviceRegistry({
      getClient: () => fakeClient(() => ({ data: devices })),
      cooldownMs: 0,
      onSnapshot: (_list, { runtimeIdChanges }) => {
        for (const ch of runtimeIdChanges || []) {
          scheduled.push(ch);
        }
      },
    });
    await registry.refresh({ force: true });
    devices = [{ id: 99, uniqueId: "M1", model: "SEEWORLD" }];
    await registry.refresh({ force: true });
    assert.equal(scheduled.length, 1);
    assert.equal(scheduled[0].previousId, 1);
    assert.equal(scheduled[0].runtimeId, 99);
  });
});
