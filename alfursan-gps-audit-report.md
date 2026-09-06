# AlFursan GPS Backend Engineering Audit

**Scope:** Read-only inspection of `D:\projeccts\techno\gps-server` (entry `traccar-bridge-ontherport.js`).  
**Date:** 2026-09-06 (Africa/Cairo).  
**Method:** Source review only; no production code changes; no load tests executed.  
**Assumption labels:** `[CODE]` = verified in source; `[ASSUME]` = not observed at runtime; `[ENV]` = from `.env` / `.env.example`.

---

## 1. Executive verdict

**Can this design support 10k devices at 5–10s reporting (~1000–2000 pos/sec)?**  
**Not as currently wired.** Realtime (HTTP 202 → forward queue → classify → WS) can plausibly approach that rate on a single Node process if business work stays off the hot path. Persistence cannot: the persistence worker processes **one IPC batch at a time**, and each ordinary GPS item **awaits** parking + device-status upsert + geofence lookup + trip updates before ACK. That alone caps throughput far below 1k–2k/sec.

**Recommended reporting interval for current code:** treat **30–60s** as the practical ceiling for full fidelity (realtime + gpspoints + trips/status), or keep 5–10s **only if** persistence/business is drastically slimmed. `[ASSUME]` exact ceiling needs measurement.

**What breaks first (ranked):**
1. **Persistence IPC serial `await` chain** (parking / status / geofence / trips) — worker lag, growing `ipc_queue_depth`, then RAM.
2. **Mongo ops/sec from per-packet reads/writes** (especially overspeed `findOne` + status + geofence `findOne` + trip `updateOne`).
3. **Raw ingress full-payload Mongo writes** (duplicate durability vs gpspoints).
4. **Forward queue max 5000** — HTTP 503 when full (`TRACCAR_FORWARD_QUEUE_MAX`).
5. **Tenant-room WS fanout + JSON stringify** under many dashboard listeners (device subscribe path is better).

**Top 5 bottlenecks:**
1. Persistence worker: sequential per-item Mongo business logic (Critical).
2. Per-packet Mongo: `upsertDeviceStatus` + `evaluateGeofences` findOne + trip update + overspeed limit findOne (Critical).
3. Dual durability: `traccar_ingress_raw` + optional `gpslogs` + gpspoints spool (High).
4. Parent `gpsPointWriter` stub; all gpspoints depend on forked worker + IPC metrics (High — explains zeros).
5. Schedulers started on **both** bridge and worker (Medium–High wasted load).

---

## 2. Actual hot path (exact order)

Primary entry: `traccar-bridge-ontherport.js` listens `127.0.0.1:3053` (`:1442–1443`).

| Step | Where | What |
|------|-------|------|
| 1 | Auth middleware `:840–848` | `verifyForwardBearer` on `/traccar/position` |
| 2 | `express.json` `:850` | Parse body |
| 3 | `POST /traccar/position` `:945–992` | Content-type check → raw ingress enqueue → normalize → forward queue → **202** |
| 4 | `rawIngressWriter.enqueue(buildTraccarRawIngressDoc(...))` `:954` + `lib/traccarRawIngressWriter.js` | Disk spool → batched `TraccarIngressRaw.insertMany` |
| 5 | `normalizeForwardPayload` `lib/traccarForwardIngress.js:154–171` | Extract positions, IMEI/`uniqueId`, validate coords/command |
| 6 | `forwardQueue.enqueue` `lib/traccarForwardQueue.js` | Depth-capped queue; `setImmediate` pump |
| 7 | `processForwardIngressAsync` `:2140–2172` | Warm IMEI cache; split command vs GPS; group by IMEI |
| 8 | `processGpsBurst` `:2282–2317` | Sort by fixTime; `pickLatestLiveFromBurst`; live vs non-live bodies |
| 9 | `persistPositionBody` `:2320–2596` | Sticky attrs, distance/jump, build `doc`, **live classify + WS emit**, then `enqueuePersistForImei` |
| 10 | Realtime | `emitPositionToSocketSubscribers` → device subscribers + tenant room (throttled) via `lib/wsDelivery.js` |
| 11 | `persistenceIpc.enqueue` `:753–763` + `lib/persistenceIpc.js` | Fork `workers/persistence-worker.js`; one inflight batch |
| 12 | Worker `processPositionItem` `:255–467` | gpslogs → **enqueueGpsPoint** → parking → overspeed/ACC → status → geofence → trips → notify |

`lib/positionPipeline.js` / `createAnalyticsQueue` are **not wired** into this production path (`analyticsQueue` is a no-op stub at `:276–280`). `[CODE]`

---

## 3. Database operations per ordinary moving GPS packet

**Scenario `[CODE]`:** `type==="gps"`, valid nonzero coords, speed>5 (no parking extend), open trip already in RAM, device has speed limit set, **no fences**, no ACC transition, `GPSLOGS_WRITE_ENABLED=0` (`[ENV]` current `.env` is `0`).

| Op | Collection | When |
|----|------------|------|
| insertMany (batched) | `traccar_ingress_raw` | Raw writer flush (~1 amortized insert/request) |
| enqueue → insertMany (batched) | `gpspoints` | Worker if `doc.type==="gps"` |
| findOne | `devicestatuses` | `getSpeedLimit` in overspeed **every sample** (`overspeedService.js:25–30`) |
| findOneAndUpdate | `devicestatuses` | `upsertDeviceStatus` |
| findOne | `devicestatuses` | `evaluateGeofences` when `statusDoc` omitted — **worker omits it** (`persistence-worker.js:434–439`, `geofenceService.js:107–109`) |
| updateOne | `trips` | `appendToTrip` while moving (`persistence-worker.js:207–210`) |

**Count estimate:** ~**5 Mongo ops/packet** steady-state (1 raw + 1 gpspoint amortized + 1 overspeed read + 1 status upsert + 1 geofence read + 1 trip write ≈ 5–6; gpspoint/raw batched so peak lower, but reads/writes still fire).

**With fences:** +1 `DeviceStatus.updateOne` when fence_state updates.  
**When stopped (speed≤1):** parking `findOne`/`create`/`updateOne` adds 1–2.  
**With `GPSLOGS_WRITE_ENABLED=1`:** +1 gpslogs insert (fire-and-forget).  
**On ACC/overspeed edge:** +1 insert to `acc_events` / `overspeed_alerts` (+ notify reads).

### Ops/sec estimates from code

| Rate | Raw | gpspoints (batch 250) | Sync-ish business (status+geo+trip+overspeed) | Rough total |
|------|-----|------------------------|-----------------------------------------------|-------------|
| 1k GPS/sec | ~1k | ~4 batches/sec | ~4k | **~5k+/sec** |
| 2k GPS/sec | ~2k | ~8 batches/sec | ~8k | **~10k+/sec** |

`[ASSUME]` Mongo can absorb that only with proper indexes, wiredTiger cache, and no lock contention; the **Node worker will stall first** because those ops are awaited serially.

---

## 4. Queue inventory

| Name | Purpose | Producer | Consumer | Max size | Batch | Flush | Overflow | Retry | Spool | Ordering | IMEI blocking | Unbounded risk |
|------|---------|----------|----------|----------|-------|-------|----------|-------|-------|-----------|---------------|----------------|
| Raw ingress | Persist full Traccar JSON | POST handler | `traccarRawIngressWriter` | `maxQueueDepth` ≈ max(1000, FORWARD_QUEUE_MAX) `:122` | 250 | 100ms | reject → HTTP 503 | yes | `data/traccar-raw-ingress-spool` | file FIFO | no | spool disk growth |
| Forward queue | Decers process after 202 | POST | `processForwardIngressAsync` | 5000 `[ENV]` | 1 item/HTTP body | immediate pump | reject 503 | no | no | single runner | no | bounded |
| Persistence IPC | Business + gpspoints | `enqueuePersistForImei` | persistence worker | soft `maxQueueBatches=512`; **does not reject** when exceeded (`persistenceIpc.js:178–180`) | 1 ctx/batch typical | after ACK | backpressure counter only | worker restart requeues inflight | no | per-batch FIFO; **global serial** | **yes — one inflight for all IMEIs** | **YES — queue array can grow without hard drop** |
| GpsPoint writer (worker) | Lean archive | `enqueueGpsPoint` | `gpsPointWriter` | memHigh 10k | 250 | 100ms | spool | yes | `GPSPOINT_SPOOL_DIR` | journal FIFO | no | spool disk |
| Analytics queue | Intended heavy async | *(stub)* | *(dead)* | n/a | n/a | n/a | n/a | n/a | configured but unused | n/a | n/a | n/a |
| Device resolver pending | Wait for IMEI map | `persistPosition` | after Traccar fetch | unbounded Map lists | n/a | on resolve | warn skip | cooldown | no | per deviceId | per deviceId | **YES if many unknown deviceIds** |
| Tenant GPS throttle | Coalesce tenant room | emit path | timer flush | 1 pending/IMEI | coalesce | window/silence | drop to pending | n/a | no | last-wins pending | per IMEI slot | Map of slots (10k OK) |
| WS delivery pending | Backpressure | `wsDelivery` | flush timer | GPS map coalesces per IMEI; commands queue | coalesce GPS | bufferedAmount | terminate critical | n/a | no | GPS last-wins | per socket | pending maps |

---

## 5. CPU bottlenecks (ranked)

| Rank | Severity | Item | Evidence |
|------|----------|------|----------|
| 1 | **Critical** | Worker serial awaits (parking/status/geofence/trips) | `persistence-worker.js:341–452` |
| 2 | **Critical** | Per-packet overspeed `getSpeedLimit` Mongo read | `overspeedService.js:90` |
| 3 | **High** | JSON stringify of large subscriber payloads (merged legacy+traccar) | `mergeLegacyAndTraccarPayload` `:822–832`, `wsDelivery` serialize |
| 4 | **High** | Raw ingress `JSON.parse(JSON.stringify(rawPayload))` clone | `traccarRawIngressWriter.js:16–18,67` |
| 5 | **High** | Live classification + burst pick every position | `liveEligibility.js` / `processGpsBurst` |
| 6 | **Medium** | Haversine jump guard + sticky merge on hot path | `persistPositionBody` `:2346–2367` |
| 7 | **Medium** | Tenant throttle distance calc | `tenantRoomGpsThrottle.js` |
| 8 | **Low** | Express JSON parse of forward body | unavoidable ingress |

---

## 6. RAM risks

| Structure | File | 10k estimate | Risk |
|-----------|------|--------------|------|
| `liveFixTracker` maps | `liveEligibility.js` | ~10k entries | OK if bounded per IMEI |
| `lastGpsPointByImei` | bridge `:94` | 10k × small | OK |
| `deviceIdToImeiCache` | `:92` | ≤10k | OK |
| `imeiToRoomCache` | `:109` TTL 10min | 10k+ | OK |
| `states` / tripState | bridge + worker | 10k | OK |
| `deviceSubscribers` / `rooms` | bridge | depends on clients | fanout risk |
| `activeStops` / `activeOverspeed` / `accState` | services | ≤10k | OK |
| Forward queue | max 5000 bodies | large if bursts | bounded |
| **IPC queue** | unbounded soft | **Critical** under worker stall | can OOM |
| GpsPoint `unjournaled` | memHigh 10k | OK then spool |
| Raw spool / gpspoint spool | disk | disk full | Critical disk |
| Full `raw_payload` in Mongo + spool | raw writer | **High** RAM/disk per event |

**Leak notes:** `unresolvedTenantRoomImeis` Set (`:111`) grows without obvious prune — `[CODE]` Medium. Parent and worker both hold trip/parking/overspeed maps — duplicated.

---

## 7. Event-loop risks

- Forward queue processes with `Promise.then` + `setImmediate` — good for not blocking HTTP, but **one slow `processForwardIngressAsync`** delays the whole forward queue (`traccarForwardQueue.js:26–42`).
- Realtime emit is sync on that same async turn (before IPC) — good latency, but heavy CPU in that turn delays next forward item.
- `setImmediate` for idle/overspeed/ACC still schedules work on the **worker** loop after awaits.
- Schedulers on **both** processes (`bootBridge` `:2872–2875` **and** worker `:497–499`) add periodic timers/queries.
- WS heartbeat + throttle timers: Fine at 10k IMEIs if Maps stay lean.
- Parent stub `gpsPointWriter.flushAndStop` on shutdown does nothing real; durability depends on worker flush via IPC timeout.

---

## 8. Mongo bottlenecks

1. **Read amplification:** overspeed limit + geofence status fetch every packet even when unchanged.
2. **Write amplification:** status upsert every packet; trip `updateOne` every moving packet; parking updates while stopped; raw full documents.
3. **gpspoints index:** `{imei, packet_date}` (+ reverse). Idempotency unique partial on `(imei, traccar_position_id)` is **manual/opt-in** (`lib/gpspointsIdempotencyIndex.js`) — not auto-created at startup.
4. **`traccar_position_id` often null** when Traccar `position.id` is 0 (`normalizeTraccarPositionId` rejects ≤0) → weak idempotency under `database.memory=true`.
5. **gpslogs** (if enabled): large strict:false docs + alarm post-hooks mirroring notifications (`mongo.js:110–124`).
6. **Duplicate schedulers** hammer mileage/travel/idle/static collections from two processes.

---

## 9. Realtime / WebSocket bottlenecks

- Device subscribe path: immediate send with backpressure coalesce (`broadcastToDeviceSubscribersImmediate`).
- Tenant room: throttled (default 10 / 30s + stationary deadband) — **dashboard freshness intentionally reduced**.
- No WS auth (`TODO` at `:1383`) — security issue, not throughput.
- `normalizeSubscriberPayload` + full merge payload increases wire size.
- Critical bufferedAmount terminate protects server; slow clients get coalesced GPS (last wins) — correct for live map, bad if client expects every point.
- Command channel separate — good.

**Realtime can work while gpspoints=0** because emit is on the parent before IPC (`:2538–2576`). That matches the reported symptom. `[CODE]`

---

## 10. gpspoints correctness issues (with refs)

### Symptom alignment (raw OK, realtime OK, gpspoints counters 0)

**Verified architecture:**
- Parent `gpsPointWriter` is a **stub** returning `gpspoints_persisted_total: 0` always (`traccar-bridge-ontherport.js:137–149`).
- Real `createGpsPointWriter` + `enqueueGpsPoint` live only in `workers/persistence-worker.js:70–79,326–338`.
- Counters bump only in worker writer `enqueue` (`gpsPointWriter.js:479–481`) and are mirrored via `worker_metric` IPC (`persistence-worker.js:107–114`, `persistenceIpc.js:110–117`).

**Therefore:** `gpspoints_received_total=0` and `gpspoints_persisted_total=0` with working realtime means **either** the worker never successfully enqueues **or** worker metrics never reach the parent — not that HTTP ingress failed.

### Suspected bug: missing `type` vs `type==="gps"`

| Claim | Verdict |
|-------|---------|
| `shouldStoreGpsPoint` requires `type === "gps"` | **TRUE** `gpsPointStore.js:9–10` |
| Worker calls `enqueueGpsPoint` without `type` | **FALSE in current code** — passes `type: doc.type` (`persistence-worker.js:327–328`) |
| Parent sets `doc.type` | **TRUE** — `"alarm"` or `"gps"` (`traccar-bridge-ontherport.js:2371`, ignored alarms forced to `"gps"` `:2428–2430`) |

**Conclusion:** The “forgot to pass type” theory is **not supported by current source**. Remaining correctness gates:

1. **`doc.type === "alarm"` skips gpspoints entirely** even with valid coords (`persistence-worker.js:326`) — track gaps during alarm packets.
2. **`hasValidCoords`** false (0,0 or NaN) skips.
3. **`attrsType === 19`** skipped in `shouldStoreGpsPoint` (`gpsPointStore.js:7–15`).
4. **Worker/IPC failure** prevents any enqueue (best explanation for all-zeros).
5. **IPC soft-unbounded queue** + serial awaits → worker never catches up (received may still rise unless worker dead).

### `traccar_position_id = 0` uniqueness

- `normalizeTraccarPositionId` → `null` if not finite or `≤0` (`gpsPointStore.js:19–21`; bridge `:2384`).
- Field omitted from gpspoint doc when null (`gpsPointStore.js:45–46`).
- Unique index is **partial** `$gt: 0` (`gpspointsIdempotencyIndex.js:12–14`) — **does not reject id=0/null inserts**.
- Effect: **no idempotency** for memory-DB positions → duplicates possible; **not** a blocker that would zero `received_total`.

### Other gates preventing Mongo gpspoints

Documented conditionals that can prevent a GPS position reaching `gpspoints`:

1. Auth fail / non-JSON / raw queue full → no process (`:842–956`).
2. `normalizeForwardPayload` invalid → accepted 0 (`:959–970`) — raw may still store.
3. Forward queue full → 503 (`:981–983`).
4. Command-only (`attributes.result`) → command path, no gpspoints (`:2148–2158`).
5. Missing IMEI and failed device resolve → skip (`:2266–2273`).
6. Live classification does **not** block archive — historical still enqueued to IPC (`:2576+`).
7. Worker: `type!=="gps"` OR `!hasValidCoords` OR `shouldStoreGpsPoint` false.
8. Writer spool/disk permanent failure → quarantine / drop metrics (after receive).

---

## 11. Raw ingress performance impact

- **On every POST before 202:** clone + spool append sync (`appendFileSync`) — adds latency and disk IO on ingress path (`traccarRawIngressWriter.js:195–199`).
- Stores **full `raw_payload`** — largest collection growth driver; TTL default 7 days `[ENV]`.
- Separate Mongo insertMany stream contending with gpspoints/status/trips.
- **Value:** audit/debug when gpslogs off; proves Traccar→bridge delivery.
- **Cost at 1k–2k/sec:** ~1k–2k fat inserts/sec + sync disk journal — **High**.  
- Given raw ingress works, **gpslogs is redundant for ingress proof**; gpspoints should be the lean track store.

---

## 12. Business logic hot-path cost

| Feature | Where today | Classification | Notes |
|---------|-------------|----------------|-------|
| Live classify + WS | Parent hot path | **KEEP ON HOT PATH** | Core product |
| IMEI resolve / cache | Parent | **KEEP ON HOT PATH** / **CACHE IN RAM** | Already cached |
| Jump/distanceDiff | Parent before IPC | **MOVE TO ASYNC** or worker | CPU on live turn |
| Sticky telemetry merge | Parent | **CACHE IN RAM** | OK small |
| Raw ingress write | Ingress | **MOVE TO ASYNC** (already queued) / consider sample | Fat |
| gpspoints enqueue | Worker early | **KEEP** but before awaits; batch | Correct place |
| gpslogs | Worker | **REMOVE** or optional off | `.env` already 0 |
| Device status upsert | Worker awaited | **STATE TRANSITION ONLY** + throttle | Every packet is wasteful |
| Geofence eval | Worker awaited | **STATE TRANSITION ONLY** + RAM fence cache | Avoid findOne/packet |
| Parking | Worker awaited | **STATE TRANSITION ONLY** | Update on edges + periodic heartbeat |
| Trips | Worker awaited | **STATE TRANSITION ONLY** | Don’t `updateOne` every point; buffer distance |
| Overspeed | Worker setImmediate but `await getSpeedLimit` | **CACHE IN RAM** limit; edge persist | kill per-packet findOne |
| ACC | setImmediate | **STATE TRANSITION ONLY** | OK pattern |
| Idle notify | setImmediate | **STATE TRANSITION ONLY** | OK |
| FCM / notifications | On edges | **MOVE TO ASYNC** | Already mostly |
| Mileage/travel/idle/static schedulers | Parent **and** worker | **PERIODIC** — run **once** | Duplicate today |
| Tenant throttle | Parent emit | **KEEP** for dashboard | Don’t apply to device subscribe |

---

## 13. Features to remove or make optional

| Feature | Cost | Value | Breaks if removed | Gain | Recommendation |
|---------|------|-------|-------------------|------|----------------|
| `GPSLOGS_WRITE_ENABLED` | High write amp + alarm hooks | Legacy queries/reports | Anything reading `gps_logs` | Large | **Keep optional default 0**; do not turn on at 10k. Raw+gpspoints suffice for track/audit. Current `[ENV]=0`. |
| Full raw ingress every packet | High | Debug/durability | Lose verbatim Traccar body | Large | **Sample or compress**; or spool-only without Mongo for steady state |
| `persistPositionHeavy` + analytics stubs | Maintenance confusion | None (unwired) | Nothing | Clarity | Delete or wire — currently dead |
| Dual schedulers (parent+worker) | 2× periodic load | Stats | None if one remains | Medium | Run only in worker **or** only in parent |
| Alarm→skip gpspoints | Correctness gap | — | — | Track integrity | Store gpspoints whenever coords valid |
| Tenant room unthrottled | CPU/WS | Live fleet map | — | — | Keep throttle |
| `createPositionPipeline` unused module | Drift risk | Tests only | Tests | — | Either adopt or stop claiming it is production |

---

## 14. Legacy / dead code

| Item | Evidence | Status |
|------|----------|--------|
| `traccar-bridge.js` | 9-line redirect to ontherport | Deprecated entry |
| Old Traccar WS / cookie auth / polling | Not present in ontherport; REST bearer for commands only | Removed from active path |
| `createAnalyticsQueue` import + stub | `:28`, `:276–280` | Dead |
| `persistPositionHeavy` + `persistPositionHeavyRef` | `:2599–2854`, `:2856`; **no callers** of ref | Dead dual path |
| `createPositionPipeline` | Not required by bridge | Test/helper only |
| Parent `gpsLogsWriter` / `gpsPointWriter` stubs | `:128–149` | Placeholders; real work in worker |
| `deviceId` mapping | Still used as fallback via Traccar API + cache | Needed when forward lacks `uniqueId` |
| Dual ingress | Single HTTP forward (`traccar_ingress: http-forward`) | OK |

---

## 15. Recommended target architecture (simple)

Keep **Node + Mongo + Traccar (memory DB / parser only)**.

```
Device → Traccar (parse) → HTTP Forward → Bridge :3053
                              │
                              ├─ AUTH + normalize + IMEI(uniqueId)
                              ├─ LIVE: classify → WS (device + throttled tenant)
                              ├─ ACK 202 quickly
                              └─ Persist queue (async, batched, preferably multi-worker by IMEI hash)
                                   ├─ gpspoints batch insert (coords-valid; type irrelevant)
                                   ├─ device status (throttled / on change)
                                   ├─ trips/parking/ACC/overspeed/geofence (edge + RAM state)
                                   └─ optional raw sample / short TTL
```

Rules:
- Persistent identity = **IMEI / uniqueId** only.
- Never await business Mongo on the live emit path (already mostly true on parent).
- Never make gpspoints depend on `type==="gps"` if coords are valid.
- One process owns schedulers.
- Hard-cap IPC queue with spool-to-disk (like gpspoints), not unbounded RAM.

---

## 16. Prioritized plan P0–P3

### P0
1. **Problem:** Persistence worker serial awaits + soft-unbounded IPC → gpspoints stall while realtime lives.  
   **Evidence:** `persistenceIpc.js:157–168,171–182`; `persistence-worker.js:470–474,341–452`; parent stub writer `:137–149`.  
   **Why:** Explains counters=0 / backlog.  
   **Change:** Hard-cap IPC; spool overflow; process gpspoints enqueue without awaiting parking/geo/trips; ACK early after gpspoint journal.  
   **Benefit:** Restores archive under load.  
   **Risk:** Temporary inconsistency of trip/status lag (acceptable).

2. **Problem:** Alarm-typed packets skip gpspoints.  
   **Evidence:** `:2371` + worker `:326`.  
   **Change:** Enqueue gpspoints on valid coords regardless of alarm type.  
   **Benefit:** Continuous tracks.  
   **Risk:** Slightly more points.

3. **Problem:** Per-packet overspeed `findOne` + geofence `findOne`.  
   **Evidence:** `overspeedService.js:25–30,90`; worker omits `statusDoc`.  
   **Change:** Cache speed limits + fence list in RAM; refresh on TTL/change.  
   **Benefit:** −2 reads/packet.  
   **Risk:** Stale limit until refresh.

### P1
4. Throttle device status + trip writes (edge / 5–10s / distance).  
5. Stop double schedulers.  
6. Make raw ingress sampling or spool-first; confirm TTL.  
7. Idempotency key: `(imei, fixTime, lat, lon)` when `position.id` is 0 — do not rely on Traccar id.

### P2
8. Remove/quarantine `persistPositionHeavy` dead code; wire or delete `positionPipeline` consistently.  
9. Shrink WS payload (don’t merge full Traccar raw to clients).  
10. Cap `deviceResolver` pending queues.

### P3
11. Optional second persistence worker with IMEI hash partitions (still Node+Mongo, no Kafka).  
12. Load-test harness using existing `test/load.test.js` patterns against staging.

---

## 17. Things NOT to optimize

- Introducing Redis/Kafka/K8s before fixing serial awaits and per-packet finds — not justified by code yet.
- Micro-optimizing live eligibility math before cutting Mongo reads.
- Perfect unique `traccar_position_id` under memory DB — pick a bridge-side idempotency key instead.
- Turning `GPSLOGS_WRITE_ENABLED=1` “for safety” at scale — raw + gpspoints already cover durability layers.
- Unthrottling tenant rooms “for fairness” — will melt WS.
- Premature multi-bridge sharding before one process is lean.

---

## 18. Suggested load-test scenarios (design only — do not run)

1. **Ingress soak:** 1k and 2k POST `/traccar/position`/sec, 1 pos each, valid IMEI+coords, no alarms; watch forward depth, event_loop_lag, raw_ingress_*, ipc_queue_depth, gpspoints_*.
2. **Worker stall inject:** Pause Mongo or add 50ms sleep in worker; confirm realtime stays healthy and IPC/spool behavior (expect problem today).
3. **Alarm mix:** 20% packets with `attributes.alarm`; verify gpspoints continuity after P0#2.
4. **position.id=0:** All ids 0; ensure gpspoints insert and no unexpected unique errors.
5. **IMEI cold start:** Empty `deviceIdToImeiCache`, forward without uniqueId; measure resolver backlog.
6. **WS fanout:** 200 tenant-room clients + 500 device subscribers; measure broadcast_per_sec, slow_subscriber, terminations.
7. **Stop/move cycles:** Speed 0↔40 to stress parking+trips writes.
8. **Command responses:** Burst `attributes.result` to ensure GPS path not blocked.
9. **Raw TTL / disk:** Fill spool directories; verify 503 vs spool critical behavior.
10. **Scheduler contention:** Compare single vs dual scheduler CPU/Mongo (current code dual).

---

## Appendix A — Hot-path conditionals (cheat sheet)

```
POST /traccar/position
  !Bearer OK → 401/503
  !JSON → 415
  raw enqueue rejected → 503
  normalize invalid → 202 accepted:0 (raw kept)
  forward queue full → 503
  attributes.result → command path (no gpspoints)
  no IMEI & resolve fail → drop
  persistPositionBody → always IPC enqueue (if not command)
  WS only if liveEligible && acceptLive && emitLive
Worker:
  type!=="gps" → no gpspoints
  !hasValidCoords → no gpspoints
  attrsType 19 → shouldStore false
  writer/disk failure → retry/quarantine after receive
```

## Appendix B — Env snapshot (non-secret)

From project `.env` (read-only): `GPSLOGS_WRITE_ENABLED=0`, `TRACCAR_FORWARD_QUEUE_MAX=5000`, `TRACCAR_RAW_RETENTION_DAYS=7`, GPSPOINT batch 250 / flush 100ms / memHigh 10000.  
`.env.example` matches defaults including `GPSLOGS_WRITE_ENABLED=0`. User note that production has `GPSLOGS=1` is **not** what this `.env` currently contains — treat as environment-specific `[ASSUME]` elsewhere.

---

*End of report.*
