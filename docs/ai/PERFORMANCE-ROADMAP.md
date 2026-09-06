# AlFursan GPS Backend — Performance Roadmap

**Source of truth for AI agents.** Update after every completed phase. CURRENT HEAD > old audit reports.

Last updated: 2026-09-06 — **P0-A COMPLETE** (awaiting manual prod deploy/verify; next = P0-B)

---

## Goals

Move toward ~10,000 devices @ 5–10s (~1,000–2,000+ pos/sec) with **realtime latency first**.

Server: 12 vCPU, 31 GB RAM, ~1 TB disk. Node v20, PM2, Mongo `127.0.0.1:27017`, Traccar in-memory gateway.

**Do not claim 10k-ready** until Phase 8 soak is explicitly authorized and green.

---

## Priority order

1. Realtime  
2. GPS history  
3. Business transitions  
4. Raw forensic archive  
5. Analytics / reports  

---

## SLOs (steady-state targets)

| Area | Target |
|------|--------|
| Ingress → WS | p99 preferably &lt; 250 ms (aim lower under normal load) |
| Event loop | p99 preferably &lt; 50 ms |
| Forward queue | near zero; rejected not rising |
| Raw | accepted ≈ persisted over time; queue normally low; spool drains after shocks |
| Gpspoints | queue/spool → 0; ack tracks received |
| Business | lag bounded; **transition drops = 0** (Phase 3) |
| Mongo | no pool explosion; no sustained disk runaway |
| CPU | no single Node pegged while cores idle; leave headroom for Traccar/Mongo |
| Memory | no OOM; swap only emergency |

---

## Completed (do not regress)

- [x] Traccar = protocol gateway; Node owns realtime/business/history
- [x] `gpslogs` removed from active path
- [x] Lean `gpspoints`; raw `traccar_ingress_raw` short-term forensic
- [x] Realtime separated from business
- [x] DeviceStatus / trip write coalescing
- [x] Overspeed limit cache
- [x] Duplicate analytics schedulers on parent+worker removed (schedulers currently on **parent only**)
- [x] Persistence IPC bounded + disk spool
- [x] Parent IPC batching (`PERSISTENCE_IPC_BATCH_MAX_ITEMS=100`, `WAIT_MS=5`)
- [x] Gpspoints journaled before IPC ACK; ACK does not wait Mongo force-flush
- [x] Startup connectivity reconciliation
- [x] Traccar `tr_model` sync (verify after PUT)
- [x] No Redis/Kafka/RabbitMQ/MQTT

---

## Execution order (STRICT)

| Phase | Name | Status |
|-------|------|--------|
| **P0-A** | Fix raw writer drain deadlock | **DONE** (code+tests; deploy manually) |
| **P0-B** | Separate raw failure domain from realtime + retry dedupe | Next after prod verify |
| **P0-C** | Re-measure retry / OOO / freshness; RT latency metrics | After P0-B |
| **P0-D** | Prove listeners survive raw/Mongo degradation | After P0-C |
| **FUNCTIONAL** | SEEWORLD / `tr_model` / `attributes.power` live E2E | After P0 stable |
| **Phase 2** | Geofence transition-only Mongo writes | Later |
| **Phase 3** | Business queue transition correctness | Later |
| **Phase 4** | Exactly 8 IMEI-partition workers (`stableHash(imei)%8`) | Later |
| **Phase 5-A** | Full reporting/scheduler audit (inventory before edits) | Later |
| **Phase 5-B** | Move global schedulers to ONE analytics worker | Later |
| **Phase 5-C** | Report locks, batching, idempotency, fields, indexes + `REPORTING-DATA-CONTRACT.md` | Later |
| **Phase 6** | Mileage N+1 / remaining query amplification | Later |
| **Phase 7** | Mongo index audit + Mongo/Traccar/Node/Linux/swap/fd tuning | Later |
| **Phase 8** | Controlled load/soak — **only when explicitly authorized** | Later |

Workflow each phase: inspect HEAD → update this doc → narrow impl → syntax + focused tests → report expected metrics → update this doc → **STOP for review**.

---

## P0-A — Raw drain deadlock

### Problem (production)

`raw_ingress_queue_depth=5000`, `mongo_attempted_total=0`, massive rejects → HTTP 503 → Traccar retry storm → listeners freeze.

### Root cause

`flushCycle` refused to rotate/drain while `activeWriteQueue.length > 0`, while pump stopped at `batchSize` waiting for that flush → deadlock under sustained ingress.

### Fix

- Single flush owner; rotate only when `writingActive === false`; `rotating` gate for rename vs append
- Non-empty memory queue does not block rotate/drain
- Fair cycles: rotate → bounded spool drain → wake pump → schedule next
- Preserve HTTP 503-before-realtime (**P0-B changes this**)

### Expected post-deploy

`mongo_attempted` / `persisted` rising; queue/rejected not pinning; spool draining; forward continuing.

---

## P0-B — Raw vs realtime failure domains (NOT YET)

Preferred flow: auth → normalize → **realtime first** → persistence → independent raw durability → HTTP per safe durability rules.

Raw queue/Mongo/spool failure must never mean “user saw no GPS”.

Add low-cost Traccar retry fingerprint (not `position.id`); metrics:

- `forward_exact_retry_total`
- `forward_retry_suppressed_live_total`
- `forward_retry_suppressed_persistence_total`

---

## P0-C / P0-D

After retry storm ends, re-check live eligibility / OOO / stale. Do not loosen freshness blindly. Add aggregate ingress→broadcast latency. Prove RT survives raw degradation. **Do not change tenant throttle in P0** (`tenant_room_gps_window_ms=30000`, max 10, stationary 30s) until metrics prove a problem.

---

## FUNCTIONAL — Power / model

Mongo `tr_model` (e.g. SEEWORLD) → Traccar model sync → GT06 status `0x13` / type 19 → `attributes.power`. Node must not compute voltage.

---

## Phase 2 — Geofence

Evaluate every GPS packet; persist only membership transitions. Throttle `last_checked_at` if required. No per-packet geofence Mongo read when status already in hand.

---

## Phase 3 — Business queue

Classify LAST-VALUE SAFE vs TRANSITION-SENSITIVE. Never silently coalesce away parking/trip/ACC/geofence/connectivity edges. Bounded policy only.

---

## Phase 4 — 8 workers

`workerIndex = stableHash(imei) % 8`. Same IMEI → same worker forever (ordering).

**Global** business concurrency budget (~64–128 total → ~8–16/worker). Size Mongo `maxPoolSize` per process — do not multiply pools by 8 blindly. Crash → restart same partition; no opportunistic remap. Do not hard-pin CPUs without profiling.

---

## Phase 5 — Analytics / reporting

**Invariant:** partition workers do **not** own global scheduled analytics.

Current HEAD note (quick): mileage/travel/idle/static schedulers start on **realtime parent** (`traccar-bridge-ontherport.js`); persistence-worker does not start them. Flag: heavy parent schedulers are a **P1** risk for event-loop/Mongo contention (Phase 5 moves them). Full inventory is Phase 5-A deliverable before edits.

Also create `docs/ai/REPORTING-DATA-CONTRACT.md` in 5-C.

---

## Phase 6–8

Mileage batch/`$in`/aggregation; index audit (gpspoints write-heavy — no blind indexes); WiredTiger/JVM/Node heap/swap(~8–16G, swappiness~10)/nofile — measure first; soak only when authorized.

---

## Observability rules

- No per-IMEI high-cardinality metric labels
- Fixed `worker_0`…`worker_7` depths/totals OK later
- Health endpoint remains primary ops surface

---

## MUST NOT

Rewrite everything; Redis/Kafka/etc now; PM2 cluster whole WS app; 8 full app copies; GT06 parse / voltage in Node; gpslogs; disable raw / strip payload; unbounded queues; force Mongo flush before IPC ACK; `position.id=0` identity; skip ahead in phase order; push/deploy from agent; full test suite / load tests unless asked.
