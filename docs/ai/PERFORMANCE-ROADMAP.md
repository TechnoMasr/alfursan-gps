# AlFursan GPS Backend — Performance Roadmap

**Source of truth for AI agents.** Update after every completed phase. CURRENT HEAD > old audit reports.

Last updated: 2026-09-07 — **P0-E1 complete; E2 next; E3 dedicated writer committed**

---

## Goals

~10,000 devices @ 5–10s (~1–2k+ pos/sec) with **realtime latency first**.

Server: 12 vCPU, 31 GB RAM. Node v20, PM2, Mongo localhost, Traccar memory gateway.

**Do not claim 10k-ready** until Phase 8 soak is explicitly authorized and green.

---

## Priority order

1. Realtime  
2. GPS history  
3. Business transitions  
4. Raw forensic archive  
5. Analytics / reports  

---

## Final process map (COMMITTED)

1. realtime / bridge parent  
2. raw archive worker  
3. **gpspoints-writer** (dedicated PM2; **one** instance initially — failure-domain / event-loop isolation)  
4. **alfursan-analytics** (dedicated PM2; EXACTLY ONE; global schedulers/reports only)  
5. 8 IMEI-partitioned business/persistence workers (journal + business; **must not** start global schedulers)

No Redis. No full-app PM2 cluster. Do not create multiple full copies of the whole application.

**Both required:** correct Mongo batching **and** process isolation (CPU, event loops, crashes, Mongo pools, restarts).

---

## Execution order (STRICT)

| Phase | Name | Status |
|-------|------|--------|
| **P0-A** | Raw writer drain deadlock | **COMPLETE + PRODUCTION VERIFIED** |
| **P0-B** | Raw vs realtime failure domains + retry dedupe | **HARDENED** |
| **P0-B2** | Dedicated raw archive worker | Documented |
| **P0-C** | Live OOO/stale after retry suppression | After P0-B verify |
| **P0-D** | Degradation survival | After P0-C |
| **P0-E1** | Gpspoints: combine legacy files → useful Mongo batches + doc fairness | **COMPLETE (code + focused tests)** |
| **P0-E2** | Segmented durable journal + legacy reader | **NEXT** |
| **P0-E3** | Dedicated PM2 `gpspoints-writer` (Mongo drain only) | After E2 — **architectural; not optional long-term** |
| **FUNCTIONAL** | SEEWORLD / tr_model / power E2E | Later |
| **Phase 2** | Geofence transition-only writes | Later |
| **Phase 3** | Business queue transition correctness | Later |
| **Phase 4** | 8 IMEI workers | Later |
| **Phase 5-A/B/C** | Reporting audit → migrate to `alfursan-analytics` | Later |
| **Phase 6–8** | Query/index/OS tuning; authorized soak | Later |

---

## P0-E — GPSPoints (committed staged plan)

**Root cause (agreed):** tiny per-IPC-batch spool files (~1 doc/file) + **one `insertMany` per file** → ~115k file backlog, `docs_per_flush_avg≈1.19`. Journal/ACK OK; Mongo drain under-batched.

| Phase | Scope | Status |
|-------|--------|--------|
| **E1** | Multi-file combine → one `insertMany` (250–500); **doc**-based old/new fairness (not 2-new/1-old file slots); raise drain budget; preserve durability + ACK=journal only; **no** format change; **no** PM2 split | **DONE** |
| **E2** | Segmented journal (`active`→`sealed/ready`→`draining`→done); dual-read legacy until zero | Next |
| **E3** | Move drain to `gpspoints-writer`; producer journals+ACKs only; own Mongo pool; no WS/business/reports/model-sync/tenant broadcast | After E2 |

Batching targets (configurable, validate in prod): batch 250–500; flush size OR short deadline. At ~22 docs/s today; at 1k docs/s → ~2–4 Mongo batches/s @ 500/250; at 2k → ~4–8/s.

E1 defaults: `GPSPOINT_BATCH_SIZE=250`, `GPSPOINT_MAX_MONGO_BATCHES_PER_CYCLE=16`, `GPSPOINT_DRAIN_MAX_FILES_PER_CYCLE=500`, `GPSPOINT_DRAIN_OLD_DOC_RATIO=0.5`, `GPSPOINT_JOURNAL_COALESCE_MS=50`.

**STOP after E1** until E2 is explicitly started. Do not combine E1/E2/E3 into one rewrite.

---

## P0-A — VERIFIED

Before: queue=5000, mongo_attempted=0, rejected≈150k.  
After: accepted=received, rejected=0, mongo attempted=persisted, queue/spool≈3, old spool drained.

Do not revisit unless regression.

---

## P0-B — Raw must not kill realtime

### Problem

Raw enqueue reject → HTTP 503 **before** normalize/forward/WS.

### Fix

1. Initiate raw archival without gating realtime
2. Dispatch forward queue on first fingerprint
3. Await **local journal durable** only (not Mongo) for HTTP 202
4. On raw durable failure → 503 (Traccar retries raw) after realtime already ran
5. Exact retry fingerprint suppresses live+persistence; raw may retry

### Defaults

- `FORWARD_RETRY_DEDUPE_TTL_MS=120000`
- `FORWARD_RETRY_DEDUPE_MAX=100000` (~50 MB @ ~500 B/entry)
- Future 1–2k pkt/s: raise MAX to **250000** (~125 MB) **or** shorten TTL to 60s — watch `forward_retry_cache_evicted_capacity_total`
- Do not remove `serverTime` from fingerprint without evidence (Traccar retries reuse same Position object)

### Expected post-deploy

- Raw queue full / durable fail does **not** freeze listeners on first packet
- `raw_failure_realtime_continued_total` may increment under raw pressure
- `forward_exact_retry_total` rises when Traccar retries after 503
- Watch `raw_durable_accept_latency_p99_ms` and retry-cache eviction counters
- Live eligibility may improve further vs post-P0-A (still do **not** loosen freshness in P0-B)

---

## P0-C notes (do not implement yet)

Post-P0-A live eligibility ≈0.51, OOO still high. After P0-B exact-retry metrics, re-measure before changing freshness/order/tenant throttle.

---

## P0-E1 — COMPLETE (code)

Implemented in `lib/gpsPointWriter.js` + `workers/persistence-worker.js` env wiring:

- Open many legacy `.jsonl` files per cycle; **combine** docs into `insertMany` up to `batchSize`
- Doc-budget fairness between oldest and newest spool files
- Metrics: `gpspoints_mongo_docs_per_flush_*`, `gpspoints_spool_docs`

**Do not start E2** until authorized. After E1 deploy, watch: spool file count ↓, `docs_per_flush_avg` → ~batch size, oldest age ↓, journaled≈received, ACK still journal-only.

---

## FUNCTIONAL — power / model sync

`traccar_power_seen_total` observed (e.g. 6). Full SEEWORLD E2E later — do not calculate voltage in Node.

**Model sync (2026-09-06):** lifecycle-based, not packet-based. Negative cache default **10 min**. No per-packet `skipped: no tr_model` flood. Traccar restart (`model=null`) still forces one re-sync. See ARCHITECTURE.md.

---

## Startup reconciliation

Empty Traccar device list → mark Mongo-online devices offline remains intentional. Reporting audit must check duplicate outage records on restart (Phase 5).

---

## Completed (do not regress)

- Traccar gateway; Node owns RT/business/history; gpslogs gone; lean gpspoints; raw forensic
- Realtime ≠ business; status/trip coalesce; overspeed cache; schedulers not duplicated on worker
- IPC bound+spool; parent IPC batching; gpspoints journal before ACK (no force Mongo flush)
- Startup reconciliation; tr_model sync; **P0-A raw drain**; **P0-B raw/RT failure domains**; **P0-E1 multi-file Mongo combine**

---

## SLOs (steady-state targets)

Ingress→WS p99 preferably &lt;250ms; event-loop p99 preferably &lt;50ms; queues not growing; transition drops=0 (Phase 3); no claim of 10k-ready early.

---

## MUST NOT

Skip phase order; Redis/Kafka now; PM2 cluster whole app; weaken freshness in P0-B; optimize gpsPointWriter in P0-B; disable/sample raw; push/deploy from agent; full suite/load unless asked; combine E1+E2+E3 into one uncontrolled rewrite; create 8 gpspoints-writers without throughput proof.
