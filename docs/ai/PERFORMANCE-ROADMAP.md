# AlFursan GPS Backend — Performance Roadmap

**Source of truth for AI agents.** Update after every completed phase. CURRENT HEAD > old audit reports.

Last updated: 2026-09-06 — **P0-B implemented (awaiting manual deploy/verify)**

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

## Execution order (STRICT)

| Phase | Name | Status |
|-------|------|--------|
| **P0-A** | Raw writer drain deadlock | **COMPLETE + PRODUCTION VERIFIED** (2026-09-06) |
| **P0-B** | Separate raw failure domain from realtime + exact retry dedupe | **THIS SLICE** (code+tests; deploy manually) |
| **P0-B2** | Dedicated Raw Archive Worker (process isolation) | Documented next isolation step |
| **P0-C** | Analyze remaining live OOO/stale after exact-retry suppression; RT latency metrics | After P0-B prod verify |
| **P0-D** | Degradation survival (listeners alive when raw/Mongo sick) | After P0-C |
| **P0-E** | Gpspoints async Mongo drain / batch efficiency investigation | After fresh prod sample (spool depth/age trend) |
| **FUNCTIONAL** | SEEWORLD / `tr_model` / `attributes.power` live E2E | Note: `traccar_power_seen_total` observed >0 |
| **Phase 2** | Geofence transition-only Mongo writes | Later |
| **Phase 3** | Business queue transition correctness | Later |
| **Phase 4** | Exactly 8 IMEI-partition workers | Later |
| **Phase 5-A** | Full reporting/scheduler audit | Later |
| **Phase 5-B** | ONE analytics worker; partition workers must not start global schedulers | Later |
| **Phase 5-C** | Report locks/batching/idempotency + `REPORTING-DATA-CONTRACT.md` | Later |
| **Phase 6** | Mileage N+1 / query amplification | Later |
| **Phase 7** | Mongo/index + Mongo/Traccar/Node/Linux tuning | Later |
| **Phase 8** | Controlled load/soak — **only when explicitly authorized** | Later |

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
- `FORWARD_RETRY_DEDUPE_MAX=50000`

### Expected post-deploy

- Raw queue full / durable fail does **not** freeze listeners on first packet
- `raw_failure_realtime_continued_total` may increment under raw pressure
- `forward_exact_retry_total` rises when Traccar retries after 503
- Live eligibility may improve further vs post-P0-A (still do **not** loosen freshness in P0-B)

---

## P0-C notes (do not implement yet)

Post-P0-A live eligibility ≈0.51, OOO still high. After P0-B exact-retry metrics, re-measure before changing freshness/order/tenant throttle.

---

## P0-E notes (do not implement yet)

Observed: gpspoints journaled 8508 / persisted 6380; spool files 1327; oldest age ~100s; docs/flush avg ~1.71; IPC healthy.  
Next: another production sample to see if spool age/depth fall or grow, then investigate drain/batching.

---

## FUNCTIONAL — power

`traccar_power_seen_total` observed (e.g. 6). Model sync exists. Full E2E verification later — do not calculate voltage in Node.

---

## Startup reconciliation

Empty Traccar device list → mark Mongo-online devices offline remains intentional. Reporting audit must check duplicate outage records on restart (Phase 5).

---

## Completed (do not regress)

- Traccar gateway; Node owns RT/business/history; gpslogs gone; lean gpspoints; raw forensic
- Realtime ≠ business; status/trip coalesce; overspeed cache; schedulers not duplicated on worker
- IPC bound+spool; parent IPC batching; gpspoints journal before ACK (no force Mongo flush)
- Startup reconciliation; tr_model sync; **P0-A raw drain**; **P0-B raw/RT failure domains**

---

## SLOs (steady-state targets)

Ingress→WS p99 preferably &lt;250ms; event-loop p99 preferably &lt;50ms; queues not growing; transition drops=0 (Phase 3); no claim of 10k-ready early.

---

## MUST NOT

Skip phase order; Redis/Kafka now; PM2 cluster whole app; weaken freshness in P0-B; optimize gpsPointWriter in P0-B; disable/sample raw; push/deploy from agent; full suite/load unless asked.
