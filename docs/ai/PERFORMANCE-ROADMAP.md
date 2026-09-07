# AlFursan GPS Backend — Performance Roadmap

**Source of truth for AI agents.** Update after every completed phase. CURRENT HEAD > old audit reports.

Last updated: 2026-09-07 — **E2/E3 production hardening (writer metrics + 5s seal age)**

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
3. **gpspoints-writer** (`alfursan-gpspoints-writer`; one instance; fork)  
4. **alfursan-analytics** (exactly one; global schedulers only)  
5. 8 IMEI-partitioned business/persistence workers (journal + business; no global schedulers)

No Redis. No full-app PM2 cluster. Batching **and** process isolation both required.

---

## Execution order (STRICT)

| Phase | Name | Status |
|-------|------|--------|
| **P0-A** | Raw writer drain deadlock | **COMPLETE + PRODUCTION VERIFIED** |
| **P0-B** | Raw vs realtime failure domains + retry dedupe | **HARDENED** |
| **P0-B2** | Dedicated raw archive worker | Documented |
| **P0-C** | Live OOO/stale after retry suppression | After P0-B verify |
| **P0-D** | Degradation survival | After P0-C |
| **P0-E1** | Gpspoints multi-file Mongo combine + doc fairness | **COMPLETE + PRODUCTION VERIFIED** |
| **P0-E2** | Segmented durable journal + legacy dual-read | **COMPLETE (code + focused tests)** |
| **P0-E3** | Dedicated PM2 `alfursan-gpspoints-writer` | **COMPLETE (code + focused tests)** |
| **FUNCTIONAL** | SEEWORLD / tr_model / power E2E | Later |
| **Phase 2** | Geofence transition-only writes | Later |
| **Phase 3** | Business queue transition correctness | Later |
| **Phase 4** | 8 IMEI workers | Later |
| **Phase 5-A/B/C** | Reporting audit → migrate to `alfursan-analytics` | Later |
| **Phase 6–8** | Query/index/OS tuning; authorized soak | Later |

---

## P0-E1 — PRODUCTION VERIFIED

Immediately after E1 deploy: spool **131,281** files → catch-up Mongo batch avg ~206 / max 250.

During catch-up (temporary contention): raw durable p99 ~5s; business lag ~1794ms; event-loop p99 ~124ms; IPC ACK max ~2508ms.

After several minutes: spool **4** files / 809 bytes / oldest age **79ms**; received 16,038; persisted 169,057; batch avg 51.97 max 250; failures 0; business lag 11ms; event-loop p99 35ms; raw durable p99 6ms.

Proved: E1 batching works; legacy backlog drains faster than ingest; ~1.19 docs/flush failure fixed; dedicated writer isolation justified. Disk I/O contention can remain at OS level even after process split.

---

## P0-E2 — COMPLETE (code)

Segmented JSONL journal:

- `gps-<ts>-<id>.jsonl.active` → append → seal → `.jsonl.ready`
- Seal on docs **or** bytes **or** `GPSPOINT_SEGMENT_MAX_AGE_MS` (default **5000**; was 200 — too aggressive at ~20–30 docs/s)
- ACK = local append only (seal not required for durability)
- Legacy `hot-`/`pending-` still drained until extinct
- Focused tests: `test/gpspointsSegmentJournal.test.js` + `test/gpspointsProdHardening.test.js`

---

## P0-E3 — COMPLETE (code) + hardening

- Entrypoint: `workers/gpspoints-writer.js`
- PM2: `alfursan-gpspoints-writer` in `ecosystem.config.cjs` (instances=1, fork, autorestart)
- Persistence: `GPSPOINT_EXTERNAL_WRITER=1` → mode `producer` (journal/ACK only)
- Dual-drain forbidden via `gpspoints-drain.lock`
- Own Mongo pool: `GPSPOINT_WRITER_MONGO_MAX_POOL` default **8**
- Rich status heartbeat (~1s): `/health` exposes `gpspoints_writer_persisted_total`, `gpspoints_writer_mongo_*`, batch/latency/backlog — **separate from** producer `gpspoints_*` Mongo counters
- Focused tests: `test/gpspointsWriterProcess.test.js`, `test/gpspointsProdHardening.test.js`

### Deploy / rollback (manual — agent does not deploy)

1. Deploy code  
2. Set `GPSPOINT_EXTERNAL_WRITER=1`  
3. `pm2 start ecosystem.config.cjs` or `pm2 restart alfursan-bridge alfursan-gpspoints-writer`  
4. Rollback: stop writer → `GPSPOINT_EXTERNAL_WRITER=0` → restart persistence/bridge  

---

## A0 — Reporting data contract — COMPLETE (docs)

- [`docs/ai/REPORTING-DATA-CONTRACT.md`](./REPORTING-DATA-CONTRACT.md)
- CURRENT vs TARGET for Mileage / Travel / Idle / Static / Parking / ACC / Overspeed / Connectivity / Trips
- Documents: Cairo day TARGET, OverspeedAlert vs speed&gt;120, ignition=null idle, gps/alarm duplicate risk
- **No algorithm redesign** in A0

---

## A1 — Analytics worker + scheduler ownership — COMPLETE (code)

- Entrypoint: `workers/analytics-worker.js`
- PM2: `alfursan-analytics` (fork, instances=1)
- Mongo pool: maxPoolSize=8, minPoolSize=1 (`ANALYTICS_MONGO_*` → `MONGO_*` before `mongo.js` load)
- Ownership: `REPORT_SCHEDULER_OWNER=analytics` (default) | `bridge` (rollback)
- Dual-run guard: role skip + `data/analytics-spool/report-schedulers.lock`
- Per-job overlap gates; staggered startup; Static after DailyMileage
- Heartbeat → bridge `/health` `analytics_*`
- Focused tests: `test/analyticsOwnership.test.js`
- **Not in A1:** parking backfill migrate, revive `analyticsQueue.js`, Mileage N+1, Travel×7 redesign

### Deploy / rollback (manual — agent does not deploy)

1. Deploy code  
2. Ensure `REPORT_SCHEDULER_OWNER=analytics`  
3. `pm2 start ecosystem.config.cjs` or restart `alfursan-bridge` + `alfursan-analytics`  
4. Rollback: `pm2 stop alfursan-analytics` → set `REPORT_SCHEDULER_OWNER=bridge` → restart bridge  

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
4. On raw durable failure → 503 after realtime already ran  
5. Exact retry fingerprint suppresses live+persistence; raw may retry  

Defaults: `FORWARD_RETRY_DEDUPE_TTL_MS=120000`, `FORWARD_RETRY_DEDUPE_MAX=100000`.

---

## P0-C notes (do not implement yet)

Post-P0-A live eligibility ≈0.51, OOO still high. Re-measure after P0-B/E3 deploy before changing freshness.

---

## FUNCTIONAL — power / model sync

Do not calculate voltage in Node. Model sync is lifecycle-based (ARCHITECTURE.md).

---

## Completed (do not regress)

- Traccar gateway; lean gpspoints; raw forensic; IPC journal-before-ACK  
- **P0-A** raw drain; **P0-B** raw/RT domains; **P0-E1** verified; **P0-E2** segments; **P0-E3** dedicated writer  

---

## MUST NOT

Skip phase order; Redis/Kafka now; PM2 cluster whole app; dual gpspoints drain; 8 writers without proof; push/deploy from agent; full suite/load unless asked.
