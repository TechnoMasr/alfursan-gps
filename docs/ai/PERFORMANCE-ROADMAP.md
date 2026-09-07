# AlFursan GPS Backend — Performance Roadmap

**Source of truth for AI agents.** Update after every completed phase. CURRENT HEAD > old audit reports.

Last updated: 2026-09-07 — **FINAL REPORTING / ANALYTICS STABILIZATION CLOSED**

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
| **Phase 4** | 8 IMEI workers | **DEFERRED** (next scaling track) |
| **Phase 5** | Reporting/analytics stabilization (A0–Static + Trip recovery) | **COMPLETE (code + focused tests) — PHASE CLOSED** |
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
- **Not in A1:** parking backfill migrate, revive `analyticsQueue.js` (Mileage + Travel batching done later)

### Deploy / rollback (manual — agent does not deploy)

1. Deploy code  
2. Ensure `REPORT_SCHEDULER_OWNER=analytics`  
3. `pm2 start ecosystem.config.cjs` or restart `alfursan-bridge` + `alfursan-analytics`  
4. Rollback: `pm2 stop alfursan-analytics` → set `REPORT_SCHEDULER_OWNER=bridge` → restart bridge  

---

## Mileage Mongo N+1 removal — COMPLETE (code + focused tests)

- Batched incremental + daily mileage (`MILEAGE_CHUNK_SIZE` default 250)
- Prior boundary points: one aggregation per chunk (`$or` + sort + `$group $first`)
- DeviceStatus / DailyMileage: `bulkWrite` with `$inc` / `$set` only
- Overspeed daily: OverspeedAlert authoritative (`MILEAGE_OVERSPEED_SOURCE=alerts`; `legacy_max` rollback)
- Business day: Africa/Cairo (`MILEAGE_BUSINESS_DAY=cairo`; `utc` rollback). Static day key aligned.
- Metrics: `analytics_mileage_*` on heartbeat / `/health`
- Focused tests: `test/mileageBatch.test.js`
- **Not in this phase:** Travel×7, Idle, Parking, gps/alarm dedupe, new indexes at startup

### Query amplification (approx)

| | Old (N devices) | New (chunk C=250) |
|--|-----------------|-------------------|
| Incremental | ~2 distinct + ~4N | ~2 distinct + ~4×⌈N/C⌉ |
| Daily | ~1 distinct + ~5N | ~1 distinct + ~5×⌈N/C⌉ |

### Indexes used (no new indexes created)

- `gpspoints`: `{ imei: 1, packet_date: 1 }` / `{ imei: 1, packet_date: -1 }`
- `overspeed_alerts`: `{ imei: 1, start_time: -1 }`
- `acc_events`: `{ imei: 1, start_time: -1 }`
- `devicestatuses`: unique `imei`
- `dailymileages`: unique `{ imei: 1, day: 1 }`

Optional prod verify (not run by agent):

```js
db.gpspoints.find({ imei: { $in: ["..."] }, packet_date: { $gt: ISODate("...") } }).explain("executionStats")
```

---

## Travel ×7 Mongo redesign — COMPLETE (code + focused tests)

- One IMEI `distinct` per Travel run (not per threshold)
- Per chunk: one `GpsPoint.find({ imei:$in, packet_date range })` sorted `imei, packet_date`
- Same in-memory points → `computeSegments` for each of `[1,3,5,10,15,30,60]` (7 CPU passes; **1 Mongo read**)
- `TravelStat.bulkWrite` upsert on `(imei, day, stop_threshold_min)`
- Business day: Africa/Cairo (`TRAVEL_BUSINESS_DAY=cairo`; `utc` rollback)
- Chunk default **150** (`TRAVEL_CHUNK_SIZE`) — full-day tracks denser than mileage incremental
- Metrics: `analytics_travel_*` on heartbeat / `/health`
- Focused tests: `test/travelBatch.test.js`
- Segment semantics unchanged (`computeSegments` HEAD-compatible)
- **Not in this phase:** Idle, Parking, Trip replacement, gps/alarm dedupe

### Query amplification (approx, T=7 thresholds)

| | Old | New (C=150) |
|--|-----|-------------|
| Formula | T×(1 distinct + N finds + N upserts) | 1 distinct + ⌈N/C⌉ finds + ⌈N/C⌉ bulkWrites |
| N=10k | ~7×(1+10k+10k) ≈ **140k** | 1 + 2×67 ≈ **135** |

### Indexes used (no new indexes)

- `gpspoints` `{ imei:1, packet_date:1 }`
- `travelstats` unique `{ imei:1, day:1, stop_threshold_min:1 }`

```js
db.gpspoints.find({ imei: { $in: ["..."] }, packet_date: { $gte: ISODate("..."), $lt: ISODate("...") } }).sort({ imei:1, packet_date:1 }).explain("executionStats")
```

---

## Idle Mongo N+1 removal — COMPLETE (code + focused tests)

- One IMEI `distinct` per Idle run
- Per chunk: one `GpsPoint.find({ imei:$in, packet_date range })` sorted `imei, packet_date`
- Same `computeIdle` / `isAccOn` HEAD semantics (ignition=null → not idle)
- `IdleStat.bulkWrite` with `updateOne` upsert **and** `deleteOne` when no qualifying idle
- Business day: Africa/Cairo (`IDLE_BUSINESS_DAY=cairo`; `utc` rollback)
- Chunk default **150** (`IDLE_CHUNK_SIZE`)
- Metrics: `analytics_idle_*` including upserted/deleted counts
- Live `handleIdleNotifySample` **unchanged**
- Focused tests: `test/idleBatch.test.js`
- **Follow-on COMPLETE:** Static batching + Trip restart (see below)

### Query amplification (approx)

| | Old | New (C=150) |
|--|-----|-------------|
| Formula | 1 distinct + N finds + N writes | 1 distinct + ⌈N/C⌉ finds + ⌈N/C⌉ bulkWrites |
| N=10k | ~**20,001** | ~**135** |

### Indexes used (no new indexes)

- `gpspoints` `{ imei:1, packet_date:1 }`
- `idlestats` unique `{ imei:1, day:1 }`

---

## Static Mongo batching — COMPLETE (code + focused tests)

- Prefer one `DailyMileage.find({ day })`; GpsPoint fallback **only** for IMEIs missing DailyMileage
- Fallback uses shared `sumMileageKm` (same calc as Mileage) in bounded chunks (`STATIC_FALLBACK_CHUNK_SIZE` default **100**)
- `StaticStat.bulkWrite` in chunks (`STATIC_CHUNK_SIZE` default **500**)
- Rule unchanged: `km <= 0.5` → `is_static`; fields/key unchanged for Laravel
- Metrics: `analytics_static_*` including daily_mileage_hits / gps_fallback_devices
- Startup Static-after-Mileage preserved
- Focused tests: `test/staticBatch.test.js`

### Query amplification (approx, DailyMileage complete)

| | Old | New (C=500) |
|--|-----|-------------|
| Formula | ~1 + N upserts (+ N GpsPoint if fallback-heavy) | 1 DailyMileage + ⌈N/C⌉ bulkWrites (+ ⌈missing/100⌉ GpsPoint) |
| N=10k | ~**10k–20k** | ~**21** (+ rare fallback) |

---

## Trip restart recovery — COMPLETE (code + focused tests)

- Root cause: after restart `prevSpeed=null` / weak `lastNonZeroAt` → first moving packet could open/reopen Trip
- Fix: `lib/tripRuntimeState.js` — once-per-IMEI recovery from open Trip + DeviceStatus; wired in bridge + persistence-worker
- Tradeoff: if open Trip but no `last_speed`, assume `prevSpeed=0` (prevents duplicates; close/start only when gap ≥ BASE_GAP_MIN)
- **No** per-packet Trip/DeviceStatus/GpsPoint recovery queries
- Schema/fields unchanged; `TRIP_FLUSH_INTERVAL_MS` dirty flush preserved
- Metrics: `trip_recovery_*`
- Focused tests: `test/tripRecovery.test.js`

---

## Parking backfill operational safety — COMPLETE (manual script only)

- `backfillParkingEvents.js`: chunked, serial per IMEI, file checkpoint resume, still **manual**
- Never auto-run at analytics startup / PM2
- ParkingEvent open/close (1/5) and document shape unchanged
- Focused tests: `test/parkingBackfillSafety.test.js`

---

## Final analytics optimization summary (phase CLOSED)

| Report | Old ~10k devices | New ~ |
|--------|------------------|-------|
| Mileage | ~40k–50k | ~160–200 |
| Travel | ~140k | ~135 |
| Idle | ~20k | ~135 |
| Static | ~10k–20k | ~21 (+ rare fallback) |

**Architecture is SCALE-READY BY DESIGN.** Controlled 10k load/soak validation remains a later explicit phase — do not claim production 10k-ready from formulas alone.

### COMPLETE this phase

- analytics process isolation (A1)
- Mileage / Travel / Idle / Static optimization
- Trip restart hardening
- manual parking backfill safety
- reporting contract + roadmap docs

### DEFERRED (do not start next without explicit ask)

- new reports; Laravel/frontend redesign; business-definition changes
- Trip↔TravelStat / IdleStat↔live-idle unification
- gps/alarm dedupe; historical migrations
- **8 IMEI workers**; Raw Archive process isolation; full load/soak

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
