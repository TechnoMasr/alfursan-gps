# Reporting Data Contract

**Source of truth for AI agents and report migration.**  
Last updated: 2026-09-07 (A0 contract + A1 ownership; algorithms unchanged)

GpsLog is **removed**. Do not reintroduce it.

---

## Global timestamp rules

| Concept | Field | Notes |
|---------|--------|------|
| GPS movement time | `gpspoints.packet_date` | Prefer for mileage/travel/idle/static/parking windows |
| Legacy fallback | `gpspoints.date` | Used only when `packet_date` missing |
| Device TX time | Traccar `deviceTime` | Forensics / raw — not daily km |
| Server receive | Traccar `serverTime` / Mongo `createdAt` | Connectivity / ingress — not distance |
| ACC / overspeed event time | `start_time` / `end_time` | Event collections |
| Connectivity | Transition `at` / `start_at` / `end_at` | Server connection, not vehicle stop |

### Business day (TARGET)

- Business daily reports use **Africa/Cairo** calendar day.
- Convert Cairo `[dayStart, dayEnd)` to **UTC** for Mongo range queries.
- Store materialization keys as UTC instants corresponding to Cairo midnight (document conversion in code when implemented).

### Business day (CURRENT HEAD)

- All four global schedulers use `startOfTodayUTC()` — **UTC midnight**, not Cairo.
- TARGET Cairo conversion is **not implemented yet** (A0 documents intent only).

---

## Mileage

| | CURRENT HEAD | TARGET |
|--|--------------|--------|
| Source | `gpspoints` | `gpspoints` |
| Time | `packet_date` (fallback `date`) | Cairo day → UTC range; `packet_date` |
| Incremental state | `devicestatuses.last_mileage_at`, `$inc km_total/miles_total` | Same collections; safer batching later |
| Daily materialization | `DailyMileage` unique `(imei, day)` | Same; `day` = Cairo day start as UTC |
| Jump filter | `calcDistanceDiffSafe` | Keep |
| Speed edge | Distance counted if prev or current speed &gt; 0 | Keep until redesign |
| Packet types | GPS + alarm (valid coords) both stored | Same; **do not double-count** same physical fix if both archived — document risk below |

**gps/alarm duplicate risk (CURRENT):** `GPSPOINT_TRACK_TYPES = gps|alarm`. If Traccar emits both a GPS position and an alarm-typed position for the same movement sample, both may enter `gpspoints` and inflate distance/stop metrics. No dedupe today.

**Overspeed in daily mileage (CURRENT inconsistency):**

1. Counts points with `speed > 120` (hardcoded `OVERSPEED_LIMIT_KMH`)
2. Also `OverspeedAlert.countDocuments` for the day
3. Persists `Math.max(gpsPointCount, alertCount)`

Per-device limit lives in `devicestatuses.alert_speed_limit_value` (used by live overspeed service). Daily report **ignores** that limit for the GpsPoint scan. TARGET: prefer `OverspeedAlert` as authoritative for counts; do not hardcode 120 in daily rebuild (algorithm change later).

**ACC in daily mileage (CURRENT):** `AccEvent` counts for on/off — aligned with TARGET.

---

## Travel

| | CURRENT | TARGET |
|--|---------|--------|
| Source | `gpspoints` | `gpspoints` |
| Materialization | `TravelStat` unique `(imei, day, stop_threshold_min)` | Same |
| Thresholds (bridge call) | `[1,3,5,10,15,30,60]` — **7 full rebuilds**/tick | Same semantics; optimize multi-threshold later (not A1) |
| Segment logic | Speed&gt;0 opens; speed==0 + stop threshold closes | Keep until redesign |
| Trip collection | **Not used** | Live `Trip` remains business-path; travel report stays GpsPoint-derived |

---

## Idle

| | CURRENT | TARGET |
|--|---------|--------|
| Source | `gpspoints` | `gpspoints` + ignition semantics |
| Materialization | `IdleStat` unique `(imei, day)` | Same |
| Bridge params | `idleSpeedKph=0`, `idleMinutes=5`, `requireAccOn=true`, `maxGapSeconds=600` | Keep params; Cairo day later |
| Ignition | `isAccOn()`: true/false from `ignition` / `acc_status` / etc. | Explicit |

**ignition=null behavior (CURRENT):** When `requireAccOn=true`, `isAccOn` returns **false** for null/unknown ignition. Those points **never** contribute to idle. TARGET: document this as intentional until product decides otherwise — do **not** infer ignition from speed alone.

Live idle notify (`handleIdleNotifySample`) stays on **business worker** path — not the global IdleStat scheduler.

---

## Static

| | CURRENT | TARGET |
|--|---------|--------|
| Source | Prefer `DailyMileage.km`; fallback GpsPoint recalc | Same |
| Rule | `km <= 0.5` → `is_static` | Same threshold until redesign |
| Materialization | `StaticStat` `(imei, day)` | Same |
| Race | Could run before DailyMileage exists for today | A1: Static waits for mileage bundle |

---

## Parking

| | CURRENT | TARGET |
|--|---------|--------|
| Live | `parkingEventsService` on business path → `ParkingEvent` | Stay on IMEI business workers |
| Definition | Open speed≤1; close speed≥5 | Keep |
| Backfill | Manual `backfill:parking` — deleteMany range then insertMany | Ops/analytics-gated later; **not migrated in A1** |

---

## ACC

| | CURRENT | TARGET |
|--|---------|--------|
| Live | `accReportService` → `AccEvent` (ignition + power events) | Business workers |
| Daily counts | `AccEvent.countDocuments` in daily mileage | Prefer AccEvent |
| Null ignition | Ignored on live path (no toggle) | Keep |

---

## Overspeed

| | CURRENT | TARGET |
|--|---------|--------|
| Live | `overspeedService` + per-device limit cache → `OverspeedAlert` | Business workers |
| Daily count | Hardcoded 120 on GpsPoint **and** OverspeedAlert max | Prefer OverspeedAlert only |

---

## Connectivity

| | CURRENT | TARGET |
|--|---------|--------|
| Meaning | Tracker **connected to server**, not stopped/idle/ignition | Same |
| Live | `deviceConnectivityService` + status | Realtime / business |
| Startup | `runStartupConnectivityReconciliation` on bridge | Stay on bridge (not analytics) |
| Collection | `DeviceDisconnection` | Same |

---

## Trips

| | CURRENT | TARGET |
|--|---------|--------|
| Source | `Trip` via persistence-worker dirty flush | IMEI business workers |
| Schedulers | Do **not** recompute trips | Keep |

---

## Global scheduler ownership

| | CURRENT (A1) | TARGET |
|--|--------------|--------|
| Owner | Exactly one: `alfursan-analytics` (`REPORT_SCHEDULER_OWNER=analytics`) | Same |
| Rollback | `REPORT_SCHEDULER_OWNER=bridge` + stop `alfursan-analytics` | Same |
| Dual run | Forbidden via env role + `report-schedulers.lock` | Same |
| Overlap | Per-job gates (mileage/travel/idle/static) | Same |
| Startup | Staggered; Static after DailyMileage | Same |

Heartbeat: `data/analytics-spool/analytics-worker.heartbeat.json` → bridge `/health` as `analytics_*`.

---

## Related docs

- [`ARCHITECTURE.md`](./ARCHITECTURE.md)
- [`PERFORMANCE-ROADMAP.md`](./PERFORMANCE-ROADMAP.md)
