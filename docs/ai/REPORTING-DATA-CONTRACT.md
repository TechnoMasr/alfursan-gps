# Reporting Data Contract

**Source of truth for AI agents and report migration.**  
Last updated: 2026-09-07 — **FINAL REPORTING / ANALYTICS STABILIZATION CLOSED**

GpsLog is **removed**. Do not reintroduce it.

**Compatibility rule (frozen):** Laravel + frontend consume existing Mongo collections/fields. Do **not** rename collections, models, fields, keys, or report output shapes. Optimization must be transparent. Schema redesign belongs only to the deferred Backend + Frontend reporting phase.

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

### Business day (CURRENT)

- DailyMileage / Static / Travel / Idle day keys: **Africa/Cairo** midnight as UTC instant (per-report env; default `cairo`).
- Query window: Cairo `[dayStart, dayEnd)` converted to UTC `packet_date` ranges.
- Rollback: set corresponding `*_BUSINESS_DAY=utc`.
- **Migration:** pre-existing UTC-midnight rows are not auto-migrated; new writes use Cairo keys. Static falls back to GpsPoint when DailyMileage row missing for an IMEI.

---

## STATE / SEGMENTATION RULES (CURRENT HEAD — do not redefine this phase)

These are **separate product concepts**. Do not unify them in Node without a Backend + Frontend reporting phase.

### Trip (live `Trip` documents)

Used by current reports: daily/monthly trip count, trip history, stop-gap segmentation where supported.

| Rule | CURRENT |
|------|---------|
| Moving | `speed > 0` |
| Ignition | **Not** used for Trip segmentation |
| `BASE_GAP_MIN` | Default **1** minute (env/config) |
| Open | First movement when no open trip, **or** movement after stop gap ≥ `BASE_GAP_MIN` |
| While stopped | Existing open trip **remains open** (does not close immediately on stop) |
| Resume after gap ≥ BASE_GAP_MIN | Close previous (`end_at = lastNonZeroAt`), then start new Trip |
| Distance | Existing `distanceDiff` / dirty-flush pipeline |
| Persistence | Dirty coalesce + `TRIP_FLUSH_INTERVAL_MS`; close flushes before finalize |
| Restart recovery | Once-per-IMEI from open `Trip` + `DeviceStatus` (`last_speed`, packet times); **no** per-packet Mongo recovery |

**Trip ≠ TravelStat.** Trip is the live/business path collection Laravel already uses.

### TravelStat (historical materialization)

| Rule | CURRENT |
|------|---------|
| Source | `gpspoints` only (not live Trip) |
| Thresholds | `[1,3,5,10,15,30,60]` minutes |
| Segment | Speed&gt;0 opens; speed==0 + stopDur ≥ threshold closes; open flushed at EOF |
| `total_stop_count` | Fixed **3** min rule, independent of threshold |
| Key | `(imei, day, stop_threshold_min)` |

### IdleStat (historical idle totals)

| Rule | CURRENT |
|------|---------|
| Source | `gpspoints` |
| Condition | `isAccOn` (multi-field) when `requireAccOn` **and** `speed <= idleSpeedKph` |
| Scheduler | `idleSpeedKph=0`, `idleMinutes=5`, `requireAccOn=true`, `maxGapSeconds=600` |
| ignition=null | → not idle |
| Output | Daily idle totals on `IdleStat` |

### Live Idle (notifications)

| Rule | CURRENT |
|------|---------|
| Path | `handleIdleNotifySample` on bridge/persistence |
| ACC | `sample.accOn === true` (not the same helper as IdleStat) |
| maxGapSeconds | **None** (runtime memory only) |
| Purpose | Notifications — **not** IdleStat materialization |

**IdleStat ≠ live idle notify.** Disagreement is known and deferred.

### StaticStat

| Rule | CURRENT |
|------|---------|
| Meaning | Very little movement during the business day |
| Rule | `daily_mileage_km <= 0.5` → `is_static = true` |
| Not Static | Ignition off, idle, parking, offline, stale GPS |
| Source | Prefer `DailyMileage.km`; GpsPoint mileage recalc **only** when DailyMileage missing for that IMEI |
| Key | `(imei, day)` |

### ParkingEvent

| Rule | CURRENT |
|------|---------|
| Open | speed ≤ **1** |
| Close | speed ≥ **5** |
| Path | Live business path; manual backfill only for history |
| Meaning | Separate from Idle / Static / Trip stop-gap |

### Parking / ACC / Overspeed / Connectivity / Mileage

Unchanged business rules in this phase. See sections below. Do not “fix” audit semantic drift without Laravel/frontend redesign.

---

## Mileage

| | CURRENT (batched) | Notes |
|--|-------------------|--------|
| Source | `gpspoints` | Unchanged |
| Time | `packet_date` (fallback `date`) | Cairo day → UTC range for daily |
| Incremental | Chunked `$or` finds + prior aggregation + `DeviceStatus.bulkWrite` `$inc/$set` | `MILEAGE_CHUNK_SIZE` default **250** |
| Daily | Chunked points + AccEvent/OverspeedAlert aggregations + `DailyMileage.bulkWrite` | Not per-IMEI N+1 |
| Jump filter | `calcDistanceDiffSafe` | Unchanged |
| Speed edge | Distance if prev or current speed &gt; 0 | Unchanged |
| Overspeed count | **OverspeedAlert** only (`MILEAGE_OVERSPEED_SOURCE=alerts`) | `legacy_max` restores Math.max(gps&gt;120, alerts) |
| ACC counts | `AccEvent` aggregation | Unchanged semantics |

**gps/alarm duplicate risk (still deferred):** `gpspoints` may hold both gps and valid-position alarm samples; no dedupe in this phase.

---

## Travel

| | CURRENT (batched) | Notes |
|--|-------------------|--------|
| Source | `gpspoints` only (not live `Trip`) | Unchanged |
| Day key | Cairo (`TRAVEL_BUSINESS_DAY=cairo`) | `utc` rollback |
| Thresholds | `[1,3,5,10,15,30,60]` | Same list |
| Mongo | One IMEI discovery + one GpsPoint read/chunk → CPU × thresholds → `TravelStat.bulkWrite` | Not ×7 Mongo rebuilds |
| Chunk | `TRAVEL_CHUNK_SIZE` default **150** | Unchanged `computeSegments` |

---

## Idle

| | CURRENT (batched) | Notes |
|--|-------------------|--------|
| Source | `gpspoints` only | Unchanged |
| Day key | Cairo (`IDLE_BUSINESS_DAY=cairo`) | `utc` rollback |
| Mongo | One GpsPoint read/chunk → `computeIdle` → `IdleStat.bulkWrite` | Not per-IMEI N+1 |
| Chunk | `IDLE_CHUNK_SIZE` default **150** | Live notify untouched |

---

## Static

| | CURRENT (batched) | Notes |
|--|-------------------|--------|
| Source | Prefer `DailyMileage.km`; GpsPoint fallback **only** for missing IMEIs | Same rule |
| Rule | `km <= 0.5` → `is_static` | Unchanged threshold/fields |
| Day | Aligned with Mileage / Cairo (`STATIC_BUSINESS_DAY` / mileage resolver) | Same key semantics |
| Mongo | One `DailyMileage.find({ day })` → optional bounded GpsPoint chunks for missing → `StaticStat.bulkWrite` | Not per-IMEI DailyMileage / upsert |
| Chunk | `STATIC_CHUNK_SIZE` default **500**; `STATIC_FALLBACK_CHUNK_SIZE` default **100** | DailyMileage rows lighter than full-day tracks |
| Ordering | Startup Static waits for Mileage; recurring still gated by analytics ownership | Preserve dependency |
| Key / fields | `(imei, day)`, `daily_mileage_km`, `is_static` | Laravel-compatible |

### Static query flow

**Old:** DailyMileage for day (or per-device usage) + per-IMEI StaticStat upsert; GpsPoint recalc could fan out per missing/fallback IMEI.

**New:**

```text
DailyMileage.find({ day })           // 1 read
→ identify missing IMEIs only
→ GpsPoint.find($in chunk)           // only missing; fallback chunk ≤100
→ sumMileageKm (same helper as mileage)
→ StaticStat.bulkWrite               // chunks of ≤500
```

### Amplification (approx, N=10k, DailyMileage complete)

| | Old | New (C=500) |
|--|-----|-------------|
| Ops | ~1 + N upserts (+ N GpsPoint if fallback-heavy) ≈ **10k–20k** | ~1 + ⌈N/C⌉ bulkWrites ≈ **~21** (+ rare fallback chunks) |

---

## Parking

| | CURRENT | Notes |
|--|---------|--------|
| Live | `parkingEventsService` → `ParkingEvent` | Business path |
| Definition | Open speed≤1; close speed≥5 | Unchanged |
| Backfill | Manual `backfillParkingEvents.js` / npm script | Chunked + file checkpoint; **never** auto at analytics startup |

---

## ACC

| | CURRENT | Notes |
|--|---------|--------|
| Live | `accReportService` → `AccEvent` | Business workers |
| Daily counts | AccEvent in daily mileage | Prefer AccEvent |
| Null ignition | Ignored on live path | Keep |

---

## Overspeed

| | CURRENT | Notes |
|--|---------|--------|
| Live | `overspeedService` → `OverspeedAlert` | Business workers |
| Daily count | OverspeedAlert (`alerts` default) | Prefer alerts only |

---

## Connectivity

| | CURRENT | Notes |
|--|---------|--------|
| Meaning | Tracker connected to server | Not stop/idle/ignition |
| Startup reconciliation | Bridge | Stay on bridge |

---

## Trips (runtime)

| | CURRENT | Notes |
|--|---------|--------|
| Collection | `Trip` | Unchanged schema for Laravel |
| Owner | Persistence / future IMEI workers | Not analytics schedulers |
| Restart | `lib/tripRuntimeState.js` once-per-IMEI | Metrics `trip_recovery_*` |

---

## Global scheduler ownership

| | CURRENT | Notes |
|--|---------|--------|
| Owner | Exactly one: `alfursan-analytics` | `REPORT_SCHEDULER_OWNER=analytics` |
| Rollback | `REPORT_SCHEDULER_OWNER=bridge` + stop analytics | Same |
| Dual run | Forbidden | lock + role |
| Overlap | Per-job gates | Same |
| Startup | Staggered; Static after DailyMileage | Same |

Heartbeat: `data/analytics-spool/analytics-worker.heartbeat.json` → bridge `/health` as `analytics_*` (aggregate only).

---

## DEFERRED — BACKEND + FRONTEND REPORTING PHASE

**Do not implement in Node-only analytics work:**

- New reports / dashboard KPIs
- Laravel or frontend report redesign
- Collection/field/model renames
- Trip ↔ TravelStat unification
- IdleStat ↔ live idle unification
- User-selectable report semantic changes
- Historical UTC→Cairo migration UI
- gps/alarm dedupe redesign
- Changing Parking/ACC/Overspeed/Travel thresholds “to match audit preference”

Those require joint Backend + Frontend review with Laravel contract changes planned deliberately.

---

## Related docs

- [`ARCHITECTURE.md`](./ARCHITECTURE.md)
- [`PERFORMANCE-ROADMAP.md`](./PERFORMANCE-ROADMAP.md)
