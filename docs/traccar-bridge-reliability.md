# Traccar Bridge Reliability

## 1. Architecture before

```text
Traccar WS
  → sort (command / alarm / gps)
  → resolve IMEI (Mongo + cooldown fetch)
  → emit every GPS to subscribers as live
  → enqueueGpsPoint via setImmediate(GpsPoint.create)
  → enqueuePersistForImei: slot.pending = newest  ← DROPPED intermediate points
  → persistPositionHeavy: await GpsLog.create  ← gated all reports
  → DeviceStatus last_lat/last_fix overwritten by historical packets
```

Realtime waited behind (or was mixed with) persistence. Historical Traccar backfill (`fixTime` days ago, `serverTime` now) was replayed on client maps.

## 2. Architecture after

```text
                    TRACCAR WS
                         │
                    normalize
                         │
                 resolve device/IMEI
                         │
          ┌──────────────┼───────────────┐
          │              │               │
     COMMAND FAST      LIVE GPS       ARCHIVE
          │              │               │
     immediate       freshness        EVERY POINT
     delivery         ordering             │
          │          latest-wins       WAL journal
          │          backpressure      (setImmediate)
          │              │               │
          │              │          Mongo batch
          │              │               │
          │              │         ACK then unlink
          └──────────────┘
                           separate
                              │
                         ANALYTICS FIFO
                              │
               DeviceStatus / reports / notifications
               (never regress last_fix / last_lat)
gpslogs: optional, GPSLOGS_WRITE_ENABLED=0 by default
```

Entry points (unchanged for PM2):

- `traccar-bridge-ontherport.js` — persistence + tenant rooms (`127.0.0.1:3053`)
- `traccar-bridge.js` — legacy HTTPS/WSS relay (`:2053`). GPS relay does **not** require Mongo.

Live send never awaits disk, Mongo, fsync, or analytics.

## 3. Live vs Historical policy

Live clock is `position.fixTime`, then `position.deviceTime`. **`serverTime` is never used to decide live vs historical.**

| Decision | Live map | gpspoints | gpslogs (if enabled) | DeviceStatus coords |
|---|---|---|---|---|
| fresh | YES | STORE | STORE | update if newer |
| historical | NO | STORE | STORE | no regression |
| out_of_order | NO | STORE | STORE | no regression |
| future_invalid | NO | STORE | STORE | no regression |
| missing_time | NO | STORE | STORE | no regression |

`MAX_LIVE_FIX_AGE_MS` default `300000` (5 minutes).  
`LIVE_FIX_FUTURE_TOLERANCE_MS` default `120000`.

Helper: `lib/liveEligibility.js` (`classifyLiveFix`, `isLiveEligible`, `isArchiveEligible`).

## 4. Persistence Durability Guarantees

Do **not** call this lossless against process crash unless the point has already been journaled.

Path:

```text
Realtime immediately
  → enqueue (RAM, current turn)
  → setImmediate WAL journal (tmp file + atomic rename)
  → Mongo insertMany({ ordered: false })
  → delete spool file only after ACK or idempotency duplicate
```

`enqueue()` never blocks `ws.send`. Journal I/O runs on a later turn.

| Scenario | GPS preserved? |
|---|---|
| Mongo temporary outage | Yes, after journal tick. Replay from spool. |
| Mongo long outage | Yes, after journal tick. Spool grows. Health warning/critical. Points are **not** deleted to free disk. |
| PM2 graceful restart (SIGTERM) | Yes. `flushAndStop` journals leftover RAM then drains until timeout. |
| Node exception after journal | Yes. Next process recovers JSONL spool. |
| kill -9 / Node crash | **Only if the WAL file rename completed.** There is a one-tick RAM-only window between `enqueue` and `setImmediate` journal. Points still only in RAM are lost. |
| Server reboot | Same as kill -9: journaled files survive; unjournaled RAM does not. |
| Disk full / EACCES / EROFS | No silent drop. Points stay in RAM, `gpspoints_spool_write_failures` increments, `/health` `persistence_health=critical`. If Mongo is up, writer persists from RAM. If both disk and Mongo are down, RAM can grow (unbounded). |
| Spool corruption | Partial/`.tmp` files are not treated as persisted. Corrupt lines/files are quarantined. Valid lines are recovered. Process does not crash. |
| Duplicate retry after lost ACK | `E11000` on `{imei, traccar_position_id}` counts as already persisted. Other unique indexes are **not** treated as that idempotency key. |

`persistence_dropped` must stay `0`. Archive never drops oldest / coalesces / shifts without persist. Latest-wins exists only on the live path.

Spool path: `GPSPOINT_SPOOL_DIR` or `<project-root>/data/gpspoints-spool` (resolved from `__dirname`, not `process.cwd()`). Startup logs `gpspoint_spool_dir=/absolute/path/...`.

Writes: `*.jsonl.tmp` then `renameSync`. Startup deletes leftover `.tmp`. A spool file is unlinked only after Mongo ACK (or classified idempotency duplicate). On retryable Mongo errors the file is rewritten/kept.

Drain fairness: each cycle persists a limited number of **newest** files plus a limited number of **oldest** files (`GPSPOINT_DRAIN_NEW_FILES_PER_CYCLE`, `GPSPOINT_DRAIN_OLD_FILES_PER_CYCLE`, `GPSPOINT_MAX_MONGO_BATCHES_PER_CYCLE`). Recovery does not starve new GPS or monopolize Mongo.

## 5. gpspoints role

Lean playback collection. Every valid coordinate point (including historical) is archived. Optional idempotency field: `traccar_position_id`.

**Index is not created at startup and is not part of a normal deploy.**

Maintenance (diagnostic first):

```bash
MONGO_URI='mongodb://...' node scripts/ensure-gpspoints-idempotency-index.js
MONGO_URI='mongodb://...' node scripts/ensure-gpspoints-idempotency-index.js --create
```

`--create` is refused if duplicate `(imei, traccar_position_id)` groups exist. The unique index uses `partialFilterExpression` so old documents **without** `traccar_position_id` are not indexed and cannot collide.

## 6. gpslogs flag

```js
GPSLOGS_WRITE_ENABLED = boolEnv("GPSLOGS_WRITE_ENABLED", false)
```

Default **OFF**. Disables **new writes** only. Does not drop the collection, model, backfill, gpspoints, notifications, live reports (parking / overspeed / ACC / geofence / trips), DeviceStatus, commands, or realtime.

### Schedulers still reading gpslogs (not migrated)

These keep running and must not crash; they will not see **new** GPS while writes are OFF:

- `mileageService`
- travel / idle / static nightly schedulers that query `gpslogs`

Do not migrate them in this release. Convert to `gpspoints` later.

## 7. Backpressure policy

Configurable watermarks (`WS_BUFFER_*`). GPS: latest pending payload per IMEI/socket. Commands: never coalesced, queued ahead of GPS. Critical watermark → controlled `terminate` so the client reconnects.

Disk/spool errors never gate `ws.send`.

## 8. Heartbeat

`SUBSCRIBER_HEARTBEAT_MS` (default 30000). ping/pong/`isAlive`. Dead sockets are terminated and subscriptions/rooms/pending GPS are cleared.

## 9. Reconnect

Exponential backoff: 5s, 10s, 20s, 40s, 60s max + jitter.  
Every failure schedules the next attempt. Success resets to base.  
Watchdog uses `last_traccar_message_at` (any Traccar message, not GPS-only).  
`WS_REFRESH_MS` default **0** (disabled). It is not a health mechanism.

Unchanged unless a bug is found: `BASE_GAP_MIN`, `RECONNECT_MS`, `DEVICES_POLL_MS`, `DEVICE_FETCH_COOLDOWN_MS`, `DEVICES_LIST_BACKOFF_*`, `STATUS_SYNC_COOLDOWN_MS`, `WS_REFRESH_MS`.

## 10. Durable spool

Directory: absolute `GPSPOINT_SPOOL_DIR`. Atomic JSONL segments. Async vs realtime: live send first; journal on `setImmediate`; Mongo in `flushCycle`.

## 11. Environment variables

```env
GPSLOGS_WRITE_ENABLED=0

MAX_LIVE_FIX_AGE_MS=300000
LIVE_FIX_FUTURE_TOLERANCE_MS=120000

TRACCAR_RECONNECT_BASE_MS=5000
TRACCAR_RECONNECT_MAX_MS=60000
TRACCAR_SILENCE_WATCHDOG_MS=180000
WS_REFRESH_MS=0

SUBSCRIBER_HEARTBEAT_MS=30000
WS_BUFFER_HIGH_WATERMARK_BYTES=1048576
WS_BUFFER_LOW_WATERMARK_BYTES=262144
WS_BUFFER_CRITICAL_BYTES=8388608

GPSPOINT_BATCH_SIZE=250
GPSPOINT_FLUSH_MS=100
GPSPOINT_MEM_HIGH=10000
GPSPOINT_RETRY_BASE_MS=250
GPSPOINT_RETRY_MAX_MS=30000
GPSPOINT_SPOOL_DIR=data/gpspoints-spool
GPSPOINT_SPOOL_WARN_BYTES=536870912
GPSPOINT_SPOOL_CRITICAL_BYTES=2147483648
MIN_DISK_FREE_BYTES=1073741824
GPSPOINT_DRAIN_NEW_FILES_PER_CYCLE=2
GPSPOINT_DRAIN_OLD_FILES_PER_CYCLE=1
GPSPOINT_MAX_MONGO_BATCHES_PER_CYCLE=4

ANALYTICS_CONCURRENCY=8
ANALYTICS_MEM_HIGH=20000
ANALYTICS_SPOOL_DIR=data/analytics-spool

BRIDGE_LATENCY_DEBUG=0
BRIDGE_SHUTDOWN_TIMEOUT_MS=8000

DEVICES_LIST_BACKOFF_BASE_MS=60000
DEVICES_LIST_BACKOFF_MAX_MS=900000
DEVICE_FETCH_COOLDOWN_MS=60000
DEVICES_POLL_MS=900000
STATUS_SYNC_COOLDOWN_MS=60000
RECONNECT_MS=5000
BASE_GAP_MIN=1
```

Relative `GPSPOINT_SPOOL_DIR` is resolved against the project root (`lib/../`), not `process.cwd()`.

### Existing variables (still present)

| Variable | Used? | Role | Default | Realtime impact |
|---|---|---|---|---|
| `BASE_GAP_MIN` | yes | trip gap minutes | 1 | no (analytics) |
| `RECONNECT_MS` | yes, alias | leftover name; reconnect uses `TRACCAR_RECONNECT_*` | 5000 | reconnect only |
| `DEVICES_POLL_MS` | yes | full device list poll | 15 min | no |
| `DEVICE_FETCH_COOLDOWN_MS` | yes | after a **failed** fetch; in-flight fetch is shared | 60s | unknown IMEI only |
| `DEVICES_LIST_BACKOFF_BASE_MS` | yes | first failure wait = BASE (bugfix) | 60s | no |
| `DEVICES_LIST_BACKOFF_MAX_MS` | yes | cap | 15 min | no |
| `STATUS_SYNC_COOLDOWN_MS` | yes | missing DeviceStatus sync | 60s | no |
| `WS_REFRESH_MS` | optional | forced refresh, **disabled by default** | 0 | none unless set |

Do not “fix latency” by raising queue sizes or adding sleeps.

## 12. PM2

Keep the current process names and files:

```text
traccar-bridge.js
traccar-bridge-ontherport.js
```

SIGTERM/SIGINT flush gpspoints + analytics (timeout `BRIDGE_SHUTDOWN_TIMEOUT_MS`) then exit. Do not rename entry files.

## 13. Health fields

`GET /health` on `:3053` includes at least:

`traccar_ws_connected`, `last_traccar_message_at`, `gpslogs_write_enabled`, `subscriber_count`, `slow_subscriber_count`, `live_broadcast_total`, `live_stale_suppressed`, `live_out_of_order_suppressed`, `live_coalesced`, `gpspoints_queue_depth`, `gpspoints_spool_files`, `gpspoints_spool_bytes`, `gpspoints_spool_oldest_age_ms`, `gpspoints_spool_write_failures`, `gpspoints_persisted_total`, `gpspoints_retry_total`, `gpspoints_persist_failures`, `persistence_dropped`, `disk_free_bytes`, `event_loop_lag_ms`, `persistence_health`.

`:2053` is relay-only: no gpspoints spool metrics (that process does not persist). It still exposes live/reconnect/subscriber health and `event_loop_lag_ms`.

`ok: true` means the HTTP process is up. Use `persistence_health` (`ok` / `warning` / `critical`) for spool/disk pressure. Disk-full never deletes GPS.

## 14. Troubleshooting a “moving parked car”

1. Enable `BRIDGE_LATENCY_DEBUG=1`.
2. Confirm `live_decision` is `historical` / `out_of_order` for old `fixTime`.
3. If the **client** still moves, it is using playback or last cached polyline — see client notes.
4. Check `/health` `last_traccar_message_at` vs `live_stale_suppressed`.
5. If Traccar UI is correct but Node is stale, inspect reconnect metrics and subscriber `bufferedAmount`.

Latency debug sample fields: `imei`, `traccar_position_id`, `fixTime`, `deviceTime`, `serverTime`, `fix_age_ms`, `live_decision`, `live_delivery_ms`.

## 15. Deployment checklist (do not treat index creation as deploy)

See the audit report. Normal deploy: backup files, `npm install` if needed, env, spool dir + permissions + disk free, syntax, tests, PM2 reload, health, logs, WS/command tests. **Do not** run `ensure-gpspoints-idempotency-index.js` during ordinary deploy.
