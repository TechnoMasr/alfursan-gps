---
name: traccar isolation refactor
overview: Refactor the Traccar bridge so realtime ingress always emits the latest eligible live GPS first, while persistence, analytics, device status, and other heavy work move behind a separate worker boundary. Keep the existing PM2 entry points unchanged and preserve all archive/invariant behavior with regression tests.
todos:
  - id: extract-realtime-pipeline
    content: Extract a single shared realtime ingress pipeline used by both production entry points and the existing tests
    status: completed
  - id: add-persistence-worker
    content: Introduce a bounded IPC-backed persistence worker and move heavy storage/reporting responsibilities behind it
    status: completed
  - id: preserve-fast-live-ordering
    content: Ensure latest eligible live GPS is emitted before any historical batch persistence work
    status: completed
  - id: add-regression-tests
    content: Add burst-order, worker-isolation, command-fast-path, and archival-invariant tests against the production handler path
    status: completed
  - id: update-health-metrics
    content: Expose realtime/worker health, IPC depth, and live latency metrics without changing PM2 entry names
    status: completed
isProject: false
---

# Traccar Realtime Isolation

## Goal
Split the current bridge into a thin realtime ingress path and a separate persistence worker, while keeping `traccar-bridge.js` and `traccar-bridge-ontherport.js` as the only PM2 entry points.

## Proposed architecture
```mermaid
flowchart LR
  traccar[Traccar WS] --> realtime[Realtime Bridge Process]
  realtime -->|latest live first| browser[WebSocket Clients]
  realtime -->|batched IPC| ipcQueue[Bounded IPC Queue]
  ipcQueue --> worker[Persistence Worker]
  worker --> gpspoints[gpspoints / WAL]
  worker --> deviceStatus[DeviceStatus]
  worker --> analytics[Analytics / Reports / Alerts]
  worker --> mongo[(Mongo)]
```

## Implementation plan
- Extract the shared realtime ingress flow from `traccar-bridge-ontherport.js` and `traccar-bridge.js` into one production-used pipeline module, reusing the tested live-selection logic already in `lib/positionPipeline.js`.
- Make the realtime handler do only cheap work: decode, map deviceId/IMEI from cache, select the latest eligible live position per IMEI, emit live GPS immediately, and enqueue the full batch for persistence.
- Add a persistence worker process (for example `workers/persistence-worker.js`) and a small IPC helper (`lib/persistenceIpc.js`) that batches messages, tracks queue depth/backpressure, and acks durable acceptance separately from live delivery.
- Move heavy persistence-side responsibilities behind the worker boundary: gpspoints journaling/spool, Mongo retries, DeviceStatus, reporting schedulers, notifications, and analytics.
- Keep command/command-response fast paths in the realtime process so they are not delayed by historical GPS bursts.
- Preserve the archive invariant: all valid GPS positions still reach storage, including bursts and out-of-order points; no latest-only behavior in persistence.
- Keep realtime mapping cache behavior non-blocking: cache hits emit immediately, misses refresh asynchronously, and live emit must not wait on Mongo lookups.
- Remove synchronous filesystem work from realtime hot paths; any remaining WAL/spool work should live only in the persistence worker.
- Add health/metrics for realtime PID, worker PID, IPC queue depth, worker liveness, live latency, and worker ack/pending counts.

## Files likely to change
- `[traccar-bridge-ontherport.js](d:\projeccts\techno\alfursan\gps-server\traccar-bridge-ontherport.js)`
- `[traccar-bridge.js](d:\projeccts\techno\alfursan\gps-server\traccar-bridge.js)`
- `[lib/positionPipeline.js](d:\projeccts\techno\alfursan\gps-server\lib\positionPipeline.js)`
- `[lib/gpsPointWriter.js](d:\projeccts\techno\alfursan\gps-server\lib\gpsPointWriter.js)`
- `[gpsPointStore.js](d:\projeccts\techno\alfursan\gps-server\gpsPointStore.js)`
- new worker/helper modules under `d:\projeccts\techno\alfursan\gps-server\workers\` and `d:\projeccts\techno\alfursan\gps-server\lib\`

## Test strategy
- Add integration tests that exercise the real production handler path, not only helpers, to prove live GPS is emitted before historical persistence.
- Extend existing durability tests to prove that 100 same-IMEI points are all archived and that journal coalescing is batching, not overwriting.
- Add burst-order tests for "historical flood + one fresh point" and "historical flood + command response".
- Add worker-failure tests for restart/recovery and bounded IPC backlog.
- Keep all existing live-eligibility, load, and persistence tests passing.
