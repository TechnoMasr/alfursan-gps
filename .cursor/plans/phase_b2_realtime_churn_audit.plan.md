# Phase B2 — Realtime State Churn Audit

**Mode:** plan only — NO IMPLEMENTATION.
**Source of truth:** CURRENT HEAD after Phase A + B1 + single full bootstrap.

Node/Laravel frozen. Do not regress Phase A or B1.

---

## 1. Remaining realtime `setCars` paths

| Message | File | Behavior on TenantDashboard |
|---------|------|-----------------------------|
| `gps` / unwrapped `tenant_gps_update` | `useCarSocket.jsx` ~388–463 | **Always** invokes `setCars(updater)`; with `updateCarsOnGps:false` returns **same `prev`** after `patchFleetLive` |
| `alarm` (attrs) | ~494–512 | `prev.slice()` + new car object for ignition/motion/charge/telemetry |
| `heartbeat` | ~572–591 | `prev.slice()` + sets `voltage` |
| `device` | ~601–633 | `prev.slice()` + offline/status; also `patchFleetLive` for offline flags |
| `command_response*` | ~326–379 | **No** `setCars` — Redux + toast only |

No bare `car.foo =` mutations found in the GPS/heartbeat/device/alarm handlers — updates use immutable spreads then `prev.slice()`.

---

## 2–3. Fields updated + consumers

### GPS (`applyGpsPacketToCar` → patch / optional cars)
**Fields:** position, speed, direction, status, ignition_on, motion, charge, power, battery, batteryLevel, lastUpdate, lastSignel, lastSignelGPS, lastGpsAtMs, lastPacketMs, lastFixAtMs, lastLiveReceivedAtMs, lastMovingReceivedAtMs

| Consumer | Path |
|----------|------|
| Map | `subscribeFleet` / `fleetVersion` + merge |
| Sidebar | B1 dirty rematerialize → `carsWithLive` |
| Filters | `isVehicleMoving` / offline via `carsWithLive` |
| Details | selected car from `carsWithLive` |
| Store | **Yes** — `patchFleetLive` |

### Heartbeat
**Fields written to React cars:** `voltage` only (`data.data.heartbeat.externalVoltage`)

| Consumer | Notes |
|----------|------|
| CarRow power badge | Reads **`car.power`**, not `car.voltage` — heartbeat may **not** update visible sidebar power today |
| Map | No |
| Filters | No |
| Store | **No** |

### Device
**Fields:** `device_status`, `device_lastUpdate`, `isOffline`, `isInactive:false`, `lastSignel` (fallback), `lastUpdate: Date.now()`

| Consumer | Notes |
|----------|------|
| Filters offline/online | Yes via `carsWithLive` / cars |
| CarRow status | `getCarStatus` uses offline/inactive + moving |
| Store | **Partial** — already patches `isOffline`/`isInactive`/`lastUpdate` **and** still slices React cars |

### Alarm
**Cars fields (conditional):** ignition_on, motion, charge, sticky telemetry (power/battery…), lastUpdate  
**Non-cars:** toast, sound, `pushAlarmEntry`, go-to-map

| Consumer | Notes |
|----------|------|
| CarRow ignition/status | Yes |
| Alarm pool / toast | Separate — must keep |
| Store | **No** from alarm handler |

### Command
Redux `setCommandResponse` — unchanged; not a cars path.

---

## 4. Mutation patterns

Search in `useCarSocket.jsx` realtime handlers: **no** in-place `car.foo =` / `Object.assign(car)`. Pattern is always:

```
const next = prev.slice();
next[idx] = { ...existing, ... };
return next;
```

GPS under TD returns `prev` without slice when `!updateCarsOnGps`.

---

## 5. Proposed field ownership (from CURRENT code)

| Field | Owner |
|------|------|
| name, plate/car_number, driver, branch_*, model, address, tracking_url, static config | React metadata (`cars`) |
| latitude/longitude (`position`) | fleetPositionStore |
| speed, direction/heading, status (live) | fleetPositionStore |
| ignition_on, motion, charge | fleetPositionStore |
| power, battery, batteryLevel | fleetPositionStore |
| voltage (heartbeat) | **should be** fleetPositionStore (map to `power` or add `voltage` to extractLiveFields + CarRow) |
| lastGpsAtMs, lastPacketMs, lastFixAtMs, lastLiveReceivedAtMs, lastMovingReceivedAtMs, lastSignel* | fleetPositionStore |
| isOffline, isInactive, device_status | fleetPositionStore (connectivity) — Node device message remains source |
| alarm toast/pool/unread | UI-only / alarm module (not cars, not store) |
| command_response | Redux |

Categories: **A** metadata · **B** realtime telemetry · **C** connectivity · **D** UI-only · **E** alarm/event

---

## 6. Safe to move to fleetPositionStore

| Path | Safe? | Condition |
|------|-------|-----------|
| GPS `setCars` invocation | **Yes** | Short-circuit: if `useFleetStore && !updateCarsOnGps`, run apply+`patchFleetLive` **outside** `setCars` |
| Heartbeat `voltage` | **Yes** | Patch store (`power` and/or `voltage`); rely on B1 dirty row; stop `setCars` when `useFleetStore` |
| Device offline flags | **Yes** | Already patched; stop redundant `setCars` when `useFleetStore` (keep early-return if unchanged) |
| Alarm telemetry attrs | **Yes** | `patchFleetLive` for ignition/motion/charge/telemetry when `useFleetStore`; keep toast/pool outside |

## 7. Must stay in React state

- Base metadata from HTTP / modal updates (`setCars` from API, geocode address, device-updated event)
- Single-vehicle pages (DeviceTracking/OutsideTracking): defaults `updateCarsOnGps:true`, `useFleetStore:false` — keep GPS→`setCars` for their one-car UI
- Alarm toast/pool / Redux commands

---

## 8. Selected car / DetailsModal

- TD selected car: `carsWithLive.find(id)` — gets B1 rematerialized live fields
- If heartbeat/device/alarm move to store only, DetailsModal stays fresh **if** it reads `carsWithLive` / live-merged selected car, not a stale snapshot of base `cars`
- Verify on implement: DetailsModal props path; if modal holds a frozen car object from open time, that is pre-existing — do not invent second store unless needed

## 9. Filters/counters

Still O(n) on list tick (accepted). They consume `carsWithLive` (`isOffline`, `isInactive`, `isVehicleMoving`). Dirty rematerialization already feeds store fields into list rows — moving heartbeat/device/alarm into store remains compatible **if** those fields are in `extractLiveFields` / `patchFleetLive`.

## 10. Single-device pages

DeviceTracking / OutsideTracking: `useTenantRoom:false`, **no** `useFleetStore`, default `updateCarsOnGps:true`. B2 short-circuits must be gated on TD flags only. Do not change Phase A.

## 11. Risks

- Heartbeat `voltage` vs CarRow `power` mismatch — moving to store without aligning field may still leave badge stale (pre-existing)
- Device handler always slices even when status unchanged — early-return needed
- Alarm without store patch today can be overwritten by next GPS merge overlay — already a race
- Opening DetailsModal with non-live car ref

## 12. Expected files (when implementing B2)

- `src/hooks/useCarSocket.jsx` (primary)
- Possibly `src/utils/fleetPositionStore.js` (`voltage`/`power` in extractLiveFields)
- Focused tests only; CarRow only if voltage→power alignment required for visible parity

## 13. Focused tests required (B2 later)

1. GPS with `useFleetStore+!updateCarsOnGps` never calls `setCars`
2. Heartbeat patches store + dirties one id; no cars identity change when useFleetStore
3. Device same; unchanged status no dirty
4. Alarm toast still fires; car telemetry via store when useFleetStore
5. B1: one heartbeat → one rematerialized row
6. Phase A device pages still update via setCars
7. Filters still see offline after device message (via materialize)

## 14. Expected reduction

- Eliminate per-GPS React `setState` scheduler work on TD
- Eliminate heartbeat/device/alarm full-array identity churn on TD
- List stays B1: ~1 row object per affected IMEI per flush

---

## 15. RECOMMENDED B2 IMPLEMENTATION (smallest safe — do not implement now)

1. **GPS early path:** if `useFleetStore && !updateCarsOnGps`, resolve car from `carsRef`, apply packet, `patchFleetLive`, **do not call `setCars`**.
2. **Heartbeat / device / alarm telemetry:** when `useFleetStore`, `patchFleetLive` only (align `voltage`→`power` if needed for CarRow); skip `setCars` slice; preserve alarm toast/pool/sound.
3. Device: early-return if offline/status unchanged.
4. Leave Filters O(n), virtualization, maps, Phase A, B1 dirty-id, Node/Laravel untouched.

**STOP — no B2 implementation in this phase.**
