# Phase F — Default Google Sidebar / Filter CPU Audit

**Mode:** PLAN ONLY — NO IMPLEMENTATION.  
**Source of truth:** HEAD after A + B1 + full bootstrap + B2 + D + E1 + E2a.  
**Default path:** Google map, `clusters === false`.  
**Clustering (E2b):** backlog only — not default-path P0.

---

## 1. Exact one-list-flush execution graph (~800ms)

```text
GPS / live patch
  → dirtyDeviceIds.add(id)                          O(1)
  → schedule list notify (leading 800ms coalesce)   O(1)
  → flush: subscribeFleetList(version, dirtyIds)    O(k) snapshot

TenantDashboard setFleetListTick
  → render

carsWithLive = materializeCarsWithFleet(...)
  → new Set(dirtyIds)                               O(k)
  → new Map(prevMerged)                             O(N)     ← allocation
  → cars.map(...) new array                         O(N)     ← allocation
  → rematerialize dirty only                        O(k) new objects
  → unchanged rows keep prev object refs            O(1) each

branches = forEach carsWithLive → Map → sort        O(N + B log B)
  → new array of branch options                     always new

carsByBranch
  → if no branch: return carsWithLive ref           O(1) reuse ✓
  → else .filter branch                             O(N) new array

filteredCars = carsByBranch.filter(...)             O(N) NEW ARRAY ALWAYS
  → even when activeFilter === "all"                ← confirmed CURRENT HEAD
  → moving predicate: isVehicleMoving               O(1)/car if filter=moving

SideMenu render (unmemoized)
  → Search(carsByBranch): search filter useMemo
       empty searchKey → []                         O(1)
       else O(N) filter
  → Filters(carsByBranch): ONE forEach              O(N)
       counts online/offline/moving/inactive
       isVehicleMoving once per online car
       new filterTypes array                        O(1) small
  → Actions (Redux only)                            cheap
  → CarsList(filteredCars)
       virtualizer remap                            O(visible+overscan)
       CarRow memo: only dirty ∩ visible recompute
         getCarStatus + isVehicleMoving             O(1) per dirty row

GoogleMapView (default, clusters off)
  → cars=filteredCars new identity every tick
  → carsMetaById Map rebuild                        O(N)     ← side effect of F2
  → E1 icon: dirtyIds only                          O(k)
  → E2a: no supercluster.load                       O(1)
  → selectedCar .find on filteredCars               O(N)

selectedCar (dashboard) .find carsWithLive          O(N)
geocode effect: only if selected position deps change
```

| Step | Complexity | Allocates |
|------|------------|-----------|
| List flush notify | O(k) | dirtyIds array snapshot |
| materialize map + array | **O(N)** walk; **O(k)** new cars | **new array N**, Map N, ~k objects |
| branches | **O(N)** | new Map, new sorted array |
| carsByBranch (all branches) | O(1) | none (reuse) |
| carsByBranch (selected) | O(N) | new array |
| filteredCars | **O(N)** always | **new array N** (even `"all"`) |
| Filters counters | **O(N)** one pass | small types array |
| CarsList virtual | O(visible) | virtual items |
| CarRow dirty | O(k∩visible) | status object via getCarStatus |
| Google meta Map | **O(N)** | new Map |
| selectedCar finds | O(N) ×1–2 | none |

---

## 2. Remaining O(N) default-path operations

1. `materializeCarsWithFleet` `.map` + `prevById` Map  
2. `branches` full scan + sort  
3. `filteredCars.filter` (including `"all"`)  
4. `Filters` counter forEach (+ `isVehicleMoving` for online cars)  
5. GoogleMapView `cars` effect → `carsMetaById` Map rebuild (driven by #3)  
6. `selectedCar` `.find` (dashboard + Google InfoWindow)  
7. Optional: branch filter, Search when query non-empty, label effect if names on  

**Not default-path (gated):** Supercluster realtime load (E2a).

---

## 3. Full fleet scans per ~800ms tick (typical: all branches, filter=`all`)

| Scan | Count |
|------|-------|
| materialize `.map` | 1 |
| branches forEach | 1 |
| filteredCars `.filter` | 1 |
| Filters forEach | 1 |
| Google meta Map | 1 |
| selectedCar finds | 1–2 |
| **Total ≈** | **5–6 O(N) passes** |

Filters is already **one** combined pass (not 4× `.filter().length`).

---

## 4. filteredCars behavior (CURRENT HEAD verified)

```219:230:TenantDashboard.jsx
const filteredCars = useMemo(() => {
  return carsByBranch.filter((car) => {
    if (activeFilter === "all") return true;
    ...
  });
}, [carsByBranch, activeFilter]);
```

- Branch filter: separate `carsByBranch` (reuse when no branch).  
- Status filter: inside this `.filter`.  
- Search: **not** in `filteredCars` — local to `Search.jsx`.  
- Sort: **none** on fleet list.  
- `.filter` **always** runs → **new array every list tick** even for `"all"`.  
- `"all"` **can** safely reuse `carsByBranch` reference (Option C).  
- Dirty-id incremental filter: possible for non-`all` but higher risk; `"all"` reuse is enough for default.

---

## 5. Counter implementation

| Counter | Predicate | Full scan? | Duplicate classification? |
|---------|-----------|------------|---------------------------|
| all | `cars.length` | length only | no |
| online | `!isInactive && !isOffline` | shared 1× forEach | no separate pass |
| offline | `isOffline && !inactive` (via early branch) | same pass | no |
| moving | online ∧ `isVehicleMoving(c)` | same pass | classifies moving only for online |
| inactive | counted but **UI commented out** | same pass | dead UI |

**Not** multiple independent `cars.filter(...).length` — already Option-A-shaped for counters alone.

---

## 6. Duplicated classification logic

| Site | API | When |
|------|-----|------|
| Filters | `isInactive` / `isOffline` / `isVehicleMoving` | every list tick |
| filteredCars | same + `isVehicleMoving` if filter=moving | every tick |
| CarRow | `getCarStatus` + `isVehicleMoving` | dirty ∩ visible |
| Google E1 | `getCarStatus` via `getMarkerVisualState` | dirty ids |
| Google InfoWindow | merge + find | fleetVersion |
| getCarStatus | offline/inactive/speed/moving/time strings | on call |

Authoritative: `vehicleFreshness.isVehicleMoving` + `getCarStatus` — must stay compatible.

---

## 7. Time-based freshness implications (CRITICAL)

**Can change without new GPS:**

| Rule | Clock input | Window |
|------|-------------|--------|
| `isVehicleMoving` | `lastMovingReceivedAtMs` vs `Date.now()` | 3 min (`MOVING_SIGNAL_STALE_MS`) |
| historical fix | `lastFixAtMs` | 5 min live age |
| `getCarStatus` status text | `getTimeDiffString` / `Date.now()` | continuous |
| offline/inactive | mostly flags from device/WS/HTTP | not pure client timers in dashboard |

**Periodic timer in TenantDashboard?**  
**None.** (OutsideTracking has 1s `setNow` — not this path.)

So moving counters / filter / CarRow / icons only re-evaluate when:

- a list flush rematerializes (any dirty), or  
- base `cars` changes, or  
- row/`car` identity changes.

**After last mover goes quiet for >3 min with zero fleet GPS:** moving count can stay inflated until the next tick elsewhere. **Pre-existing**, not introduced by incremental counters.

**Incremental dirty-only counters:** same time hole unless a cheap invalidation exists.  
→ Prefer **not** Option D as first phase without a simple refresh strategy.  
→ Correctness > micro-optimization: a single O(N) recount on each existing 800ms tick is acceptable and matches current time semantics.

---

## 8. Branch / base metadata scans

`branches` depends on `carsWithLive` → **rescans full fleet every realtime flush** though `branch_effective_*` is base metadata and almost never changes on GPS.

Opportunity: derive branches from base `cars` (or only when `carsBaseEpoch` / base identity changes).

`carsByBranch` already reuses `carsWithLive` when no branch selected.

---

## 9. Selected-car cost

- Dashboard: `carsWithLive.find` every `carsWithLive` change → every list tick if selected.  
- Google: `filteredCars.find` + merge on `fleetVersion`.  
- Store: `getFleetLive(id)` is O(1); merged selected needs base+live.  
- **Rank: P3 / negligible** vs 4–5 other O(N) passes (~µs for find at 2k). Optimize only if touching the file anyway (Option E cheap).

---

## 10. SideMenu rerender fan-out

| Child | Memo? | Rerender every flush? | Cost |
|-------|-------|----------------------|------|
| SideMenu | no | yes | shell |
| Search | no | yes | cheap if search empty |
| Filters | no | yes | **O(N) recount** |
| Actions | no | yes | Redux read only |
| CarsList | no | yes | virtualizer; rows memoized |
| CarRow | **memo** | dirty ∩ visible only | good |

Inline props: stable callbacks mostly (`handleSelectCar`); **new `filteredCars` / `branches` / `filterTypes`** drive child work.

Do **not** blanket `React.memo(SideMenu)` as the primary fix — cut O(N) inputs first.

---

## 11. Allocation summary (N=2000, one dirty id, filter=`all`, no branch)

| Allocation | Scale |
|------------|-------|
| Rematerialized car objects | **~1** (B1) ✓ |
| `carsWithLive` new array | **N** |
| `prevById` Map | **N** |
| `branches` Map + sorted array | **N** scan / **B** out |
| `filteredCars` | **N** (unnecessary for `"all"`) |
| Filters `filterTypes` | tiny |
| Google `carsMetaById` Map | **N** |
| Status objects | O(k∩visible) + Filters moving checks |

**Still scale with N:** arrays/Maps from materialize, branches, filteredCars(`all`), Filters pass, Google meta Map.

---

## 12. Ranked remaining DEFAULT-path bottlenecks

1. **`filteredCars` always cloning N** (cascades Google meta Map) — easiest win  
2. **Filters O(N) recount every tick** (already single pass; still N)  
3. **`materializeCarsWithFleet` O(N) array/Map** (harder; keep for correctness)  
4. **`branches` O(N) on every live tick** — should be base-only  
5. SideMenu parent rerender fan-out (secondary)  
6. selectedCar `.find` (minor)  
7. E2b cluster paint — **not default** (`clusters=false`)

---

## 13. Options A–F

| Option | Gain | Complexity | Correctness | Time-freshness risk |
|--------|------|------------|-------------|---------------------|
| **A** One combined classify/counter pass | Low–med (Filters already 1 pass; could merge with filter build) | Low–med | Low | Same as today |
| **B** Memoize branches from base `cars` | Med (drop 1 O(N)/tick) | Low | Low | None |
| **C** `filter==="all"` reuse `carsByBranch` | **High** (drop clone + Google Map churn) | **Very low** | Low | None |
| **D** Incremental dirty-id counters | High in theory | Med–high | Med | **High** without timer; defer |
| **E** O(1) selected lookup | Tiny | Low | Low | None |
| **F** SideMenu memoization | Low alone | Low | Low | None |

**Prefer:** C then B (simple), before D.  
**Avoid now:** workers, Redux redesign, entity frameworks, schedulers.

---

## 14. RECOMMENDED NEXT IMPLEMENTATION — ONE SMALL PHASE ONLY

### Phase F1 — Fast-path `filteredCars` when `activeFilter === "all"` (+ optional base-only `branches`)

**Scope (narrow):**

1. **Required:** If `activeFilter === "all"`, `filteredCars = carsByBranch` (same reference) — no `.filter` clone.  
2. **Optional same PR if tiny:** Compute `branches` from base `cars` / `carsBaseEpoch`, not every `carsWithLive` tick.  
3. Do **not**: incremental counters, timers, E2b, materialize rewrite, SideMenu memo sprawl.

**Expected after:**

- Default filter=`all`: **−1 O(N) array + −1 Google meta Map rebuild per tick** when `filteredCars` identity stable across GPS (carsByBranch reuses carsWithLive when no branch… wait: carsWithLive is NEW array every dirty flush, so carsByBranch still new identity when it returns carsWithLive!)

**Important correction for implementer:**

When `!activeBranchId`, `carsByBranch` returns `carsWithLive`, which is a **new array every materialize**. So Option C alone makes `filteredCars === carsByBranch` but both still change every tick.

**True win of C:** avoid second N-length copy (filteredCars); Google still sees new `cars` identity from materialize.

**Stronger F1 variant (still simple):**

- C: skip redundant filter copy for `"all"`.  
- Plus: GoogleMapView cars meta effect should not rebuild Map when `carIdsKey` unchanged (membership-stable) — **or** pass stable cars ref for map when only dirty rematerialize…  

Actually Google receives `filteredCars`. If filteredCars === carsWithLive (all + no branch), still new array from materialize every tick → meta Map still O(N).

So **highest-value smallest phase** should be either:

**F1a (simplest):** `activeFilter==="all"` reuse + **GoogleMapView: rebuild `carsMetaById` only when `carIdsKey` changes** (membership), not on every `cars` identity. Refs already update list for icons via dirty path; meta byId for position subscribe needs current car objects — can patch dirty ids into Map O(k) on fleetDirtyIds / carsBaseEpoch.

That's slightly more than "filter all reuse" but fixes the cascade.

**Audit recommendation for ONE phase:**

> **Phase F1: (1) `filteredCars` reference reuse when filter is `all`; (2) GoogleMapView `carsMetaById` update O(k)/membership-stable instead of full Map rebuild every `cars` identity change; (3) optionally move `branches` off `carsWithLive`.**

If forced to pick the absolute minimum single change: **(1)+(2)** because (1) alone is incomplete for Google default path.

**E2b:** clustering-enabled backlog only.

---

No implementation in this audit. No push. No deploy.
