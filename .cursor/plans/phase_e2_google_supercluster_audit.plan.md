# Phase E2 — Google Supercluster Performance Audit

**Mode:** PLAN ONLY — NO IMPLEMENTATION.  
**Source of truth:** HEAD after A + B1 + full bootstrap + B2 + D + E1.  
**Scope:** Google Supercluster path only. Node/Laravel frozen.

---

## 1. Exact current Supercluster flow

```text
GPS / tenant_gps_update
  → patchFleetLive(deviceId)                    [O(1)]
  → subscribeFleet(applyFleetPositions)         [immediate]
      → animate marker via RAF setPosition      [O(1) — KEEP]
      → mutate geojsonFeaturesRef feature coords if lat/lng changed
      → clusterDirty = true (any coordinate inequality)
      → scheduleClusterReload()                 [200ms leading coalesce]
            → IF timer already armed: no-op (do not reset)
            → ELSE setTimeout 200ms:
                  supercluster.load(geojsonFeaturesRef)   // FULL N features
                  triggerClusterRefresh()                 // another 200ms coalesce
                        → google.maps.event.trigger(map, "idle")
                              → updateClusters()
                                    → IF clusters===false:
                                          clear cluster markers
                                          markers.forEach setVisible(true)  // O(N)
                                    → IF clusters===true:
                                          markers.forEach setVisible(false) // O(N)
                                          force selected marker visible
                                          clear + recreate cluster Markers
                                          getClusters(bbox, zoom)           // viewport query
                                          setVisible(true) for leaf cars in result
```

**Parallel membership path (not GPS):**

```text
carIdsKey change (filter/branch/add/remove)
  → buildGeoJsonFeatures(carsMeta)   // O(N) allocate all features
  → replace geojsonFeaturesRef + featureByCarIdRef
  → supercluster.load(features)      // immediate, not 200ms
  → triggerClusterRefresh()
```

**Config (current):**

| Setting | Value |
|--------|--------|
| Supercluster `radius` | 60 |
| Supercluster `maxZoom` | 18 (index clustering depth) |
| Supercluster `minPoints` | 3 |
| Reload coalesce | 200ms |
| Paint refresh coalesce | 200ms |
| Redux `clusters` default | **false** |
| Query | `getClusters(viewport bbox, map.getZoom())` |

There is **no** separate clustering helper module — all logic lives in `GoogleMapView.jsx`.

---

## 2. Exact triggers for `supercluster.load`

| ID | Trigger | Code path | Full load required? |
|----|---------|-----------|---------------------|
| A | Realtime position change (coords differ) | `scheduleClusterReload` → `load` | **MAYBE** — index wants updated coords; live markers already moved |
| B | Device added (new feature pushed) | same + `carIdsKey` effect | **YES** (membership) |
| C | Device removed | `carIdsKey` effect rebuilds features | **YES** |
| D | Base metadata change (same ids) | does **not** call `load` by itself | **NO** for cluster index (coords unchanged) |
| E | Filtering | `carIdsKey` change → immediate `load` | **YES** (membership) |
| F | Branch change | same as filter via filtered cars | **YES** |
| G | Zoom change | does **not** call `load`; only `idle` → `getClusters` | **NO** for load |
| H | Bounds change | same — paint only | **NO** for load |
| I | Map provider lifecycle / Google mount | subscribe + carIdsKey effects | **YES** on init |
| J | Initial bootstrap | carIdsKey populate → `load` | **YES** |

**Critical finding:** `scheduleClusterReload` / `load` do **not** read Redux `clusters`. Index rebuild runs even when clustering UI is **off** (default).

---

## 3. Actual rebuild frequency under continuous GPS traffic

```176:195:GoogleMapView.jsx
// both timers: if (timerRef.current) return;  // arm once, ignore until fire
setTimeout(..., 200);
// on fire: timerRef = 0  → next event can arm again
```

| Question | Answer from code |
|----------|------------------|
| Configured delay | **200ms** |
| Trailing vs leading | **Leading-edge coalesce** (not trailing reset) |
| Timer resets on every packet? | **No** — subsequent packets while armed are ignored |
| Under continuous traffic | After fire, next packet re-arms → **≈ every 200ms indefinitely** |
| Idle-trailing (“200ms after quiet”)? | **No** — not that pattern |

With 200 moving vehicles sending continuously: cluster `load` ≈ **5×/second**, not “once after idle.”

---

## 4. Feature-generation cost

**Realtime path (good-ish):** does **not** `cars.map` full rebuild. Mutates existing feature coordinates in place for changed ids → **O(k)** before `load`.

**Membership path (`carIdsKey`):** `buildGeoJsonFeatures`:

- forEach cars + `mergeCarWithFleet`
- `Map` dedupe
- `Array.from(...).map` → new GeoJSON Feature objects  

→ **O(N)** allocations + then `load`.

---

## 5. `supercluster.load` complexity/cost

Always loads **`geojsonFeaturesRef.current` = full current filtered fleet (N)**, not dirty subset.

Cost: Supercluster rebuild ≈ **O(N log N)** (KDBush-style index), up to ~5/s under continuous GPS.

Viewport does **not** shrink `load` input. Only `getClusters(bbox, zoom)` is viewport-scoped.

---

## 6. Visibility / paint O(N) cost

Every `updateClusters` (idle / refresh / clusters toggle / carIdsKey / selectedCarId):

| Mode | Work |
|------|------|
| `clusters === false` | `markers.forEach` → `setVisible(true)` + ensure on map → **O(N)** |
| `clusters === true` | `markers.forEach` → `setVisible(false)` **O(N)**; then show leaves from `getClusters` (≈ viewport); recreate all cluster circle markers from scratch |

No visibility diffing. Most `setVisible` calls are no-ops in value but still **O(N) API calls**.

Cluster markers: full clear + recreate each paint (not incremental).

This paint cost is **as important as `load`**, and fires after every coalesced reload **and** on map idle (pan/zoom).

---

## 7. Zoom behavior

| Layer | Behavior |
|-------|----------|
| Supercluster `maxZoom: 18` | Above 18, index returns points not clusters |
| GoogleMapView | **No zoom gate** on `scheduleClusterReload` |
| High zoom (e.g. 14–17) | Many/most leaves visible; **`load` still runs every ~200ms** if positions move |
| LOW ZOOM | Clusters matter visually |
| HIGH ZOOM | Individual markers + E1 icons matter; cluster index rebuild is low value |

**Answer to E2.6:** YES — at high zoom, Supercluster still rebuilds on the same 200ms path. Skipping reload when clustering is off **or** when zoom ≥ useful clustering range is high-value.

---

## 8. Selected-car implications

- Selected id forced `setVisible(true)` before leaf reveal (stays visible even if still “inside” a cluster feature).
- `selectedCarId` change → immediate `updateClustersRef()`.
- Selection centers via `TenantDashboard.handleSelectCar` → `setCenter` (one-shot), **not** continuous GPS follow via cluster path.
- Live selected marker position still updates via immediate `subscribeFleet` RAF path (independent of cluster lag).
- Throttling cluster index **must not** hide selected marker; keep force-visible. InfoWindow uses merged live state + `fleetVersion` — OK if cluster paint lags.

---

## 9. Dirty-id usefulness (verdicts)

| Option | Verdict |
|--------|---------|
| A. Incremental Supercluster mutate API | **NOT SUPPORTED** (no safe invent) |
| B. Dirty ids decide WHETHER rebuild needed | **POSSIBLE** (e.g. only if any dirty had coord change — already roughly true via `clusterDirty`) |
| C. Batch dirties into less frequent full rebuild | **SAFE** / **POSSIBLE** (raise coalesce; leading→longer interval) |
| D. Skip rebuild at high zoom / clusters off | **SAFE** / **POSSIBLE** (highest value) |
| E. Rebuild only on meaningful spatial displacement | **POSSIBLE** (cluster-index deadband only; not Node live deadband) |
| F. No safe dirty-id optimization | Partial — dirty ids don’t replace `load(N)`; they help **gating/batching**, not incremental index |

Dirty ids alone cannot make `load` O(k). They help **when** to pay O(N log N).

---

## 10. Safe vs unsafe strategies

| Strategy | CPU gain | Complexity | Correctness | UX | Files |
|----------|----------|------------|-------------|-----|-------|
| **A** Raise realtime coalesce 200→750–1000ms | Medium (when clusters ON) | Low | Low risk | Cluster blobs lag 1s behind markers | `GoogleMapView.jsx` |
| **B** Skip reload when `!clusters` and/or zoom where clustering irrelevant | **High** (default `clusters:false`) | Low | Low | None when off; at high zoom clusters already rare | `GoogleMapView.jsx` |
| **C** Cluster-index positional deadband | Medium | Medium | Medium (membership edge cases at low zoom) | Slight cluster lag | `GoogleMapView.jsx` |
| **D** Diff visibility vs O(N) `setVisible` | High on every paint | Medium | Medium (must preserve selected) | None if correct | `GoogleMapView.jsx` |
| **E** Dirty-driven “must rebuild?” only | Low alone | Low | Low | — | small |
| **F** Leave clustering; optimize Filters | Separate win | — | — | — | Filters (out of E2 scope) |

**Unsafe / out of scope:** replace Supercluster, MarkerClusterer rewrite, workers, server clustering, viewport-only `load` without UX proof.

---

## 11. Ranked bottlenecks inside clustering

1. **`load(full N)` while `clusters === false` (default)** — pure waste  
2. **`load` ~5/s under continuous GPS when clusters ON** — O(N log N) churn  
3. **O(N) `setVisible` on every paint/idle** (both modes)  
4. Full cluster-marker recreate each paint  
5. Membership `buildGeoJsonFeatures` O(N) (acceptable on filter/branch; not GPS)  
6. No zoom skip when leaves dominate  

---

## 12. Recommended ONE smallest implementation phase

**Gate Supercluster realtime `load` + avoid useless paint work when clustering is disabled; optionally skip reload above Supercluster `maxZoom`.**

Do **not** yet: deadband, visibility diff, Filters, longer interval (unless bundled as tiny constant).

---

## 13. Expected before / after

| | Before | After (proposed E2 impl) |
|--|--------|---------------------------|
| Default (`clusters:false`) + 200 movers | `load` ≈ every 200ms + O(N) setVisible via idle | **No realtime `load`**; no cluster-driven idle spam from reload |
| `clusters:true`, low zoom | `load` ≈ every 200ms | Still needed; follow-up can lengthen coalesce |
| `clusters:true`, zoom > 18 | `load` still | Skip realtime `load` (index unused for clusters) |
| Immediate marker motion | O(1) | **Unchanged** |
| E1 icons | O(k) | **Unchanged** |
| Filter/branch | immediate `load` | **Unchanged** (prompt) |

---

## 14. Exact files expected to change (future impl)

- `src/pages/TenantDashboard/Maps/GoogleMapView.jsx` (primary)
- `test/phaseE2GoogleClusterGate.test.js` (new focused tests)
- Possibly tiny shared constant helper — only if needed

**Do not change:** other map providers, E1 helpers (except if shared timer constants), Filters, CarsList, Node, Laravel.

---

## 15. Focused tests required (future)

1. `clusters === false` + position patches → **`load` not scheduled / not called**  
2. `clusters === true` + position change → reload still coalesces  
3. Leading 200ms coalesce behavior preserved when ON (or documented if interval changed)  
4. Zoom > `maxZoom` → skip realtime reload (if that gate ships)  
5. `carIdsKey` / filter membership → **immediate** rebuild still occurs  
6. Selected marker remains force-visible when clusters ON  
7. Immediate `subscribeFleet` position path unchanged (no regression)  
8. Phase A/B1/B2/D/E1 tests still pass  
9. `npm run build`

No synthetic 10k load test.

---

## Live marker vs cluster index (E2.5)

Realtime markers already show truth via RAF. Cluster index is a **spatial summary** for low-zoom blobs. Being 0.2–2s behind is usually acceptable; 200ms full rebuild is not required for correctness of individual movement. UX risk of slower rebuild: cluster counts/positions lag; clicking a cluster may expand based on slightly stale coords — acceptable if gated/throttled carefully. Selected car remains on immediate path.

---

## RECOMMENDED PHASE E2 IMPLEMENTATION

**E2a — Gate realtime Supercluster reload (and its idle paint cascade) when `clusters` is false; skip realtime `load` when map zoom > Supercluster `maxZoom`; keep membership/`carIdsKey` rebuilds immediate; leave E1 and marker RAF untouched.**

STOP. NO IMPLEMENTATION in this audit.
