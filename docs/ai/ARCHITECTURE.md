# AlFursan GPS Backend — Architecture

**Source of truth for AI agents.** Prefer CURRENT HEAD over old Grok/Codex audit reports.

Last updated: 2026-09-07 (P0-E architecture locked; E1 implemented)

---

## Priority order

1. REALTIME latency (listeners must receive movement immediately)
2. GPS history correctness (`gpspoints`)
3. Device/business transition correctness
4. Raw forensic archive (`traccar_ingress_raw`)
5. Analytics / reports

---

## Final process map (COMMITTED)

Exactly these responsibilities — **not** multiple full copies of the app, **not** PM2 cluster of the WS server, **no Redis**:

```text
1. realtime / bridge parent     (WS, forward, live, IPC dispatch)
2. raw archive worker           (P0-B2 — dedicated; forensic traccar_ingress_raw)
3. gpspoints-writer             (P0-E3 — dedicated PM2; Mongo drain only)
4. alfursan-analytics           (Phase 5 — EXACTLY ONE; global schedulers/reports)
5. 8 IMEI-partition workers     (Phase 4 — business/persistence; NO global schedulers)
```

| Process | Owns | Must NOT own |
|---------|------|----------------|
| Realtime parent | Auth, WS, live, IPC enqueue, light orchestration | Heavy reports, long gpspoints Mongo drain (final), raw Mongo drain (final) |
| Raw archive worker | `traccar_ingress_raw` journal→Mongo | WS, business, reports |
| **gpspoints-writer** | Legacy spool + segmented journal → Mongo batches, retry, quarantine, gpspoints metrics | WS, business, reports, model sync, tenant broadcast |
| **alfursan-analytics** | Global scheduled analytics/report generation (**exactly one** PM2 instance) | Packet path, WS |
| Workers 0..7 | Per-IMEI gpspoint **journal** + business | Global schedulers, owning long Mongo gpspoints drain (final) |

**Batching + isolation are both required.** Process split does not replace useful `insertMany` batches. Do **not** create 8 gpspoints-writers; partition later only if one properly batched writer cannot meet throughput.

### GPSPoints final handoff (filesystem)

```text
persistence / business side:
  receive GPSPoint → normalize/build → durable append/journal → ACK → continue business

Must NOT own long-running GPSPoints Mongo drain (final architecture).

gpspoints-writer:
  active → sealed/ready → draining → Mongo ACK → done/delete
```

Atomic renames. Crash at any state recoverable. Prefer filesystem durable handoff. **No Redis.**

### GPSPoints phases (P0-E) — staged, not one rewrite

| Phase | Work | Status |
|-------|------|--------|
| **E1** | Combine legacy spool files into useful Mongo batches (250–500); doc-based old/new fairness; raise drain budget; preserve durability + journal-before-ACK; **no** journal format change; **no** PM2 split | **COMPLETE (code)** |
| **E2** | Segmented durable journal + legacy reader coexistence until backlog zero | Next |
| **E3** | Move Mongo drain to dedicated PM2 `gpspoints-writer` (architectural isolation — not postponed solely on event-loop measurements) | After E2 |

Producer ACK remains: **local durability only** (never wait Mongo).

E1 knobs (defaults): `GPSPOINT_BATCH_SIZE=250`, `GPSPOINT_MAX_MONGO_BATCHES_PER_CYCLE=16`, `GPSPOINT_DRAIN_MAX_FILES_PER_CYCLE=500`, `GPSPOINT_DRAIN_OLD_DOC_RATIO=0.5`, `GPSPOINT_JOURNAL_COALESCE_MS=50`.
---

## High-level flow (current after P0-B)

```text
GPS devices
    → Traccar 6.13.2 (protocol decode, sessions, ACK; database.memory=true)
    → HTTP JSON Forward → Node realtime parent (traccar-bridge-ontherport.js)
         ├─ auth (Bearer)
         ├─ initiate raw local journal (async; full raw_payload; every attempt)
         ├─ normalize + exact-retry fingerprint
         ├─ FIRST attempt: forward queue → live eligibility → WS + persistence IPC
         ├─ RETRY of same forward: skip live + persistence; still attempt raw
         ├─ await raw LOCAL durable accept (disk journal) — NOT Mongo
         └─ HTTP 202 if raw durable OK; 503 if raw durable failed (realtime may already have run)
```

Frontend / Laravel consume Node realtime and Mongo-backed data.

---

## Target architecture (aligned with final process map)

```text
NODE REALTIME PARENT
  ├─ RAW ARCHIVE WORKER          (dedicated process)
  ├─ gpspoints-writer            (dedicated process — Mongo drain)
  ├─ alfursan-analytics          (EXACTLY ONE — global reports)
  └─ Worker 0..7                 (stableHash(IMEI)%8 — journal + business only)
```

**P0-B** separates semantic failure domains (realtime vs raw).  
**P0-B2 / E3 / Phase 5** separate process failure domains.

Do **not** PM2-cluster the entire WebSocket application. Do **not** introduce Redis/Kafka/RabbitMQ/MQTT unless later evidence requires it.

**Invariant:** Global reports/schedulers run only in `alfursan-analytics` (one instance). Partition workers and gpspoints-writer must not start them. Realtime parent must not run heavy report generation.

---

## Authoritative identity

- Permanent identity: **IMEI == Traccar `uniqueId`**
- Traccar numeric `device.id` is runtime-only (memory mode)
- Traccar `position.id` may be `0` — never use as persistent identity or retry fingerprint

---

## HTTP `/traccar/position` semantics (P0-B)

| Step | Behavior |
|------|----------|
| Auth fail | 401 / 503 token missing — no raw, no realtime |
| Invalid JSON / non-JSON | 400 / 415 |
| Raw initiate | Always attempted for authenticated JSON body |
| Normalize + retry dedupe | Exact Traccar retry fingerprint (in-memory TTL/LRU) |
| Downstream | First fingerprint: forward queue (WS + persistence). Retry: suppressed |
| Raw durable | Await **local journal append** only |
| Success | **202** when raw durable OK |
| Raw durable fail | **503** so Traccar retries raw; realtime already ran on first attempt |
| Forward queue full | **503**; fingerprint abandoned so retry can reprocess downstream |

### Raw durable acceptance (definition)

- **Not** Mongo `insertMany` completion
- **Is** successful async append into the local `.jsonl.active` spool file
- `enqueue().accepted` = accepted into bounded RAM queue
- `enqueue().durable` Promise = resolves when journaled to disk (or fails)
- Realtime must **never** await raw Mongo

### Raw forensic vs downstream dedupe

- **Raw archive:** every authenticated HTTP forward attempt may be stored (retries included) — at-least-once forensic
- **Realtime / gpspoints / business:** exact retries suppressed via fingerprint

---

## Exact retry fingerprint

Module: [`lib/forwardRetryDedupe.js`](../../lib/forwardRetryDedupe.js)

Ingredients (stable on Traccar retry of the same Position):

`imei | protocol | fixTime | deviceTime | serverTime | lat6 | lon6 | attributes.type | attributes.alarm | result[:64]`

- Does **not** use `position.id`
- Legitimate stationary samples with new timestamps are **not** suppressed
- Defaults: `FORWARD_RETRY_DEDUPE_TTL_MS=120000`, `FORWARD_RETRY_DEDUPE_MAX=100000` (~50MB order)

### Fingerprint / `serverTime` stability (Traccar 6.13.2)

Source: `PositionForwardingHandler` keeps the **same** `PositionData` (same `Position` object) across HTTP forward retries (`AsyncRequestAndCallback` resends `this.positionData`). Therefore retries of the **same** forward attempt preserve:

- `fixTime`, `deviceTime`, `serverTime` (set once when the Position is created/enriched)
- coordinates, protocol, attributes snapshot on that Position

`serverTime` is **not** rewritten on each HTTP delivery attempt for the same Position. It remains in the fingerprint.

**Caveat (not observed as retry mutation):** the `Device` object is read from `CacheManager` at first forward construction; if a live mutable Device were mutated between retries, device-side fields in the JSON could change — our fingerprint uses Position fields + IMEI/`uniqueId`, not mutable device status. **Do not remove `serverTime` without production evidence** of retry fingerprint mismatch.

Production verification: set `BRIDGE_DEBUG_IMEI=<imei>` to log `forward_retry_fingerprint` / `forward_retry_state` on the forward path.

### Retry cache memory & cleanup

| Entries | ≈ memory @ 500 B/entry |
|---------|-------------------------|
| 50k | ~25 MB |
| 100k | ~50 MB (current default) |
| 250k | ~125 MB (for ~2k pkt/s × 120s) |

Cleanup: expire walk is O(expired prefix), capped at 64 deletes/call — no full-map scan. Capacity eviction prefers non-`processing` entries. Metrics: `forward_retry_cache_size`, `forward_retry_cache_expired_total`, `forward_retry_cache_evicted_capacity_total`.

### Processing states

- `processing` — fingerprint claimed; concurrent identical HTTP attempts **suppress**
- `processed` — only after forward queue **accepted** (`commit`)
- `abandon` — forward queue rejected → entry removed → Traccar retry may dispatch downstream again

### Raw durable latency

HTTP awaits local journal only. Metrics: `raw_durable_accept_latency_ms` (last) + p50/p95/p99 from a 128-sample ring.

---

## Realtime path invariant

Realtime must **not** await:

- Mongo (including raw Mongo)
- parking / geofence / trip / notification DB work
- analytics / reports

HTTP may await **local raw journal** only (for Traccar retry contract).

---

## Collections (active)

| Collection | Role |
|------------|------|
| `traccar_ingress_raw` | Short-term forensic archive; full `raw_payload`; TTL |
| `gpspoints` | Lean historical track |
| `devicestatuses` | Latest status / fence / connectivity |
| `device_details` | Includes `tr_model` |
| event collections | ACC, overspeed, etc. |

**Do not reintroduce `gpslogs`.**

---

## Metric meanings (selected)

| Metric | Meaning |
|--------|---------|
| `raw_durable_accept_success_total` | Local journal durable OK |
| `raw_durable_accept_failure_total` | Queue full / journal fail |
| `raw_failure_realtime_continued_total` | Raw reject but first-attempt realtime still dispatched |
| `forward_exact_retry_total` | Fingerprint matched prior forward |
| `forward_retry_suppressed_live_total` | Live path skipped due to exact retry |
| `forward_retry_suppressed_persistence_total` | Persistence path skipped due to exact retry |
| `forward_retry_cache_size` | Dedupe map size |
| `raw_ingress_mongo_attempted_total` | Async Mongo (after journal) — not on HTTP critical path |

---

## Persistence IPC (completed)

Parent batches `PERSISTENCE_IPC_BATCH_MAX_ITEMS=100` / `WAIT_MS=5`. ACK ≠ Mongo force-flush.

---

## What MUST NOT be reintroduced

- Raw gating realtime (pre-P0-B 503-before-forward)
- `gpslogs`; Redis/Kafka/etc; PM2 cluster whole WS app
- GT06 parse / voltage in Node; `position.id` identity
- Sampling / stripping `raw_payload`
- Global analytics in all future partition workers

---

## Traccar model sync (lifecycle, not packet)

**Invariant:** Traccar `device.model` synchronization is **device-lifecycle triggered**, never per-packet.

Hot path after resolution: **Map lookup only** — no Mongo, no Traccar HTTP, no log spam.

| Case | Behavior |
|------|----------|
| First see IMEI | Resolve `device_details.tr_model` (fallback `devicestatuses`) **once** |
| `tr_model` missing | `no_model` negative cache (default **10 min**); log **once**; later packets silent |
| Negative TTL expiry | One re-lookup; if still missing, stay quiet (no repeated `skipped: no tr_model`) |
| Synced + matching model | Silent |
| Forwarded `model=null` after Traccar restart | Invalidate runtime sync; re-PUT using **cached desired model** (no Mongo if positive cache warm) |
| Runtime device id changes | New runtime lifecycle; re-sync once with cached desired model |
| `MODEL_A`→`MODEL_B` | Positive metadata TTL refresh, then one sync |

Defaults: `TRACCAR_MODEL_NEGATIVE_CACHE_TTL_MS=600000`, `TRACCAR_MODEL_CACHE_TTL_MS=300000`.  
Debug: `TRACCAR_MODEL_SYNC_DEBUG_IMEI`. Invalidate: `traccarModelSync.invalidate(imei)`.

Metrics: `requested` / `skipped_no_model` count **resolution/sync work**, not every GPS packet.

---

## Related docs

- [`PERFORMANCE-ROADMAP.md`](./PERFORMANCE-ROADMAP.md)
- [`../traccar-bridge-reliability.md`](../traccar-bridge-reliability.md)
