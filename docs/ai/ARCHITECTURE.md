# AlFursan GPS Backend — Architecture

**Source of truth for AI agents.** Prefer CURRENT HEAD over old Grok/Codex audit reports.

Last updated: 2026-09-06 (P0-B)

---

## Priority order

1. REALTIME latency (listeners must receive movement immediately)
2. GPS history correctness (`gpspoints`)
3. Device/business transition correctness
4. Raw forensic archive (`traccar_ingress_raw`)
5. Analytics / reports

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

## Target architecture (not fully built)

```text
NODE REALTIME PARENT
  ├─ RAW ARCHIVE WORKER (isolated process)   ← P0-B2 / follow-on; still needed
  ├─ Worker 0..7  (stableHash(IMEI) % 8) — gpspoints + business
  └─ ONE analytics / reports worker
```

**P0-B** separates **semantic** failure domains (realtime continues if raw queue/Mongo unhealthy).  
**Raw process isolation** (dedicated worker so disk/Mongo raw work cannot stall the parent event loop) remains **P0-B2** — not implemented in P0-B.

Do **not** PM2-cluster the entire WebSocket application. Do **not** introduce Redis/Kafka/RabbitMQ/MQTT unless later evidence requires it.

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

## Related docs

- [`PERFORMANCE-ROADMAP.md`](./PERFORMANCE-ROADMAP.md)
- [`../traccar-bridge-reliability.md`](../traccar-bridge-reliability.md)
