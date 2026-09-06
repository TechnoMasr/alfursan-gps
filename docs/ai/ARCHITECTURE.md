# AlFursan GPS Backend — Architecture

**Source of truth for AI agents.** Prefer CURRENT HEAD over old Grok/Codex audit reports.

Last updated: 2026-09-06 (P0-A)

---

## Priority order

1. REALTIME latency (listeners must receive movement immediately)
2. GPS history correctness (`gpspoints`)
3. Device/business transition correctness
4. Raw forensic archive (`traccar_ingress_raw`)
5. Analytics / reports

---

## High-level flow (current)

```text
GPS devices
    → Traccar 6.13.2 (protocol decode, sessions, ACK; database.memory=true)
    → HTTP JSON Forward → Node realtime parent (traccar-bridge-ontherport.js)
         ├─ raw ingress writer (in-process today) → disk spool → traccar_ingress_raw
         ├─ forward queue → live eligibility → WebSocket broadcast
         └─ persistence IPC (batched) → persistence-worker
                ├─ gpspoints (journaled before IPC ACK; Mongo flush async)
                └─ business (parking / status / geofence / trips / ACC / …)
```

Frontend / Laravel consume Node realtime and Mongo-backed data.

---

## Target architecture (not fully built)

```text
NODE REALTIME PARENT
  ├─ RAW ARCHIVE WORKER (isolated process)
  ├─ Worker 0..7  (stableHash(IMEI) % 8) — gpspoints + business
  └─ ONE analytics / reports worker
```

Do **not** PM2-cluster the entire WebSocket application. Do **not** introduce Redis/Kafka/RabbitMQ/MQTT unless later evidence requires it.

---

## Authoritative identity

- Permanent identity: **IMEI == Traccar `uniqueId`**
- Traccar numeric `device.id` is runtime-only (memory mode)
- Traccar `position.id` may be `0` — never use as persistent identity

---

## Realtime path invariant

Realtime must **not** await:

- Mongo
- raw Mongo flush
- parking / geofence / trip / notification DB work
- analytics / reports

Conceptually: Traccar HTTP packet → realtime WS **and** (separately) persistence/business + raw archive.

### Failure domains (important)

| Phase | Behavior |
|-------|----------|
| **HEAD before P0-B** | Raw enqueue reject → HTTP **503 before** normalize/forward/WS. Raw can kill realtime. |
| **P0-A** | Fixes raw consumer deadlock only; **preserves** 503-before-realtime for clean production attribution. |
| **P0-B (next)** | Raw failure must not prevent realtime. Retry fingerprint / durability semantics change then. |

---

## Collections (active)

| Collection | Role |
|------------|------|
| `traccar_ingress_raw` | Short-term forensic archive of every authenticated forward; full `raw_payload`; TTL retention |
| `gpspoints` | Lean historical track (`gps` / `alarm` with valid coords; ignition when available) |
| `devicestatuses` | Latest device status / fence state / connectivity |
| `device_details` | Device metadata including `tr_model` |
| event collections | ACC, overspeed, etc. |

**Do not reintroduce `gpslogs`.**

---

## Raw ingress writer (P0-A semantics)

In-process writer: [`lib/traccarRawIngressWriter.js`](../../lib/traccarRawIngressWriter.js)

```text
enqueue (bounded RAM queue)
  → async append to .jsonl.active (no sync FS on hot path)
  → rotateActive when batch ready (only if writingActive === false)
  → drain stable .jsonl via insertMany (single flush owner)
  → unlink after full-file success
  → on failure: keep spool, backoff retry
```

**Invariants (P0-A):**

- Single drain owner (`flushing`): no concurrent rotate/drain/persist on the same files from flush + retry + startup + shutdown.
- Never `rotateActive` while `writingActive` (append in progress). Extra `rotating` gate blocks new appends only during rename.
- Non-empty memory queue must **not** block rotate/Mongo drain.
- New ingress journaling and old spool → Mongo must both progress (fair bounded cycles).
- No sampling; no `raw_payload` removal.

### Metric meanings

| Metric | Meaning |
|--------|---------|
| `raw_ingress_received_total` | HTTP forwards seen (bridge increments) |
| `raw_ingress_accepted_total` | Enqueue accepted into writer |
| `raw_ingress_queue_rejected_total` | Enqueue rejected (full/closed) |
| `raw_ingress_queue_depth` | `active_journal_docs + memory_queue` (not yet in stable pending spool) |
| `raw_ingress_memory_queue_depth` | Docs waiting in RAM to be journaled |
| `raw_ingress_active_journal_docs` | Docs in current `.jsonl.active` file |
| `raw_ingress_pending_spool_docs` | Docs in stable `.jsonl` waiting for Mongo |
| `raw_ingress_spooled_total` | Lifetime successful journal appends |
| `raw_ingress_spool_depth` | `queue_depth + pending_spool_docs` (all not-yet-Mongo-acked) |
| `raw_ingress_mongo_attempted_total` | Docs passed to `insertMany` |
| `raw_ingress_persisted_total` | Docs after successful `insertMany` |
| `raw_ingress_persist_failures` | Flush/append/recovery failures |

At-least-once: Mongo success then crash before unlink, or partial multi-batch file retry, can duplicate raw docs. Silent loss of accepted journaled data is not acceptable.

---

## Persistence IPC (completed Phase 1)

- Parent batches: `PERSISTENCE_IPC_BATCH_MAX_ITEMS=100`, `PERSISTENCE_IPC_BATCH_MAX_WAIT_MS=5`
- One inflight IPC batch, one persistence worker (today)
- ACK = durably accepted/recoverable (journal/spool), **not** Mongo force-flush complete
- Do not remove batching; do not put Mongo force flush back before ACK

---

## Connectivity meaning

**Offline** = tracker lost connection to Traccar/server.  
Not: speed 0, stale fix, ignition off, no movement.

---

## What MUST NOT be reintroduced

- `gpslogs` fat archive
- Redis / Kafka / RabbitMQ / MQTT (unless forced by evidence later)
- PM2 cluster of the whole realtime app
- Parsing GT06 / calculating voltage in Node
- Unbounded queues / unbounded `Promise.all`
- Treating `position.id = 0` as durable identity
- Forcing Mongo flush before IPC ACK
- Sampling or stripping raw `raw_payload`
- Global analytics schedulers started in all 8 future partition workers

---

## Related docs

- [`PERFORMANCE-ROADMAP.md`](./PERFORMANCE-ROADMAP.md) — phases, SLOs, execution order
- [`../traccar-bridge-reliability.md`](../traccar-bridge-reliability.md) — live vs historical policy
