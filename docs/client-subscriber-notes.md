# Client / subscriber notes (WebSocket)

Public payload contract is **unchanged**:

- `type` / `data` / `imei` / `gps` / `legacy`
- `command_response`
- `command_response_chanel` (spelling kept)
- `subscribe` / `unsubscribe`
- tenant room protocol (`subscribe_tenant_room`, `tenant_gps_update`)

New diagnostic fields, if any, are additive only.

## Recommended client-side behavior

These are recommendations. The server now filters historical GPS, but a client that treats **every** message as “move the car now” can still look wrong if it:

1. Interpolates leftover points
2. Uses `serverTime` instead of `fixTime` / `packet_date`
3. Replays a buffer after reconnect

### 1. Use fix time, not arrival time

Prefer `data.packet_date` or `data.traccar_fix_time` / `data.fixTime`. Do **not** use wall-clock “message received now” as the vehicle timestamp.

### 2. Latest-wins on the map

For the live map, keep `lastFixAt` per IMEI. Ignore a GPS update whose fix time is older than the last rendered fix.

### 3. Do not animate historical jumps

If `packet_date` is older than ~5 minutes, treat it as archive/playback, not live movement. The server already suppresses these for live send; keep the same rule locally as defense in depth.

### 4. Commands

Subscribe to `command_response_chanel` for command replies. Do not wait for GPS rooms. Command replies are not coalesced.

### 5. Heartbeat / reconnect

The server pings. Respond to WebSocket ping (browsers do this automatically). If the socket is terminated due to backpressure, reconnect; you will get the **latest** live position, not the missed intermediates. That is expected.

### 6. Playback

Historical movement belongs in playback APIs (`gpspoints`), not in the live socket. After this change, a 7-day Traccar backfill will **not** drive the live marker.

### 7. Optional: ignore `device` heartbeats for coordinates

`type: "device"` is status. Do not move the marker from it unless your existing app already did (unchanged).
