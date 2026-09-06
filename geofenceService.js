const { DeviceStatus } = require('./mongo');
const { getGpsLogsWriter } = require('./lib/gpsLogsWriter');

function toRad(x) {
  return (x * Math.PI) / 180;
}

// Returns distance in meters
function haversineMeters(lat1, lon1, lat2, lon2) {
  const R = 6371000; // meters
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

function isValidCoord(lat, lon) {
  return (
    Number.isFinite(lat) &&
    Number.isFinite(lon) &&
    lat >= -90 &&
    lat <= 90 &&
    lon >= -180 &&
    lon <= 180
  );
}

function normalizePolygonCoordinates(coords) {
  // Accept:
  // - [[lat,lon], ...]
  // - [[[lat,lon], ...], ...] (GeoJSON rings) -> take first ring
  if (!Array.isArray(coords) || coords.length === 0) return [];
  if (Array.isArray(coords[0]) && typeof coords[0][0] === 'number') {
    return coords;
  }
  if (Array.isArray(coords[0]) && Array.isArray(coords[0][0])) {
    return coords[0] || [];
  }
  return [];
}

// Ray casting algorithm
function pointInPolygon(lat, lon, coords) {
  const poly = normalizePolygonCoordinates(coords);
  if (poly.length < 3) return false;

  // coords are [lat, lon]
  let inside = false;
  for (let i = 0, j = poly.length - 1; i < poly.length; j = i++) {
    const yi = poly[i][0];
    const xi = poly[i][1];
    const yj = poly[j][0];
    const xj = poly[j][1];

    const intersect =
      yi > lat !== yj > lat &&
      lon < ((xj - xi) * (lat - yi)) / (yj - yi + 0.0) + xi;
    if (intersect) inside = !inside;
  }
  return inside;
}

function isInsideFence({ fence, lat, lon }) {
  if (!fence || !isValidCoord(lat, lon)) return false;
  const type = String(fence.type || '').toLowerCase();

  if (type === 'circle') {
    const cLat = Number(fence.latitude);
    const cLon = Number(fence.longitude);
    const radiusM = Number(fence.radius);
    if (!isValidCoord(cLat, cLon) || !Number.isFinite(radiusM) || radiusM <= 0) return false;
    const d = haversineMeters(lat, lon, cLat, cLon);
    return d <= radiusM;
  }

  if (type === 'polygon') {
    return pointInPolygon(lat, lon, fence.coordinates);
  }

  return false;
}

function fenceKey(fence) {
  const id = fence?._id ?? fence?.id ?? fence?.fence_id;
  return id == null ? null : String(id);
}

/**
 * Evaluate fences for a point and generate events (enter/exit) based on DeviceStatus.fences.
 *
 * - Stores per-fence state in DeviceStatus.fence_state.<fenceId> = { inside, last_change_at, last_checked_at }
 * - Creates events in GpsLog as alarms:
 *    - Exit  -> type='alarm', alarmType=1001
 *    - Enter -> type='alarm', alarmType=1000
 *    - Overspeed in zone -> type='alarm', alarmType=1002 (على انتقال بداية السرعة الزائدة فقط)
 *
 * Returns: Array of created GpsLog docs (plain objects).
 */
async function evaluateGeofences({ statusDoc, imei, lat, lon, speed, packetDate }) {
  const pointLat = Number(lat);
  const pointLon = Number(lon);
  if (!imei || !isValidCoord(pointLat, pointLon)) return [];

  let status = statusDoc;
  if (!status || String(status.imei) !== String(imei)) {
    status = await DeviceStatus.findOne({ imei }).lean();
  } else if (typeof status.toObject === 'function') {
    status = status.toObject();
  }

  const fences = Array.isArray(status?.fences) ? status.fences : [];
  if (!fences.length) return [];

  const now = new Date();
  const at = packetDate ? new Date(packetDate) : now;

  const prevState = status?.fence_state && typeof status.fence_state === 'object' ? status.fence_state : {};
  const setOps = {};
  const createdEvents = [];

  for (const fence of fences) {
    const key = fenceKey(fence);
    if (!key) continue;

    const insideNow = isInsideFence({ fence, lat: pointLat, lon: pointLon });
    const prevInside = prevState?.[key]?.inside ;
    const prevOverspeed = !!prevState?.[key]?.overspeed?.active;

    // Always update last_checked; update inside + last_change only if changed or uninitialized
    setOps[`fence_state.${key}.last_checked_at`] = at;
    // console.log('fence', fence);
    // First observation: initialize without generating event
    if (typeof prevInside !== 'boolean') {
      setOps[`fence_state.${key}.inside`] = insideNow;
      setOps[`fence_state.${key}.last_change_at`] = at;
      // init overspeed state too (no alert on first observation)
      setOps[`fence_state.${key}.overspeed.active`] = false;
      setOps[`fence_state.${key}.overspeed.last_change_at`] = at;
      continue;
    }

    if (prevInside === insideNow) {
      // no transition: still update overspeed state and possibly emit overspeed-start once
      // (نعملها هنا حتى لا تعتمد على تغير inside)
    }

    // 1) Handle enter/exit transitions
    if (prevInside !== insideNow) {
      setOps[`fence_state.${key}.inside`] = insideNow;
      setOps[`fence_state.${key}.last_change_at`] = at;

      const notifyOnEnter = !!fence.notify_on_enter;
      const notifyOnExit = !!fence.notify_on_exit;
      const event = insideNow ? 'enter' : 'exit';
      const shouldNotify = event === 'enter' ? notifyOnEnter : notifyOnExit;
    
      if (shouldNotify) {
        const alarmType = event === 'exit' ? 1001 : 1000;
        const fenceName = fence?.name ?? null;
        const alarmText = event === 'exit'
          ? `Geofence Exit${fenceName ? `: ${fenceName}` : ''}`
          : `Geofence Enter${fenceName ? `: ${fenceName}` : ''}`;
        const alarmTextAr = event === 'exit'
          ? `خروج من السياج${fenceName ? `: ${fenceName}` : ''}`
          : `دخول إلى السياج${fenceName ? `: ${fenceName}` : ''}`;

        const evtDoc = {
          imei,
          type: 'alarm',
          subType: 'geofence',
          alarmType,
          alarmText,
          alarmTextAr,
          geofence_event: event, // 'enter' | 'exit'
          fence_id: fence?._id ?? fence?.id ?? null,
          fence_name: fenceName,
          fence_type: fence?.type ?? null,
          latitude: pointLat,
          longitude: pointLon,
          speed: Number(speed) || 0,
          date: at,
          packet_date: at,
        };

        createdEvents.push(evtDoc);
      }
    }

    

true

Boolean

20



    // 2) Overspeed inside zone (event on start only)
    const notifyOnOverspeed = !!(fence.speed_limit_enabled ?? fence.speed_limit);
    const limit = Number(  fence.speed_limit  );
    
    const currentSpeed = Number(speed) || 0;
    const overspeedNow = insideNow && Number.isFinite(limit) && limit > 0 && currentSpeed > limit;

    // update overspeed state always
    setOps[`fence_state.${key}.overspeed.active`] = overspeedNow;
    if (overspeedNow !== prevOverspeed) {
      setOps[`fence_state.${key}.overspeed.last_change_at`] = at;
    }

    // emit only on transition false->true
    if (notifyOnOverspeed && overspeedNow && !prevOverspeed) {
      const fenceName = fence?.name ?? null;
      const alarmText = `Geofence Overspeed${fenceName ? `: ${fenceName}` : ''}`;
      const alarmTextAr = `تجاوز السرعة في المنطقة${fenceName ? `: ${fenceName}` : ''}`;
      createdEvents.push({
        imei,
        type: 'alarm',
        subType: 'geofence_overspeed',
        alarmType: 1002,
        alarmText,
        alarmTextAr,
        geofence_event: 'overspeed',
        fence_id: fence?._id ?? fence?.id ?? null,
        fence_name: fenceName,
        fence_type: fence?.type ?? null,
        speed_limit_kmh: limit,
        latitude: pointLat,
        longitude: pointLon,
        speed: currentSpeed,
        date: at,
        packet_date: at,
      });
    }
  }

  if (Object.keys(setOps).length) {
    await DeviceStatus.updateOne({ imei }, { $set: setOps }, { upsert: true });
  }

  if (!createdEvents.length) return [];

  // gpslogs write is optional; callers still receive events for notify/realtime.
  return getGpsLogsWriter().writeMany(createdEvents);
}

module.exports = {
  evaluateGeofences,
};


