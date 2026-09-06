const { ParkingEvent } = require('./mongo');

const STOP_START_SPEED_KPH = 1;   // يعتبر توقف عندما السرعة <= 1 كم/س
const STOP_RELEASE_SPEED_KPH = 5; // يغلق التوقف عند الحركة الحقيقية

const activeStops = new Map();

function toNumber(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function toCoordinate(value) {
  if (value === null || value === undefined || value === '') {
    return null;
  }

  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function isFiniteCoord(lat, lon) {
  return Number.isFinite(lat) && Number.isFinite(lon);
}

function toDate(value) {
  if (value instanceof Date) return value;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
}

async function ensureActiveStop(imei, startAt, lat, lon, speed) {
  let state = activeStops.get(imei);
  if (state?.eventId) return state;

  let event = await ParkingEvent.findOne({ imei, is_open: true }).sort({ start_at: -1 });

  if (!event) {
    event = await ParkingEvent.create({
      imei,
      start_at: startAt,
      end_at: startAt,
      duration_seconds: 0,
      start_lat: lat,
      start_lon: lon,
      end_lat: lat,
      end_lon: lon,
      start_speed: speed,
      end_speed: speed,
      min_speed_kph: speed,
      max_speed_kph: speed,
      is_open: true,
    });
  }

  const startDate = toDate(event.start_at) || startAt;
  state = {
    eventId: event._id,
    startAt: startDate,
  };
  activeStops.set(imei, state);
  return state;
}

async function updateStopEvent(eventId, payload, speed) {
  const update = {
    $set: payload,
  };

  if (speed !== undefined) {
    update.$min = { min_speed_kph: speed };
    update.$max = { max_speed_kph: speed };
  }

  await ParkingEvent.updateOne({ _id: eventId }, update);
}

async function closeActiveStop(imei, timestamp, lat, lon, speed) {
  const state = activeStops.get(imei);
  if (!state?.eventId) return;

  const startAt = state.startAt || timestamp;
  const durationSec = Math.max(0, Math.round((timestamp - startAt) / 1000));

  await updateStopEvent(
    state.eventId,
    {
      end_at: timestamp,
      end_lat: lat,
      end_lon: lon,
      end_speed: speed,
      duration_seconds: durationSec,
      is_open: false,
    },
    speed
  );

  activeStops.delete(imei);
}

async function extendStop(imei, timestamp, lat, lon, speed) {
  const state = await ensureActiveStop(imei, timestamp, lat, lon, speed);
  const startAt = state.startAt || timestamp;
  const durationSec = Math.max(0, Math.round((timestamp - startAt) / 1000));

  await updateStopEvent(
    state.eventId,
    {
      end_at: timestamp,
      end_lat: lat,
      end_lon: lon,
      end_speed: speed,
      duration_seconds: durationSec,
      is_open: true,
    },
    speed
  );
}

async function handleParkingSample({ imei, timestamp, lat, lon, speed }) {
  if (!imei) return;
  const ts = toDate(timestamp);
  if (!ts) return;

  const latNum = toCoordinate(lat);
  const lonNum = toCoordinate(lon);
  if (!isFiniteCoord(latNum, lonNum)) return;

  const currentSpeed = toNumber(speed, 0);

  try {
    if (currentSpeed <= STOP_START_SPEED_KPH) {
      await extendStop(imei, ts, latNum, lonNum, currentSpeed);
      return;
    }

    if (currentSpeed >= STOP_RELEASE_SPEED_KPH) {
      await closeActiveStop(imei, ts, latNum, lonNum, currentSpeed);
      return;
    }

    // سرعة بين 1 و5 كم/س -> نعتبرها ما زالت داخل نفس التوقف إن وُجد
    const state = activeStops.get(imei);
    if (state?.eventId) {
      await extendStop(imei, ts, latNum, lonNum, currentSpeed);
    }
  } catch (error) {
    console.error('Parking events tracker error:', error.message);
  }
}

module.exports = {
  handleParkingSample,
};

