/**
 * Atomic DeviceStatus GPS updates — never regress last_fix / coords from older packets.
 */

function newerFixCondition(fixAt) {
  return {
    $or: [
      { $eq: [{ $ifNull: ["$last_fix_at", null] }, null] },
      { $gt: [fixAt, "$last_fix_at"] },
    ],
  };
}

/**
 * Pipeline $set for GPS/alarm packets.
 * Coordinate / last_fix / speed / direction / motion only advance when incoming fix is newer.
 * last_position_at is ingress-based and skipped for attrsType=19.
 */
function buildGpsStatusUpdatePipeline({
  update,
  fixAt,
  fixValid,
  ingressAt,
  ingressValid,
  attrsTypeNum,
  lat,
  lon,
  speed,
  direction,
  hasCoords,
}) {
  const now = new Date();
  const newer = fixValid ? newerFixCondition(fixAt) : { $literal: false };
  const skipPositionAt = attrsTypeNum === 19  || !speed || speed <=1;

  const $set = {
    last_type: update.last_type,
    km_total: { $ifNull: ["$km_total", 0] },
    miles_total: { $ifNull: ["$miles_total", 0] },
    updatedAt: now,
  };

  if (ingressValid) {
    $set.last_packet_at = ingressAt;
    $set.last_activity_at = ingressAt;
    if (!skipPositionAt) {
      $set.last_position_at = ingressAt;
    }
  }

  if (fixValid) {
    $set.last_fix_at = {
      $cond: [newer, fixAt, { $ifNull: ["$last_fix_at", fixAt] }],
    };
  }

  if (hasCoords) {
    $set.last_lat = {
      $cond: [newer, lat, { $ifNull: ["$last_lat", lat] }],
    };
    $set.last_lon = {
      $cond: [newer, lon, { $ifNull: ["$last_lon", lon] }],
    };
  }

  if (direction !== undefined) {
    $set.last_direction = {
      $cond: [newer, direction, { $ifNull: ["$last_direction", direction] }],
    };
  }

  if (speed !== undefined ) {
    $set.last_speed = {
      $cond: [newer, speed, { $ifNull: ["$last_speed", speed] }],
    };
  }

  const isIgnoredGpsType = attrsTypeNum === 19;
  const speedNum = Number(speed);
  const isMovingGps = Number.isFinite(speedNum) && speedNum > 0;
  if (hasCoords && !isIgnoredGpsType && isMovingGps && fixValid) {
    $set.last_gps_at = {
      $cond: [newer, fixAt, { $ifNull: ["$last_gps_at", null] }],
    };
  }

  if (update.last_voltage !== undefined) $set.last_voltage = update.last_voltage;
  if (update.last_voltage_unit !== undefined) $set.last_voltage_unit = update.last_voltage_unit;
  if (update.last_protocol !== undefined) $set.last_protocol = update.last_protocol;
  if (update.last_device_id !== undefined) $set.last_device_id = update.last_device_id;
  if (update.last_valid_fix !== undefined) $set.last_valid_fix = update.last_valid_fix;
  if (update.ignition_on !== undefined) $set.ignition_on = update.ignition_on;
  if (update.motion !== undefined) $set.motion = update.motion;
  if (update.is_parked !== undefined) $set.is_parked = update.is_parked;
  if (update.blocked !== undefined) $set.blocked = update.blocked;
  if (update.charge !== undefined) $set.charge = update.charge;
  if (update.signal_rssi !== undefined) $set.signal_rssi = update.signal_rssi;
  if (update.satellites !== undefined) $set.satellites = update.satellites;
  if (update.pdop !== undefined) $set.pdop = update.pdop;
  if (update.hdop !== undefined) $set.hdop = update.hdop;
  if (update.status_code !== undefined) $set.status_code = update.status_code;
  if (update.status !== undefined) $set.status = update.status;
  if (update.traccar_device_status !== undefined) $set.traccar_device_status = update.traccar_device_status;
  if (update.io !== undefined) $set.io = update.io;
  if (update.hours !== undefined) $set.hours = update.hours;
  if (update.device_total_distance !== undefined) $set.device_total_distance = update.device_total_distance;
  if (update.total_distance_m !== undefined) $set.total_distance_m = update.total_distance_m;
  if (update.distance_m !== undefined) $set.distance_m = update.distance_m;
  if (update.last_alarm !== undefined) $set.last_alarm = update.last_alarm;
  if (update.km_total !== undefined) $set.km_total = update.km_total;
  if (update.miles_total !== undefined) $set.miles_total = update.miles_total;
  if (update.last_mileage_at !== undefined) $set.last_mileage_at = update.last_mileage_at;
  if (update.power_voltage !== undefined) $set.power_voltage = update.power_voltage;
  if (update.battery_voltage !== undefined) $set.battery_voltage = update.battery_voltage;
  if (update.battery_level !== undefined) $set.battery_level = update.battery_level;

  return [{ $set }];
}

async function upsertDeviceStatus(params) {
  const { DeviceStatus } = require("./mongo");
  const {
    imei,
    packetDate,
    serverDate,
    lat,
    lon,
    speed,
    type,
    attrsType,
    voltage,
    voltageUnit,
    direction,
    ignition,
    motion,
    charge,
    blocked,
    rssi,
    alarm,
    protocol,
    deviceId,
    valid,
    deviceStatus,
    sat,
    pdop,
    hdop,
    status,
    hours,
    distance,
    totalDistance,
    ignoredAlarmOnly = false,
  } = params;

  if (!imei) return;

  const fixAt = packetDate ? new Date(packetDate) : null;
  const fixValid = !!(fixAt && !Number.isNaN(fixAt.getTime()));
  const ingressAt = serverDate ? new Date(serverDate) : fixAt;
  const ingressValid = !!(ingressAt && !Number.isNaN(ingressAt.getTime()));

  const update = {
    last_type: type,
  };

  const attrsTypeNum = Number(attrsType);
  const hasCoords = lat !== undefined && lon !== undefined;

  if (voltage !== undefined && voltage !== null) {
    update.last_voltage = voltage;
    if (voltageUnit) update.last_voltage_unit = voltageUnit;
  }
  const speedNum = Number(speed);
  const totalDistanceM = Number(totalDistance);
  const distanceM = Number(distance);
  const powerVoltage = Number(params.power ?? params.voltage);
  const batteryVoltage = Number(params.battery);
  const batteryLevel = Number(params.batteryLevel);
  update.last_protocol = protocol ?? null;
  update.last_device_id = deviceId ?? null;
  update.last_valid_fix = valid === true;
  update.ignition_on = typeof ignition === "boolean" ? ignition : null;
  update.motion = typeof motion === "boolean" ? motion : null;
  update.is_parked =
    motion === false || ((ignition === false || ignition == null) && Number.isFinite(speedNum) && speedNum <= 1);
  update.blocked = typeof blocked === "boolean" ? blocked : null;
  update.charge = typeof charge === "boolean" ? charge : null;
  update.signal_rssi = rssi ?? null;
  update.satellites = sat ?? null;
  update.pdop = pdop ?? null;
  update.hdop = hdop ?? null;
  update.status_code = status ?? null;
  update.status = deviceStatus ?? null;
  update.traccar_device_status = deviceStatus ?? null;
  update.io = Object.fromEntries(Object.entries(params.attrs || {}).filter(([k]) => /^io\d+$/i.test(k)));
  update.hours = hours ?? null;
  update.device_total_distance = Number.isFinite(totalDistanceM) ? totalDistanceM : null;
  update.total_distance_m = Number.isFinite(totalDistanceM) ? totalDistanceM : null;
  update.distance_m = Number.isFinite(distanceM) ? distanceM : null;
  if (!ignoredAlarmOnly) update.last_alarm = alarm ?? null;
  if (Number.isFinite(totalDistanceM)) {
    const km = totalDistanceM / 1000;
    update.km_total = km;
    update.miles_total = km * 0.621371;
    update.last_mileage_at = ingressAt || fixAt;
  }
  if (Number.isFinite(powerVoltage)) update.power_voltage = powerVoltage;
  if (Number.isFinite(batteryVoltage)) update.battery_voltage = batteryVoltage;
  if (Number.isFinite(batteryLevel)) update.battery_level = batteryLevel;

  return DeviceStatus.findOneAndUpdate(
    { imei },
    buildGpsStatusUpdatePipeline({
      update,
      fixAt,
      fixValid,
      ingressAt,
      ingressValid,
      attrsTypeNum,
      lat,
      lon,
      speed,
      direction,
      hasCoords,
    }),
    { upsert: true, new: true }
  );
}

module.exports = {
  upsertDeviceStatus,
  buildGpsStatusUpdatePipeline,
  newerFixCondition,
};
