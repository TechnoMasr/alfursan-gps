/**
 * Atomic DeviceStatus GPS updates — never regress last_fix / coords from older packets.
 */

function isOnlineStatus(status) {
  return status != null && String(status).toLowerCase() === "online";
}

function buildStatusUpsertPipeline(update) {
  const now = new Date();
  const $set = {
    ...update,
    km_total: { $ifNull: ["$km_total", 0] },
    miles_total: { $ifNull: ["$miles_total", 0] },
    updatedAt: now,
  };

  if (isOnlineStatus(update.status)) {
    $set.activation_at = { $ifNull: ["$activation_at", now] };
    $set.createdAt = { $ifNull: ["$createdAt", now] };
  }

  return [{ $set }];
}

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
  if (update.status !== undefined) $set.status = update.status;
  if (update.lastUpdate !== undefined) $set.lastUpdate = update.lastUpdate;
  if (update.truccer_dev_status !== undefined) $set.truccer_dev_status = update.truccer_dev_status;

  if (isOnlineStatus(update.status)) {
    $set.activation_at = { $ifNull: ["$activation_at", now] };
    $set.createdAt = { $ifNull: ["$createdAt", now] };
  }

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
    status,
    lastUpdate,
    truccer_dev_status,
  } = params;

  if (!imei) return;

  const fixAt = packetDate ? new Date(packetDate) : null;
  const fixValid = !!(fixAt && !Number.isNaN(fixAt.getTime()));
  const ingressAt = serverDate ? new Date(serverDate) : fixAt;
  const ingressValid = !!(ingressAt && !Number.isNaN(ingressAt.getTime()));
  const isDeviceOnly = type === "device";

  const update = {
    last_type: type,
  };

  if (isDeviceOnly) {
    if (ingressValid) {
      update.last_device_online_at = ingressAt;
    }
    if (status !== undefined) {
      update.status = status;
    }
    if (lastUpdate !== undefined && lastUpdate !== null && lastUpdate !== "") {
      const d = new Date(lastUpdate);
      update.lastUpdate = !Number.isNaN(d.getTime()) ? d : lastUpdate;
    }
    if (truccer_dev_status !== undefined) {
      update.truccer_dev_status = truccer_dev_status;
    }
    return DeviceStatus.findOneAndUpdate(
      { imei },
      buildStatusUpsertPipeline(update),
      { upsert: true, new: true }
    );
  }

  const attrsTypeNum = Number(attrsType);
  const hasCoords = lat !== undefined && lon !== undefined;

  if (voltage !== undefined && voltage !== null) {
    update.last_voltage = voltage;
    if (voltageUnit) update.last_voltage_unit = voltageUnit;
  }
  if (status !== undefined) update.status = status;
  if (lastUpdate !== undefined && lastUpdate !== null && lastUpdate !== "") {
    const d = new Date(lastUpdate);
    update.lastUpdate = !Number.isNaN(d.getTime()) ? d : lastUpdate;
  }
  if (truccer_dev_status !== undefined) update.truccer_dev_status = truccer_dev_status;

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
  buildStatusUpsertPipeline,
  buildGpsStatusUpdatePipeline,
  newerFixCondition,
  isOnlineStatus,
};
