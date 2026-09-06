const crypto = require("crypto");

function bearerTokenFromHeader(header) {
  const raw = String(header || "").trim();
  const match = raw.match(/^Bearer\s+(.+)$/i);
  return match ? match[1].trim() : "";
}

function timingSafeEqualString(a, b) {
  const left = Buffer.from(String(a || ""));
  const right = Buffer.from(String(b || ""));
  if (!left.length || !right.length || left.length !== right.length) return false;
  return crypto.timingSafeEqual(left, right);
}

function verifyForwardBearer(authHeader, expectedToken) {
  const expected = String(expectedToken || "").trim();
  if (!expected) return { ok: false, reason: "forward_token_not_configured" };
  const actual = bearerTokenFromHeader(authHeader);
  if (!timingSafeEqualString(actual, expected)) return { ok: false, reason: "unauthorized" };
  return { ok: true };
}

function isJsonContentType(req) {
  if (req && typeof req.is === "function") return !!req.is("application/json");
  const raw = String(req?.headers?.["content-type"] || req?.headers?.["Content-Type"] || "");
  return /^application\/json(?:\s*;|$)/i.test(raw);
}

function firstPresent(...values) {
  for (const value of values) {
    if (value !== undefined && value !== null && value !== "") return value;
  }
  return undefined;
}

function asObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function normalizeTime(value) {
  const raw = firstPresent(value);
  if (raw === undefined) return undefined;
  const d = raw instanceof Date ? raw : new Date(raw);
  if (Number.isNaN(d.getTime())) return raw;
  return d.toISOString();
}

function extractUniqueId(root, position, device) {
  return firstPresent(
    position.uniqueId,
    position.imei,
    position.deviceUniqueId,
    position.device?.uniqueId,
    position.device?.imei,
    root.uniqueId,
    root.imei,
    root.deviceUniqueId,
    root.deviceImei,
    device.uniqueId,
    device.imei,
    device.attributes?.uniqueId,
    device.attributes?.imei
  );
}

function extractDeviceId(root, position, device) {
  return firstPresent(
    position.deviceId,
    position.device_id,
    position.device?.id,
    root.deviceId,
    root.device_id,
    device.id
  );
}

function extractPositionList(body) {
  if (Array.isArray(body)) return body;
  const root = asObject(body);
  if (Array.isArray(root.positions)) return root.positions;
  if (Array.isArray(root.position)) return root.position;
  if (root.position && typeof root.position === "object") return [root.position];
  if (root.data?.position && typeof root.data.position === "object") return [root.data.position];
  if (Array.isArray(root.data?.positions)) return root.data.positions;
  return [root];
}

function normalizeForwardPosition(positionInput, rootInput = {}) {
  const root = asObject(rootInput);
  const position = asObject(positionInput);
  const device = asObject(position.device || root.device);
  const attrs = asObject(firstPresent(position.attributes, root.attributes));
  const network = firstPresent(position.network, root.network);
  const deviceIdRaw = extractDeviceId(root, position, device);
  const deviceId = Number(deviceIdRaw);
  const uniqueIdRaw = extractUniqueId(root, position, device);
  const imei = uniqueIdRaw != null ? String(uniqueIdRaw).trim() : null;

  const normalized = {
    id: firstPresent(position.id, position.positionId, root.positionId, root.id),
    deviceId: Number.isFinite(deviceId) ? deviceId : deviceIdRaw,
    protocol: firstPresent(position.protocol, root.protocol, device.protocol),
    serverTime: normalizeTime(firstPresent(position.serverTime, root.serverTime)),
    deviceTime: normalizeTime(firstPresent(position.deviceTime, root.deviceTime)),
    fixTime: normalizeTime(firstPresent(position.fixTime, position.time, root.fixTime, root.time)),
    latitude: firstPresent(position.latitude, position.lat, position.gps?.latitude, root.latitude, root.lat),
    longitude: firstPresent(position.longitude, position.lon, position.lng, position.gps?.longitude, root.longitude, root.lon, root.lng),
    altitude: firstPresent(position.altitude, root.altitude),
    speed: firstPresent(position.speed, root.speed),
    course: firstPresent(position.course, position.direction, root.course, root.direction),
    accuracy: firstPresent(position.accuracy, root.accuracy),
    valid: firstPresent(position.valid, root.valid),
    outdated: firstPresent(position.outdated, root.outdated),
    network,
    geofenceIds: firstPresent(position.geofenceIds, root.geofenceIds),
    deviceStatus: firstPresent(device.status, root.device?.status),
    deviceModel: firstPresent(device.model, root.device?.model),
    attributes: attrs,
    _forward_uniqueId: imei || null,
  };

  if (normalized.id === undefined) delete normalized.id;
  if (normalized.protocol === undefined) normalized.protocol = null;
  if (normalized.serverTime === undefined) delete normalized.serverTime;
  if (normalized.deviceTime === undefined) delete normalized.deviceTime;
  if (normalized.fixTime === undefined) delete normalized.fixTime;
  if (normalized.latitude !== undefined) normalized.latitude = Number(normalized.latitude);
  if (normalized.longitude !== undefined) normalized.longitude = Number(normalized.longitude);
  if (normalized.altitude !== undefined) normalized.altitude = Number(normalized.altitude);
  if (normalized.speed !== undefined) normalized.speed = Number(normalized.speed);
  if (normalized.course !== undefined) normalized.course = Number(normalized.course);
  if (normalized.accuracy !== undefined) normalized.accuracy = Number(normalized.accuracy);
  if (normalized.valid === undefined) normalized.valid = true;

  return {
    imei,
    runtimeDeviceId: normalized.deviceId,
    runtimeDevice: Object.keys(device).length ? { ...device } : null,
    position: normalized,
    hasCommandResponse: String(attrs.result || "").trim().length > 0,
  };
}

function validateForwardItem(item) {
  const deviceIdNum = Number(item.runtimeDeviceId);
  const hasDevice = !!item.imei || Number.isFinite(deviceIdNum);
  if (!hasDevice) return "missing_uniqueId_or_deviceId";
  const hasCommand = item.hasCommandResponse;
  const lat = Number(item.position?.latitude);
  const lon = Number(item.position?.longitude);
  const hasCoords = Number.isFinite(lat) && Number.isFinite(lon);
  if (!hasCommand && !hasCoords) return "missing_position_coordinates";
  return null;
}

function normalizeForwardPayload(body) {
  const root = asObject(body);
  const list = extractPositionList(body);
  const items = [];
  const invalid = [];

  for (const rawPosition of list) {
    const item = normalizeForwardPosition(rawPosition, root);
    const reason = validateForwardItem(item);
    if (reason) invalid.push({ reason, runtimeDeviceId: item.runtimeDeviceId ?? null, imei: item.imei ?? null });
    else items.push(item);
  }

  return {
    ok: items.length > 0,
    items,
    invalid,
  };
}

module.exports = {
  verifyForwardBearer,
  isJsonContentType,
  normalizeForwardPayload,
  normalizeForwardPosition,
};
