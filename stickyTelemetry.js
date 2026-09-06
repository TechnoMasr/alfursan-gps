/**
 * Sticky power / battery telemetry — never overwrite last good values with null.
 */

const lastTelemetryByImei = new Map();

function parseTelemetryNumber(value) {
  if (value === null || value === undefined || value === "") return undefined;
  const n = Number(value);
  return Number.isFinite(n) ? n : undefined;
}

function pickStickyBool(value) {
  return typeof value === "boolean" ? value : undefined;
}

function seedCacheFromDoc(imei, doc) {
  if (!imei || !doc) return;
  const cur = lastTelemetryByImei.get(imei) || {};
  const seed = { ...cur };
  const p = parseTelemetryNumber(doc.power_voltage);
  const b = parseTelemetryNumber(doc.battery_voltage);
  const bl = parseTelemetryNumber(doc.battery_level);
  const ch = pickStickyBool(doc.charge);
  if (p !== undefined && seed.power === undefined) seed.power = p;
  if (b !== undefined && seed.battery === undefined) seed.battery = b;
  if (bl !== undefined && seed.batteryLevel === undefined) seed.batteryLevel = bl;
  if (ch !== undefined && seed.charge === undefined) seed.charge = ch;
  lastTelemetryByImei.set(imei, seed);
}

/**
 * Merge Traccar attributes with per-IMEI cache (and optional Mongo snapshot).
 */
function mergeStickyAttributesForImei(imei, attrs, existingDoc) {
  if (existingDoc) seedCacheFromDoc(imei, existingDoc);
  const prev = lastTelemetryByImei.get(imei) || {};
  const next = { ...prev };
  const merged = { ...(attrs || {}) };

  const p = parseTelemetryNumber(attrs?.power);
  const b = parseTelemetryNumber(attrs?.battery);
  const bl = parseTelemetryNumber(attrs?.batteryLevel);
  const ch = pickStickyBool(attrs?.charge);

  if (p !== undefined) {
    next.power = p;
    merged.power = p;
  } else if (prev.power !== undefined) {
    merged.power = prev.power;
  }

  if (b !== undefined) {
    next.battery = b;
    merged.battery = b;
  } else if (prev.battery !== undefined) {
    merged.battery = prev.battery;
  }

  if (bl !== undefined) {
    next.batteryLevel = bl;
    merged.batteryLevel = bl;
  } else if (prev.batteryLevel !== undefined) {
    merged.batteryLevel = prev.batteryLevel;
  }

  if (ch !== undefined) {
    next.charge = ch;
    merged.charge = ch;
  } else if (prev.charge !== undefined) {
    merged.charge = prev.charge;
  }

  lastTelemetryByImei.set(imei, next);
  return merged;
}

/**
 * Apply sticky fields to devicestatuses $set payload (omit null overwrites).
 */
function applyStickyTelemetryToStatusSet(statusSet, attrs, existingDoc) {
  const ex = existingDoc || {};

  const assignNum = (mongoKey, attrKey) => {
    const n = parseTelemetryNumber(attrs?.[attrKey]);
    if (n !== undefined) {
      statusSet[mongoKey] = n;
      return;
    }
    const prev = parseTelemetryNumber(ex[mongoKey]);
    if (prev !== undefined) statusSet[mongoKey] = prev;
  };

  assignNum("power_voltage", "power");
  assignNum("battery_voltage", "battery");
  assignNum("battery_level", "batteryLevel");

  const ch = pickStickyBool(attrs?.charge);
  if (ch !== undefined) statusSet.charge = ch;
  else if (typeof ex.charge === "boolean") statusSet.charge = ex.charge;
}

module.exports = {
  mergeStickyAttributesForImei,
  applyStickyTelemetryToStatusSet,
  parseTelemetryNumber,
};
