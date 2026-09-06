/**
 * Parse GT06-style command response text (same rules as helpers.js 0x15).
 */
function parseCommandResponseLegacyFields(responseText) {
  const text = String(responseText || "").trim();
  const out = {};

  const locationMatch = text.match(
    /Lat:(N|S)?([\d.]+),Lon:(E|W)?([\d.]+),Course:(\d+),Speed:(\d+),DateTime:([^\s]+)/
  );
  if (locationMatch) {
    const [, latDir, lat, lonDir, lon, course, speed, datetime] = locationMatch;
    out.position = {
      lat: (latDir === "S" ? -1 : 1) * parseFloat(lat),
      lon: (lonDir === "W" ? -1 : 1) * parseFloat(lon),
      speed: parseInt(speed, 10),
      course: parseInt(course, 10),
      datetime: datetime.trim(),
    };
  }

  if (text.includes("External power:")) {
    const result = {};
    const parts = text.split(/[;,]/);
    for (const part of parts) {
      const [k, v] = part.split(":").map((s) => s?.trim());
      if (!k || !v) continue;
      const key = k.toLowerCase().replace(/\s+/g, "_");
      result[key] = v;
    }
    out.status = {
      external_power: result.external_power,
      voltage: result.external_power?.match(/(\d+(\.\d+)?)v/i)?.[1],
      gprs: result.gprs,
      gsm: result.gsm,
      gps: result.gps,
      svs_used: result.svs_used_in_fix,
      acc: result.acc,
      defense: result.defense,
      relay: result.relay,
    };
  }

  const equalsMatch = text.match(/^([A-Z]+)=([\w\s!]+)$/i);
  if (equalsMatch) {
    out.command_result = {
      command: equalsMatch[1],
      result: equalsMatch[2],
    };
  }

  return out;
}

module.exports = { parseCommandResponseLegacyFields };
