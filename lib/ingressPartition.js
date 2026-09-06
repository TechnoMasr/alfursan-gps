/**
 * Ingress helpers: command / alarm / GPS ordering inside one Traccar payload.
 */

function positionHasCommandResponse(position) {
  return String(position?.attributes?.result || "").trim().length > 0;
}

function positionIngressPriority(position) {
  const attrs = position?.attributes || {};
  if (String(attrs.result || "").trim()) return 0;
  if (attrs.alarm) return 1;
  return 2;
}

function sortPositionsForIngress(positions) {
  return [...(positions || [])].sort(
    (a, b) => positionIngressPriority(a) - positionIngressPriority(b)
  );
}

function partitionPositions(positions) {
  const sorted = sortPositionsForIngress(positions);
  const commands = [];
  const alarms = [];
  const gps = [];
  for (const position of sorted) {
    if (positionHasCommandResponse(position)) commands.push(position);
    else if (position?.attributes?.alarm) alarms.push(position);
    else gps.push(position);
  }
  return { commands, alarms, gps };
}

function groupByDeviceId(positions) {
  const map = new Map();
  for (const position of positions || []) {
    const id = Number(position?.deviceId);
    const key = Number.isFinite(id) ? id : "unknown";
    if (!map.has(key)) map.set(key, []);
    map.get(key).push(position);
  }
  return map;
}

module.exports = {
  positionHasCommandResponse,
  positionIngressPriority,
  sortPositionsForIngress,
  partitionPositions,
  groupByDeviceId,
};
