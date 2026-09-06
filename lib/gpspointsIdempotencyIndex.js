/**
 * Manual gpspoints idempotency index helpers.
 * Never called from process startup. Diagnostic-first; create is opt-in.
 */

const INDEX_NAME = "imei_1_traccar_position_id_1_unique_partial";
const INDEX_KEYS = { imei: 1, traccar_position_id: 1 };
const INDEX_OPTIONS = {
  unique: true,
  name: INDEX_NAME,
  background: true,
  partialFilterExpression: {
    traccar_position_id: { $exists: true, $type: "number" },
  },
};

const MATCH_WITH_POSITION_ID = {
  traccar_position_id: { $exists: true, $type: "number" },
};

const DUPLICATE_PIPELINE = [
  { $match: MATCH_WITH_POSITION_ID },
  {
    $group: {
      _id: { imei: "$imei", traccar_position_id: "$traccar_position_id" },
      n: { $sum: 1 },
    },
  },
  { $match: { n: { $gt: 1 } } },
];

function summarizeDiagnostic({
  total = 0,
  withTraccarPositionId = 0,
  withoutTraccarPositionId = 0,
  duplicateGroups = [],
} = {}) {
  const groups = Array.isArray(duplicateGroups) ? duplicateGroups : [];
  const duplicatedRows = groups.reduce((sum, g) => sum + Number(g.n || g.count || 0), 0);
  const extraRows = groups.reduce((sum, g) => sum + Math.max(0, Number(g.n || g.count || 0) - 1), 0);
  return {
    total,
    with_traccar_position_id: withTraccarPositionId,
    without_traccar_position_id: withoutTraccarPositionId,
    duplicate_groups: groups.length,
    duplicated_rows: duplicatedRows,
    extra_duplicate_rows: extraRows,
    can_create_unique_index: groups.length === 0,
  };
}

function shouldCreateIndex(summary) {
  return !!summary && summary.can_create_unique_index === true;
}

async function collectDiagnostic(coll) {
  const total = await coll.countDocuments({});
  const withTraccarPositionId = await coll.countDocuments(MATCH_WITH_POSITION_ID);
  const withoutTraccarPositionId = total - withTraccarPositionId;
  const duplicateGroups = await coll.aggregate(DUPLICATE_PIPELINE).toArray();
  return summarizeDiagnostic({
    total,
    withTraccarPositionId,
    withoutTraccarPositionId,
    duplicateGroups,
  });
}

module.exports = {
  INDEX_NAME,
  INDEX_KEYS,
  INDEX_OPTIONS,
  MATCH_WITH_POSITION_ID,
  DUPLICATE_PIPELINE,
  summarizeDiagnostic,
  shouldCreateIndex,
  collectDiagnostic,
};
