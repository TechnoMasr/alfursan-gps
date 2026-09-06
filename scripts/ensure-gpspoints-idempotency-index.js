/**
 * Optional unique index for gpspoints idempotency.
 *
 * Default: diagnostic only. Does NOT create the index unless --create is passed
 * AND no duplicates exist.
 *
 *   node scripts/ensure-gpspoints-idempotency-index.js
 *   node scripts/ensure-gpspoints-idempotency-index.js --create
 *
 * Do NOT run this as part of Node startup or a normal PM2 deploy.
 */
const mongoose = require("mongoose");
const {
  INDEX_NAME,
  INDEX_KEYS,
  INDEX_OPTIONS,
  collectDiagnostic,
  shouldCreateIndex,
} = require("../lib/gpspointsIdempotencyIndex");

function parseArgs(argv) {
  return {
    create: argv.includes("--create"),
  };
}

function printReport(summary) {
  console.log("GPSPOINTS IDEMPOTENCY INDEX DIAGNOSTIC");
  console.log(
    JSON.stringify(
      {
        recommended_index: INDEX_KEYS,
        partialFilterExpression: INDEX_OPTIONS.partialFilterExpression,
        index_name: INDEX_NAME,
        ...summary,
      },
      null,
      2
    )
  );
}

function stableJson(value) {
  if (!value || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
  return `{${Object.keys(value)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${stableJson(value[key])}`)
    .join(",")}}`;
}

function isExpectedIndex(index) {
  return (
    stableJson(index?.key) === stableJson(INDEX_KEYS) &&
    index?.unique === true &&
    stableJson(index?.partialFilterExpression) ===
      stableJson(INDEX_OPTIONS.partialFilterExpression)
  );
}

async function main() {
  const { create } = parseArgs(process.argv.slice(2));
  const uri = process.env.MONGO_URI || process.env.MONGODB_URI;
  if (!uri) {
    console.error("Set MONGO_URI (or MONGODB_URI). Refusing to use hardcoded production credentials.");
    process.exit(1);
  }
  await mongoose.connect(uri);
  const coll = mongoose.connection.collection("gpspoints");
  const summary = await collectDiagnostic(coll);
  printReport(summary);

  const existing = await coll.indexes();
  const already = existing.find((idx) => idx.name === INDEX_NAME);
  if (already) {
    if (!isExpectedIndex(already)) {
      console.error("Existing index has this name but does not match the expected positive-ID partial filter.");
      console.error("Review first, then run during a maintenance window if appropriate:");
      console.error(`db.gpspoints.dropIndex(${JSON.stringify(INDEX_NAME)})`);
      console.error(
        `db.gpspoints.createIndex(${JSON.stringify(INDEX_KEYS)}, ${JSON.stringify(INDEX_OPTIONS)})`
      );
      await mongoose.disconnect();
      process.exit(2);
    }
    console.log("Index already exists:", INDEX_NAME);
    await mongoose.disconnect();
    return;
  }

  if (!create) {
    console.log("Diagnostic only. Re-run with --create after reviewing duplicates.");
    await mongoose.disconnect();
    return;
  }

  if (!shouldCreateIndex(summary)) {
    console.error("REFUSING to create unique index: duplicate (imei, traccar_position_id) groups exist.");
    console.error("Do not delete data from this script. Run a separate cleanup later if needed.");
    await mongoose.disconnect();
    process.exit(2);
  }

  await coll.createIndex(INDEX_KEYS, INDEX_OPTIONS);
  console.log("Created", INDEX_NAME);
  await mongoose.disconnect();
}

if (require.main === module) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}

module.exports = { parseArgs, main, isExpectedIndex };
