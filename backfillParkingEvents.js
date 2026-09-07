#!/usr/bin/env node
/**
 * MANUAL ParkingEvent backfill — ops-triggered only.
 * Never run from analytics startup or PM2 ecosystem.
 *
 * Safety (this phase):
 * - chunked IMEI processing
 * - file checkpoint for resume after interrupt
 * - serial per-IMEI (no Promise.all)
 * - ParkingEvent open/close semantics UNCHANGED (speed <=1 open, >=5 close)
 * - document shape UNCHANGED for Laravel
 */
const fs = require("fs");
const path = require("path");

const START_SPEED_KPH = 1;
const RELEASE_SPEED_KPH = 5;
const DEFAULT_CHUNK = 25;
const DEFAULT_CHECKPOINT = path.join(
  __dirname,
  "data",
  "analytics-spool",
  "parking-backfill.checkpoint.json"
);

async function main() {
  const mongoose = require("mongoose");
  require("dotenv").config({ path: path.join(__dirname, ".env") });
  const { GpsPoint, ParkingEvent } = require("./mongo");

  const options = parseOptions(process.argv.slice(2));
  if (options.help) {
    printHelp();
    process.exit(0);
  }

  const { start, end } = buildRange(options.days);
  const checkpointPath = options.checkpoint || DEFAULT_CHECKPOINT;
  const chunkSize = options.chunkSize || DEFAULT_CHUNK;

  let imeis = await resolveImeis(GpsPoint, options.imeis, start, end);
  if (imeis.length === 0) {
    console.log("No IMEIs found for the selected window.");
    await mongoose.disconnect();
    return;
  }

  const checkpoint = loadCheckpoint(checkpointPath);
  const done = new Set(checkpoint?.completedImeis || []);
  if (
    checkpoint?.start === start.toISOString() &&
    checkpoint?.end === end.toISOString()
  ) {
    imeis = imeis.filter((i) => !done.has(i));
    console.log(
      `Resuming parking backfill: ${done.size} done, ${imeis.length} remaining`
    );
  } else if (checkpoint?.completedImeis?.length) {
    console.log("Checkpoint range mismatch — starting fresh progress file.");
    done.clear();
  }

  console.log(
    `Backfilling parking events for ${imeis.length} IMEI(s) between ${start.toISOString()} and ${end.toISOString()} (chunk=${chunkSize})`
  );

  let processed = 0;
  for (let i = 0; i < imeis.length; i += chunkSize) {
    const chunk = imeis.slice(i, i + chunkSize);
    for (const imei of chunk) {
      try {
        await rebuildForImei(GpsPoint, ParkingEvent, imei, start, end);
        done.add(imei);
        processed += 1;
      } catch (err) {
        console.error(`IMEI ${imei}: ${err.message}`);
        saveCheckpoint(checkpointPath, {
          start: start.toISOString(),
          end: end.toISOString(),
          completedImeis: [...done],
          lastError: { imei, message: err.message, at: new Date().toISOString() },
        });
        throw err;
      }
    }
    saveCheckpoint(checkpointPath, {
      start: start.toISOString(),
      end: end.toISOString(),
      completedImeis: [...done],
      updatedAt: new Date().toISOString(),
    });
    console.log(
      `Progress: ${done.size} completed (chunk ending ${chunk[chunk.length - 1]})`
    );
  }

  try {
    fs.unlinkSync(checkpointPath);
  } catch {
    /* ignore */
  }

  await mongoose.disconnect();
  console.log(`Done. Processed ${processed} IMEI(s) this run.`);
}

function printHelp() {
  console.log(`Usage: node backfillParkingEvents.js [--days=5] [--imei=a,b] [--chunk-size=25] [--checkpoint=path]

Manual only. Does not start from PM2/analytics.
Resumable via checkpoint file (default: data/analytics-spool/parking-backfill.checkpoint.json).
ParkingEvent field shapes are unchanged.`);
}

function parseOptions(args) {
  const opts = {
    days: 5,
    imeis: null,
    chunkSize: DEFAULT_CHUNK,
    checkpoint: null,
    help: false,
  };
  for (const arg of args) {
    if (arg === "--help" || arg === "-h") opts.help = true;
    else if (arg.startsWith("--days=")) {
      const num = parseInt(arg.split("=")[1], 10);
      if (!Number.isNaN(num) && num > 0) opts.days = num;
    } else if (arg.startsWith("--imei=")) {
      const list = arg.split("=")[1];
      if (list) opts.imeis = list.split(",").map((s) => s.trim()).filter(Boolean);
    } else if (arg.startsWith("--chunk-size=")) {
      const num = parseInt(arg.split("=")[1], 10);
      if (!Number.isNaN(num) && num > 0) opts.chunkSize = num;
    } else if (arg.startsWith("--checkpoint=")) {
      opts.checkpoint = arg.split("=")[1];
    }
  }
  return opts;
}

function buildRange(days) {
  const end = new Date();
  end.setUTCHours(23, 59, 59, 999);
  const start = new Date(end);
  start.setUTCDate(start.getUTCDate() - Math.max(1, days) + 1);
  start.setUTCHours(0, 0, 0, 0);
  return { start, end };
}

async function resolveImeis(GpsPoint, imeis, start, end) {
  if (Array.isArray(imeis) && imeis.length > 0) return imeis;
  const found = await GpsPoint.distinct("imei", {
    packet_date: { $gte: start, $lte: end },
  });
  return found.filter(Boolean);
}

function loadCheckpoint(filePath) {
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch {
    return null;
  }
}

function saveCheckpoint(filePath, body) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  const tmp = `${filePath}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(body, null, 2), "utf8");
  fs.renameSync(tmp, filePath);
}

async function rebuildForImei(GpsPoint, ParkingEvent, imei, start, end) {
  const points = await GpsPoint.find({
    imei,
    packet_date: { $gte: start, $lte: end },
  })
    .sort({ packet_date: 1 })
    .lean()
    .exec();

  if (points.length === 0) {
    console.log(`IMEI ${imei}: no points in range, skipping.`);
    return;
  }

  await ParkingEvent.deleteMany({
    imei,
    start_at: { $gte: start, $lte: end },
  }).exec();

  const events = buildEventsFromPoints(points);
  if (events.length === 0) {
    console.log(`IMEI ${imei}: no stops detected.`);
    return;
  }

  const docs = events.map((event) => ({
    imei,
    start_at: event.startAt,
    end_at: event.endAt,
    duration_seconds: event.duration,
    start_lat: event.startLat,
    start_lon: event.startLon,
    end_lat: event.endLat,
    end_lon: event.endLon,
    start_speed: event.startSpeed,
    end_speed: event.endSpeed,
    min_speed_kph: event.minSpeed,
    max_speed_kph: event.maxSpeed,
    is_open: false,
    created_at: new Date(),
    updated_at: new Date(),
  }));

  await ParkingEvent.insertMany(docs);
  console.log(`IMEI ${imei}: inserted ${docs.length} parking events.`);
}

function buildEventsFromPoints(points) {
  const events = [];
  let current = null;

  for (const point of points) {
    const ts = toDate(point.packet_date || point.date);
    if (!ts) continue;
    const lat = pickCoord(point, "latitude");
    const lon = pickCoord(point, "longitude");
    if (!isFiniteCoord(lat, lon)) continue;
    const speed = pickSpeed(point);

    if (speed <= START_SPEED_KPH) {
      current = current
        ? extendStop(current, ts, lat, lon, speed)
        : startStop(ts, lat, lon, speed);
      continue;
    }

    if (speed < RELEASE_SPEED_KPH) {
      if (current) current = extendStop(current, ts, lat, lon, speed);
      continue;
    }

    if (current) {
      current = extendStop(current, ts, lat, lon, speed);
      pushStop(events, current);
      current = null;
    }
  }

  if (current) pushStop(events, current);
  return events;
}

function startStop(ts, lat, lon, speed) {
  return {
    startAt: new Date(ts),
    endAt: new Date(ts),
    startLat: lat,
    startLon: lon,
    endLat: lat,
    endLon: lon,
    startSpeed: speed,
    endSpeed: speed,
    minSpeed: speed,
    maxSpeed: speed,
  };
}

function extendStop(stop, ts, lat, lon, speed) {
  stop.endAt = new Date(ts);
  stop.endLat = lat;
  stop.endLon = lon;
  stop.endSpeed = speed;
  stop.minSpeed = Math.min(stop.minSpeed, speed);
  stop.maxSpeed = Math.max(stop.maxSpeed, speed);
  return stop;
}

function pushStop(events, stop) {
  if (!stop.startAt || !stop.endAt) return;
  const duration = Math.round((stop.endAt - stop.startAt) / 1000);
  if (duration <= 0) return;
  events.push({ ...stop, duration });
}

function pickCoord(point, key) {
  const num = Number(point[key]);
  return Number.isFinite(num) ? num : null;
}

function pickSpeed(point) {
  const num = Number(point.speed);
  return Number.isFinite(num) ? num : 0;
}

function isFiniteCoord(lat, lon) {
  return Number.isFinite(lat) && Number.isFinite(lon);
}

function toDate(value) {
  if (value instanceof Date) return value;
  if (value && typeof value === "object" && typeof value.toDate === "function") {
    const d = value.toDate();
    return Number.isNaN(d.getTime()) ? null : d;
  }
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
}

module.exports = {
  buildEventsFromPoints,
  parseOptions,
  loadCheckpoint,
  saveCheckpoint,
  START_SPEED_KPH,
  RELEASE_SPEED_KPH,
};

if (require.main === module) {
  main().catch((err) => {
    console.error(err);
    try {
      require("mongoose").disconnect().finally(() => process.exit(1));
    } catch {
      process.exit(1);
    }
  });
}
