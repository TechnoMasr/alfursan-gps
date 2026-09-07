const { describe, it } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const os = require("os");
const path = require("path");
const {
  buildEventsFromPoints,
  parseOptions,
  loadCheckpoint,
  saveCheckpoint,
  START_SPEED_KPH,
  RELEASE_SPEED_KPH,
} = require("../backfillParkingEvents");

describe("parking backfill safety (manual)", () => {
  it("preserves 1/5 speed hysteresis semantics", () => {
    assert.equal(START_SPEED_KPH, 1);
    assert.equal(RELEASE_SPEED_KPH, 5);
    const t0 = Date.parse("2026-09-07T10:00:00Z");
    const points = [
      { latitude: 30, longitude: 31, speed: 0, packet_date: new Date(t0) },
      { latitude: 30, longitude: 31, speed: 0, packet_date: new Date(t0 + 60_000) },
      { latitude: 30, longitude: 31, speed: 3, packet_date: new Date(t0 + 120_000) },
      { latitude: 30, longitude: 31, speed: 6, packet_date: new Date(t0 + 180_000) },
    ];
    const events = buildEventsFromPoints(points);
    assert.equal(events.length, 1);
    assert.ok(events[0].duration > 0);
  });

  it("parseOptions supports chunk-size and checkpoint", () => {
    const opts = parseOptions(["--days=3", "--chunk-size=10", "--checkpoint=/tmp/x.json"]);
    assert.equal(opts.days, 3);
    assert.equal(opts.chunkSize, 10);
    assert.equal(opts.checkpoint, "/tmp/x.json");
  });

  it("checkpoint save/load roundtrip", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "park-bf-"));
    const file = path.join(dir, "cp.json");
    saveCheckpoint(file, {
      start: "a",
      end: "b",
      completedImeis: ["1", "2"],
    });
    const loaded = loadCheckpoint(file);
    assert.deepEqual(loaded.completedImeis, ["1", "2"]);
    fs.rmSync(dir, { recursive: true, force: true });
  });
});
