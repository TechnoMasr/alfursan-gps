const { describe, it } = require("node:test");
const assert = require("node:assert/strict");
const {
  isNewerFix,
  newerFixCondition,
  buildGpsStatusUpdatePipeline,
  MAX_FUTURE_FIX_MS,
} = require("../deviceStatus");

const REF = new Date("2026-09-30T20:37:27.788Z");

describe("last_fix_at future/poison guard", () => {
  it("1. valid newer fix -> accepted", () => {
    const stored = new Date("2026-09-30T20:10:00.000Z");
    const candidate = new Date("2026-09-30T20:11:00.000Z");
    assert.equal(isNewerFix(candidate, stored, REF), true);
  });

  it("2. valid older fix -> rejected", () => {
    const stored = new Date("2026-09-30T20:10:00.000Z");
    const candidate = new Date("2026-09-30T20:05:00.000Z");
    assert.equal(isNewerFix(candidate, stored, REF), false);
  });

  it("3. candidate +10 minutes vs reference -> accepted", () => {
    const candidate = new Date(REF.getTime() + 10 * 60 * 1000);
    assert.equal(isNewerFix(candidate, null, REF), true);
  });

  it("4. candidate +16 minutes vs reference -> rejected", () => {
    const candidate = new Date(REF.getTime() + 16 * 60 * 1000);
    assert.equal(isNewerFix(candidate, null, REF), false);
  });

  it("5. candidate year 2094 -> rejected", () => {
    const candidate = new Date("2094-10-04T04:49:48.000Z");
    assert.equal(isNewerFix(candidate, null, REF), false);
  });

  it("6. stored last_fix_at year 2094 + valid current candidate -> accepts (self-heal)", () => {
    const stored = new Date("2094-10-04T04:49:48.000Z");
    const candidate = new Date("2026-09-30T20:37:24.000Z");
    assert.equal(isNewerFix(candidate, stored, REF), true);
  });

  it("7. normal stored + older candidate within 15 minutes of reference -> still rejected", () => {
    const stored = new Date("2026-09-30T20:37:00.000Z");
    const candidate = new Date("2026-09-30T20:30:00.000Z"); // older than stored, within 15m of REF
    assert.equal(isNewerFix(candidate, stored, REF), false);
  });

  it("8. null stored last_fix_at + valid candidate -> accepted", () => {
    const candidate = new Date("2026-09-30T20:37:24.000Z");
    assert.equal(isNewerFix(candidate, null, REF), true);
  });

  it("max future tolerance is 15 minutes", () => {
    assert.equal(MAX_FUTURE_FIX_MS, 15 * 60 * 1000);
    const edgeOk = new Date(REF.getTime() + MAX_FUTURE_FIX_MS);
    const edgeBad = new Date(REF.getTime() + MAX_FUTURE_FIX_MS + 1);
    assert.equal(isNewerFix(edgeOk, null, REF), true);
    assert.equal(isNewerFix(edgeBad, null, REF), false);
  });

  it("pipeline newerFixCondition uses reference + poison branch", () => {
    const fixAt = new Date("2026-09-30T20:37:24.000Z");
    const ingressAt = REF;
    const pipeline = buildGpsStatusUpdatePipeline({
      update: { last_type: "gps" },
      fixAt,
      fixValid: true,
      ingressAt,
      ingressValid: true,
      attrsTypeNum: 1,
      lat: 24.8,
      lon: 46.5,
      speed: 10,
      direction: 1,
      hasCoords: true,
    });
    const cond = pipeline[0].$set.last_fix_at.$cond[0];
    const expected = newerFixCondition(fixAt, ingressAt);
    assert.deepEqual(cond, expected);
    assert.ok(cond.$and);
    assert.equal(cond.$and.length, 2);
    // Rejected future must not seed last_fix_at from candidate
    assert.deepEqual(pipeline[0].$set.last_fix_at.$cond[2], {
      $ifNull: ["$last_fix_at", null],
    });
  });
});
