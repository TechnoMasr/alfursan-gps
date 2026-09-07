const { describe, it } = require("node:test");
const assert = require("node:assert/strict");
const {
  buildRecoveredTripState,
  ensureTripRuntimeState,
} = require("../lib/tripRuntimeState");

const BASE_GAP_MIN = 1;

function minutesDiff(a, b) {
  return Math.floor((b.getTime() - a.getTime()) / 60000);
}

/**
 * Mirrors CURRENT applyTripLogic decisions without Mongo writes.
 * Used to prove recovery preserves segmentation semantics.
 */
function decideTripActions(st, { speed, packetDate }) {
  const isMoving = speed > 0;
  const prevSpeed = st.prevSpeed;
  const prevLastMove = st.lastNonZeroAt;
  let shouldClose = false;
  if (st.currentTripId && prevSpeed === 0 && isMoving && prevLastMove) {
    if (minutesDiff(prevLastMove, packetDate) >= BASE_GAP_MIN) shouldClose = true;
  }
  let shouldStart = false;
  if (isMoving) {
    if (prevSpeed === null && !st.currentTripId) {
      shouldStart = true;
    } else if (prevSpeed === 0) {
      const gap = prevLastMove ? minutesDiff(prevLastMove, packetDate) : BASE_GAP_MIN;
      if (gap >= BASE_GAP_MIN) shouldStart = true;
    }
  }
  return { shouldClose, shouldStart, isMoving };
}

function applyDecision(st, decision, packetDate, newTripId = "new") {
  if (decision.shouldClose) {
    st.currentTripId = null;
  }
  if (decision.shouldStart) {
    st.currentTripId = newTripId;
    st.currentDistKm = 0;
  }
  if (st.currentTripId && decision.isMoving) {
    /* append */
  }
  if (decision.isMoving) st.lastNonZeroAt = packetDate;
  st.prevSpeed = decision.isMoving ? 40 : 0;
  st.prevPacketAt = packetDate;
  return st;
}

describe("trip recovery state builder", () => {
  it("no open trip -> prevSpeed null (first move can start)", () => {
    const st = buildRecoveredTripState({});
    assert.equal(st.currentTripId, null);
    assert.equal(st.prevSpeed, null);
    assert.equal(st.recoveryKind, "none");
  });

  it("open trip + last_speed restores without fabricating duplicate start", () => {
    const openTrip = {
      _id: "T1",
      distance_km: 12,
      end_at: new Date("2026-09-07T10:00:00Z"),
      start_at: new Date("2026-09-07T09:00:00Z"),
    };
    const st = buildRecoveredTripState({
      openTrip,
      deviceStatus: { last_speed: 45, last_packet_at: new Date("2026-09-07T10:00:00Z") },
    });
    assert.equal(st.currentTripId, "T1");
    assert.equal(st.prevSpeed, 45);
    assert.equal(st.currentDistKm, 12);
    const d = decideTripActions(st, {
      speed: 40,
      packetDate: new Date("2026-09-07T10:00:30Z"),
    });
    assert.equal(d.shouldStart, false);
    assert.equal(d.shouldClose, false);
  });

  it("open trip without status assumes stopped — short resume continues", () => {
    const openTrip = {
      _id: "T2",
      distance_km: 3,
      end_at: new Date("2026-09-07T10:00:00Z"),
      start_at: new Date("2026-09-07T09:00:00Z"),
    };
    const st = buildRecoveredTripState({ openTrip });
    assert.equal(st.prevSpeed, 0);
    assert.equal(st.recoveryKind, "open_assume_stopped");
    const d = decideTripActions(st, {
      speed: 30,
      packetDate: new Date("2026-09-07T10:00:30Z"), // < 1 min
    });
    assert.equal(d.shouldClose, false);
    assert.equal(d.shouldStart, false);
  });

  it("open trip assume stopped — long resume closes then starts once", () => {
    const openTrip = {
      _id: "T3",
      distance_km: 3,
      end_at: new Date("2026-09-07T10:00:00Z"),
      start_at: new Date("2026-09-07T09:00:00Z"),
    };
    const st = buildRecoveredTripState({ openTrip });
    const d = decideTripActions(st, {
      speed: 30,
      packetDate: new Date("2026-09-07T10:05:00Z"),
    });
    assert.equal(d.shouldClose, true);
    assert.equal(d.shouldStart, true);
    applyDecision(st, d, new Date("2026-09-07T10:05:00Z"), "T4");
    assert.equal(st.currentTripId, "T4");
  });
});

describe("trip ensureTripRuntimeState lazy recovery", () => {
  it("loads once per IMEI then uses memory", async () => {
    let tripFinds = 0;
    let statusFinds = 0;
    const Trip = {
      findOne() {
        tripFinds += 1;
        return {
          sort() {
            return this;
          },
          lean: async () => ({
            _id: "TX",
            distance_km: 1,
            end_at: new Date("2026-09-07T10:00:00Z"),
            start_at: new Date("2026-09-07T09:00:00Z"),
          }),
        };
      },
    };
    const DeviceStatus = {
      findOne() {
        statusFinds += 1;
        return {
          select() {
            return this;
          },
          lean: async () => ({ last_speed: 20, last_packet_at: new Date("2026-09-07T10:00:00Z") }),
        };
      },
    };
    const map = new Map();
    const metrics = {};
    const a = await ensureTripRuntimeState({
      imei: "x",
      stateMap: map,
      Trip,
      DeviceStatus,
      metrics,
    });
    const b = await ensureTripRuntimeState({
      imei: "x",
      stateMap: map,
      Trip,
      DeviceStatus,
      metrics,
    });
    assert.equal(tripFinds, 1);
    assert.equal(statusFinds, 1);
    assert.equal(a, b);
    assert.equal(a.currentTripId, "TX");
    assert.equal(metrics.trip_recovery_open_restored_total, 1);
  });

  it("stopped open trip after restart does not duplicate on zero-speed packet", () => {
    const st = buildRecoveredTripState({
      openTrip: {
        _id: "TS",
        distance_km: 2,
        end_at: new Date("2026-09-07T10:00:00Z"),
        start_at: new Date("2026-09-07T09:00:00Z"),
      },
    });
    const d = decideTripActions(st, {
      speed: 0,
      packetDate: new Date("2026-09-07T10:02:00Z"),
    });
    assert.equal(d.shouldStart, false);
    assert.equal(d.shouldClose, false);
    assert.equal(st.currentTripId, "TS");
  });

  it("no open trip first moving packet starts one", () => {
    const st = buildRecoveredTripState({});
    const d = decideTripActions(st, {
      speed: 40,
      packetDate: new Date("2026-09-07T10:00:00Z"),
    });
    assert.equal(d.shouldStart, true);
    assert.equal(d.shouldClose, false);
  });

  it("OOO / older packet with open moving trip does not start duplicate", () => {
    const st = buildRecoveredTripState({
      openTrip: {
        _id: "TO",
        distance_km: 5,
        end_at: new Date("2026-09-07T10:10:00Z"),
        start_at: new Date("2026-09-07T09:00:00Z"),
      },
      deviceStatus: { last_speed: 50, last_packet_at: new Date("2026-09-07T10:10:00Z") },
    });
    // Older historical packet while prevSpeed>0: shouldStart only on null/0 branches
    const d = decideTripActions(st, {
      speed: 40,
      packetDate: new Date("2026-09-07T09:30:00Z"),
    });
    assert.equal(d.shouldStart, false);
    assert.equal(d.shouldClose, false);
  });
});
