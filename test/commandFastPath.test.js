const { describe, it } = require("node:test");
const assert = require("node:assert/strict");
const { createPositionPipeline } = require("../lib/positionPipeline");

describe("command fast path", () => {
  it("command response is emitted immediately and does not wait GPS persistence", () => {
    const commands = [];
    const archiveTimes = [];
    const pipeline = createPositionPipeline({
      now: () => Date.now(),
      emitCommand: (imei, payload) => commands.push({ imei, t: Date.now(), payload }),
      enqueueArchive: () => {
        archiveTimes.push(Date.now());
      },
    });
    const t0 = Date.now();
    pipeline.handleCommand("123", { attributes: { result: "OK!" } }, { type: "command_response", data: { response: "OK!" } });
    const latency = Date.now() - t0;
    assert.equal(commands.length, 1);
    assert.ok(latency < 20, `command latency ${latency}ms`);
    assert.equal(archiveTimes.length, 0);
  });
});
