const fs = require("node:fs");

process.on("message", (msg) => {
  if (!msg || typeof msg !== "object") return;
  if (msg.type === "batch") {
    if (process.env.IPC_FIXTURE_BATCH_LOG) {
      fs.appendFileSync(
        process.env.IPC_FIXTURE_BATCH_LOG,
        `${JSON.stringify(msg.batch?.items || [])}\n`,
        "utf8"
      );
    }
    setTimeout(() => {
      if (process.send) {
        process.send({
          type: "ack",
          batchId: msg.batch?.batchId,
          count: msg.batch?.items?.length || 0,
        });
      }
    }, 5);
  }
});

if (process.send) {
  process.send({ type: "ready" });
}
