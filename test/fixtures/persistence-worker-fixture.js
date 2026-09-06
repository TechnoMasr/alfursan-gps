process.on("message", (msg) => {
  if (!msg || typeof msg !== "object") return;
  if (msg.type === "batch") {
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
