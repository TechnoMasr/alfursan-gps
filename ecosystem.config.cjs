/**
 * PM2 ecosystem — AlFursan GPS backend process map.
 *
 * Current:
 *   alfursan-bridge            realtime / bridge parent
 *   alfursan-gpspoints-writer  GPSPoints Mongo drain (E3)
 *
 * Later (not started here):
 *   raw archive worker, alfursan-analytics (exactly 1), 8 IMEI workers
 *
 * Deploy notes:
 *   1. Set GPSPOINT_EXTERNAL_WRITER=1 in .env (persistence worker = producer only).
 *   2. pm2 start ecosystem.config.cjs
 *   3. Never run two drain consumers on the same spool (lock enforced in code).
 *   Rollback: stop writer, set GPSPOINT_EXTERNAL_WRITER=0, restart bridge/worker (full mode).
 */
module.exports = {
  apps: [
    {
      name: "alfursan-bridge",
      script: "traccar-bridge-ontherport.js",
      instances: 1,
      exec_mode: "fork",
      autorestart: true,
      max_restarts: 20,
      restart_delay: 2000,
      kill_timeout: 10000,
      env: {
        NODE_ENV: "production",
      },
    },
    {
      name: "alfursan-gpspoints-writer",
      script: "workers/gpspoints-writer.js",
      instances: 1,
      exec_mode: "fork",
      autorestart: true,
      max_restarts: 50,
      restart_delay: 2000,
      kill_timeout: 12000,
      // Independent log streams from the bridge.
      error_file: "logs/gpspoints-writer-error.log",
      out_file: "logs/gpspoints-writer-out.log",
      merge_logs: true,
      env: {
        NODE_ENV: "production",
        // Writer always drains; persistence side must set GPSPOINT_EXTERNAL_WRITER=1.
        GPSPOINT_WRITER_MONGO_MAX_POOL: "8",
      },
    },
  ],
};
