/**
 * PM2 ecosystem — AlFursan GPS backend process map.
 *
 * Current:
 *   alfursan-bridge            realtime / bridge parent
 *   alfursan-gpspoints-writer  GPSPoints Mongo drain (E3)
 *   alfursan-analytics         global report materializers (A1)
 *
 * Later (not started here):
 *   raw archive worker, 8 IMEI workers
 *
 * Deploy notes:
 *   1. Set GPSPOINT_EXTERNAL_WRITER=1 in .env (persistence worker = producer only).
 *   2. Keep REPORT_SCHEDULER_OWNER=analytics (default) and run alfursan-analytics.
 *   3. pm2 start ecosystem.config.cjs
 *   4. Never run two drain consumers on the same spool (lock enforced in code).
 *   5. Never run report schedulers on bridge + analytics together.
 *   Rollback GPSPoints: stop writer, set GPSPOINT_EXTERNAL_WRITER=0, restart bridge/worker.
 *   Rollback analytics: stop alfursan-analytics, set REPORT_SCHEDULER_OWNER=bridge, restart bridge.
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
        REPORT_SCHEDULER_OWNER: "analytics",
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
    {
      name: "alfursan-analytics",
      script: "workers/analytics-worker.js",
      instances: 1,
      exec_mode: "fork",
      autorestart: true,
      max_restarts: 50,
      restart_delay: 2000,
      kill_timeout: 15000,
      error_file: "logs/analytics-worker-error.log",
      out_file: "logs/analytics-worker-out.log",
      merge_logs: true,
      env: {
        NODE_ENV: "production",
        REPORT_SCHEDULER_OWNER: "analytics",
        ANALYTICS_MONGO_MAX_POOL: "8",
        ANALYTICS_MONGO_MIN_POOL: "1",
      },
    },
  ],
};
