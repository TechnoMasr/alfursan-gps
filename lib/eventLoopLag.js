const { monitorEventLoopDelay } = require("perf_hooks");

function createEventLoopLagMonitor(options = {}) {
  const resolution = Number(options.resolutionMs) > 0 ? Number(options.resolutionMs) : 20;
  let histogram = null;
  try {
    histogram = monitorEventLoopDelay({ resolution });
    histogram.enable();
  } catch {
    return {
      available: false,
      snapshot() {
        return {
          event_loop_lag_ms: null,
          event_loop_lag_p50_ms: null,
          event_loop_lag_p99_ms: null,
          event_loop_lag_available: false,
        };
      },
      reset() {},
      stop() {},
    };
  }

  function nsToMs(ns) {
    return Math.round(Number(ns || 0) / 1e6);
  }

  return {
    available: true,
    snapshot({ reset = true } = {}) {
      const out = {
        event_loop_lag_ms: nsToMs(histogram.percentile(99)),
        event_loop_lag_p50_ms: nsToMs(histogram.percentile(50)),
        event_loop_lag_p99_ms: nsToMs(histogram.percentile(99)),
        event_loop_lag_mean_ms: nsToMs(histogram.mean),
        event_loop_lag_available: true,
      };
      if (reset) histogram.reset();
      return out;
    },
    reset() {
      histogram.reset();
    },
    stop() {
      try {
        histogram.disable();
      } catch {
        /* ignore */
      }
    },
  };
}

module.exports = { createEventLoopLagMonitor };
