/**
 * Optional gpslogs writer.
 * GPSLOGS_WRITE_ENABLED=0 (default) blocks NEW writes only.
 * Business flows still receive the in-memory documents.
 */

function cloneDoc(doc) {
  if (!doc || typeof doc !== "object") return doc;
  if (typeof doc.toObject === "function") return doc.toObject();
  return { ...doc };
}

function createGpsLogsWriter(options = {}) {
  const {
    enabled = false,
    GpsLog = null,
    onAlarm = null,
    metrics = {},
    log = console,
  } = options;

  metrics.gpslogs_write_enabled = !!enabled;

  async function writeMany(docs) {
    const list = Array.isArray(docs) ? docs.filter(Boolean) : [];
    if (!list.length) return [];

    if (!enabled || !GpsLog) {
      metrics.gpslogs_writes_skipped = (metrics.gpslogs_writes_skipped || 0) + list.length;
      if (typeof onAlarm === "function") {
        for (const doc of list) {
          if (doc?.type === "alarm") {
            try {
              onAlarm(cloneDoc(doc));
            } catch (err) {
              log.warn?.("[gpslogs] alarm mirror failed", err.message);
            }
          }
        }
      }
      return list.map(cloneDoc);
    }

    metrics.gpslogs_writes_attempted = (metrics.gpslogs_writes_attempted || 0) + list.length;
    try {
      const inserted = await GpsLog.insertMany(list, { ordered: false });
      return inserted.map(cloneDoc);
    } catch (err) {
      log.warn?.("[gpslogs] insertMany failed; returning in-memory docs", err.message);
      if (typeof onAlarm === "function") {
        for (const doc of list) {
          if (doc?.type === "alarm") {
            try {
              onAlarm(cloneDoc(doc));
            } catch (_) {
              /* ignore */
            }
          }
        }
      }
      return list.map(cloneDoc);
    }
  }

  async function writeOne(doc) {
    const out = await writeMany(doc ? [doc] : []);
    return out[0] || null;
  }

  function writeOneFireAndForget(doc) {
    setImmediate(() => {
      writeOne(doc).catch((err) => log.warn?.("[gpslogs] write failed", err.message));
    });
  }

  return {
    enabled: !!enabled,
    writeOne,
    writeMany,
    writeOneFireAndForget,
  };
}

let singleton = null;

function configureGpsLogsWriter(options) {
  singleton = createGpsLogsWriter(options);
  return singleton;
}

function getGpsLogsWriter() {
  if (!singleton) {
    singleton = createGpsLogsWriter({
      enabled: String(process.env.GPSLOGS_WRITE_ENABLED ?? "0") === "1",
    });
  }
  return singleton;
}

module.exports = {
  createGpsLogsWriter,
  configureGpsLogsWriter,
  getGpsLogsWriter,
};
