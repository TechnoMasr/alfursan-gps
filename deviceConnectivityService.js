const ONLINE_NO_OPEN_REPORT_CHECK_TTL_MS = Math.max(
  1000,
  Number(process.env.CONNECTIVITY_REPORT_CHECK_TTL_MS ?? 60000)
);
const onlineNoOpenReportCheckedAtByImei = new Map();

function normalizeImei(value) {
  return String(value ?? "").trim();
}

function normalizeDate(value, fallback = new Date()) {
  const date = value instanceof Date ? value : new Date(value);
  return Number.isNaN(date.getTime()) ? fallback : date;
}

function onlineStatusQuery() {
  return {
    $or: [
      { status: "online" },
      { traccar_device_status: "online" },
      { connected: true },
      { is_connected: true },
      { connection_state: "online" },
    ],
  };
}

function isOnlineDeviceStatus(value) {
  const normalized = String(value ?? "").trim().toLowerCase();
  return normalized === "online";
}

function isOfflineDeviceStatus(value) {
  const normalized = String(value ?? "").trim().toLowerCase();
  return normalized === "offline";
}

async function openDeviceDisconnection({ DeviceDisconnection, imei, at, reason, source, now }) {
  try {
    const doc = await DeviceDisconnection.findOneAndUpdate(
      { imei, is_open: true },
      {
        $setOnInsert: {
          imei,
          start_at: at,
          is_open: true,
          reason,
          opened_by: source,
          duration_sec: 0,
        },
        $set: {
          last_seen_offline_at: at,
          updatedAt: now,
        },
      },
      { upsert: true, new: true, setDefaultsOnInsert: true }
    );
    return { doc, opened: !!doc };
  } catch (err) {
    if (err?.code !== 11000) throw err;
    const doc = await DeviceDisconnection.findOne({ imei, is_open: true });
    return { doc, opened: false };
  }
}

async function markDeviceOffline({
  imei,
  at = new Date(),
  reason = "runtime_disconnect",
  source = "runtime",
} = {}) {
  const normalizedImei = normalizeImei(imei);
  if (!normalizedImei) return { imei: normalizedImei, transitioned: false, reportOpened: false };

  const { DeviceStatus, DeviceDisconnection } = require("./mongo");
  const now = new Date();
  const disconnectedAt = normalizeDate(at, now);
  onlineNoOpenReportCheckedAtByImei.delete(normalizedImei);
  const previous = await DeviceStatus.findOneAndUpdate(
    { imei: normalizedImei, ...onlineStatusQuery() },
    {
      $set: {
        status: "offline",
        traccar_device_status: "offline",
        connected: false,
        is_connected: false,
        connection_state: "offline",
        disconnected_at: disconnectedAt,
        last_disconnect_at: disconnectedAt,
        disconnect_reason: reason,
        disconnect_source: source,
        updatedAt: now,
      },
    },
    { new: false }
  ).lean();

  if (!previous) {
    return { imei: normalizedImei, transitioned: false, reportOpened: false };
  }

  const report = await openDeviceDisconnection({
    DeviceDisconnection,
    imei: normalizedImei,
    at: disconnectedAt,
    reason,
    source,
    now,
  });

  return {
    imei: normalizedImei,
    transitioned: true,
    reportOpened: report.opened,
    report: report.doc,
  };
}

async function markDeviceOnline({
  imei,
  at = new Date(),
  status = "online",
  source = "runtime_reconnect",
  skipRecentNoOpen = false,
} = {}) {
  const normalizedImei = normalizeImei(imei);
  if (!normalizedImei) return { imei: normalizedImei, transitioned: false, reportClosed: false };

  const { DeviceStatus, DeviceDisconnection } = require("./mongo");
  const now = new Date();
  const connectedAt = normalizeDate(at, now);
  const normalizedStatus = String(status || "online").trim().toLowerCase() || "online";
  if (!isOnlineDeviceStatus(normalizedStatus)) {
    return { imei: normalizedImei, transitioned: false, reportClosed: false };
  }

  const lastNoOpenCheckAt = onlineNoOpenReportCheckedAtByImei.get(normalizedImei) || 0;
  if (skipRecentNoOpen && Date.now() - lastNoOpenCheckAt < ONLINE_NO_OPEN_REPORT_CHECK_TTL_MS) {
    return { imei: normalizedImei, transitioned: false, reportClosed: false, skippedRecentNoOpen: true };
  }

  const previous = await DeviceStatus.findOneAndUpdate(
    {
      imei: normalizedImei,
      $or: [
        { status: { $ne: normalizedStatus } },
        { traccar_device_status: { $ne: normalizedStatus } },
        { connected: { $ne: true } },
        { is_connected: { $ne: true } },
        { connection_state: { $ne: "online" } },
        { disconnect_reason: { $exists: true } },
      ],
    },
    {
      $set: {
        status: normalizedStatus,
        traccar_device_status: normalizedStatus,
        connected: true,
        is_connected: true,
        connection_state: "online",
        connected_at: connectedAt,
        last_connect_at: connectedAt,
        connect_source: source,
        updatedAt: now,
      },
      $unset: {
        disconnect_reason: "",
        disconnect_source: "",
      },
    },
    { new: false }
  ).lean();

  const closedReport = await DeviceDisconnection.findOneAndUpdate(
    { imei: normalizedImei, is_open: true },
    [
      {
        $set: {
          end_at: connectedAt,
          duration_sec: {
            $max: [
              0,
              {
                $toInt: {
                  $divide: [{ $subtract: [connectedAt, "$start_at"] }, 1000],
                },
              },
            ],
          },
          is_open: false,
          closed_by: source,
          close_reason: "reconnect",
          updatedAt: now,
        },
      },
    ],
    { new: true }
  ).lean();

  if (closedReport) {
    onlineNoOpenReportCheckedAtByImei.delete(normalizedImei);
  } else {
    onlineNoOpenReportCheckedAtByImei.set(normalizedImei, Date.now());
  }

  return {
    imei: normalizedImei,
    transitioned: !!previous,
    reportClosed: !!closedReport,
    report: closedReport,
  };
}

module.exports = {
  normalizeImei,
  onlineStatusQuery,
  isOnlineDeviceStatus,
  isOfflineDeviceStatus,
  markDeviceOffline,
  markDeviceOnline,
};
