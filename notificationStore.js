const mongoose = require("mongoose");

function getNotificationModel() {
  return require("./mongo").Notification;
}

function pickNotificationFields(data = {}) {
  const d = data && typeof data === "object" ? data : {};
  const alarmText = d.alarmText || d.alarm_text || null;
  const alarmTextAr = d.alarmTextAr || d.alarm_text_ar || null;
  const codes = Array.isArray(d.alarmCodes)
    ? d.alarmCodes
    : d.alarmCode
      ? [d.alarmCode]
      : [];

  return {
    type: d.type || "alarm",
    subType: d.subType || d.sub_type || null,
    alarmType: d.alarmType ?? d.alarm_type ?? null,
    alarmText,
    alarmTextAr,
    latitude: d.latitude ?? d.lat ?? d.gps?.latitude ?? null,
    longitude: d.longitude ?? d.lon ?? d.gps?.longitude ?? null,
    speed:
      d.speed != null && Number.isFinite(Number(d.speed))
        ? Number(d.speed)
        : d.gps?.speed != null && Number.isFinite(Number(d.gps.speed))
          ? Number(d.gps.speed)
          : null,
    fence_name: d.fence_name || d.fenceName || null,
    alarmCodes: codes,
    acc_status: d.acc_status || null,
  };
}

function plainGpsDoc(doc) {
  if (!doc) return null;
  if (typeof doc.toObject === "function") return doc.toObject();
  return { ...doc };
}

/**
 * Mirror one gps_logs alarm row into notifications (same fields as GpsLog).
 */
async function mirrorGpsAlarmToNotification(doc) {
  const plain = plainGpsDoc(doc);
  if (!plain || plain.type !== "alarm") return null;

  const imei = String(plain.imei || "").trim();
  if (!imei) return null;

  let user_id = null;
  let device_name = "";
  let carnum = "";
  try {
    const deviceDetails = await mongoose.connection
      .collection("device_details")
      .findOne({ imei });
    if (deviceDetails) {
      user_id = deviceDetails.device_owner_id ?? null;
      device_name = deviceDetails.name || "";
      carnum = deviceDetails.carnum || "";
    }
  } catch (err) {
    console.warn("mirrorGpsAlarm device_details lookup failed:", err.message);
  }

  const fields = pickNotificationFields(plain);
  const alarmText = fields.alarmText || plain.alarmText || "";
  const alarmTextAr = fields.alarmTextAr || plain.alarmTextAr || "";
  const body = alarmTextAr || alarmText || "تنبيه";
  const title =
    `${device_name}${carnum ? " - " + carnum : ""}`.trim() || `تنبيه ${imei}`;

  const Notification = getNotificationModel();
  return Notification.create({
    ...fields,
    alarmText: alarmText || body,
    alarmTextAr: alarmTextAr || body,
    user_id,
    imei,
    title,
    body,
    data: {
      ...plain,
      type: "alarm",
      subType: fields.subType,
      alarmType: fields.alarmType,
      alarmText: alarmText || body,
      alarmTextAr: alarmTextAr || body,
      latitude: fields.latitude,
      longitude: fields.longitude,
      speed: fields.speed,
      fence_name: fields.fence_name,
      alarmCodes: fields.alarmCodes,
      acc_status: fields.acc_status,
    },
    device_name,
    carnum,
    sent_at: plain.packet_date || plain.date || new Date(),
    tokens_count: 0,
    success_count: 0,
    failed_count: 0,
    is_read: false,
  });
}

/**
 * Persist notification (commands / non-gpslog paths). FCM is optional — handled by caller.
 */
async function persistTenantNotification({
  imei,
  user_id = null,
  title,
  body,
  data = {},
  device_name = "",
  carnum = "",
  tokens_count = 0,
  success_count = 0,
  failed_count = 0,
}) {
  const fields = pickNotificationFields(data);

  // command_response: prefer explicit alarm texts; else build from command + response
  let alarmText = fields.alarmText || null;
  let alarmTextAr = fields.alarmTextAr || null;
  let resolvedBody = body || null;

  if (data?.type === "command_response") {
    const commandRaw = String(data.command || "").trim() || "—";
    const responseRaw = String(data.response || body || "").trim() || "—";
    const arBuilt = `الأمر: ${commandRaw}\nالرد: ${responseRaw}`;
    const enBuilt = `Command: ${commandRaw}\nResponse: ${responseRaw}`;
    alarmTextAr = alarmTextAr || arBuilt;
    alarmText = alarmText || data.enBody || enBuilt;
    resolvedBody = resolvedBody || alarmTextAr;
  }

  alarmText = alarmText || resolvedBody || title || "";
  alarmTextAr = alarmTextAr || resolvedBody || title || "";
  resolvedBody = resolvedBody || alarmTextAr;

  const Notification = getNotificationModel();
  return Notification.create({
    ...fields,
    alarmText,
    alarmTextAr,
    user_id,
    imei,
    title: title || alarmTextAr,
    body: resolvedBody,
    data: {
      ...data,
      type: fields.type,
      subType: fields.subType,
      alarmType: fields.alarmType,
      alarmText,
      alarmTextAr,
      latitude: fields.latitude,
      longitude: fields.longitude,
      speed: fields.speed,
      fence_name: fields.fence_name,
      alarmCodes: fields.alarmCodes,
      acc_status: fields.acc_status,
    },
    device_name,
    carnum,
    sent_at: new Date(),
    tokens_count,
    success_count,
    failed_count,
    is_read: false,
  });
}

function scheduleMirrorGpsAlarm(doc) {
  setImmediate(() => {
    mirrorGpsAlarmToNotification(doc).catch((err) => {
      console.error("mirrorGpsAlarmToNotification error:", err.message);
    });
  });
}

module.exports = {
  pickNotificationFields,
  mirrorGpsAlarmToNotification,
  persistTenantNotification,
  scheduleMirrorGpsAlarm,
};
