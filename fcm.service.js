const admin = require("./firebase");

/**
 * Build a Mongo query for fcm_tokens.user_id that matches Number or String
 * without schema changes or data migration.
 */
function buildFcmUserIdQuery(userId) {
  if (userId == null || userId === "") return null;

  const candidates = [userId];
  const numericUserId = Number(userId);
  if (Number.isFinite(numericUserId)) {
    candidates.push(numericUserId);
  }
  candidates.push(String(userId));

  const normalizedCandidates = [...new Set(candidates)];
  return { user_id: { $in: normalizedCandidates } };
}

function isFcmFailureLogEnabled() {
  const raw = String(process.env.FCM_FAILURE_LOG_ENABLED ?? "true")
    .trim()
    .toLowerCase();
  return raw === "1" || raw === "true" || raw === "yes";
}

/**
 * Optional diagnostics only. Default off (FCM_FAILURE_LOG_ENABLED=false).
 * Never throws — must not affect FCM or GPS flow.
 */
async function maybeLogFcmFailure({
  imei = null,
  user_id = null,
  token = null,
  error = null,
  notification_type = null,
} = {}) {
  if (!isFcmFailureLogEnabled()) return;

  try {
    const mongoose = require("mongoose");
    if (!mongoose?.connection || mongoose.connection.readyState !== 1) return;

    await mongoose.connection.collection("fcm_failures").insertOne({
      imei: imei != null ? String(imei) : null,
      user_id: user_id ?? null,
      token_tail: token ? String(token).slice(-8) : null,
      error_code: error?.errorInfo?.code || error?.code || null,
      error_message: String(error?.message || error || "").slice(0, 500),
      notification_type: notification_type != null ? String(notification_type) : null,
      created_at: new Date(),
    });
  } catch (_) {
    /* ignore diagnostics failures */
  }
}

async function sendPushNotification({
  token,
  title,
  body,
  data = {},
  imei = null,
  user_id = null,
}) {
  if (!token) return null;

  if (
    !title ||
    !body ||
    body === "body" ||
    title === "title" ||
    body === "Body" ||
    title === "Title"
  ) {
    console.log("⚠️ Invalid title or body, skipping FCM notification");
    return null;
  }

  const message = {
    token,
    notification: { title, body },
    data: Object.fromEntries(
      Object.entries(data || {}).map(([k, v]) => [k, String(v)])
    ),
  };

  try {
    const response = await admin.messaging().send(message);
    console.log("Successfully sent message:", response);
    return response;
  } catch (error) {
    console.error("Error sending message:", error?.message || error);
    await maybeLogFcmFailure({
      imei,
      user_id,
      token,
      error,
      notification_type: data?.type ?? null,
    });
    throw error;
  }
}

module.exports = {
  sendPushNotification,
  buildFcmUserIdQuery,
  maybeLogFcmFailure,
  isFcmFailureLogEnabled,
};
