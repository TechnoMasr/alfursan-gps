const admin = require("./firebase");

async function sendPushNotification({
  token,
  title,
  body,
  data = {},
}) {
  if (!token) return;

  if (
    !title ||
    !body ||
    body === 'body'||
    title === 'title' ||
    body === 'Body'||
    title === 'Title'
  ) {
    console.log(`⚠️ Invalid title or body, skipping notification for IMEI: ${imei}`);
    return null;
  }
  const message = {
    token,
    notification: { title, body },
    data: Object.fromEntries(
      Object.entries(data).map(([k, v]) => [k, String(v)])
    ),
  };

  return admin.messaging().send(message).then((response) => {
    console.log('Successfully sent message:', response);
  })
  .catch((error) => {
    console.log('Error sending message:', error);
  });
}

module.exports = {
  sendPushNotification,
};
