const admin = require("firebase-admin");
const path = require("path");

admin.initializeApp({
  credential: admin.credential.cert(
    require(path.join(__dirname, "fcmconfig.json"))
  ),
});

module.exports = admin;
