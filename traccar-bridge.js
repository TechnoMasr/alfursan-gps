const path = require("path");

require("dotenv").config({ path: path.join(__dirname, ".env") });

console.warn(
  "[startup] traccar-bridge.js is deprecated; launching traccar-bridge-ontherport.js instead."
);

require("./traccar-bridge-ontherport");
