// mongo.js
const mongoose = require('mongoose');
const path = require('path');

require('dotenv').config({ path: path.join(__dirname, '.env') });

const uri = process.env.MONGODB_URI || 'mongodb://127.0.0.1:27017/admin';
const mongoMaxPoolSize = Number(process.env.MONGO_MAX_POOL_SIZE || 100) || 100;

mongoose.connect(uri, {
  maxPoolSize: mongoMaxPoolSize,
  minPoolSize: Number(process.env.MONGO_MIN_POOL_SIZE || 0) || 0,
})
  .then(async () => {
    console.log('✅ Connected to MongoDB');
    await ensureTraccarRawIngressRetentionIndex();
  })
  .catch(err => console.error('❌ MongoDB connection error:', err.message));

/**
 * 🔹 Collection: gpspoints — lean track points for fast replay
 * (imei + lat/lng + speed + direction + packet_date + ignition)
 */
const gpsPointSchema = new mongoose.Schema(
  {
    imei: { type: String, required: true, index: true },
    latitude: { type: Number, required: true },
    longitude: { type: Number, required: true },
    speed: { type: Number, default: 0 },
    direction: { type: Number, default: 0 },
    packet_date: { type: Date, required: true, index: true },
    date: { type: Date, index: true },
    ignition: { type: Boolean, default: null },
    traccar_position_id: { type: Number },
  },
  {
    strict: true,
    timestamps: true,
  }
);
gpsPointSchema.index({ imei: 1, packet_date: 1 });
gpsPointSchema.index({ imei: 1, packet_date: -1 });
gpsPointSchema.set("collection", "gpspoints");

const traccarIngressRawSchema = new mongoose.Schema(
  {
    received_at: { type: Date, required: true, index: true },
    source: { type: String, default: "traccar_http_forward", index: true },
    imei: { type: String, default: null, index: true },
    runtime_device_id: { type: Number, default: null },
    protocol: { type: String, default: null },
    position_id: { type: mongoose.Schema.Types.Mixed, default: null },
    fix_time: { type: mongoose.Schema.Types.Mixed, default: null },
    device_time: { type: mongoose.Schema.Types.Mixed, default: null },
    server_time: { type: mongoose.Schema.Types.Mixed, default: null },
    latitude: { type: Number, default: null },
    longitude: { type: Number, default: null },
    has_command_response: { type: Boolean, default: false },
    raw_payload: { type: mongoose.Schema.Types.Mixed, required: true },
  },
  {
    strict: true,
    timestamps: true,
  }
);
traccarIngressRawSchema.set("collection", "traccar_ingress_raw");

async function ensureTraccarRawIngressRetentionIndex() {
  const days = Number(process.env.TRACCAR_RAW_RETENTION_DAYS ?? 7);
  const collectionName = "traccar_ingress_raw";
  const coll = mongoose.connection.collection("traccar_ingress_raw");
  const indexName = "traccar_raw_received_at_ttl";
  const isMissingNamespace = (err) => err?.codeName === "NamespaceNotFound" || err?.code === 26 || /ns does not exist/i.test(String(err?.message || ""));

  async function safeIndexes() {
    try {
      return await coll.indexes();
    } catch (err) {
      if (!isMissingNamespace(err)) throw err;
      await mongoose.connection.db.createCollection(collectionName);
      return coll.indexes();
    }
  }

  async function updateExistingReceivedAtIndex(existing, expireAfterSeconds) {
    try {
      await mongoose.connection.db.command({
        collMod: collectionName,
        index: {
          name: existing.name,
          expireAfterSeconds,
        },
      });
      console.log(`[mongo] updated ${collectionName}.${existing.name} TTL to ${expireAfterSeconds}s`);
      return true;
    } catch (err) {
      console.warn(
        `[mongo] existing ${collectionName}.${existing.name} index is not TTL-compatible; keeping it unchanged: ${err.message}`
      );
      return false;
    }
  }

  if (Number.isFinite(days) && days > 0) {
    const expireAfterSeconds = Math.floor(days * 24 * 60 * 60);
    const indexes = await safeIndexes();
    const existing = indexes.find((index) => index.name === indexName);
    if (existing && Number(existing.expireAfterSeconds) !== expireAfterSeconds) {
      await coll.dropIndex(indexName);
    }
    const sameReceivedAtKey = indexes.find(
      (index) =>
        index.name !== indexName &&
        index.key &&
        Object.keys(index.key).length === 1 &&
        Number(index.key.received_at) === 1
    );
    if (sameReceivedAtKey) {
      if (Number(sameReceivedAtKey.expireAfterSeconds) === expireAfterSeconds) return;
      await updateExistingReceivedAtIndex(sameReceivedAtKey, expireAfterSeconds);
      return;
    }
    await coll.createIndex(
      { received_at: 1 },
      {
        name: indexName,
        expireAfterSeconds,
        background: true,
      }
    );
    return;
  }
  try {
    await coll.dropIndex(indexName);
  } catch (err) {
    if (err?.codeName !== "IndexNotFound" && err?.code !== 27 && !isMissingNamespace(err)) throw err;
  }
}

/**
 * 🔹 Collection 2: gps_buffers
 * يحتفظ فقط بالـ buffer للرجوع إليه لاحقًا
 */
const gpsBufferSchema = new mongoose.Schema(
  {
    imei: { type: String, index: true },
    type: String,
    buffer: String,
    date: { type: Date, index: true }
  },
  { timestamps: true ,
    strict: false,

  }
);


const TeltonikaParserSchema = new mongoose.Schema(
  {
    imei: { type: String, index: true },
    type: String,
    buffer: String,
    date: { type: Date, index: true }
  },
  {
    timestamps: true,
    strict: false,
  }
);

// 🔹 Snapshot of latest device status (create-or-update per IMEI)
const deviceStatusSchema = new mongoose.Schema(
  {
    imei: { type: String, index: true, unique: true },
    last_packet_at: { type: Date, index: true },
    last_activity_at: { type: Date },
    last_gps_at: { type: Date },
    last_lat: Number,
    last_lon: Number,
    last_speed: Number,
    last_type: String,
    last_voltage: Number,
    last_voltage_unit: String, // e.g. 'mv', 'level', 'v'
    km_total: { type: Number, default: 0 },
    miles_total: { type: Number, default: 0 },
    last_mileage_at: { type: Date, default: null }, // آخر نقطة تم احتسابها في التجميع التراكمي
    // keep naming consistent with other snapshot fields
    last_direction: Number,

  },
  {
    timestamps: true,
    strict: false,
  }
);

const dailyMileageSchema = new mongoose.Schema(
  {
    imei: { type: String, index: true },
    day: { type: Date, index: true }, // بداية اليوم UTC
    km: Number,
    miles: Number,
    overspeed_count: Number,
    total_stop_minutes: Number,
    total_stop_count: { type: Number, default: 0 },
    acc_on_count: { type: Number, default: 0 },
    acc_off_count: { type: Number, default: 0 },
  },
  { timestamps: true }
);
dailyMileageSchema.index({ imei: 1, day: 1 }, { unique: true });

const travelStatSchema = new mongoose.Schema(
  {
    imei: { type: String, index: true },
    day: { type: Date, index: true }, // بداية اليوم UTC
    stop_threshold_min: { type: Number, default: 1 },
    segments: [
      {
        start_at: Date,
        end_at: Date,
        distance_km: Number,
        driving_minutes: Number,
        start_loc: { lat: Number, lon: Number },
        end_loc: { lat: Number, lon: Number },
      }
    ],
    total_distance_km: Number,
    total_driving_minutes: Number,
    total_segments: Number,
    total_stop_count: { type: Number, default: 0 }, // عدد مرات التوقف (حسب عتبة محددة)
  },
  { timestamps: true }
);
travelStatSchema.index({ imei: 1, day: 1, stop_threshold_min: 1 }, { unique: true });

// 🔹 Collection: idle_stats (تشغيل بدون حركة)
const parkingEventSchema = new mongoose.Schema(
  {
    imei: { type: String, index: true },
    start_at: { type: Date, index: true },
    end_at: { type: Date, index: true },
    duration_seconds: { type: Number, default: 0 },
    start_lat: Number,
    start_lon: Number,
    end_lat: Number,
    end_lon: Number,
    start_speed: Number,
    end_speed: Number,
    min_speed_kph: Number,
    max_speed_kph: Number,
    is_open: { type: Boolean, default: true },
  },
  { timestamps: true, strict: false }
);
parkingEventSchema.index({ imei: 1, start_at: -1 });
parkingEventSchema.index({ imei: 1, end_at: -1 });

const idleStatSchema = new mongoose.Schema(
  {
    imei: { type: String, index: true },
    day: { type: Date, index: true }, // بداية اليوم UTC
    idle_duration_seconds: { type: Number, default: 0 },
    idle_count: { type: Number, default: 0 },
    first_idle_start: Date,
    last_idle_end: Date,
    fuel_waste_liters: Number,
  },
  { timestamps: true }
);
idleStatSchema.index({ imei: 1, day: 1 }, { unique: true });

// 🔹 Collection: static_stats (أيام السكون)
const staticStatSchema = new mongoose.Schema(
  {
    imei: { type: String, index: true },
    day: { type: Date, index: true }, // بداية اليوم UTC
    daily_mileage_km: { type: Number, default: 0 },
    is_static: { type: Boolean, default: false },
  },
  { timestamps: true }
);
staticStatSchema.index({ imei: 1, day: 1 }, { unique: true });

// Tracker connectivity outage intervals. These are based on connection state,
// not GPS freshness, motion, speed, or ignition.
const deviceDisconnectionSchema = new mongoose.Schema(
  {
    imei: { type: String, index: true, required: true },
    start_at: { type: Date, required: true, index: true },
    end_at: { type: Date, default: null, index: true },
    duration_sec: { type: Number, default: 0 },
    is_open: { type: Boolean, default: true, index: true },
    reason: String,
    opened_by: String,
    closed_by: String,
    close_reason: String,
    last_seen_offline_at: Date,
  },
  { timestamps: true, strict: false }
);
deviceDisconnectionSchema.index({ imei: 1, start_at: -1 });
deviceDisconnectionSchema.index(
  { imei: 1, is_open: 1 },
  {
    unique: true,
    partialFilterExpression: { is_open: true },
  }
);
deviceDisconnectionSchema.set("collection", "device_disconnections");

// 🔹 Collection: command_response (تخزين الأوامر والردود)
const commandResponseSchema = new mongoose.Schema(
  {
    imei: { type: String, index: true },
    command: String,
    commandId: String,
    response: String, // سيتم تحديثه عند استلام الرد
    sentAt: { type: Date, default: Date.now, index: true },
    respondedAt: Date, // تاريخ استلام الرد
    status: { type: String, default: 'pending', enum: ['pending', 'responded'] }, // pending أو responded
    // عناوين ومحتوى إشعار الإرسال (من API أو افتراضي)
    arTitle: String,
    enTitle: String,
    arBody: String,
    enBody: String,
    // عناوين ومحتوى إشعار عند استلام الرد من الجهاز
    responseArTitle: String,
    responseEnTitle: String,
    responseArBody: String,
    responseEnBody: String,
  },
  { timestamps: true, strict: false }
);
commandResponseSchema.index({ imei: 1, sentAt: -1 });
commandResponseSchema.index({ imei: 1, status: 1 });

// 🔹 Collection: notifications (تخزين كل التنبيهات — مع أو بدون FCM)
const notificationSchema = new mongoose.Schema(
  {
    user_id: { type: mongoose.Schema.Types.Mixed, index: true },
    imei: { type: String, index: true },
    title: String,
    body: String,
    type: { type: String, index: true },
    subType: String,
    alarmType: { type: mongoose.Schema.Types.Mixed, index: true },
    alarmText: String,
    alarmTextAr: String,
    latitude: Number,
    longitude: Number,
    speed: Number,
    fence_name: String,
    alarmCodes: [String],
    acc_status: String,
    data: { type: mongoose.Schema.Types.Mixed },
    device_name: String,
    carnum: String,
    sent_at: { type: Date, default: Date.now, index: true },
    tokens_count: { type: Number, default: 0 },
    success_count: { type: Number, default: 0 },
    failed_count: { type: Number, default: 0 },
    is_read: { type: Boolean, default: false },
    read_at: Date,
  },
  { timestamps: true, strict: false }
);
notificationSchema.index({ user_id: 1, sent_at: -1 });
notificationSchema.index({ imei: 1, sent_at: -1 });
notificationSchema.index({ user_id: 1, is_read: 1 });
notificationSchema.index({ imei: 1, createdAt: -1 });
notificationSchema.index({ imei: 1, alarmType: 1, createdAt: -1 });
notificationSchema.index({ createdAt: -1 });

// 🔹 Collection: overspeed_alerts (تقرير تجاوز السرعة)
const overspeedAlertSchema = new mongoose.Schema(
  {
    imei: { type: String, index: true },
    start_time: { type: Date, index: true },
    end_time: { type: Date, index: true },
    start_lat: Number,
    start_lon: Number,
    end_lat: Number,
    end_lon: Number,
    speed_kmh: Number,           // أقصى سرعة خلال الحدث
    speed_limit_kmh: Number,     // حد السرعة من devicestatuses.alert_speed_limit_value آنذاك
    duration_sec: { type: Number, default: 0 },
    distance_km: { type: Number, default: 0 },
  },
  { timestamps: true, strict: false }
);
overspeedAlertSchema.index({ imei: 1, start_time: -1 });
overspeedAlertSchema.set('collection', 'overspeed_alerts');

// 🔹 Collection: acc_events (تقرير حالة المحرك ACC، من ignition + تنبيهات الطاقة)
const accEventSchema = new mongoose.Schema(
  {
    imei: { type: String, index: true },
    acc_status: { type: String, enum: ["on", "off"], index: true },
    start_time: { type: Date, index: true },
    end_time: { type: Date, index: true },
    duration_sec: { type: Number, default: 0 },
    start_lat: Number,
    start_lon: Number,
    end_lat: Number,
    end_lon: Number,
    triggered_by: String, // 'ignition' | 'poweron' | 'poweroff' | 'powercut' | 'powerrestored'
  },
  { timestamps: true, strict: false }
);
accEventSchema.index({ imei: 1, start_time: -1 });
accEventSchema.set("collection", "acc_events");

gpsBufferSchema.index({ imei: 1, date: -1 });

const GpsPoint = mongoose.model('GpsPoint', gpsPointSchema);
const TraccarIngressRaw = mongoose.model('TraccarIngressRaw', traccarIngressRawSchema);
const GpsBuffer = mongoose.model('GpsBuffer', gpsBufferSchema);
const TeltonikaParser = mongoose.model('TeltonikaParser', TeltonikaParserSchema);
const DeviceStatus = mongoose.model('DeviceStatus', deviceStatusSchema);
const OverspeedAlert = mongoose.model("OverspeedAlert", overspeedAlertSchema);
const AccEvent = mongoose.model("AccEvent", accEventSchema);
const DailyMileage = mongoose.model("DailyMileage", dailyMileageSchema);
const TravelStat = mongoose.model('TravelStat', travelStatSchema);
const IdleStat = mongoose.model('IdleStat', idleStatSchema);
const StaticStat = mongoose.model('StaticStat', staticStatSchema);
const ParkingEvent = mongoose.model('ParkingEvent', parkingEventSchema);
const DeviceDisconnection = mongoose.model("DeviceDisconnection", deviceDisconnectionSchema);
const CommandResponse = mongoose.model('command_response', commandResponseSchema);
const Notification = mongoose.model('Notification', notificationSchema);

module.exports = {
  GpsPoint,
  TraccarIngressRaw,
  GpsBuffer,
  TeltonikaParser,
  DeviceStatus,
  OverspeedAlert,
  AccEvent,
  DailyMileage,
  TravelStat,
  IdleStat,
  StaticStat,
  ParkingEvent,
  DeviceDisconnection,
  CommandResponse,
  Notification,
};
