// models/trip.js
const mongoose = require('mongoose');

const tripSchema = new mongoose.Schema({
  imei:        { type: String, required: true },
  start_at:    { type: Date,   index: true, required: true },
  end_at:      { type: Date,   index: true, default: null },
  start_loc:   { lat: Number, lon: Number },
  end_loc:     { lat: Number, lon: Number },
  distance_km: { type: Number, default: 0 },         // مجموع distanceDiff أثناء الحركة
  duration_min:{ type: Number, default: 0 },         // محسوبة عند الإغلاق (end-start)
  gap_after_min:{ type: Number, default: null },     // تُملأ عندما تبدأ الرحلة التالية
  is_open:     { type: Boolean, default: true },     // true حتى تُغلق عند توقف كافٍ
}, { timestamps: true });

tripSchema.index({ imei: 1, start_at: -1 });
tripSchema.index({ imei: 1, is_open: 1, start_at: -1 });

module.exports = mongoose.model('Trip', tripSchema);
