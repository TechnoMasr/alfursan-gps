/**
 * Business-day boundaries for reporting.
 * Mongo stores UTC Date instants; calendar day is interpreted in a named zone.
 */
const CAIRO_TZ = "Africa/Cairo";

function pad2(n) {
  return String(n).padStart(2, "0");
}

/**
 * Format a Date as YYYY-MM-DD in the given IANA time zone.
 */
function zonedYmd(date, timeZone) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(date);
  const get = (type) => parts.find((p) => p.type === type)?.value;
  return `${get("year")}-${get("month")}-${get("day")}`;
}

/**
 * UTC instant of local midnight for YYYY-MM-DD in timeZone.
 * Binary-search so DST transitions stay correct without extra libraries.
 */
function zonedMidnightUtc(ymd, timeZone) {
  const [y, m, d] = ymd.split("-").map(Number);
  let lo = Date.UTC(y, m - 1, d - 1, 0, 0, 0);
  let hi = Date.UTC(y, m - 1, d + 1, 0, 0, 0);
  while (lo < hi) {
    const mid = Math.floor((lo + hi) / 2);
    const midYmd = zonedYmd(new Date(mid), timeZone);
    if (midYmd < ymd) lo = mid + 1;
    else hi = mid;
  }
  return new Date(lo);
}

/**
 * [dayStart, dayEnd) as UTC Dates for the calendar day containing `now` in timeZone.
 * dayStart is the UTC instant of that zone's midnight (materialization key).
 */
function businessDayRange(now = new Date(), timeZone = CAIRO_TZ) {
  const ymd = zonedYmd(now, timeZone);
  const dayStart = zonedMidnightUtc(ymd, timeZone);
  const [y, m, d] = ymd.split("-").map(Number);
  const nextUtc = new Date(Date.UTC(y, m - 1, d + 1));
  const nextYmd = `${nextUtc.getUTCFullYear()}-${pad2(nextUtc.getUTCMonth() + 1)}-${pad2(
    nextUtc.getUTCDate()
  )}`;
  const dayEnd = zonedMidnightUtc(nextYmd, timeZone);
  return { dayStart, dayEnd, ymd, timeZone };
}

function startOfBusinessDay(now = new Date(), timeZone = CAIRO_TZ) {
  return businessDayRange(now, timeZone).dayStart;
}

function startOfTodayUTC(now = new Date()) {
  return new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
}

/**
 * Resolve daily report day bounds from env / options.
 * mode: "cairo" (TARGET) | "utc" (legacy HEAD keys)
 */
function resolveMileageDay(options = {}) {
  const now = options.now ? new Date(options.now) : new Date();
  if (options.dayUtc) {
    const dayStart = new Date(options.dayUtc);
    const dayEnd = new Date(dayStart);
    dayEnd.setUTCDate(dayEnd.getUTCDate() + 1);
    return { dayStart, dayEnd, mode: "explicit", ymd: dayStart.toISOString().slice(0, 10) };
  }
  const mode = String(
    options.businessDay || process.env.MILEAGE_BUSINESS_DAY || "cairo"
  )
    .trim()
    .toLowerCase();
  if (mode === "utc") {
    const dayStart = startOfTodayUTC(now);
    const dayEnd = new Date(dayStart);
    dayEnd.setUTCDate(dayEnd.getUTCDate() + 1);
    return { dayStart, dayEnd, mode: "utc", ymd: dayStart.toISOString().slice(0, 10) };
  }
  const { dayStart, dayEnd, ymd, timeZone } = businessDayRange(now, CAIRO_TZ);
  return { dayStart, dayEnd, mode: "cairo", ymd, timeZone };
}

module.exports = {
  CAIRO_TZ,
  zonedYmd,
  zonedMidnightUtc,
  businessDayRange,
  startOfBusinessDay,
  startOfTodayUTC,
  resolveMileageDay,
  pad2,
};
