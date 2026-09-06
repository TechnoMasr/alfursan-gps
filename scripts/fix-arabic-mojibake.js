/**
 * Fix UTF-8 Arabic/emoji stored via Windows-1252 mojibake (not pure Latin-1).
 * Usage: node scripts/fix-arabic-mojibake.js [file]
 */
const fs = require("fs");
const path = require("path");

const target =
  process.argv[2] ||
  path.join(__dirname, "..", "traccar-bridge-ontherport.js");

/** Unicode → original Windows-1252 byte for C1 range overrides */
const CP1252_REVERSE = new Map([
  [0x20ac, 0x80],
  [0x201a, 0x82],
  [0x0192, 0x83],
  [0x201e, 0x84],
  [0x2026, 0x85],
  [0x2020, 0x86],
  [0x2021, 0x87],
  [0x02c6, 0x88],
  [0x2030, 0x89],
  [0x0160, 0x8a],
  [0x2039, 0x8b],
  [0x0152, 0x8c],
  [0x017d, 0x8e],
  [0x2018, 0x91],
  [0x2019, 0x92],
  [0x201c, 0x93],
  [0x201d, 0x94],
  [0x2022, 0x95],
  [0x2013, 0x96],
  [0x2014, 0x97],
  [0x02dc, 0x98],
  [0x2122, 0x99],
  [0x0161, 0x9a],
  [0x203a, 0x9b],
  [0x0153, 0x9c],
  [0x017e, 0x9e],
  [0x0178, 0x9f],
]);

function charToCp1252Byte(cp) {
  if (CP1252_REVERSE.has(cp)) return CP1252_REVERSE.get(cp);
  if (cp <= 0xff) return cp;
  return null;
}

function looksMojibakeChar(cp) {
  if (cp >= 0x80 && cp <= 0xff) return true;
  return CP1252_REVERSE.has(cp);
}

function fixMojibake(s) {
  const bytes = [];
  for (const ch of s) {
    const cp = ch.codePointAt(0);
    const b = charToCp1252Byte(cp);
    if (b == null) return s;
    bytes.push(b);
  }
  const fixed = Buffer.from(bytes).toString("utf8");
  if (fixed.includes("\uFFFD")) return s;
  const ok =
    /[\u0600-\u06FF]/.test(fixed) ||
    /[\u{1F300}-\u{1FAFF}]/u.test(fixed) ||
    /[—–…≥≤≠✅✔]/.test(fixed);
  return ok ? fixed : s;
}

const text = fs.readFileSync(target, "utf8");

// Runs of cp1252-mojibake chars (may include U+2013 etc.), split by ASCII.
let runs = 0;
const out = text.replace(/(?:[\u0080-\u00FF\u0152\u0153\u0160\u0161\u0178\u017D\u017E\u0192\u02C6\u02DC\u2013\u2014\u2018\u2019\u201A\u201C\u201D\u201E\u2020\u2021\u2022\u2026\u2030\u2039\u203A\u20AC\u2122])+/gu, (chunk) => {
  // Skip pure punctuation that is intentionally in source (rare); require Ø/Ù/ð/â seed
  if (!/[ØÙðâ]/.test(chunk) && !/[\u00C0-\u00FF]/.test(chunk)) return chunk;
  const fixed = fixMojibake(chunk);
  if (fixed !== chunk) runs += 1;
  return fixed;
});

if (out === text) {
  console.log("No changes:", target);
  process.exit(0);
}

fs.writeFileSync(target, out, "utf8");
console.log(`Recovered ${runs} mojibake run(s) in ${target}`);

const verify = fs.readFileSync(target, "utf8");
const still = verify.split(/\n/).filter((l) => /Ø[^\x00-\x7F]|Ù[^\x00-\x7F]/.test(l));
console.log("Lines still looking mojibake:", still.length);
const samples = verify
  .split(/\n/)
  .filter((l) => /[\u0600-\u06FF]/.test(l))
  .slice(0, 15);
console.log("--- samples ---");
for (const l of samples) console.log(l.trim().slice(0, 160));

const idle = verify.split(/\n/).find((l) => l.includes("حالة خمول") || l.includes('idle: { en: "Idle Alarm"'));
console.log("--- idle ---");
console.log(idle && idle.trim());
