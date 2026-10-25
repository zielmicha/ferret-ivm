// Number, size, time and LSN formatting shared by every view.

const compactUnits = [
  [1e12, 'T'],
  [1e9, 'B'],
  [1e6, 'M'],
  [1e3, 'K'],
];

/** 1234 -> "1,234"; null -> "–". */
export function int(n) {
  if (n === null || n === undefined || Number.isNaN(n)) return '–';
  return Math.round(n).toLocaleString('en-US');
}

/** Auto-compact for tiles and labels: 1,284 / 12.9K / 4.2M. */
export function compact(n, digits = 1) {
  if (n === null || n === undefined || Number.isNaN(n)) return '–';
  const a = Math.abs(n);
  if (a < 10000) return a < 10 && !Number.isInteger(n) ? n.toFixed(digits) : int(n);
  for (const [v, s] of compactUnits) {
    if (a >= v) return (n / v).toFixed(a / v >= 100 ? 0 : digits) + s;
  }
  return int(n);
}

/** Rates: 0.25 -> "0.25", 12.3 -> "12.3", 1520 -> "1.5K". */
export function rate(n) {
  if (n === null || n === undefined || Number.isNaN(n)) return '–';
  const a = Math.abs(n);
  if (a === 0) return '0';
  if (a < 1) return n.toFixed(2);
  if (a < 100) return n.toFixed(1);
  return compact(n);
}

export function bytes(n) {
  if (n === null || n === undefined || Number.isNaN(n)) return '–';
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  let v = n;
  let i = 0;
  while (Math.abs(v) >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return (i === 0 ? String(Math.round(v)) : v.toFixed(Math.abs(v) >= 100 ? 0 : 1)) + ' ' + units[i];
}

export function pct(n, digits = 0) {
  if (n === null || n === undefined || Number.isNaN(n)) return '–';
  return n.toFixed(digits) + '%';
}

/** Seconds -> "3d 4h", "5m 12s", "850 ms". */
export function duration(secs) {
  if (secs === null || secs === undefined || Number.isNaN(secs)) return '–';
  if (secs < 1) return Math.round(secs * 1000) + ' ms';
  if (secs < 60) return (secs < 10 ? secs.toFixed(1) : Math.round(secs)) + ' s';
  const m = Math.floor(secs / 60);
  if (m < 60) return `${m}m ${Math.round(secs % 60)}s`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ${m % 60}m`;
  return `${Math.floor(h / 24)}d ${h % 24}h`;
}

/** Nanoseconds -> human time. */
export function nanos(ns) {
  if (ns === null || ns === undefined) return '–';
  if (ns < 1e3) return Math.round(ns) + ' ns';
  if (ns < 1e6) return (ns / 1e3).toFixed(ns < 1e4 ? 1 : 0) + ' µs';
  if (ns < 1e9) return (ns / 1e6).toFixed(ns < 1e7 ? 1 : 0) + ' ms';
  return duration(ns / 1e9);
}

/** Unix ms -> "12s ago". */
export function ago(ms, now = Date.now()) {
  if (!ms) return '–';
  const s = Math.max(0, (now - ms) / 1000);
  if (s < 2) return 'just now';
  return duration(s) + ' ago';
}

export function clock(ms) {
  if (!ms) return '–';
  const d = new Date(ms);
  return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

export function dateTime(ms) {
  if (!ms) return '–';
  const d = new Date(ms);
  return d.toISOString().replace('T', ' ').replace(/\.\d+Z$/, ' UTC');
}

/** PostgreSQL LSN text from a number. */
export function lsn(n) {
  if (!n) return '0/0';
  const hi = Math.floor(n / 4294967296);
  const lo = n % 4294967296;
  return hi.toString(16).toUpperCase() + '/' + lo.toString(16).toUpperCase();
}

export function plural(n, one, many = one + 's') {
  return `${int(n)} ${n === 1 ? one : many}`;
}
