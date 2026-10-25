// Live samples: the server's buffered history, then one sample per second
// over Server-Sent Events. Reconnects backfill the gap from the history.

import { api } from './api.js';

const RING = 1200;
const listeners = { sample: new Set(), topology: new Set(), status: new Set() };

export const live = {
  samples: [],
  status: 'connecting',
  /** Subscribe; returns an unsubscribe function. */
  on(event, fn) {
    listeners[event].add(fn);
    return () => listeners[event].delete(fn);
  },
  last() {
    return this.samples.length ? this.samples[this.samples.length - 1] : null;
  },
  /** Last sample carrying engine counters. */
  lastEngine() {
    for (let i = this.samples.length - 1; i >= 0; i--) if (this.samples[i].engine) return this.samples[i];
    return null;
  },
};

function emit(event, data) {
  for (const fn of listeners[event]) {
    try {
      fn(data);
    } catch (e) {
      console.error(e);
    }
  }
}

function setStatus(s) {
  if (live.status === s) return;
  live.status = s;
  emit('status', s);
}

function push(sample) {
  const last = live.last();
  if (last && sample.t <= last.t) return;
  live.samples.push(sample);
  if (live.samples.length > RING) live.samples.splice(0, live.samples.length - RING);
  emit('sample', sample);
}

async function backfill() {
  const last = live.last();
  try {
    const res = await api.metrics(last ? last.t : 0);
    for (const s of res.samples) push(s);
  } catch {
    /* the stream will retry */
  }
}

let source = null;

export function startLive() {
  backfill();
  source = new EventSource('api/events');
  source.addEventListener('open', () => {
    setStatus('ok');
    backfill();
  });
  source.addEventListener('sample', (e) => {
    try {
      const s = JSON.parse(e.data);
      push(s);
      setStatus(s.engine_busy_ms && s.engine_busy_ms > 2000 ? 'busy' : 'ok');
    } catch (err) {
      console.error(err);
    }
  });
  source.addEventListener('topology', (e) => {
    try {
      emit('topology', JSON.parse(e.data));
    } catch {
      emit('topology', {});
    }
  });
  source.addEventListener('error', () => setStatus('bad'));
}

/** Per-interval rates of a cumulative counter (null where unknown or reset). */
export function rateSeries(samples, get) {
  const out = new Array(samples.length).fill(null);
  for (let i = 1; i < samples.length; i++) {
    const a = get(samples[i - 1]);
    const b = get(samples[i]);
    const dt = (samples[i].t - samples[i - 1].t) / 1000;
    if (a === null || a === undefined || b === null || b === undefined || dt <= 0 || b < a) continue;
    out[i] = (b - a) / dt;
  }
  return out;
}

/** Rate of a cumulative counter over roughly the last `secs` seconds. */
export function recentRate(get, secs = 5) {
  const ss = live.samples;
  if (ss.length < 2) return null;
  const b = ss[ss.length - 1];
  let j = ss.length - 2;
  while (j > 0 && b.t - ss[j].t < secs * 1000) j--;
  for (; j < ss.length - 1; j++) {
    const va = get(ss[j]);
    const vb = get(b);
    if (va !== null && va !== undefined && vb !== null && vb !== undefined && vb >= va) {
      const dt = (b.t - ss[j].t) / 1000;
      return dt > 0 ? (vb - va) / dt : null;
    }
  }
  return null;
}

/** Run fn now and every `ms` while the page is visible; returns stop(). */
export function poll(fn, ms) {
  let timer = null;
  let stopped = false;
  let running = false;
  const tick = async () => {
    if (stopped) return;
    if (document.visibilityState === 'visible' && !running) {
      running = true;
      try {
        await fn();
      } catch (e) {
        console.error(e);
      }
      running = false;
    }
    if (!stopped) timer = setTimeout(tick, ms);
  };
  const onVis = () => {
    if (document.visibilityState === 'visible' && !running && !stopped) {
      clearTimeout(timer);
      tick();
    }
  };
  document.addEventListener('visibilitychange', onVis);
  tick();
  return () => {
    stopped = true;
    clearTimeout(timer);
    document.removeEventListener('visibilitychange', onVis);
  };
}
