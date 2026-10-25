// App shell: hash router, navigation, theme, header status.

import { api } from './api.js';
import { replace, errorBox } from './dom.js';
import * as fmt from './format.js';
import { live, startLive } from './live.js';
import overview from './views/overview.js';
import sources from './views/sources.js';
import catalog from './views/catalog.js';
import dataflow from './views/dataflow.js';
import arrangements from './views/arrangements.js';
import activity from './views/activity.js';
import errors from './views/errors.js';
import history from './views/history.js';
import consoleView from './views/console.js';

const views = { overview, sources, catalog, dataflow, arrangements, activity, errors, history, console: consoleView };

const main = document.getElementById('main');
let current = { name: null, inst: null };

function parseHash() {
  const raw = location.hash.replace(/^#\/?/, '');
  const [name, ...rest] = raw.split('/');
  const params = rest.map((p) => {
    try {
      return decodeURIComponent(p);
    } catch {
      return p;
    }
  });
  return { name: views[name] ? name : 'overview', params };
}

function route() {
  const { name, params } = parseHash();
  for (const a of document.querySelectorAll('.sidebar > a')) {
    const on = a.dataset.route === name;
    a.classList.toggle('active', on);
    if (on) a.setAttribute('aria-current', 'page');
    else a.removeAttribute('aria-current');
  }
  document.getElementById('sidebar').classList.remove('open');
  document.getElementById('nav-toggle').setAttribute('aria-expanded', 'false');
  if (current.name === name && current.inst && current.inst.update) {
    current.inst.update(params);
    return;
  }
  if (current.inst && current.inst.unmount) current.inst.unmount();
  current = { name, inst: null };
  replace(main);
  document.title = `${views[name].title} · ivm`;
  try {
    current.inst = views[name].mount(main, params) || null;
  } catch (e) {
    console.error(e);
    replace(main, errorBox(`This page failed to render: ${e.message}`));
  }
  main.focus({ preventScroll: true });
  window.scrollTo(0, 0);
}

// theme: an explicit choice is remembered; otherwise follow the OS
function effectiveTheme() {
  const t = document.documentElement.dataset.theme;
  if (t === 'light' || t === 'dark') return t;
  return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
}

function applyTheme(t) {
  if (t) document.documentElement.dataset.theme = t;
  else delete document.documentElement.dataset.theme;
  window.dispatchEvent(new Event('themechange'));
}

try {
  const saved = localStorage.getItem('ivm-theme');
  if (saved === 'light' || saved === 'dark') document.documentElement.dataset.theme = saved;
} catch {
  /* storage unavailable */
}
window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', () => {
  if (!document.documentElement.dataset.theme) window.dispatchEvent(new Event('themechange'));
});
document.getElementById('theme-toggle').addEventListener('click', () => {
  const next = effectiveTheme() === 'dark' ? 'light' : 'dark';
  try {
    localStorage.setItem('ivm-theme', next);
  } catch {
    /* ignore */
  }
  applyTheme(next);
});

// on phones the navigation is a drawer: a tap outside closes it
document.getElementById('main').addEventListener('click', () => {
  const sb = document.getElementById('sidebar');
  if (sb.classList.contains('open')) {
    sb.classList.remove('open');
    document.getElementById('nav-toggle').setAttribute('aria-expanded', 'false');
  }
});
document.getElementById('nav-toggle').addEventListener('click', () => {
  const sb = document.getElementById('sidebar');
  const open = !sb.classList.contains('open');
  sb.classList.toggle('open', open);
  document.getElementById('nav-toggle').setAttribute('aria-expanded', String(open));
});

// header status
const liveEl = document.getElementById('live');
const gsnEl = document.getElementById('gsn-now');
const errBadge = document.getElementById('error-badge');
function showStatus(s) {
  liveEl.classList.remove('ok', 'bad', 'busy');
  const text = { ok: 'live', bad: 'reconnecting', busy: 'engine busy', connecting: 'connecting' }[s] || s;
  if (s === 'ok' || s === 'bad' || s === 'busy') liveEl.classList.add(s);
  liveEl.querySelector('.live-text').textContent = text;
  liveEl.title = s === 'busy' ? 'The engine has not answered the last status request yet' : `Live updates: ${text}`;
}
live.on('status', showStatus);
live.on('sample', (smp) => {
  gsnEl.textContent = fmt.int(smp.processed_gsn);
  if (smp.engine) {
    const n = smp.engine.view_errors;
    errBadge.hidden = !n;
    errBadge.textContent = n > 99 ? '99+' : String(n);
  }
});

async function serverInfo() {
  try {
    const o = await api.overview();
    const el = document.getElementById('server-info');
    replace(el, `v${o.version} · ${o.config.data_dir} · up ${fmt.duration(o.server.uptime_secs)}`);
    el.title = `pid ${o.process.pid}`;
  } catch {
    /* shown by the pages */
  }
}

window.addEventListener('hashchange', route);
startLive();
serverInfo();
setInterval(serverInfo, 60000);
route();

