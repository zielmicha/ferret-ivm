// Small DOM helpers. Text always goes through textContent: names, SQL and
// sample values come from the server and are never parsed as HTML.

/**
 * h('div.card#x', {onclick, title, style: {...}, dataset: {...}}, child, ...)
 * Children may be strings, numbers, nodes, arrays, null/false (skipped).
 */
export function h(spec, attrs, ...children) {
  const m = /^([a-z0-9-]+)?((?:[.#][\w-]+)*)$/i.exec(spec) || [];
  const el = document.createElement(m[1] || 'div');
  for (const part of (m[2] || '').match(/[.#][\w-]+/g) || []) {
    if (part[0] === '.') el.classList.add(part.slice(1));
    else el.id = part.slice(1);
  }
  if (attrs && (typeof attrs !== 'object' || attrs instanceof Node || Array.isArray(attrs))) {
    children.unshift(attrs);
    attrs = null;
  }
  if (attrs) setAttrs(el, attrs);
  append(el, children);
  return el;
}

const SVG_NS = 'http://www.w3.org/2000/svg';

export function s(tag, attrs, ...children) {
  const el = document.createElementNS(SVG_NS, tag);
  if (attrs) {
    for (const [k, v] of Object.entries(attrs)) {
      if (v === null || v === undefined || v === false) continue;
      if (k.startsWith('on')) el.addEventListener(k.slice(2), v);
      else if (k === 'class') el.setAttribute('class', v);
      else if (k === 'text') el.textContent = v;
      else el.setAttribute(k, v);
    }
  }
  append(el, children);
  return el;
}

function setAttrs(el, attrs) {
  for (const [k, v] of Object.entries(attrs)) {
    if (v === null || v === undefined || v === false) continue;
    if (k.startsWith('on') && typeof v === 'function') el.addEventListener(k.slice(2), v);
    else if (k === 'style' && typeof v === 'object') {
      for (const [p, x] of Object.entries(v)) {
        if (p.startsWith('--')) el.style.setProperty(p, x);
        else el.style[p] = x;
      }
    }
    else if (k === 'dataset') Object.assign(el.dataset, v);
    else if (k === 'class') el.className = v;
    else if (k === 'text') el.textContent = v;
    else if (k in el && typeof v !== 'string') el[k] = v;
    else el.setAttribute(k, v === true ? '' : v);
  }
}

export function append(el, children) {
  for (const c of children.flat(Infinity)) {
    if (c === null || c === undefined || c === false) continue;
    el.appendChild(c instanceof Node ? c : document.createTextNode(String(c)));
  }
  return el;
}

export function clear(el) {
  while (el.firstChild) el.removeChild(el.firstChild);
  return el;
}

export function replace(el, ...children) {
  clear(el);
  return append(el, children);
}

/** Categorical slot (1..8) for an operator family. */
export const KIND_FAMILY = {
  Table: 1, Constant: 1,
  Mfp: 2, FlatMap: 2, Negate: 2,
  DeltaJoin: 3,
  Reduce: 4, Distinct: 4,
  TopK: 5, Window: 5,
  Union: 6, SetOp: 6,
  TemporalFilter: 7, Recursive: 7,
};

export const FAMILY_NAMES = {
  1: 'source', 2: 'map / filter', 3: 'join', 4: 'aggregate', 5: 'order / limit', 6: 'set ops', 7: 'time / recursion',
};

export function kindColor(kind) {
  const slot = KIND_FAMILY[kind];
  return slot ? `var(--s${slot})` : 'var(--muted)';
}

/** Catalog object kinds share the categorical slots, in a fixed order. */
export const OBJECT_SLOT = { source: 2, table: 1, view: 5, 'materialized view': 3, index: 4, sink: 7, subscription: 6 };

export function objectColor(kind) {
  const slot = OBJECT_SLOT[kind];
  return slot ? `var(--s${slot})` : 'var(--muted)';
}

export function kindLabel(kind, text = kind) {
  return h('span.kind', { style: { '--kc': kindColor(kind) } }, text);
}

export function objectKindLabel(kind) {
  return h('span.kind', { style: { '--kc': objectColor(kind) } }, kind);
}

/** Status badge: color never alone, always dot + text. */
export function statusBadge(status) {
  const s = String(status || 'unknown');
  let cls = '';
  if (/^error/i.test(s)) cls = '.crit';
  else if (/^(streaming|running)/i.test(s)) cls = '.good';
  else if (/^(snapshotting|starting|loading)/i.test(s)) cls = '.info';
  else if (/^stopped/i.test(s)) cls = '';
  const label = s.length > 60 ? s.slice(0, 57) + '…' : s;
  return h('span.badge' + cls, { title: s }, h('span.dot'), label);
}

export function badge(text, cls = '') {
  return h('span.badge' + (cls ? '.' + cls : ''), text);
}

export function empty(title, body, extra) {
  return h('div.empty', h('strong', title), body ? h('div', body) : null, extra || null);
}

export function errorBox(msg) {
  return h('div.error-box', h('div', String(msg)));
}

export function loading(text = 'Loading…') {
  return h('div.loading', text);
}

/** Card with a header (title + optional actions) and a body. */
export function card(title, body, { actions = null, flush = false, cls = '' } = {}) {
  return h('section.card' + (cls ? '.' + cls : ''),
    title !== null ? h('div.card-head', h('h2', title), actions) : null,
    h('div.card-body' + (flush ? '.flush' : ''), body));
}

export function tile(label, value, sub, { unit = null, hero = false, meter = null, title = null } = {}) {
  return h('div.tile' + (hero ? '.hero' : ''), { title },
    h('div.tile-label', label),
    h('div.tile-value', value, unit ? h('span.unit', unit) : null),
    sub ? h('div.tile-sub', sub) : null,
    meter);
}

export function meter(fraction, { warnAt = 0.8, critAt = 0.95 } = {}) {
  const f = Math.max(0, Math.min(1, fraction || 0));
  const cls = f >= critAt ? '.crit' : f >= warnAt ? '.warn' : '';
  return h('div.meter' + cls, { role: 'meter', 'aria-valuenow': Math.round(f * 100), 'aria-valuemin': 0, 'aria-valuemax': 100 },
    h('span', { style: { width: (f * 100).toFixed(1) + '%' } }));
}

/** Tabs: [{id, label, count?}], returns {el, select(id)}; onChange(id). */
export function tabs(items, active, onChange) {
  const el = h('div.tabs', { role: 'tablist' });
  const buttons = new Map();
  for (const it of items) {
    const b = h('button', { role: 'tab', type: 'button', onclick: () => select(it.id) },
      it.label, it.count !== undefined && it.count !== null ? h('span.count', String(it.count)) : null);
    buttons.set(it.id, b);
    el.appendChild(b);
  }
  function select(id, silent = false) {
    for (const [k, b] of buttons) {
      b.classList.toggle('on', k === id);
      b.setAttribute('aria-selected', k === id ? 'true' : 'false');
    }
    if (!silent) onChange(id);
  }
  select(active, true);
  return { el, select };
}

/** Segmented control: options [{value,label}]. */
export function segmented(options, value, onChange) {
  const el = h('div.seg', { role: 'group' });
  for (const o of options) {
    const b = h('button', { type: 'button', onclick: () => { set(o.value); onChange(o.value); } }, o.label);
    b.dataset.value = String(o.value);
    el.appendChild(b);
  }
  function set(v) {
    for (const b of el.children) b.classList.toggle('on', b.dataset.value === String(v));
  }
  set(value);
  return el;
}

/**
 * Sortable table. columns: [{key, label, num?, render?(row) -> node|string,
 * sort?(row) -> value, title?, cls?}]. Options: {onRowClick, rowClass, sortKey,
 * sortDir, footer}.
 */
export function dataTable(columns, rows, opts = {}) {
  const state = { key: opts.sortKey ?? null, dir: opts.sortDir ?? 1 };
  const wrap = h('div.table-wrap' + (opts.wrapCls ? '.' + opts.wrapCls : ''));
  const table = h('table.data');
  const thead = h('thead');
  const tbody = h('tbody');
  table.append(thead, tbody);
  if (opts.footer) table.appendChild(h('tfoot', opts.footer));
  wrap.appendChild(table);

  function header() {
    const tr = h('tr');
    for (const c of columns) {
      const sortable = c.sortable !== false && opts.sortable !== false;
      const th = h('th' + (c.num ? '.num' : '') + (sortable ? '.sortable' : ''), { title: c.title || null, scope: 'col' },
        c.label,
        state.key === c.key ? h('span.arrow', state.dir > 0 ? '▲' : '▼') : null,
        c.type ? h('span.ty', c.type) : null);
      if (sortable) {
        th.addEventListener('click', () => {
          if (state.key === c.key) state.dir = -state.dir;
          else {
            state.key = c.key;
            state.dir = c.num ? -1 : 1;
          }
          render();
        });
      }
      tr.appendChild(th);
    }
    replace(thead, tr);
  }

  function value(c, r) {
    if (c.sort) return c.sort(r);
    const v = r[c.key];
    return v;
  }

  function render() {
    header();
    let list = rows.slice();
    if (state.key !== null) {
      const c = columns.find((x) => x.key === state.key);
      if (c) {
        list.sort((a, b) => {
          const va = value(c, a);
          const vb = value(c, b);
          if (va === vb) return 0;
          if (va === null || va === undefined) return 1;
          if (vb === null || vb === undefined) return -1;
          return (typeof va === 'number' && typeof vb === 'number' ? va - vb : String(va).localeCompare(String(vb), undefined, { numeric: true })) * state.dir;
        });
      }
    }
    const frag = document.createDocumentFragment();
    for (const r of list) {
      const tr = h('tr' + (opts.onRowClick ? '.clickable' : ''));
      if (opts.rowClass) {
        const cls = opts.rowClass(r);
        if (cls) tr.classList.add(cls);
      }
      if (opts.onRowClick) {
        tr.tabIndex = 0;
        tr.addEventListener('click', () => opts.onRowClick(r));
        tr.addEventListener('keydown', (e) => {
          if (e.key === 'Enter') opts.onRowClick(r);
        });
      }
      for (const c of columns) {
        const v = c.render ? c.render(r) : r[c.key];
        const td = h('td' + (c.num ? '.num' : '') + (c.cls ? '.' + c.cls : ''));
        if (v === null || v === undefined) td.textContent = c.blank ?? '';
        else append(td, [v]);
        tr.appendChild(td);
      }
      frag.appendChild(tr);
    }
    replace(tbody, frag);
  }

  render();
  return {
    el: wrap,
    setRows(next) {
      rows = next;
      render();
    },
  };
}

/** Table of sample rows: [{values, weight}] with column headers. */
export function sampleTable(columns, rows, { showWeight = true } = {}) {
  const multiset = showWeight && rows.some((r) => r.weight !== 1);
  const table = h('table.data');
  const tr = h('tr');
  for (const c of columns) tr.appendChild(h('th', { scope: 'col' }, c.name, c.type ? h('span.ty', c.type) : null));
  if (multiset) tr.appendChild(h('th.num', { scope: 'col', title: 'Multiplicity (weight)' }, '×'));
  table.appendChild(h('thead', tr));
  const tb = h('tbody');
  for (const r of rows) {
    const row = h('tr');
    for (const v of r.values) {
      row.appendChild(v === null ? h('td.cell.null', 'NULL') : h('td.cell', { title: v.length > 40 ? v : null }, v));
    }
    if (multiset) row.appendChild(h('td.num', String(r.weight)));
    tb.appendChild(row);
  }
  table.appendChild(tb);
  return h('div.table-wrap', table);
}

/** Lightweight SQL highlighting into spans (text only, no HTML parsing). */
const SQL_KW = new Set(('select from where group by order having limit offset join left right full inner outer cross on using as and or not ' +
  'in is null case when then else end distinct union all except intersect with recursive create materialized view table index source sink ' +
  'drop alter replace if exists primary key into values insert update delete set publication connection postgres lateral over partition ' +
  'rows range between unbounded preceding following current row asc desc nulls first last fetch next only ties filter within interval ' +
  'true false refresh cascade').split(' '));

export function sqlCode(sql, { wrap = true } = {}) {
  const pre = h('pre.code' + (wrap ? '.wrap' : ''));
  const re = /(--[^\n]*)|('(?:[^']|'')*')|("(?:[^"]|"")*")|(\b\d+(?:\.\d+)?\b)|([A-Za-z_][A-Za-z0-9_]*)|(\s+)|([^\sA-Za-z0-9_'"-]+|-)/g;
  let m;
  while ((m = re.exec(sql)) !== null) {
    if (m[1]) pre.appendChild(h('span.sql-com', m[1]));
    else if (m[2]) pre.appendChild(h('span.sql-str', m[2]));
    else if (m[3]) pre.appendChild(document.createTextNode(m[3]));
    else if (m[4]) pre.appendChild(h('span.sql-num', m[4]));
    else if (m[5]) pre.appendChild(SQL_KW.has(m[5].toLowerCase()) ? h('span.sql-kw', m[5]) : document.createTextNode(m[5]));
    else pre.appendChild(document.createTextNode(m[0]));
  }
  return pre;
}

let toastTimer = null;
export function toast(msg, ms = 2600) {
  const el = document.getElementById('toast');
  if (!el) return;
  el.textContent = msg;
  el.hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => (el.hidden = true), ms);
}

export function copyText(text) {
  if (navigator.clipboard && window.isSecureContext) {
    navigator.clipboard.writeText(text).then(() => toast('Copied'), () => toast('Copy failed'));
    return;
  }
  const ta = h('textarea', { style: { position: 'fixed', left: '-1000px' } });
  ta.value = text;
  document.body.appendChild(ta);
  ta.select();
  try {
    document.execCommand('copy');
    toast('Copied');
  } catch {
    toast('Copy failed');
  }
  ta.remove();
}

/** Navigate to a route; pieces are URL-encoded (re-routes if unchanged). */
export function go(...parts) {
  const hash = '#' + parts.map((p) => encodeURIComponent(String(p))).join('/');
  if (location.hash === hash) window.dispatchEvent(new HashChangeEvent('hashchange'));
  else location.hash = hash;
}

/** href for a route. */
export function href(...parts) {
  return '#' + parts.map((p) => encodeURIComponent(String(p))).join('/');
}
