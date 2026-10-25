// SQL console: read-only statements (SELECT, SHOW, EXPLAIN) with a row cap.

import { api } from '../api.js';
import { h, replace, card, errorBox, segmented, toast } from '../dom.js';
import * as fmt from '../format.js';

const HISTORY_KEY = 'ivm-console-history';
const EXAMPLES = [
  'SHOW VIEWS',
  'SHOW ARRANGEMENTS',
  'SHOW FRONTIERS',
  'SELECT * FROM ivm.catalog_history ORDER BY 1 DESC LIMIT 20',
];

function loadHistory() {
  try {
    const v = JSON.parse(localStorage.getItem(HISTORY_KEY) || '[]');
    return Array.isArray(v) ? v.filter((x) => typeof x === 'string').slice(0, 25) : [];
  } catch {
    return [];
  }
}

function saveHistory(list) {
  try {
    localStorage.setItem(HISTORY_KEY, JSON.stringify(list.slice(0, 25)));
  } catch {
    /* ignore */
  }
}

const NUMERIC = /^(smallint|integer|bigint|numeric|real|double precision)/;

function resultTable(res) {
  const table = h('table.data');
  const tr = h('tr');
  const num = res.columns.map((c) => NUMERIC.test(c.type));
  res.columns.forEach((c, i) => tr.appendChild(h('th' + (num[i] ? '.num' : ''), { scope: 'col' }, c.name, h('span.ty', c.type))));
  table.appendChild(h('thead', tr));
  const tb = h('tbody');
  const frag = document.createDocumentFragment();
  for (const row of res.rows) {
    const r = h('tr');
    row.forEach((v, i) => r.appendChild(v === null ? h('td.cell.null', 'NULL') : h('td.cell' + (num[i] ? '.num' : ''), { title: v.length > 40 ? v : null }, v)));
    frag.appendChild(r);
  }
  tb.appendChild(frag);
  table.appendChild(tb);
  return h('div.table-wrap.result-wrap', table);
}

export default {
  title: 'SQL console',
  mount(root, params) {
    let mode = 'fresh';
    let limit = 1000;
    let running = false;
    let hist = loadHistory();
    const editor = h('textarea.console-editor', {
      spellcheck: false, 'aria-label': 'SQL statement', placeholder: 'SELECT … (Ctrl+Enter to run)',
      onkeydown: (e) => {
        if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
          e.preventDefault();
          run();
        }
      },
    });
    editor.value = params[0] || (hist[0] ?? 'SHOW VIEWS');
    const runBtn = h('button.btn.primary', { type: 'button', onclick: () => run() }, 'Run');
    const status = h('span.console-status', 'Only SELECT, SHOW and EXPLAIN run here.');
    const limitSel = h('select.input', { 'aria-label': 'Row limit', onchange: (e) => { limit = Number(e.target.value); } },
      ...[100, 1000, 10000].map((n) => h('option', { value: n, selected: n === limit }, `${fmt.int(n)} rows`)));
    const out = h('div');
    const histEl = h('div');
    root.append(
      h('div.page-head', h('div.page-title', h('h1', 'SQL console'), h('div.sub', 'Read-only queries against the engine, exactly as a psql session would run them'))),
      h('div.split', { style: { gridTemplateColumns: 'minmax(0, 1fr) 280px' } },
        h('div.stack',
          card(null, h('div.stack',
            editor,
            h('div.row', runBtn,
              segmented([{ value: 'fresh', label: 'fresh' }, { value: 'latest', label: 'latest' }], mode, (v) => { mode = v; }),
              limitSel, status))),
          out),
        h('div.stack', card('Recent', histEl), card('Examples', h('div', ...EXAMPLES.map((q) => h('button.hist-item', { type: 'button', title: q, onclick: () => { editor.value = q; editor.focus(); } }, q)))))));

    function drawHistory() {
      replace(histEl, hist.length
        ? hist.map((q) => h('button.hist-item', { type: 'button', title: q, onclick: () => { editor.value = q; editor.focus(); } }, q))
        : h('div.muted.small', 'Statements you run appear here.'));
    }
    drawHistory();

    async function run() {
      const sql = editor.value.trim();
      if (!sql || running) return;
      running = true;
      runBtn.disabled = true;
      status.textContent = 'Running…';
      const t0 = performance.now();
      try {
        const res = await api.query(sql, mode, limit);
        hist = [sql, ...hist.filter((q) => q !== sql)].slice(0, 25);
        saveHistory(hist);
        drawHistory();
        if (res.command !== null && res.command !== undefined) {
          status.textContent = `${res.command} · ${fmt.int(res.elapsed_ms)} ms`;
          replace(out, card(null, h('div.muted', res.command)));
        } else {
          const shown = res.rows.length;
          status.textContent = `${fmt.plural(res.row_count, 'row')}${res.truncated ? ` (showing ${fmt.int(shown)})` : ''} · ${res.elapsed_ms < 10 ? res.elapsed_ms.toFixed(1) : fmt.int(res.elapsed_ms)} ms`;
          replace(out, card(null, res.columns.length ? resultTable(res) : h('div.muted', 'No columns'), { flush: true }));
        }
      } catch (e) {
        status.textContent = `Failed after ${fmt.int(performance.now() - t0)} ms`;
        replace(out, errorBox(e.code ? `${e.message} (SQLSTATE ${e.code})` : e.message));
        if (e.status === 0) toast('The server is not reachable');
      } finally {
        running = false;
        runBtn.disabled = false;
      }
    }

    if (params[0]) run();
    editor.focus();
    return {
      update(p) {
        if (p[0]) {
          editor.value = p[0];
          run();
        }
      },
    };
  },
};
