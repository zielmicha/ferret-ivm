// Sources: replication status, LSN positions and lag, ingest rate, and the
// initial snapshot's progress per table.

import { api } from '../api.js';
import { h, replace, card, statusBadge, badge, empty, errorBox, dataTable, href, copyText } from '../dom.js';
import * as fmt from '../format.js';
import { live, poll, rateSeries } from '../live.js';
import { sparkline } from '../charts.js';

function snapshotCell(t) {
  if (t.snapshot === 'done') {
    return h('div.row', h('div.progress.done', h('span', { style: { width: '100%' } })), badge('done', 'good'));
  }
  if (t.snapshot === 'loading') {
    return h('div.row', h('div.progress.indeterminate', { title: 'Loading in primary key order; the upstream row count is not known' }, h('span')), badge('loading', 'info'));
  }
  return h('div.row', h('div.progress', h('span', { style: { width: '0%' } })), badge('pending'));
}

function sourceCard(src) {
  const samples = live.samples.slice(-120);
  const rates = rateSeries(samples, (s) => {
    const x = s.sources.find((y) => y.id === src.id);
    return x ? x.changes : null;
  });
  const lastRate = [...rates].reverse().find((v) => v !== null) ?? null;
  const meta = (label, value, title) => h('div', { title: title || null }, h('div.lbl', label), h('div.val', value));
  const lagText = src.lag_bytes === null || src.lag_bytes === undefined ? '–' : fmt.bytes(src.lag_bytes);
  const pctDone = src.tables.length ? src.tables_done / src.tables.length : 1;
  const allDone = src.tables_done === src.tables.length;
  const overall = h('div.stack', { style: { gap: '6px' } },
    h('div.row', h('strong', 'Initial snapshot'), h('span.muted', `${src.tables_done} of ${src.tables.length} tables loaded`), h('span.spacer'),
      allDone ? badge('complete', 'good') : badge('in progress', 'info')),
    h('div.progress' + (allDone ? '.done' : ''), h('span', { style: { width: (pctDone * 100).toFixed(1) + '%' } })));
  const tables = dataTable([
    { key: 'name', label: 'Table', render: (t) => h('a', { href: href('catalog', t.name) }, t.name) },
    { key: 'upstream', label: 'Upstream', cls: 'nowrap' },
    { key: 'pk', label: 'Key', render: (t) => (t.pk.length ? t.pk.join(', ') : h('span.muted', 'none (multiset)')), sortable: false },
    { key: 'rows', label: 'Rows', num: true, render: (t) => fmt.int(t.rows) },
    { key: 'snapshot', label: 'Snapshot', render: snapshotCell, sort: (t) => ({ done: 2, loading: 1, pending: 0 })[t.snapshot] },
    {
      key: 'boundary', label: 'Snapshot boundary', sortable: false,
      render: (t) => (t.snapshot === 'done' ? h('span.muted', '—') : t.boundary ? h('span.mono', t.boundary.join(', ')) : h('span.muted', 'start')),
      title: 'Primary key of the last row loaded; the next chunk starts after it',
    },
  ], src.tables, { sortKey: 'name', wrapCls: 'snap-table' });
  return h('section.card.src-card',
    h('div.card-head',
      h('h2', src.name),
      statusBadge(src.status),
      h('span.muted.small', `publication ${src.publication} · slot ${src.slot}`)),
    h('div.card-body.stack',
      h('div.src-meta',
        meta('Frontier LSN', src.frontier, 'Every upstream commit at or below this LSN is in the input log'),
        meta('Last commit LSN', src.last_commit),
        meta('Upstream LSN', src.upstream_lsn ? src.upstream : '–', 'WAL position the walsender reported last'),
        meta('Lag', lagText, 'Upstream LSN minus frontier'),
        meta('Last message', src.last_message_ms ? fmt.ago(src.last_message_ms) : '–', src.server_time_ms ? `upstream clock ${fmt.dateTime(src.server_time_ms)}` : null),
        meta('Commits / changes', `${fmt.compact(src.commits)} / ${fmt.compact(src.changes)}`, 'Transactions and row changes logged since start'),
        meta('Ingest now', lastRate === null ? '–' : `${fmt.rate(lastRate)} rows/s`)),
      h('div', h('div.lbl.small.muted', 'Row changes per second, last 2 minutes'), sparkline(rates, { width: 600, height: 34 })),
      overall,
      tables.el,
      h('div.row.small',
        h('span.muted', 'Connection'),
        h('code.ellipsis', { style: { maxWidth: '100%' } }, src.conninfo),
        h('button.btn.small', { type: 'button', onclick: () => copyText(src.conninfo) }, 'Copy'),
        h('span.muted', `created at GSN ${fmt.int(src.created_gsn)}`))));
}

export default {
  title: 'Sources',
  mount(root) {
    const body = h('div.stack');
    const sub = h('div.sub', '');
    root.append(h('div.page-head', h('div.page-title', h('h1', 'Sources'), sub)), body);
    let data = null;
    const draw = () => {
      if (!data) return;
      sub.textContent = `As of GSN ${fmt.int(data.gsn)} · refreshed every 2 s`;
      if (!data.sources.length) {
        replace(body, card(null, empty('No sources yet',
          'A source replicates the tables of a PostgreSQL publication through logical replication.',
          h('pre.code', "CREATE SOURCE pg FROM POSTGRES\n  CONNECTION 'host=... dbname=app user=...'\n  PUBLICATION 'app_pub';"))));
        return;
      }
      replace(body, ...data.sources.map(sourceCard));
    };
    const stop = poll(async () => {
      try {
        data = await api.sources();
        draw();
      } catch (e) {
        replace(body, errorBox(e.message));
      }
    }, 2000);
    return { unmount: stop };
  },
};
