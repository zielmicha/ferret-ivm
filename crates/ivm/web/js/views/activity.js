// Activity: subscriptions, reads waiting for a GSN / LSN / snapshot, sinks
// and the engine's request queue.

import { api } from '../api.js';
import { h, replace, card, errorBox, dataTable, statusBadge, badge, tile, href } from '../dom.js';
import * as fmt from '../format.js';
import { poll } from '../live.js';

export default {
  title: 'Activity',
  mount(root) {
    const sub = h('div.sub', 'Loading…');
    const tilesEl = h('div.tiles');
    const body = h('div.stack');
    root.append(h('div.page-head', h('div.page-title', h('h1', 'Activity'), sub)), tilesEl, body);

    const stop = poll(async () => {
      let d;
      try {
        d = await api.sessions();
      } catch (e) {
        replace(body, errorBox(e.message));
        return;
      }
      sub.textContent = `As of GSN ${fmt.int(d.gsn)} · refreshed every 2 s`;
      replace(tilesEl,
        tile('Subscriptions', fmt.int(d.subscriptions.length), `${d.subscriptions.filter((x) => x.target === null).length} on ad hoc queries`),
        tile('Waiting reads', fmt.int(d.parked.length), 'parked until ready'),
        tile('Pending requests', fmt.int(d.pending_requests), 'awaiting their log record'),
        tile('DML queue', fmt.int(d.dml_queued), d.dml_inflight ? 'one statement in flight' : 'idle'),
        tile('Sinks', fmt.int(d.sinks.length), `${d.sinks.filter((x) => /^error/.test(x.status)).length} failing`));
      const subs = d.subscriptions.length
        ? dataTable([
          { key: 'node', label: 'Node', num: true, render: (r) => h('a', { href: href('dataflow', 'node', r.node) }, `#${r.node}`) },
          { key: 'target', label: 'Subscribed to', render: (r) => (r.target ? h('a', { href: href('catalog', r.target) }, r.target) : h('span.muted', 'ad hoc query (its own operators)')) },
          { key: 'progress', label: 'Progress messages', render: (r) => (r.progress ? 'yes' : 'no') },
          { key: 'cancelled', label: 'State', render: (r) => (r.cancelled ? badge('closing') : badge('streaming', 'good')) },
        ], d.subscriptions).el
        : h('div.muted', { style: { padding: '0 16px 16px' } }, 'No SUBSCRIBE sessions.');
      const parked = d.parked.length
        ? dataTable([
          { key: 'wait', label: 'Waiting for', render: (r) => badge(r.wait === 'gsn' ? `GSN ${fmt.int(r.gsn)}` : r.wait === 'lsn' ? 'source LSN' : 'initial snapshot', 'info') },
          { key: 'sql', label: 'Statement', render: (r) => h('span.mono', r.sql) },
        ], d.parked).el
        : h('div.muted', { style: { padding: '0 16px 16px' } }, 'No reads are waiting.');
      const sinks = d.sinks.length
        ? dataTable([
          { key: 'name', label: 'Sink' },
          { key: 'from', label: 'From', render: (r) => h('a', { href: href('catalog', r.from) }, r.from) },
          { key: 'table', label: 'Target table', render: (r) => h('span.mono', r.table) },
          { key: 'status', label: 'Status', render: (r) => statusBadge(r.status) },
          { key: 'committed_gsn', label: 'Committed GSN', num: true, render: (r) => fmt.int(r.committed_gsn) },
          { key: 'lag_gsns', label: 'Behind', num: true, render: (r) => (r.lag_gsns === null ? '–' : `${fmt.int(r.lag_gsns)} GSN`) },
        ], d.sinks).el
        : h('div.muted', { style: { padding: '0 16px 16px' } }, 'No sinks. CREATE SINK writes a view back to PostgreSQL.');
      replace(body,
        card('Subscriptions', subs, { flush: true }),
        card('Waiting reads', parked, { flush: true }),
        card('Sinks', sinks, { flush: true }));
    }, 2000);
    return { unmount: stop };
  },
};
