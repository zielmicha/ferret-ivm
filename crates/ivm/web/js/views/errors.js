// Errors: error records held by views (reads of them fail until the
// offending rows are gone), sources or sinks in an error state, and broken
// (unmaintained) views and indexes.

import { api } from '../api.js';
import { h, replace, card, errorBox, dataTable, empty, statusBadge, href } from '../dom.js';
import * as fmt from '../format.js';
import { poll } from '../live.js';

export default {
  title: 'Errors',
  mount(root) {
    const sub = h('div.sub', 'Loading…');
    const body = h('div.stack');
    root.append(h('div.page-head', h('div.page-title', h('h1', 'Errors'), sub)), body);
    const stop = poll(async () => {
      let d;
      try {
        d = await api.errors();
      } catch (e) {
        replace(body, errorBox(e.message));
        return;
      }
      const broken = d.broken || [];
      const n = d.views.length + d.sources.length + d.sinks.length + broken.length;
      sub.textContent = `As of GSN ${fmt.int(d.gsn)} · ${n ? fmt.plural(n, 'problem') : 'no problems'}`;
      if (!n) {
        replace(body, card(null, empty('No errors', 'Every view reads cleanly and every source and sink is healthy.')));
        return;
      }
      const parts = [];
      if (d.views.length) {
        parts.push(card('View error records', h('div.stack',
          h('div.small.text-2', { style: { padding: '0 16px' } }, 'Errors are data: an expression error in a view becomes an error record, and reads of the view fail until the offending rows are gone.'),
          dataTable([
            { key: 'name', label: 'View', render: (r) => h('a', { href: href('catalog', r.name, 'errors') }, r.name) },
            { key: 'code', label: 'SQLSTATE', render: (r) => h('span.mono', r.code) },
            { key: 'message', label: 'Message' },
            { key: 'count', label: 'Rows', num: true, render: (r) => fmt.int(r.count) },
          ], d.views, { sortKey: 'count', sortDir: -1 }).el), { flush: true }));
      }
      const statusTable = (rows, what) => dataTable([
        { key: 'name', label: what, render: (r) => h('a', { href: href(what === 'Source' ? 'sources' : 'activity') }, r.name) },
        { key: 'status', label: 'Status', render: (r) => statusBadge(r.status) },
      ], rows).el;
      if (d.sources.length) parts.push(card('Sources', statusTable(d.sources, 'Source'), { flush: true }));
      if (d.sinks.length) parts.push(card('Sinks', statusTable(d.sinks, 'Sink'), { flush: true }));
      if (broken.length) {
        parts.push(card('Broken views and indexes', dataTable([
          { key: 'name', label: 'Object', render: (r) => h('a', { href: href('catalog', r.name) }, r.name) },
          { key: 'status', label: 'Status', render: (r) => statusBadge(r.status) },
        ], broken).el, { flush: true }));
      }
      replace(body, ...parts);
    }, 5000);
    return { unmount: stop };
  },
};
