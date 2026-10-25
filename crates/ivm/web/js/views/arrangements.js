// Arrangements: every index and operator state, sortable by size.

import { api } from '../api.js';
import { h, replace, card, errorBox, dataTable, kindLabel, badge, go, empty } from '../dom.js';
import * as fmt from '../format.js';
import { poll } from '../live.js';

export default {
  title: 'Arrangements',
  mount(root) {
    const sub = h('div.sub', 'Loading…');
    let filter = '';
    let rows = [];
    let table = null;
    const search = h('input.input', {
      type: 'search', placeholder: 'Filter by operator, object or key', 'aria-label': 'Filter arrangements',
      oninput: (e) => { filter = e.target.value.toLowerCase(); draw(); },
    });
    const body = h('div');
    const totals = h('div.tiles');
    root.append(h('div.page-head', h('div.page-title', h('h1', 'Arrangements'), sub), h('div.grow'), h('div.search', { style: { width: '280px' } }, search)), totals, body);

    function draw() {
      const list = filter
        ? rows.filter((r) => `${r.id} ${r.kind} ${r.label} ${r.key} ${r.usersText} ${r.role}`.toLowerCase().includes(filter))
        : rows;
      if (!table) {
        table = dataTable([
          { key: 'id', label: 'Id', num: true, render: (r) => `#${r.id}` },
          { key: 'kind', label: 'Owner', render: (r) => h('span.row', { style: { gap: '6px', flexWrap: 'nowrap' } }, kindLabel(r.kind, `#${r.node} ${r.kind}`), r.label ? h('span.muted', r.label) : null), sort: (r) => r.node },
          { key: 'role', label: 'Role', render: (r) => badge(r.role) },
          { key: 'key', label: 'Key', render: (r) => h('span.mono', r.key) },
          { key: 'records', label: 'Records', num: true, render: (r) => fmt.int(r.records) },
          { key: 'resident_bytes', label: 'In memory', num: true, render: (r) => fmt.bytes(r.resident_bytes), title: 'Bytes of this arrangement\'s tree nodes in the buffer pool' },
          { key: 'resident_nodes', label: 'Pages', num: true, render: (r) => fmt.int(r.resident_nodes), title: 'Tree nodes in the buffer pool' },
          { key: 'refs', label: 'Refs', num: true },
          { key: 'readers', label: 'Read by', render: (r) => (r.readers.length ? r.readers.map((x) => '#' + x).join(', ') : h('span.muted', '—')), sort: (r) => r.readers.length },
          { key: 'usersText', label: 'Used by', render: (r) => (r.usersText ? h('span', r.usersText) : h('span.muted', '—')) },
          { key: 'hydrated', label: 'State', render: (r) => (r.hydrated ? badge('ready', 'good') : badge('hydrating', 'warn')), sort: (r) => (r.hydrated ? 1 : 0) },
        ], list, { sortKey: 'records', sortDir: -1, onRowClick: (r) => go('dataflow', 'node', r.node) });
        replace(body, card(null, table.el, { flush: true }));
      } else {
        table.setRows(list);
      }
      if (!rows.length) replace(body, card(null, empty('No arrangements', 'Tables, materialized views and indexes create arrangements.')));
    }

    const stop = poll(async () => {
      try {
        const df = await api.dataflow();
        const byNode = new Map(df.nodes.map((n) => [n.id, n]));
        rows = df.arrangements.map((a) => {
          const n = byNode.get(a.node);
          const users = n ? [...new Set(n.users.map((i) => df.objects[i]).filter(Boolean).map((o) => o.name))] : [];
          return { ...a, kind: n ? n.kind : '?', label: n && n.label ? n.label : '', key: a.key.join(', '), usersText: users.join(', ') };
        });
        const records = rows.reduce((x, r) => x + r.records, 0);
        const mem = rows.reduce((x, r) => x + r.resident_bytes, 0);
        sub.textContent = `As of GSN ${fmt.int(df.gsn)} · click a row to open its operator`;
        replace(totals,
          h('div.tile', h('div.tile-label', 'Arrangements'), h('div.tile-value', fmt.int(rows.length)), h('div.tile-sub', `${rows.filter((r) => r.role === 'index').length} indexes, ${rows.filter((r) => r.role === 'state').length} operator state`)),
          h('div.tile', h('div.tile-label', 'Records'), h('div.tile-value', fmt.compact(records)), h('div.tile-sub', 'rows counting multiplicity')),
          h('div.tile', h('div.tile-label', 'In memory'), h('div.tile-value', fmt.bytes(mem)), h('div.tile-sub', 'resident tree nodes')),
          h('div.tile', h('div.tile-label', 'Shared'), h('div.tile-value', fmt.int(rows.filter((r) => r.refs > 1).length)), h('div.tile-sub', 'arrangements with several references')));
        draw();
      } catch (e) {
        replace(body, errorBox(e.message));
        table = null;
      }
    }, 5000);
    return { unmount: stop };
  },
};
