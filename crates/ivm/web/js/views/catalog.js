// Catalog browser: objects by kind, an object's definition, columns,
// lineage, plans and a sample of its rows, and the catalog's dependency graph.

import { api } from '../api.js';
import {
  h, s, replace, card, tabs, empty, errorBox, loading, dataTable, sampleTable, sqlCode, statusBadge,
  objectKindLabel, objectColor, go, href, copyText,
} from '../dom.js';
import * as fmt from '../format.js';
import { live, poll } from '../live.js';
import { DagView, fitText } from '../graph.js';

const KIND_ORDER = ['source', 'table', 'materialized view', 'view', 'index', 'sink'];
const KIND_TITLE = { source: 'Sources', table: 'Tables', 'materialized view': 'Materialized views', view: 'Views', index: 'Indexes', sink: 'Sinks' };

function objMeta(o) {
  if (o.rows !== null && o.rows !== undefined) return fmt.compact(o.rows) + ' rows';
  if (o.kind === 'view') return 'virtual';
  return '';
}

/** Lineage graph of catalog objects. */
function lineageGraph(container, objects, focusId) {
  const byId = new Map(objects.map((o) => [o.id, o]));
  let ids;
  if (focusId === null) {
    ids = new Set(objects.map((o) => o.id));
  } else {
    ids = new Set([focusId]);
    const walk = (id, key) => {
      const o = byId.get(id);
      if (!o) return;
      for (const d of o[key]) {
        if (ids.has(d)) continue;
        ids.add(d);
        walk(d, key);
      }
    };
    walk(focusId, 'depends_on');
    walk(focusId, 'dependents');
  }
  const nodes = [...ids].filter((id) => byId.has(id)).map((id) => ({ id, width: 190, height: 50, o: byId.get(id) }));
  const edges = [];
  for (const n of nodes) for (const d of n.o.depends_on) if (ids.has(d) && byId.has(d)) edges.push({ from: d, to: n.id });
  const view = new DagView(container, {
    direction: 'LR', nodesep: 16, ranksep: 56, ariaLabel: 'catalog dependency graph',
    label: (n) => `${n.o.kind} ${n.o.name}`,
    renderNode: (n, g, size) => {
      g.style.setProperty('--kc', objectColor(n.o.kind));
      g.appendChild(s('rect', { class: 'box', width: size.w, height: size.h, rx: 8 }));
      g.appendChild(s('rect', { class: 'stripe', width: 4, height: size.h - 12, x: 0, y: 6, rx: 2 }));
      g.appendChild(s('text', { class: 't1', x: 14, y: 21, text: fitText(n.o.name, 24) }));
      g.appendChild(s('text', { class: 't3', x: 14, y: 38, text: fitText(`${n.o.kind}${objMeta(n.o) ? ' · ' + objMeta(n.o) : ''}`, 30) }));
      g.appendChild(s('title', { text: `${n.o.kind} ${n.o.name}` }));
    },
    onSelect: (id) => {
      if (id !== null) {
        const o = byId.get(Number(id));
        if (o) go('catalog', o.name);
      }
    },
  });
  view.setGraph(nodes, edges, { fit: true });
  if (focusId !== null) view.select(focusId);
  return view;
}

export default {
  title: 'Catalog',
  mount(root, params) {
    let catalog = null;
    let selected = params[0] || null;
    let tab = params[1] || 'overview';
    let detail = null;
    let detailFor = null;
    let filter = '';
    let graph = null;

    const listEl = h('div.obj-list');
    const search = h('input.input', { type: 'search', placeholder: 'Filter objects', 'aria-label': 'Filter objects', oninput: (e) => { filter = e.target.value.toLowerCase(); drawList(); } });
    const left = h('section.card', h('div.card-body', { style: { paddingBottom: '8px' } }, h('div.search', search)), listEl);
    const right = h('div.stack');
    const sub = h('div.sub');
    root.append(h('div.page-head', h('div.page-title', h('h1', 'Catalog'), sub)), h('div.split', left, right));

    function drawList() {
      if (!catalog) return;
      const groups = new Map(KIND_ORDER.map((k) => [k, []]));
      for (const o of catalog.objects) {
        if (filter && !o.name.toLowerCase().includes(filter)) continue;
        (groups.get(o.kind) || groups.set(o.kind, []).get(o.kind)).push(o);
      }
      const els = [];
      for (const [kind, objs] of groups) {
        if (!objs.length) continue;
        objs.sort((a, b) => a.name.localeCompare(b.name));
        els.push(h('div.obj-group',
          h('h3', h('span', KIND_TITLE[kind] || kind), h('span.muted', String(objs.length))),
          ...objs.map((o) => h('a.obj-item' + (o.name === selected ? '.sel' : ''), { href: href('catalog', o.name), style: { '--kc': objectColor(o.kind) } },
            h('span.kind', { style: { '--kc': objectColor(o.kind) } }, ''),
            h('span.name', o.name),
            o.errors ? h('span.badge.crit', { title: 'error records' }, h('span.dot'), String(o.errors)) : null,
            o.status && /^error/.test(o.status) ? h('span.badge.crit', h('span.dot'), 'error') : null,
            h('span.meta', objMeta(o))))));
      }
      replace(listEl, els.length ? els : h('div.empty', filter ? 'No matching objects' : 'The catalog is empty'));
    }

    function overviewTab(o, d) {
      const facts = h('dl.kv',
        h('dt', 'Kind'), h('dd', objectKindLabel(o.kind)),
        h('dt', 'Object id'), h('dd', String(o.id)),
        h('dt', 'Created at'), h('dd', `GSN ${fmt.int(o.created_gsn)}`),
        o.rows !== null ? [h('dt', 'Rows'), h('dd', fmt.int(o.rows))] : null,
        o.detail ? [h('dt', 'Details'), h('dd', o.detail)] : null,
        o.status ? [h('dt', 'Status'), h('dd', statusBadge(o.status))] : null,
        o.node !== null ? [h('dt', 'Dataflow'), h('dd', h('a', { href: href('dataflow', 'node', o.node) }, `node #${o.node}`),
          o.arrangement !== null ? h('span.muted', ` · arrangement #${o.arrangement}`) : null)] : null,
        o.errors ? [h('dt', 'Errors'), h('dd', h('span.badge.crit', h('span.dot'), `${o.errors} error records`))] : null);
      const chipsFor = (ids) => {
        const byId = new Map(catalog.objects.map((x) => [x.id, x]));
        const list = ids.map((i) => byId.get(i)).filter(Boolean);
        return list.length
          ? h('div.chips', ...list.map((x) => h('a.chip', { href: href('catalog', x.name) }, objectKindLabel(x.kind), x.name)))
          : h('span.muted', 'none');
      };
      const parts = [
        card('Definition', h('div.stack', sqlCode(o.sql), h('div.row', h('button.btn.small', { type: 'button', onclick: () => copyText(o.sql) }, 'Copy SQL')))),
        h('div.grid.cols-2', card('Facts', facts), card('Dependencies', h('div.stack',
          h('div', h('h3', 'Depends on'), chipsFor(o.depends_on)),
          h('div', h('h3', 'Used by'), chipsFor(o.dependents))))),
      ];
      if (o.columns.length) {
        parts.push(card(`Columns (${o.columns.length})`, dataTable([
          { key: 'name', label: 'Name', render: (c) => h('span.mono', c.name) },
          { key: 'type', label: 'Type', render: (c) => h('span.mono', c.type) },
          { key: 'nullable', label: 'Nullable', render: (c) => (c.nullable ? 'yes' : 'not null') },
          { key: 'key', label: 'Key', render: (c) => (c.key ? 'primary key' : '') },
        ], o.columns).el, { flush: true }));
      }
      if (d && d.problems.length) parts.push(errorBox(d.problems.join('\n')));
      return parts;
    }

    function drawDetail() {
      if (graph) {
        graph.destroy();
        graph = null;
      }
      if (!catalog) return;
      if (!selected) {
        if (!catalog.objects.length) {
          replace(right, card(null, empty('The catalog is empty', 'Create a table, a source or a view to get started.')));
          return;
        }
        const box = h('div.lineage', { style: { height: 'calc(100vh - 240px)', minHeight: '360px', position: 'relative' } });
        replace(right, card('Dependency graph', box, {
          flush: true,
          actions: h('span.muted.small', 'Sources feed tables, tables feed views; select an object for details'),
        }));
        if (catalog.objects.length > 600) {
          replace(box, h('div.empty', 'Too many objects to draw; pick one from the list.'));
          return;
        }
        graph = lineageGraph(box, catalog.objects, null);
        return;
      }
      const o = catalog.objects.find((x) => x.name === selected);
      if (!o) {
        replace(right, card(null, empty(`No object named “${selected}”`, 'It may have been dropped.')));
        return;
      }
      const d = detailFor === selected ? detail : null;
      const items = [{ id: 'overview', label: 'Overview' }, { id: 'lineage', label: 'Lineage' }];
      if (o.kind === 'view' || o.kind === 'materialized view') items.push({ id: 'plan', label: 'Plan' });
      if (o.kind === 'materialized view') {
        items.push({ id: 'dataflow', label: 'Dataflow' }, { id: 'maintenance', label: 'Maintenance' });
      }
      if (o.kind === 'table' || o.kind === 'materialized view') items.push({ id: 'sample', label: 'Rows', count: o.rows !== null ? fmt.compact(o.rows) : null });
      if (o.kind === 'materialized view') items.push({ id: 'errors', label: 'Errors', count: o.errors || 0 });
      if (!items.some((i) => i.id === tab)) tab = 'overview';
      const body = h('div.stack');
      const t = tabs(items, tab, (id) => {
        tab = id;
        history.replaceState(null, '', href('catalog', selected, id));
        drawTab();
      });
      function drawTab() {
        if (graph) {
          graph.destroy();
          graph = null;
        }
        const needs = ['plan', 'dataflow', 'maintenance', 'sample', 'errors'].includes(tab);
        if (needs && !d) {
          replace(body, loading());
          return;
        }
        switch (tab) {
          case 'overview':
            replace(body, ...overviewTab(o, d));
            break;
          case 'lineage': {
            const box = h('div.lineage', { style: { position: 'relative' } });
            replace(body, card('Lineage', box, { flush: true, actions: h('span.muted.small', 'Everything this object depends on and everything built on it') }));
            graph = lineageGraph(box, catalog.objects, o.id);
            break;
          }
          case 'plan':
            replace(body, card('Logical plan (EXPLAIN)', d.plan ? h('pre.code', d.plan.join('\n')) : h('div.muted', 'No plan available')));
            break;
          case 'dataflow':
            replace(body, card('Dataflow (EXPLAIN DATAFLOW)', h('div.stack',
              d.dataflow ? h('pre.code', d.dataflow.join('\n')) : h('div.muted', 'Not available'),
              h('div.row', h('a.btn.small', { href: href('dataflow', 'object', o.name) }, 'Show in the dataflow graph'),
                h('span.muted.small', `${d.nodes.length} operators`)))));
            break;
          case 'maintenance':
            replace(body, card('Expected maintenance work (EXPLAIN MAINTENANCE)', d.maintenance && d.maintenance.length
              ? dataTable([
                { key: 'source', label: 'Source table' },
                { key: 'delta_rows_per_tick', label: 'Delta rows / step', num: true, sort: (r) => Number(r.delta_rows_per_tick) },
                { key: 'work_per_delta', label: 'Work per delta', num: true, sort: (r) => Number(r.work_per_delta) },
                { key: 'path', label: 'Path', render: (r) => h('span.mono', r.path) },
              ], d.maintenance).el
              : h('div.muted', 'No source tables'), { flush: !!(d.maintenance && d.maintenance.length) }));
            break;
          case 'sample': {
            const smp = d.sample;
            if (!smp) {
              replace(body, card(null, h('div.muted', 'No rows to show')));
              break;
            }
            const note = smp.truncated ? `First ${fmt.int(smp.rows.length)} rows in index order` : `${fmt.int(smp.rows.length)} rows`;
            replace(body, card('Rows', h('div.stack', h('div.muted.small', note),
              smp.rows.length ? sampleTable(smp.columns, smp.rows) : h('div.muted', 'Empty'),
              h('div.row', h('a.btn.small', { href: href('console', `SELECT * FROM ${/^[a-z_][a-z0-9_]*$/.test(o.name) ? o.name : '"' + o.name.replace(/"/g, '""') + '"'} LIMIT 100`) }, 'Query in the console')))));
            break;
          }
          case 'errors':
            replace(body, card('Error records', d.errors.length
              ? dataTable([
                { key: 'code', label: 'SQLSTATE', render: (r) => h('span.mono', r.code) },
                { key: 'message', label: 'Message' },
                { key: 'count', label: 'Rows', num: true },
              ], d.errors).el
              : empty('No errors', 'Reads of this view succeed.'), { flush: d.errors.length > 0 }));
            break;
          default:
            replace(body);
        }
      }
      replace(right,
        h('section.card',
          h('div.obj-head', h('div', { style: { flex: '1', minWidth: '0' } }, h('h1', o.name)), objectKindLabel(o.kind),
            o.status ? statusBadge(o.status) : null),
          h('div.obj-facts',
            h('span', 'id ', h('b', String(o.id))),
            h('span', 'created at GSN ', h('b', fmt.int(o.created_gsn))),
            o.rows !== null ? h('span', h('b', fmt.int(o.rows)), ' rows') : null,
            o.errors ? h('span', h('b', String(o.errors)), ' error records') : null),
          h('div', { style: { padding: '0 8px' } }, t.el)),
        body);
      drawTab();
    }

    let catalogJson = '';
    let detailJson = '';
    let structure = '';
    // graphs are only rebuilt when objects or dependencies change, so live
    // row counts do not reset their pan and zoom
    const showsGraph = () => !selected || tab === 'lineage';
    const structureOf = (objs) => objs.map((o) => `${o.id}:${o.name}:${o.kind}:${o.depends_on.join(',')}`).join('|');
    async function loadCatalog() {
      try {
        const next = await api.catalog();
        const json = JSON.stringify(next.objects);
        if (catalog && json === catalogJson) return;
        catalogJson = json;
        catalog = next;
        sub.textContent = `${fmt.plural(catalog.objects.length, 'object')} · as of GSN ${fmt.int(catalog.gsn)}`;
        drawList();
        const st = structureOf(catalog.objects);
        if (showsGraph() && st === structure && graph) return;
        structure = st;
        drawDetail();
      } catch (e) {
        replace(right, errorBox(e.message));
      }
    }

    async function loadDetail() {
      if (!selected) return;
      const name = selected;
      try {
        const d = await api.object(name, 100);
        if (name !== selected) return;
        const json = JSON.stringify({ ...d, gsn: 0 });
        if (detailFor === name && json === detailJson) return;
        detailJson = json;
        const first = detailFor !== name;
        detail = d;
        detailFor = name;
        if (!first && tab === 'lineage' && graph) return;
        // object facts may be fresher than the list
        if (catalog) {
          const i = catalog.objects.findIndex((x) => x.name === name);
          if (i >= 0) catalog.objects[i] = d.object;
        }
        drawDetail();
      } catch (e) {
        if (name === selected && e.status !== 404) replace(right, errorBox(e.message));
      }
    }

    const stopList = poll(loadCatalog, 15000);
    const offTopo = live.on('topology', () => loadCatalog().then(loadDetail));
    let stopDetail = selected ? poll(loadDetail, 10000) : null;

    return {
      update(p) {
        const next = p[0] || null;
        tab = p[1] || (next === selected ? tab : 'overview');
        if (next !== selected) {
          selected = next;
          detail = null;
          detailFor = null;
          if (stopDetail) stopDetail();
          stopDetail = selected ? poll(loadDetail, 10000) : null;
        }
        drawList();
        drawDetail();
      },
      unmount() {
        stopList();
        offTopo();
        if (stopDetail) stopDetail();
        if (graph) graph.destroy();
      },
    };
  },
};
