// The active dataflow: operators laid out as a DAG, colored by operator
// family, annotated with arrangement sizes and live activity. Selecting an
// object highlights the operators it depends on; operators shared with other
// objects carry a "×n" badge. Clicking an operator opens the inspector.

import { api } from '../api.js';
import {
  h, s, replace, card, empty, errorBox, loading, kindLabel, kindColor, objectKindLabel, sampleTable,
  KIND_FAMILY, FAMILY_NAMES, segmented, go, href,
} from '../dom.js';
import * as fmt from '../format.js';
import { poll } from '../live.js';
import { DagView, fitText } from '../graph.js';

const NODE_W = 214;
const NODE_H = 66;
const BIG_GRAPH = 1200;

let direction = 'auto';

function activityLabel(rate) {
  if (!rate) return null;
  return `${fmt.rate(rate)} rows/s`;
}

export default {
  title: 'Dataflow',
  mount(root, params) {
    let df = null;
    let prev = null; // previous snapshot (for rates)
    let rates = new Map(); // node id -> {rows, steps, nanos} per second
    let selectedNode = null;
    let selectedObject = null; // object key "kind:name"
    let onlyObject = false;
    let nodeDetail = null;
    let view = null;
    let error = null;
    let centerPending = params[0] === 'node';
    let focusPending = params[0] === 'object';

    const sub = h('div.sub', 'Loading…');
    const objSelect = h('select.input', { 'aria-label': 'Highlight an object', onchange: (e) => selectObject(e.target.value || null) });
    let onlyTouched = false;
    const onlyBox = h('input', { type: 'checkbox', onchange: (e) => { onlyTouched = true; onlyObject = e.target.checked; drawGraph(true); } });
    const searchInput = h('input.input', {
      type: 'search', placeholder: 'Find node: id, kind or name', 'aria-label': 'Find node',
      onkeydown: (e) => { if (e.key === 'Enter') find(e.target.value); },
    });
    const toolbar = h('div.df-toolbar',
      objSelect,
      h('label.row.small', { style: { gap: '6px' } }, onlyBox, 'only its operators'),
      h('div.search', { style: { width: '230px' } }, searchInput),
      h('div.spacer'),
      segmented([{ value: 'auto', label: 'auto' }, { value: 'LR', label: '→ left to right' }, { value: 'TB', label: '↓ top down' }], direction, (v) => {
        direction = v;
        if (view) view.setDirection(v);
      }),
      h('button.btn.small', { type: 'button', onclick: () => view && view.fit() }, 'Fit'));
    const canvasBox = h('div', { style: { position: 'absolute', inset: '0' } });
    const legend = h('div.graph-legend');
    const zoom = h('div.graph-zoom',
      h('button', { type: 'button', 'aria-label': 'Zoom in', onclick: () => view && view.zoomBy(1.25) }, '+'),
      h('button', { type: 'button', 'aria-label': 'Zoom out', onclick: () => view && view.zoomBy(0.8) }, '−'),
      h('button', { type: 'button', 'aria-label': 'Fit to screen', onclick: () => view && view.fit() }, '⤢'));
    const canvasMsg = h('div');
    const canvas = h('section.card.df-canvas', canvasBox, canvasMsg, legend, zoom);
    const inspector = h('section.card.inspector');
    const layout = h('div.df-layout', canvas, inspector);
    root.append(h('div.page-head', h('div.page-title', h('h1', 'Dataflow'), sub)), toolbar, layout);

    for (const [slot, name] of Object.entries(FAMILY_NAMES)) {
      legend.appendChild(h('span.kind', { style: { '--kc': `var(--s${slot})` } }, name));
    }
    legend.appendChild(h('span.kind.small', { style: { '--kc': 'var(--surface-3)' } }, '×n shared'));

    function objKey(o) {
      return `${o.kind}:${o.name}`;
    }

    function objectIndex() {
      if (!df || !selectedObject) return -1;
      return df.objects.findIndex((o) => objKey(o) === selectedObject);
    }

    function applyParams(p) {
      if (p[0] === 'node' && p[1] !== undefined) {
        selectedNode = String(p[1]);
      } else if (p[0] === 'object' && p[1] !== undefined) {
        const name = p[1];
        const o = df ? df.objects.find((x) => x.name === name && x.kind !== 'index') || df.objects.find((x) => x.name === name) : null;
        selectedObject = o ? objKey(o) : `?:${name}`;
      }
    }

    function highlighted() {
      const oi = objectIndex();
      return oi >= 0 ? df.nodes.filter((n) => n.users.includes(oi)).map((n) => n.id) : [];
    }

    /** In a large dataflow an object's operators are scattered: show only
     * them, unless the user chose otherwise. */
    function autoOnly() {
      if (onlyTouched || !df) return;
      onlyObject = !!selectedObject && df.nodes.length > 80;
      onlyBox.checked = onlyObject;
    }

    function selectObject(key) {
      selectedObject = key;
      objSelect.value = key || '';
      const before = onlyObject;
      autoOnly();
      drawGraph(onlyObject || before !== onlyObject);
      // zoom to the object's operators (the whole graph when none is chosen)
      if (view && !onlyObject) {
        if (key) view.fitTo(highlighted());
        else view.fit(false);
      }
      drawInspector();
    }

    function find(q) {
      if (!df || !q) return;
      const t = q.trim().toLowerCase().replace(/^#/, '');
      const n = df.nodes.find((x) => String(x.id) === t)
        || df.nodes.find((x) => (x.label || '').toLowerCase() === t)
        || df.nodes.find((x) => (x.label || '').toLowerCase().includes(t))
        || df.nodes.find((x) => x.kind.toLowerCase() === t || x.kind.toLowerCase().includes(t));
      if (!n) {
        searchInput.setCustomValidity('No such node');
        searchInput.reportValidity();
        setTimeout(() => searchInput.setCustomValidity(''), 1500);
        return;
      }
      selectNode(String(n.id));
      if (view) view.centerOn(n.id);
    }

    function selectNode(id) {
      selectedNode = id;
      nodeDetail = null;
      history.replaceState(null, '', id === null ? href('dataflow') : href('dataflow', 'node', id));
      if (view) view.select(id);
      drawInspector();
      if (id !== null) loadNode();
    }

    function computeRates() {
      rates = new Map();
      if (!prev || !df) return;
      const dt = (df._t - prev._t) / 1000;
      if (dt <= 0) return;
      const before = new Map(prev.nodes.map((n) => [n.id, n.counters]));
      for (const n of df.nodes) {
        const b = before.get(n.id);
        if (!b) continue;
        const c = n.counters;
        rates.set(n.id, {
          rows: Math.max(0, c.out_rows - b.out_rows) / dt,
          inRows: Math.max(0, c.in_rows - b.in_rows) / dt,
          steps: Math.max(0, c.steps - b.steps) / dt,
          share: Math.max(0, c.nanos - b.nanos) / (dt * 1e9),
        });
      }
    }

    function renderNode(n, g, size) {
      const r = rates.get(n.id);
      g.style.setProperty('--kc', kindColor(n.kind));
      g.classList.toggle('shared', n.users.length > 1);
      g.classList.toggle('cold', !n.hydrated);
      const glow = s('rect', { class: 'glow', x: -3, y: -3, width: size.w + 6, height: size.h + 6, rx: 11 });
      if (r && r.rows > 0) glow.style.opacity = String(Math.min(0.55, 0.12 + Math.log10(1 + r.rows) / 6));
      g.appendChild(glow);
      g.appendChild(s('rect', { class: 'box', width: size.w, height: size.h, rx: 8 }));
      g.appendChild(s('rect', { class: 'stripe', width: 5, height: size.h - 14, x: 0, y: 7, rx: 2.5 }));
      const t1 = s('text', { class: 't1', x: 14, y: 20 });
      t1.appendChild(s('tspan', { text: n.kind + ' ' }));
      t1.appendChild(s('tspan', { class: 'id', text: '#' + n.id }));
      g.appendChild(t1);
      const line2 = n.label ? n.label + (n.kind === 'Table' ? '' : ` · ${n.summary}`) : n.summary;
      g.appendChild(s('text', { class: 't2', x: 14, y: 38, text: fitText(line2, 31) }));
      const t3 = s('text', { class: 't3', x: 14, y: 55 });
      const parts = [];
      if (n.arrangements.length) parts.push(`${fmt.compact(n.records)} rec in ${n.arrangements.length} arr`);
      else parts.push(`${n.schema.length} cols`);
      t3.appendChild(s('tspan', { text: parts.join(' · ') }));
      const act = r ? activityLabel(r.rows) : null;
      if (act) {
        t3.appendChild(s('tspan', { text: ' · ' }));
        t3.appendChild(s('tspan', { class: 'act', text: act }));
      }
      g.appendChild(t3);
      if (n.users.length > 1) {
        const label = `×${n.users.length}`;
        const w = 10 + label.length * 6.5;
        g.appendChild(s('rect', { class: 'badge-bg', x: size.w - w - 6, y: 6, width: w, height: 16, rx: 8 }));
        g.appendChild(s('text', { class: 'badge-tx', x: size.w - w / 2 - 6, y: 17.5, 'text-anchor': 'middle', text: label }));
      }
      const users = n.users.map((i) => df.objects[i]).filter(Boolean).map((o) => o.name);
      g.appendChild(s('title', {
        text: `#${n.id} ${n.kind}${n.label ? ' (' + n.label + ')' : ''}\n${n.summary}\nused by: ${users.join(', ') || 'nothing'}` +
          `\nrefs ${n.refs}${n.hydrated ? '' : ' · not hydrated'}`,
      }));
    }

    function visibleNodes() {
      const oi = objectIndex();
      if ((onlyObject || df.nodes.length > BIG_GRAPH) && oi >= 0) return df.nodes.filter((n) => n.users.includes(oi));
      if (df.nodes.length > BIG_GRAPH) return null;
      return df.nodes;
    }

    function drawGraph(fit = false) {
      if (!df) return;
      const nodes = visibleNodes();
      if (!nodes) {
        if (view) {
          view.destroy();
          view = null;
        }
        replace(canvasMsg, h('div.empty', { style: { paddingTop: '80px' } }, h('strong', `${fmt.int(df.nodes.length)} operators`), 'That is too many to lay out at once; pick an object above to see its operators.'));
        return;
      }
      replace(canvasMsg);
      if (!nodes.length) {
        if (view) {
          view.destroy();
          view = null;
        }
        replace(canvasMsg, h('div.empty', { style: { paddingTop: '80px' } },
          h('strong', 'No dataflow yet'), 'Tables and materialized views create operators; virtual views do not.'));
        return;
      }
      const ids = new Set(nodes.map((n) => n.id));
      const gnodes = nodes.map((n) => ({ ...n, width: NODE_W, height: NODE_H }));
      const edges = df.edges.filter(([a, b]) => ids.has(a) && ids.has(b)).map(([a, b]) => ({ from: a, to: b }));
      if (!view) {
        view = new DagView(canvasBox, {
          direction, ariaLabel: 'dataflow graph', renderNode, padBottom: 56,
          label: (n) => `${n.kind} node ${n.id}${n.label ? ', ' + n.label : ''}`,
          onSelect: (id) => selectNode(id),
        });
      }
      view.setGraph(gnodes, edges, { fit });
      const oi = objectIndex();
      view.setHighlight(oi >= 0 ? { nodes: new Set(df.nodes.filter((n) => n.users.includes(oi)).map((n) => String(n.id))), dim: true } : null);
      const active = new Set();
      for (const [id, r] of rates) if (r.rows > 0) active.add(String(id));
      view.setActiveNodes(active);
      view.select(selectedNode);
      if (centerPending && selectedNode !== null) {
        centerPending = false;
        view.centerOn(selectedNode);
      } else if (focusPending && objectIndex() >= 0) {
        focusPending = false;
        const before = onlyObject;
        autoOnly();
        if (before !== onlyObject) drawGraph(true);
        else if (!onlyObject) view.fitTo(highlighted());
      }
    }

    function drawObjectSelect() {
      const cur = selectedObject;
      replace(objSelect, h('option', { value: '' }, 'All objects'),
        ...df.objects.map((o) => h('option', { value: objKey(o) }, `${o.name} (${o.kind})`)));
      objSelect.value = cur && df.objects.some((o) => objKey(o) === cur) ? cur : '';
    }

    function drawSummary() {
      const nodes = df.nodes;
      const records = df.arrangements.reduce((a, x) => a + x.records, 0);
      const totalShare = [...rates.values()].reduce((a, r) => a + r.share, 0);
      sub.textContent = `${fmt.plural(nodes.length, 'operator')} · ${fmt.plural(df.arrangements.length, 'arrangement')} · ${fmt.compact(records)} records · GSN ${fmt.int(df.gsn)}` +
        (rates.size ? ` · operators busy ${fmt.pct(totalShare * 100, 1)}` : '') + (df.truncated ? ' · truncated' : '');
    }

    function inspectorSummary() {
      const counts = new Map();
      for (const n of df.nodes) counts.set(n.kind, (counts.get(n.kind) || 0) + 1);
      const byTime = [...df.nodes].map((n) => ({ n, r: rates.get(n.id) })).filter((x) => x.r && x.r.share > 0).sort((a, b) => b.r.share - a.r.share).slice(0, 6);
      const byRecords = [...df.nodes].filter((n) => n.records > 0).sort((a, b) => b.records - a.records).slice(0, 6);
      const byTotal = [...df.nodes].filter((n) => n.counters.nanos > 0).sort((a, b) => b.counters.nanos - a.counters.nanos).slice(0, 6);
      const nodeLink = (n, extra) => h('a.chip', { href: href('dataflow', 'node', n.id), onclick: (e) => { e.preventDefault(); selectNode(String(n.id)); view && view.centerOn(n.id); } },
        kindLabel(n.kind, `#${n.id} ${n.kind}`), n.label ? h('span.muted', fitText(n.label, 20)) : null, extra ? h('b', extra) : null);
      const oi = objectIndex();
      const obj = oi >= 0 ? df.objects[oi] : null;
      const parts = [];
      if (obj) {
        const own = df.nodes.filter((n) => n.users.includes(oi));
        const shared = own.filter((n) => n.users.length > 1);
        parts.push(h('div',
          h('h3', 'Selected object'),
          h('div.row', objectKindLabel(obj.kind), h('strong', obj.name)),
          h('div.small.text-2', { style: { marginTop: '6px' } }, `${fmt.plural(own.length, 'operator')}, ${shared.length} shared with other objects`),
          obj.id !== null ? h('div', { style: { marginTop: '6px' } }, h('a', { href: href('catalog', obj.name) }, 'Open in catalog')) : null));
      }
      parts.push(h('div', h('h3', 'Operators by kind'),
        h('div.chips', ...[...counts.entries()].sort((a, b) => b[1] - a[1]).map(([k, c]) => h('span.chip', { onclick: () => find(k) }, kindLabel(k), h('b', String(c)))))));
      parts.push(h('div', h('h3', 'Busiest now'), byTime.length
        ? h('div.chips', ...byTime.map((x) => nodeLink(x.n, fmt.pct(x.r.share * 100, 1))))
        : h('div.muted.small', 'No operator ran in the last interval')));
      parts.push(h('div', h('h3', 'Most time since start'), byTotal.length
        ? h('div.chips', ...byTotal.map((n) => nodeLink(n, fmt.nanos(n.counters.nanos))))
        : h('div.muted.small', 'No steps yet')));
      parts.push(h('div', h('h3', 'Largest state'), byRecords.length
        ? h('div.chips', ...byRecords.map((n) => nodeLink(n, fmt.compact(n.records))))
        : h('div.muted.small', 'No arrangements')));
      parts.push(h('div.small.muted', 'Click an operator for details. Drag or scroll to pan; pinch or Ctrl + scroll to zoom; + / − / 0 on the focused graph.'));
      replace(inspector, h('div.card-head', h('h2', 'Dataflow')), h('div.card-body', ...parts));
    }

    function drawInspector() {
      if (!df) return;
      if (selectedNode === null) {
        inspectorSummary();
        return;
      }
      const n = df.nodes.find((x) => String(x.id) === selectedNode);
      const close = h('button.icon-btn.close', { type: 'button', 'aria-label': 'Close', onclick: () => selectNode(null) }, '✕');
      if (!n) {
        replace(inspector, h('div.card-head', h('h2', `Node #${selectedNode}`), close), h('div.card-body', empty('This operator no longer exists', 'It was released when its last user was dropped.')));
        return;
      }
      const d = nodeDetail && String(nodeDetail.node.id) === selectedNode ? nodeDetail : null;
      const r = rates.get(n.id);
      const c = n.counters;
      const counter = (l, v) => h('div.c', h('div.l', l), h('div.v', v));
      const users = n.users.map((i) => df.objects[i]).filter(Boolean);
      const nodeChip = (id) => {
        const x = df.nodes.find((y) => y.id === id);
        return h('a.chip', { href: href('dataflow', 'node', id), onclick: (e) => { e.preventDefault(); selectNode(String(id)); view && view.centerOn(id); } },
          x ? kindLabel(x.kind, `#${id} ${x.kind}`) : `#${id}`, x && x.label ? h('span.muted', fitText(x.label, 18)) : null);
      };
      const parts = [
        h('div', h('div.text-2', n.summary), n.label ? h('div', { style: { marginTop: '4px' } }, h('strong', n.label)) : null),
        h('div.counter-grid',
          counter('Rows out now', r ? `${fmt.rate(r.rows)}/s` : '–'),
          counter('Busy now', r ? fmt.pct(r.share * 100, 1) : '–'),
          counter('Steps', fmt.int(c.steps)),
          counter('Time', fmt.nanos(c.nanos)),
          counter('Rows in', fmt.compact(c.in_rows)),
          counter('Rows out', fmt.compact(c.out_rows)),
          counter('Avg per step', c.steps ? fmt.nanos(c.nanos / c.steps) : '–'),
          counter('Refs', `${n.refs}${n.hydrated ? '' : ' (not hydrated)'}`)),
        h('div', h('h3', `Used by (${users.length})`), users.length
          ? h('div.chips', ...users.map((o) => h('span.chip', { onclick: () => selectObject(objKey(o)), title: 'Highlight its operators' }, objectKindLabel(o.kind), o.name)))
          : h('div.muted.small', 'Nothing (pinned by the engine)')),
      ];
      if (d) {
        parts.push(h('div', h('h3', 'Operator'), h('div.det-list', ...d.details.map((x) => h('div.det', h('div.l', x.label), h('div.v', x.value))))));
        const cols = n.schema.map((t, i) => ({ name: d.columns[i] || `#${i}`, type: t }));
        parts.push(h('div', h('h3', `Output columns (${cols.length})`), h('div.det-list', ...cols.map((x, i) => h('div.det', h('div.l', `#${i}`), h('div.v', `${x.name !== '#' + i ? x.name + '  ' : ''}${x.type}`))))));
      } else {
        parts.push(loading('Loading operator details…'));
      }
      parts.push(h('div', h('h3', 'Inputs'), n.inputs.length ? h('div.chips', ...[...new Set(n.inputs)].map(nodeChip)) : h('div.muted.small', 'none (a base table)')));
      const consumers = df.edges.filter(([a]) => a === n.id).map(([, b]) => b);
      parts.push(h('div', h('h3', 'Consumers'), consumers.length ? h('div.chips', ...[...new Set(consumers)].map(nodeChip)) : h('div.muted.small', 'none')));
      if (d) {
        if (d.arrangements.length) {
          parts.push(h('div', h('h3', `Arrangements (${d.arrangements.length})`), h('div.stack', { style: { gap: '10px' } }, ...d.arrangements.map((a) => h('div.arr-block',
            h('div.arr-head',
              h('strong', `arr#${a.id}`), h('span.badge', a.role), h('span.mono', `key (${a.key.join(', ')})`),
              h('span', `${fmt.int(a.records)} records`), h('span.muted', `${fmt.bytes(a.resident_bytes)} in memory`),
              h('span.muted', `refs ${a.refs}`), a.readers.length ? h('span.muted', `read by ${a.readers.map((x) => '#' + x).join(', ')}`) : null,
              a.hydrated ? null : h('span.badge.warn', h('span.dot'), 'not hydrated')),
            a.sample.rows.length
              ? sampleTable(a.columns, a.sample.rows)
              : h('div.small.muted', { style: { padding: '8px 10px' } }, a.records ? 'No live rows among the first entries examined' : 'Empty'),
            a.sample.truncated ? h('div.small.muted', { style: { padding: '6px 10px' } }, `first ${a.sample.rows.length} of ${fmt.int(a.records)} rows in key order`) : null)))));
        } else {
          parts.push(h('div', h('h3', 'Arrangements'), h('div.muted.small', 'None: this operator keeps no state of its own')));
        }
      }
      replace(inspector,
        h('div.card-head', kindLabel(n.kind, n.kind), h('h2', `#${n.id}`), close),
        h('div.card-body', ...parts));
    }

    async function load() {
      try {
        const next = await api.dataflow();
        next._t = Date.now();
        prev = df;
        df = next;
        error = null;
        if (selectedObject && selectedObject.startsWith('?:')) applyParams(['object', selectedObject.slice(2)]);
        computeRates();
        drawObjectSelect();
        drawSummary();
        drawGraph();
        drawInspector();
      } catch (e) {
        error = e;
        replace(canvasMsg, errorBox(e.message));
      }
    }

    let nodeTimer = null;
    async function loadNode() {
      const id = selectedNode;
      if (id === null) return;
      try {
        const d = await api.node(id, 20);
        if (id !== selectedNode) return;
        nodeDetail = d;
        drawInspector();
      } catch (e) {
        if (id === selectedNode && e.status !== 404) replace(inspector, h('div.card-body', errorBox(e.message)));
        else if (id === selectedNode) drawInspector();
      }
      clearTimeout(nodeTimer);
      nodeTimer = setTimeout(() => {
        if (document.visibilityState === 'visible') loadNode();
      }, 4000);
    }

    applyParams(params);
    replace(inspector, h('div.card-body', loading()));
    const stop = poll(load, 2000);
    if (selectedNode !== null) loadNode();

    return {
      update(p) {
        applyParams(p);
        if (p[0] === 'object') {
          objSelect.value = selectedObject || '';
          const before = onlyObject;
          autoOnly();
          drawGraph(onlyObject || before !== onlyObject);
          if (view && !onlyObject) view.fitTo(highlighted());
          drawInspector();
        } else if (p[0] === 'node') {
          selectNode(selectedNode);
          if (view && selectedNode !== null) view.centerOn(selectedNode);
        }
      },
      unmount() {
        stop();
        clearTimeout(nodeTimer);
        if (view) view.destroy();
      },
    };
  },
};
