// Layered DAG view: dagre (vendored) computes the layout, nodes and edges are
// SVG, and the viewport pans and zooms with mouse, wheel, keyboard and touch.
// Node contents are drawn by the caller, so the dataflow graph and the
// catalog lineage share this code.

import { s, clear } from './dom.js';

const MIN_K = 0.08;
const MAX_K = 2.5;
let viewSeq = 0;

function edgePath(points) {
  if (!points.length) return '';
  let d = `M${points[0].x.toFixed(1)},${points[0].y.toFixed(1)}`;
  if (points.length === 2) return d + ` L${points[1].x.toFixed(1)},${points[1].y.toFixed(1)}`;
  for (let i = 1; i < points.length - 1; i++) {
    const p = points[i];
    const n = points[i + 1];
    const mx = i === points.length - 2 ? n.x : (p.x + n.x) / 2;
    const my = i === points.length - 2 ? n.y : (p.y + n.y) / 2;
    d += ` Q${p.x.toFixed(1)},${p.y.toFixed(1)} ${mx.toFixed(1)},${my.toFixed(1)}`;
  }
  return d;
}

export class DagView {
  /**
   * opts: {
   *   renderNode(node, g, size) - draw a node into the SVG group g,
   *   onSelect(id|null), direction: 'LR'|'TB', nodesep, ranksep,
   *   label(node) - accessible name
   * }
   */
  constructor(container, opts) {
    this.opts = { direction: 'LR', nodesep: 22, ranksep: 64, ...opts };
    this.container = container;
    this.uid = ++viewSeq;
    this.t = { x: 0, y: 0, k: 1 };
    this.nodes = new Map();
    this.edges = [];
    this.layoutKey = '';
    this.size = { w: 0, h: 0 };
    this.selected = null;
    this.svg = s('svg', { class: 'graph-svg', role: 'application', 'aria-label': opts.ariaLabel || 'graph' });
    const arrow = (id, cls) =>
      s('marker', { id, viewBox: '0 0 10 10', refX: 9, refY: 5, markerWidth: 7, markerHeight: 7, orient: 'auto-start-reverse' },
        s('path', { d: 'M0,0 L10,5 L0,10 z', class: cls }));
    this.svg.appendChild(s('defs', null, arrow(`arr-${this.uid}`, 'arrow'), arrow(`arrhl-${this.uid}`, 'arrow-hl')));
    this.viewport = s('g', { class: 'viewport' });
    this.edgeLayer = s('g', { class: 'edges' });
    this.nodeLayer = s('g', { class: 'nodes' });
    this.viewport.append(this.edgeLayer, this.nodeLayer);
    this.svg.appendChild(this.viewport);
    container.appendChild(this.svg);
    this.bindInput();
    this.positioned = false;
    this.ro = new ResizeObserver(() => {
      this.size = { w: this.svg.clientWidth, h: this.svg.clientHeight };
      if (!this.positioned && this.layoutKey && this.size.w > 0) this.fit(false);
    });
    this.ro.observe(this.svg);
  }

  setDirection(dir) {
    if (dir === this.opts.direction) return;
    this.opts.direction = dir;
    this.layoutKey = '';
    if (this.lastInput) this.setGraph(this.lastInput.nodes, this.lastInput.edges, { fit: true });
  }

  /**
   * nodes: [{id, width, height, ...}], edges: [{from, to}]. Re-layouts only
   * when the topology or node sizes changed; otherwise just redraws nodes.
   */
  setGraph(nodes, edges, { fit = false } = {}) {
    this.lastInput = { nodes, edges };
    const key = this.opts.direction + '|' + nodes.map((n) => `${n.id}:${n.width}x${n.height}`).join(',') + '|' + edges.map((e) => `${e.from}>${e.to}`).join(',');
    const relayout = key !== this.layoutKey;
    if (relayout) {
      this.layout(nodes, edges);
      this.layoutKey = key;
    }
    for (const n of nodes) {
      const entry = this.nodes.get(String(n.id));
      if (entry) entry.data = n;
    }
    this.drawNodes();
    if (relayout) this.drawEdges();
    if (relayout && (fit || this.firstLayout !== false)) {
      this.firstLayout = false;
      this.fit(false);
    }
    this.restyle();
  }

  layout(nodes, edges) {
    const g = new window.dagre.graphlib.Graph();
    g.setGraph({ rankdir: this.opts.direction, nodesep: this.opts.nodesep, ranksep: this.opts.ranksep, edgesep: 14, marginx: 24, marginy: 24 });
    g.setDefaultEdgeLabel(() => ({}));
    for (const n of nodes) g.setNode(String(n.id), { width: n.width, height: n.height });
    const seen = new Map();
    for (const e of edges) {
      const k = `${e.from}>${e.to}`;
      seen.set(k, (seen.get(k) || 0) + 1);
    }
    for (const k of seen.keys()) {
      const [a, b] = k.split('>');
      if (g.hasNode(a) && g.hasNode(b)) g.setEdge(a, b);
    }
    window.dagre.layout(g);
    this.nodes = new Map();
    for (const n of nodes) {
      const p = g.node(String(n.id));
      this.nodes.set(String(n.id), { data: n, x: p.x - n.width / 2, y: p.y - n.height / 2, w: n.width, h: n.height, el: null });
    }
    this.edges = g.edges().map((e) => ({ from: e.v, to: e.w, points: g.edge(e).points, count: seen.get(`${e.v}>${e.w}`) || 1, el: null }));
    const gg = g.graph();
    this.bounds = { w: gg.width || 1, h: gg.height || 1 };
  }

  drawEdges() {
    clear(this.edgeLayer);
    for (const e of this.edges) {
      e.el = s('path', { class: 'edge', d: edgePath(e.points), 'marker-end': `url(#arr-${this.uid})` });
      if (e.count > 1) e.el.appendChild(s('title', { text: `${e.count} inputs` }));
      this.edgeLayer.appendChild(e.el);
    }
  }

  drawNodes() {
    for (const [id, n] of this.nodes) {
      if (!n.el) {
        n.el = s('g', { class: 'gnode', transform: `translate(${n.x.toFixed(1)},${n.y.toFixed(1)})`, tabindex: 0, role: 'button' });
        n.el.dataset.id = id;
        n.el.addEventListener('keydown', (e) => {
          if (e.key === 'Enter' || e.key === ' ') {
            e.preventDefault();
            this.select(id, true);
          }
        });
      }
      clear(n.el);
      this.opts.renderNode(n.data, n.el, { w: n.w, h: n.h });
      if (this.opts.label) n.el.setAttribute('aria-label', this.opts.label(n.data));
    }
    // keep DOM order stable and drop removed nodes
    clear(this.nodeLayer);
    for (const n of this.nodes.values()) this.nodeLayer.appendChild(n.el);
  }

  /** Re-render node contents (e.g. live counters) without layout. */
  refreshNodes() {
    this.drawNodes();
    this.restyle();
  }

  /**
   * highlight: {nodes: Set<id>|null, dim: bool}; edges between highlighted
   * nodes are highlighted too.
   */
  setHighlight(hl) {
    this.hl = hl;
    this.restyle();
  }

  setActiveNodes(ids) {
    this.active = ids;
    this.restyle();
  }

  restyle() {
    const hl = this.hl && this.hl.nodes ? this.hl.nodes : null;
    const dim = !!(this.hl && this.hl.dim && hl);
    const sel = this.selected;
    const neighbors = new Set();
    if (sel !== null) {
      for (const e of this.edges) {
        if (e.from === sel) neighbors.add(e.to);
        if (e.to === sel) neighbors.add(e.from);
      }
    }
    for (const [id, n] of this.nodes) {
      const inHl = hl ? hl.has(id) : false;
      n.el.classList.toggle('sel', id === sel);
      n.el.classList.toggle('hl', inHl || neighbors.has(id));
      n.el.classList.toggle('dim', dim && !inHl && id !== sel);
    }
    for (const e of this.edges) {
      const inHl = hl ? hl.has(e.from) && hl.has(e.to) : false;
      const touchesSel = sel !== null && (e.from === sel || e.to === sel);
      const lit = inHl || touchesSel;
      e.el.classList.toggle('hl', lit);
      e.el.classList.toggle('dim', dim && !inHl);
      e.el.classList.toggle('active', !!(this.active && this.active.has(e.from)));
      e.el.setAttribute('marker-end', `url(#${lit ? 'arrhl' : 'arr'}-${this.uid})`);
    }
  }

  select(id, notify = false) {
    this.selected = id === null || id === undefined ? null : String(id);
    this.restyle();
    if (notify && this.opts.onSelect) this.opts.onSelect(this.selected);
  }

  centerOn(id) {
    const n = this.nodes.get(String(id));
    if (!n) return;
    const { w, h } = this.viewSize();
    const k = Math.max(this.t.k, 0.8);
    this.t = { k, x: w / 2 - (n.x + n.w / 2) * k, y: h / 2 - (n.y + n.h / 2) * k };
    this.apply();
  }

  viewSize() {
    return { w: this.svg.clientWidth || 800, h: this.svg.clientHeight || 500 };
  }

  /**
   * Fit the graph into view. With `all` false, a graph that would shrink
   * below readability is fitted along its long side's cross axis instead
   * (a tall graph to the width, a wide one to the height) and panned.
   */
  fit(all = true) {
    if (!this.bounds) return;
    const { w, h } = this.viewSize();
    const kw = (w - 16) / this.bounds.w;
    const kh = (h - 16) / this.bounds.h;
    const kAll = Math.max(MIN_K, Math.min(1.2, kw, kh));
    if (all || kAll >= 0.45) {
      this.t = { k: kAll, x: (w - this.bounds.w * kAll) / 2, y: (h - this.bounds.h * kAll) / 2 };
    } else if (kw >= kh) {
      const k = Math.max(kAll, Math.min(1, kw));
      this.t = { k, x: Math.max(8, (w - this.bounds.w * k) / 2), y: 8 };
    } else {
      const k = Math.max(kAll, Math.min(1, kh));
      this.t = { k, x: 8, y: Math.max(8, (h - this.bounds.h * k) / 2) };
    }
    this.apply();
  }

  zoomBy(f, cx, cy) {
    const { w, h } = this.viewSize();
    const px = cx ?? w / 2;
    const py = cy ?? h / 2;
    const k = Math.max(MIN_K, Math.min(MAX_K, this.t.k * f));
    const r = k / this.t.k;
    this.t = { k, x: px - (px - this.t.x) * r, y: py - (py - this.t.y) * r };
    this.apply();
  }

  apply() {
    if (this.svg.clientWidth > 0) this.positioned = true;
    this.viewport.setAttribute('transform', `translate(${this.t.x.toFixed(1)},${this.t.y.toFixed(1)}) scale(${this.t.k.toFixed(4)})`);
  }

  bindInput() {
    const pointers = new Map();
    let start = null;
    let pinch = null;
    const pt = (e) => {
      const r = this.svg.getBoundingClientRect();
      return { x: e.clientX - r.left, y: e.clientY - r.top };
    };
    this.svg.addEventListener('pointerdown', (e) => {
      if (e.button !== 0 && e.pointerType === 'mouse') return;
      this.svg.setPointerCapture(e.pointerId);
      pointers.set(e.pointerId, pt(e));
      if (pointers.size === 1) {
        start = { ...pt(e), tx: this.t.x, ty: this.t.y, moved: false, target: e.target };
      } else if (pointers.size === 2) {
        const [a, b] = [...pointers.values()];
        pinch = { d: Math.hypot(a.x - b.x, a.y - b.y), k: this.t.k };
        start = null;
      }
    });
    this.svg.addEventListener('pointermove', (e) => {
      if (!pointers.has(e.pointerId)) return;
      pointers.set(e.pointerId, pt(e));
      if (pinch && pointers.size === 2) {
        const [a, b] = [...pointers.values()];
        const d = Math.hypot(a.x - b.x, a.y - b.y);
        const f = (pinch.k * (d / Math.max(1, pinch.d))) / this.t.k;
        this.zoomBy(f, (a.x + b.x) / 2, (a.y + b.y) / 2);
      } else if (start) {
        const p = pt(e);
        const dx = p.x - start.x;
        const dy = p.y - start.y;
        if (!start.moved && Math.hypot(dx, dy) > 4) {
          start.moved = true;
          this.svg.classList.add('dragging');
        }
        if (start.moved) {
          this.t.x = start.tx + dx;
          this.t.y = start.ty + dy;
          this.apply();
        }
      }
    });
    const end = (e) => {
      if (!pointers.has(e.pointerId)) return;
      pointers.delete(e.pointerId);
      if (pointers.size < 2) pinch = null;
      if (start && pointers.size === 0) {
        if (!start.moved) {
          const g = start.target && start.target.closest ? start.target.closest('.gnode') : null;
          this.select(g ? g.dataset.id : null, true);
        }
        start = null;
        this.svg.classList.remove('dragging');
      }
    };
    this.svg.addEventListener('pointerup', end);
    this.svg.addEventListener('pointercancel', end);
    // wheel and two-finger scroll pan; pinch (reported with ctrlKey) and
    // Ctrl/Cmd + wheel zoom around the pointer
    this.svg.addEventListener('wheel', (e) => {
      e.preventDefault();
      const scale = e.deltaMode === 1 ? 16 : e.deltaMode === 2 ? this.viewSize().h : 1;
      if (e.ctrlKey || e.metaKey) {
        const p = pt(e);
        this.zoomBy(Math.exp(-e.deltaY * scale * 0.01), p.x, p.y);
      } else {
        this.t.x -= (e.shiftKey && !e.deltaX ? e.deltaY : e.deltaX) * scale;
        this.t.y -= (e.shiftKey && !e.deltaX ? 0 : e.deltaY) * scale;
        this.apply();
      }
    }, { passive: false });
    this.svg.addEventListener('keydown', (e) => {
      if (e.target !== this.svg) return;
      const step = 60;
      if (e.key === '+' || e.key === '=') this.zoomBy(1.2);
      else if (e.key === '-') this.zoomBy(1 / 1.2);
      else if (e.key === '0') this.fit();
      else if (e.key === 'ArrowLeft') this.t.x += step;
      else if (e.key === 'ArrowRight') this.t.x -= step;
      else if (e.key === 'ArrowUp') this.t.y += step;
      else if (e.key === 'ArrowDown') this.t.y -= step;
      else return;
      e.preventDefault();
      this.apply();
    });
    this.svg.setAttribute('tabindex', '0');
  }

  destroy() {
    this.ro.disconnect();
    this.svg.remove();
  }
}

/** Wrap text to at most `max` characters with an ellipsis. */
export function fitText(text, max) {
  const t = String(text ?? '');
  return t.length > max ? t.slice(0, Math.max(1, max - 1)) + '…' : t;
}
