// Time-series charts on uPlot (vendored). One measure per chart, one y axis;
// colors come from the theme tokens and charts rebuild when the theme flips.
// The header shows the latest value, or the value under the crosshair.

import { h, s, replace } from './dom.js';
import { clock } from './format.js';

function token(name) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

function withAlpha(color, alpha) {
  const m = /^#([0-9a-f]{6})$/i.exec(color);
  if (!m) return color;
  const n = parseInt(m[1], 16);
  return `rgba(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255}, ${alpha})`;
}

const charts = new Set();
window.addEventListener('themechange', () => {
  for (const c of charts) c.rebuild();
});

export class TimeChart {
  /**
   * opts: {title, subtitle, format(v) -> string, series: [{label, slot}],
   * area (single series wash), height, empty (text when no data), max (fixed y max)}
   */
  constructor(opts) {
    this.opts = { height: 150, empty: 'Collecting data…', ...opts };
    this.data = [[], ...this.opts.series.map(() => [])];
    this.valueEl = h('span.chart-value', '–');
    this.whenEl = h('span.chart-when', 'now');
    this.legendEl = this.opts.series.length > 1 ? h('div.chart-legend') : null;
    this.box = h('div.chart-box', { dataset: { empty: this.opts.empty } });
    this.el = h('section.card.chart-card',
      h('div.chart-head', h('span.chart-title', this.opts.title), this.valueEl, this.whenEl),
      h('div.chart-sub', this.opts.subtitle || ''),
      this.box,
      this.legendEl);
    this.plot = null;
    this.idx = null;
    this.ro = new ResizeObserver(() => this.resize());
    this.ro.observe(this.box);
    charts.add(this);
  }

  colors() {
    return this.opts.series.map((sr) => token(`--s${sr.slot || 1}`));
  }

  build() {
    if (!window.uPlot || this.box.clientWidth === 0) return;
    const fmt = this.opts.format || ((v) => String(v));
    const muted = token('--muted');
    const grid = token('--grid');
    const surface = token('--surface');
    const colors = this.colors();
    const font = '11px ' + token('--font');
    const max = this.opts.max;
    const opts = {
      width: this.box.clientWidth,
      height: this.opts.height,
      padding: [6, 6, 0, 0],
      legend: { show: false },
      cursor: {
        drag: { x: false, y: false, setScale: false },
        points: { size: 7, width: 2, stroke: () => surface },
      },
      scales: {
        x: { time: true },
        y: {
          range: (u, lo, hi) => {
            const top = max !== undefined ? max : Math.max(hi > 0 ? hi * 1.15 : 0, this.opts.minTop ?? 1);
            return [Math.min(0, lo), top];
          },
        },
      },
      axes: [
        {
          stroke: muted, font, space: 80, size: 26,
          grid: { stroke: grid, width: 1 }, ticks: { stroke: grid, width: 1, size: 4 },
          values: (u, splits) => {
            const span = splits.length > 1 ? splits[splits.length - 1] - splits[0] : 0;
            return splits.map((v) => {
              const d = new Date(v * 1000);
              const hh = String(d.getHours()).padStart(2, '0');
              const mm = String(d.getMinutes()).padStart(2, '0');
              return span < 180 ? `${hh}:${mm}:${String(d.getSeconds()).padStart(2, '0')}` : `${hh}:${mm}`;
            });
          },
        },
        {
          stroke: muted, font, size: 56, space: 30,
          grid: { stroke: grid, width: 1 }, ticks: { show: false },
          values: (u, vals) => vals.map((v) => fmt(v)),
        },
      ],
      series: [
        {},
        ...this.opts.series.map((sr, i) => ({
          label: sr.label,
          stroke: colors[i],
          width: 2,
          fill: this.opts.area && this.opts.series.length === 1 ? withAlpha(colors[i], 0.1) : undefined,
          points: { show: false },
          spanGaps: false,
        })),
      ],
      hooks: {
        setCursor: [(u) => {
          this.idx = u.cursor.idx ?? null;
          this.readout();
        }],
      },
    };
    this.plot = new window.uPlot(opts, this.data, this.box);
    this.readout();
  }

  rebuild() {
    if (this.plot) {
      this.plot.destroy();
      this.plot = null;
    }
    this.build();
  }

  resize() {
    if (!this.plot) {
      this.build();
      return;
    }
    const w = this.box.clientWidth;
    if (w > 0 && w !== this.plot.width) this.plot.setSize({ width: w, height: this.opts.height });
  }

  /** xs: Unix seconds; ys: one array per series (null = gap). */
  setData(xs, ys) {
    this.data = [xs, ...ys];
    const hasData = ys.some((a) => a.some((v) => v !== null && v !== undefined));
    this.box.classList.toggle('empty-chart', !hasData);
    if (!this.plot) this.build();
    else this.plot.setData(this.data);
    this.readout();
  }

  readout() {
    const fmt = this.opts.format || ((v) => String(v));
    const xs = this.data[0];
    let i = this.idx;
    const live = i === null || i === undefined;
    if (live) {
      // latest index with any value
      i = xs.length - 1;
      while (i > 0 && this.data.slice(1).every((a) => a[i] === null || a[i] === undefined)) i--;
    }
    const at = (k) => (i >= 0 && i < xs.length ? this.data[k + 1][i] : null);
    if (this.opts.series.length === 1) {
      const v = at(0);
      this.valueEl.textContent = v === null || v === undefined ? '–' : fmt(v);
    } else {
      this.valueEl.textContent = '';
    }
    this.whenEl.textContent = live ? 'now' : clock(xs[i] * 1000);
    if (this.legendEl) {
      const colors = this.colors();
      replace(this.legendEl, ...this.opts.series.map((sr, k) => {
        const v = at(k);
        return h('span.key', { style: { '--c': colors[k] } }, h('i'), sr.label, h('b', v === null || v === undefined ? '–' : fmt(v)));
      }));
    }
  }

  destroy() {
    this.ro.disconnect();
    charts.delete(this);
    if (this.plot) this.plot.destroy();
    this.plot = null;
  }
}

/** Inline SVG sparkline of the last values (null = gap). */
export function sparkline(values, { width = 160, height = 34 } = {}) {
  const svg = s('svg', { class: 'spark', viewBox: `0 0 ${width} ${height}`, preserveAspectRatio: 'none', 'aria-hidden': 'true' });
  const vals = values.map((v) => (v === null || v === undefined || Number.isNaN(v) ? null : v));
  const max = Math.max(1e-9, ...vals.filter((v) => v !== null));
  const n = vals.length;
  if (n < 2) return svg;
  const x = (i) => (i / (n - 1)) * width;
  const y = (v) => height - 2 - (v / max) * (height - 4);
  let line = '';
  let area = '';
  let open = false;
  let start = 0;
  vals.forEach((v, i) => {
    if (v === null) {
      if (open) area += `L${x(i - 1).toFixed(1)},${height} L${x(start).toFixed(1)},${height} Z `;
      open = false;
      return;
    }
    if (!open) {
      line += `M${x(i).toFixed(1)},${y(v).toFixed(1)} `;
      area += `M${x(i).toFixed(1)},${height} L${x(i).toFixed(1)},${y(v).toFixed(1)} `;
      start = i;
      open = true;
    } else {
      line += `L${x(i).toFixed(1)},${y(v).toFixed(1)} `;
      area += `L${x(i).toFixed(1)},${y(v).toFixed(1)} `;
    }
  });
  if (open) area += `L${x(n - 1).toFixed(1)},${height} Z`;
  svg.appendChild(s('path', { class: 'area', d: area }));
  svg.appendChild(s('path', { class: 'line', d: line }));
  return svg;
}
