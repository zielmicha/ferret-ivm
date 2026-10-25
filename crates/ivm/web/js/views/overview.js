// Overview: live tiles and charts from the sample stream, plus engine facts
// polled from /api/overview.

import { api } from '../api.js';
import { h, replace, card, tile, meter, segmented, statusBadge, errorBox, href } from '../dom.js';
import * as fmt from '../format.js';
import { live, poll, rateSeries, recentRate } from '../live.js';
import { TimeChart } from '../charts.js';

const RANGES = [
  { value: 60, label: '1 min' },
  { value: 300, label: '5 min' },
  { value: 1200, label: '20 min' },
];

let rangeSecs = 300;

function sourceNames(ov) {
  const m = new Map();
  if (ov) for (const s of ov.sources) m.set(s.id, s.name);
  return m;
}

export default {
  title: 'Overview',
  mount(root) {
    let ov = null;
    let ovError = null;
    const head = h('div.page-head',
      h('div.page-title', h('h1', 'Overview'), h('div.sub', 'Live engine state; charts update every second')),
      h('div.grow'),
      segmented(RANGES, rangeSecs, (v) => {
        rangeSecs = v;
        drawCharts();
      }));
    const tilesEl = h('div.tiles');
    const chartsEl = h('div.charts');
    const lowerEl = h('div.grid.cols-2');
    const errEl = h('div');
    root.append(head, errEl, tilesEl, chartsEl, h('div', { style: { height: '16px' } }), lowerEl);

    // charts are created once per series signature (sources may come and go)
    let chartSig = '';
    let charts = {};
    function ensureCharts() {
      const names = sourceNames(ov);
      const ids = [...new Set(live.samples.flatMap((s) => s.sources.map((x) => x.id)))].sort((a, b) => a - b);
      const sig = ids.map((id) => `${id}:${names.get(id) || ''}`).join(',');
      if (sig === chartSig && Object.keys(charts).length) return ids;
      chartSig = sig;
      for (const c of Object.values(charts)) c.destroy();
      const srcSeries = ids.length
        ? ids.slice(0, 8).map((id, i) => ({ label: names.get(id) || `source ${id}`, slot: i + 1 }))
        : [{ label: 'changes', slot: 1 }];
      charts = {
        gsn: new TimeChart({ title: 'Throughput', subtitle: 'Log records applied per second (GSN/s)', format: fmt.rate, series: [{ label: 'GSN/s', slot: 1 }], area: true }),
        ingest: new TimeChart({ title: 'Ingest', subtitle: ids.length ? 'Row changes logged per second, by source' : 'No sources: local DML only', format: fmt.rate, series: srcSeries, area: srcSeries.length === 1 }),
        lag: new TimeChart({ title: 'Apply lag', subtitle: 'Durable GSN minus processed GSN', format: fmt.compact, minTop: 4, series: [{ label: 'GSNs behind', slot: 1 }], area: true }),
        repl: new TimeChart({ title: 'Replication lag', subtitle: ids.length ? 'Upstream WAL position minus source frontier' : 'No sources', format: fmt.bytes, minTop: 1024, series: srcSeries, area: srcSeries.length === 1, empty: ids.length ? 'Collecting data…' : 'No sources' }),
        busy: new TimeChart({ title: 'Engine time', subtitle: 'Dataflow operators and process CPU (100% = one core)', format: (v) => fmt.pct(v, v < 10 ? 1 : 0), series: [{ label: 'dataflow', slot: 1 }, { label: 'process CPU', slot: 2 }] }),
        mem: new TimeChart({ title: 'Memory', subtitle: 'Buffer pool, its dirty part, and process RSS', format: fmt.bytes, minTop: 1 << 20, series: [{ label: 'buffer pool', slot: 1 }, { label: 'dirty', slot: 2 }, { label: 'RSS', slot: 3 }] }),
      };
      replace(chartsEl, ...Object.values(charts).map((c) => c.el));
      return ids;
    }

    function drawCharts() {
      const ids = ensureCharts();
      const now = live.last() ? live.last().t : Date.now();
      const ss = live.samples.filter((s) => s.t >= now - rangeSecs * 1000 - 1000);
      const xs = ss.map((s) => s.t / 1000);
      charts.gsn.setData(xs, [rateSeries(ss, (s) => s.processed_gsn)]);
      const src = (s, id) => s.sources.find((x) => x.id === id);
      const ingest = ids.length
        ? ids.slice(0, 8).map((id) => rateSeries(ss, (s) => (src(s, id) ? src(s, id).changes : null)))
        : [ss.map(() => null)];
      charts.ingest.setData(xs, ingest);
      charts.lag.setData(xs, [ss.map((s) => Math.max(0, s.durable_gsn - s.processed_gsn))]);
      const repl = ids.length
        ? ids.slice(0, 8).map((id) => ss.map((s) => {
          const x = src(s, id);
          return x && x.upstream_lsn > 0 && x.frontier_lsn > 0 ? Math.max(0, x.upstream_lsn - x.frontier_lsn) : null;
        }))
        : [ss.map(() => null)];
      charts.repl.setData(xs, repl);
      const busy = rateSeries(ss, (s) => (s.engine ? s.engine.steps.nanos : null)).map((v) => (v === null ? null : v / 1e7));
      charts.busy.setData(xs, [busy, ss.map((s) => s.cpu_percent ?? null)]);
      charts.mem.setData(xs, [
        ss.map((s) => (s.engine ? s.engine.cache_used_bytes : null)),
        ss.map((s) => (s.engine ? s.engine.dirty_bytes : null)),
        ss.map((s) => s.rss_bytes ?? null),
      ]);
    }

    function drawTiles() {
      const last = live.last();
      const le = live.lastEngine();
      const e = le ? le.engine : ov ? ov.stats : null;
      const gsnRate = recentRate((s) => s.processed_gsn, 5);
      const ingestRate = recentRate((s) => (s.sources.length ? s.sources.reduce((a, x) => a + x.changes, 0) : null), 5);
      const rowsRate = recentRate((s) => (s.engine ? s.engine.steps.out_rows : null), 5);
      const busyRate = recentRate((s) => (s.engine ? s.engine.steps.nanos : null), 5);
      const processed = last ? last.processed_gsn : e ? e.processed_gsn : null;
      const durable = last ? last.durable_gsn : e ? e.durable_gsn : null;
      const lag = processed !== null && durable !== null ? Math.max(0, durable - processed) : null;
      const busyMs = last && last.engine_busy_ms;
      const cache = e ? e.cache_used_bytes : null;
      const cap = e ? e.cache_capacity_bytes : null;
      const proc = ov ? ov.process : null;
      replace(tilesEl,
        tile('Processed GSN', fmt.int(processed), `durable ${fmt.int(durable)}`, { hero: true, title: 'Every log record up to this GSN is applied' }),
        tile('Apply lag', fmt.compact(lag), busyMs ? h('span', statusBadge(`engine busy ${fmt.duration(busyMs / 1000)}`)) : 'logged, not yet applied', { unit: 'GSN' }),
        tile('Throughput', fmt.rate(gsnRate), 'log records per second', { unit: '/s' }),
        tile('Ingest', fmt.rate(ingestRate), ov ? fmt.plural(ov.counts.sources, 'source') : 'row changes per second', { unit: 'rows/s' }),
        tile('Dataflow', fmt.rate(rowsRate), busyRate === null ? 'delta rows out per second' : `busy ${fmt.pct(busyRate / 1e7, 1)} of one core`, { unit: 'rows/s' }),
        tile('Last checkpoint', e ? fmt.ago(e.last_checkpoint_ms) : '–', e ? `at GSN ${fmt.int(e.checkpoint_gsn)} · ${fmt.int(e.records_since_checkpoint)} records since` : '', { title: 'Checkpoints persist the catalog and all arrangements; recovery replays the log from there' }),
        tile('Buffer pool', fmt.bytes(cache), e ? `of ${fmt.bytes(cap)} · dirty ${fmt.bytes(e.dirty_bytes)}` : '', { meter: cap ? meter(cache / cap) : null }),
        tile('Process', proc && proc.rss_bytes ? fmt.bytes(last && last.rss_bytes ? last.rss_bytes : proc.rss_bytes) : '–',
          `CPU ${last && last.cpu_percent !== null && last.cpu_percent !== undefined ? fmt.pct(last.cpu_percent, 1) : '–'}${proc && proc.threads ? ` · ${proc.threads} threads` : ''}`),
        tile('Logical clock', e && e.clock_ms ? fmt.clock(e.clock_ms) : 'no ticks', e ? `retention from GSN ${fmt.int(e.retention_frontier)}` : ''),
        tile('Disk', ov ? fmt.bytes(ov.server.store_bytes + ov.server.log_bytes) : '–', ov ? `store ${fmt.bytes(ov.server.store_bytes)} · log ${fmt.bytes(ov.server.log_bytes)}` : ''),
      );
    }

    function drawLower() {
      if (!ov) return;
      const c = ov.counts;
      const link = (label, route, n) => h('a.chip', { href: href(route) }, h('b', fmt.compact(n)), label);
      const objects = card('Catalog and dataflow', h('div.stack',
        h('div.chips',
          link('tables', 'catalog', c.tables), link('materialized views', 'catalog', c.materialized_views), link('views', 'catalog', c.views),
          link('indexes', 'catalog', c.indexes), link('sources', 'sources', c.sources), link('sinks', 'activity', c.sinks)),
        h('div.chips',
          link('dataflow nodes', 'dataflow', c.nodes), link('arrangements', 'arrangements', c.arrangements), link('records', 'arrangements', c.records)),
      ));
      const srcList = ov.sources.length
        ? h('div.stack', ...ov.sources.map((s) => h('div.row', h('a', { href: href('sources') }, s.name), h('span.spacer'), statusBadge(s.status))),
          ...ov.sinks.map((s) => h('div.row', h('span', s.name), h('span.muted.small', 'sink'), h('span.spacer'), statusBadge(s.status))))
        : h('div.muted', 'No sources. Create one with CREATE SOURCE … FROM POSTGRES.');
      const cfg = ov.config;
      const st = ov.storage;
      const config = card('Engine', h('dl.kv',
        h('dt', 'Version'), h('dd', ov.version),
        h('dt', 'Data directory'), h('dd.mono', cfg.data_dir),
        h('dt', 'Memory budget'), h('dd', fmt.bytes(cfg.memory_bytes)),
        h('dt', 'Checkpoints'), h('dd', `every ${fmt.duration(cfg.checkpoint_interval_secs)} (or when half the pool is dirty)`),
        h('dt', 'Retention'), h('dd', `${fmt.int(cfg.retention_gsns)} GSNs readable with AS OF`),
        h('dt', 'Log sync'), h('dd', cfg.sync ? 'fsync per commit batch' : 'off (--no-sync)'),
        h('dt', 'Clock ticks'), h('dd', cfg.tick_ms ? `every ${fmt.duration(cfg.tick_ms / 1000)}` : 'off'),
        h('dt', 'Storage'), h('dd', `${fmt.int(st.trees)} trees · ${fmt.int(st.loaded_nodes)} nodes in memory (${fmt.int(st.dirty_nodes)} dirty) · ${fmt.int(st.segments)} segments, ${fmt.bytes(st.live_bytes)} live of ${fmt.bytes(st.segment_bytes)}`),
        h('dt', 'Uptime'), h('dd', `${fmt.duration(ov.server.uptime_secs)} (since ${fmt.dateTime(ov.server.started_ms)})`),
        h('dt', 'HTTP auth'), h('dd', ov.server.auth ? 'Basic auth (password)' : 'none (local access only)'),
      ));
      replace(lowerEl, h('div.stack', objects, card('Sources and sinks', srcList)), config);
    }

    function redraw() {
      drawTiles();
      drawCharts();
    }

    const offSample = live.on('sample', redraw);
    const stop = poll(async () => {
      try {
        ov = await api.overview();
        ovError = null;
      } catch (e) {
        ovError = e;
      }
      replace(errEl, ovError ? errorBox(ovError.message) : null);
      drawLower();
      drawTiles();
    }, 5000);
    redraw();

    return {
      unmount() {
        offSample();
        stop();
        for (const c of Object.values(charts)) c.destroy();
      },
    };
  },
};
