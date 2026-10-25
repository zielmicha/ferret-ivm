// Catalog history: every DDL statement with the GSN it took effect at.

import { api } from '../api.js';
import { h, replace, card, errorBox, empty, sqlCode, segmented } from '../dom.js';
import * as fmt from '../format.js';
import { live, poll } from '../live.js';

const VERB_SLOT = { CREATE: 1, DROP: 8, ALTER: 4 };

export default {
  title: 'History',
  mount(root) {
    const sub = h('div.sub', 'Loading…');
    let filter = '';
    let verb = 'all';
    let data = null;
    const search = h('input.input', {
      type: 'search', placeholder: 'Filter statements', 'aria-label': 'Filter statements',
      oninput: (e) => { filter = e.target.value.toLowerCase(); draw(); },
    });
    const body = h('div');
    root.append(
      h('div.page-head', h('div.page-title', h('h1', 'Catalog history'), sub), h('div.grow'),
        segmented([{ value: 'all', label: 'All' }, { value: 'CREATE', label: 'Create' }, { value: 'DROP', label: 'Drop' }, { value: 'ALTER', label: 'Alter' }], verb, (v) => { verb = v; draw(); }),
        h('div.search', { style: { width: '240px' } }, search)),
      body);

    function draw() {
      if (!data) return;
      const entries = data.entries.filter((e) => (verb === 'all' || e.action.startsWith(verb)) && (!filter || e.sql.toLowerCase().includes(filter)));
      if (!data.entries.length) {
        replace(body, card(null, empty('No DDL yet', 'CREATE, DROP and ALTER statements appear here with the GSN they took effect at.')));
        return;
      }
      replace(body, card(null, h('div.stack',
        h('div.small.text-2', 'The catalog is versioned by GSN: "what was the catalog at g" is a query against ivm.catalog_history.'),
        entries.length ? h('ol.timeline', ...entries.map((e) => {
          const v = e.action.split(' ')[0];
          return h('li.tl-item',
            h('div.tl-gsn', fmt.int(e.gsn)),
            h('div.tl-dot', { style: { '--kc': `var(--s${VERB_SLOT[v] || 7})` } }),
            h('div.tl-body', h('div.act', e.action), sqlCode(e.sql)));
        })) : h('div.muted', 'No matching statements'),
        data.total > data.entries.length ? h('div.muted.small', `Showing the latest ${fmt.int(data.entries.length)} of ${fmt.int(data.total)} statements`) : null)));
    }

    async function load() {
      try {
        data = await api.history(1000);
        sub.textContent = `${fmt.plural(data.total, 'statement')} · newest first · as of GSN ${fmt.int(data.gsn)}`;
        draw();
      } catch (e) {
        replace(body, errorBox(e.message));
      }
    }
    const stop = poll(load, 30000);
    const off = live.on('topology', load);
    return {
      unmount() {
        stop();
        off();
      },
    };
  },
};
