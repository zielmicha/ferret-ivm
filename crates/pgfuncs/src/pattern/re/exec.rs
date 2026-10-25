//! Regex execution: NFA simulation per subexpression node, plus faithful
//! ports of Spencer's find(), cfind()/cfindloop() and the cdissect()
//! family from PostgreSQL 17's src/backend/regex/regexec.c and the
//! semantics of longest()/shortest()/matchuntil()/dfa_backref() from
//! rege_dfa.c.
//!
//! Portions Copyright (c) 1998, 1999 Henry Spencer. All rights reserved.
//! Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group.

use super::charset::{CharSet, wc_tolower};
use super::dfa::{self, ACC, ClassMap, Dfa, RESET, STATE_MASK};
use super::compile::{
    self, AHEAD, ARC_BOS, ARC_EMPTY, ARC_EOS, ARC_LACON, ARC_PLAIN, BEHIND, DUPINF, LATYPE_AHEAD_NEG,
    LATYPE_AHEAD_POS, LATYPE_BEHIND_POS, Parsed, SHORTER,
};
use super::fxhash::FxHashMap as HashMap;

pub(crate) const REG_OKAY: i32 = 0;
pub(crate) const REG_NOMATCH: i32 = 1;

/// Non-consuming arc of a program.
#[derive(Clone, Copy, Debug)]
pub(crate) enum EArc {
    Empty(u32),
    Bos(u32),
    Eos(u32),
    Ahead(u32, u32),
    Behind(u32, u32),
    Lacon(u32, u32),
}

/// A compiled sub-NFA (one per subexpression node / lookaround constraint).
#[derive(Clone, Debug, Default)]
pub(crate) struct Prog {
    pub(crate) nstates: usize,
    pub(crate) start: u32,
    pub(crate) accept: u32,
    /// non-consuming arcs of state s: eps_arcs[eps_off[s]..eps_off[s+1]]
    eps_off: Vec<u32>,
    eps_arcs: Vec<EArc>,
    /// consuming arcs (set id, target) of state s: chr_arcs[chr_off[s]..chr_off[s+1]]
    chr_off: Vec<u32>,
    chr_arcs: Vec<(u32, u32)>,
    /// no lookaround (LACON) constraints
    pub(crate) simple: bool,
    /// anchored-mode DFA
    pub(crate) adfa: Option<Dfa>,
    /// search-mode DFA (implicit leading .*)
    pub(crate) sdfa: Option<Dfa>,
}

impl Prog {
    /// Extracts the sub-NFA reachable from `begin`, with `end` as the
    /// accepting state (Spencer's dupnfa + compact for a tree node).
    fn from_region(g: &compile::Graph, begin: u32, end: u32) -> Prog {
        let mut map: HashMap<u32, u32> = HashMap::default();
        let mut order: Vec<u32> = Vec::new();
        let get = |s: u32, map: &mut HashMap<u32, u32>, order: &mut Vec<u32>| -> u32 {
            if let Some(&i) = map.get(&s) {
                return i;
            }
            let i = order.len() as u32;
            map.insert(s, i);
            order.push(s);
            i
        };
        let start = get(begin, &mut map, &mut order);
        let accept = get(end, &mut map, &mut order);
        let mut p = Prog { start, accept, ..Default::default() };
        let mut eps: Vec<(u32, EArc)> = Vec::new();
        let mut chr: Vec<(u32, (u32, u32))> = Vec::new();
        if begin == end {
            p.accept = 1;
            order.push(u32::MAX); // second state
            eps.push((0, EArc::Empty(1)));
        } else {
            let mut idx = 0;
            while idx < order.len() {
                let s = order[idx];
                idx += 1;
                if s == end {
                    continue;
                }
                let from = map[&s];
                for &a in &g.states[s as usize].outs {
                    let arc = &g.arcs[a as usize];
                    let to = get(arc.to, &mut map, &mut order);
                    match arc.t {
                        ARC_PLAIN => chr.push((from, (arc.co, to))),
                        ARC_EMPTY => eps.push((from, EArc::Empty(to))),
                        ARC_BOS if arc.co == 1 => eps.push((from, EArc::Bos(to))),
                        ARC_EOS if arc.co == 1 => eps.push((from, EArc::Eos(to))),
                        AHEAD => eps.push((from, EArc::Ahead(arc.co, to))),
                        BEHIND => eps.push((from, EArc::Behind(arc.co, to))),
                        ARC_LACON => eps.push((from, EArc::Lacon(arc.co, to))),
                        _ => {}
                    }
                }
            }
        }
        p.nstates = order.len();
        // flatten (arcs were generated grouped by source state, in order)
        fn flatten<T: Copy>(n: usize, list: &[(u32, T)]) -> (Vec<u32>, Vec<T>) {
            let mut off = vec![0u32; n + 1];
            for &(f, _) in list {
                off[f as usize + 1] += 1;
            }
            for i in 0..n {
                off[i + 1] += off[i];
            }
            let mut arcs: Vec<T> = Vec::with_capacity(list.len());
            let mut sorted: Vec<&(u32, T)> = list.iter().collect();
            sorted.sort_by_key(|e| e.0);
            arcs.extend(sorted.into_iter().map(|e| e.1));
            (off, arcs)
        }
        (p.eps_off, p.eps_arcs) = flatten(p.nstates, &eps);
        (p.chr_off, p.chr_arcs) = flatten(p.nstates, &chr);
        p.simple = !p.eps_arcs.iter().any(|a| matches!(a, EArc::Lacon(..)));
        p
    }

    /// Non-consuming arcs leaving state `st`.
    #[inline]
    pub(crate) fn eps(&self, st: u32) -> &[EArc] {
        let st = st as usize;
        &self.eps_arcs[self.eps_off[st] as usize..self.eps_off[st + 1] as usize]
    }

    /// Consuming arcs (set id, target) leaving state `st`.
    #[inline]
    pub(crate) fn chr(&self, st: u32) -> &[(u32, u32)] {
        let st = st as usize;
        &self.chr_arcs[self.chr_off[st] as usize..self.chr_off[st + 1] as usize]
    }

    pub(crate) fn all_eps(&self) -> &[EArc] {
        &self.eps_arcs
    }

    /// Builds the requested DFAs (if eligible).
    fn build_dfas(&mut self, sets: &[CharSet], cm: &ClassMap, anchored: bool, search: bool) {
        if !self.simple {
            return;
        }
        if anchored {
            self.adfa = dfa::build(self, sets, cm, false);
        }
        if search {
            self.sdfa = dfa::build(self, sets, cm, true);
        }
    }

    fn used_sets(&self, out: &mut Vec<u32>) {
        out.extend(self.chr_arcs.iter().map(|&(s, _)| s));
        out.extend(self.eps_arcs.iter().filter_map(|a| match *a {
            EArc::Ahead(s, _) | EArc::Behind(s, _) => Some(s),
            _ => None,
        }));
    }

    #[inline]
    fn words(&self) -> usize {
        self.nstates.div_ceil(64)
    }
}

/// A node of the subexpression tree (Spencer's `struct subre`).
#[derive(Clone, Debug)]
pub(crate) struct Node {
    pub op: u8,
    pub flags: u32,
    pub capno: usize,
    pub backno: usize,
    pub min: i32,
    pub max: i32,
    pub children: Vec<usize>,
    pub prog: Prog,
}

#[derive(Clone, Debug)]
pub(crate) struct LaconProg {
    prog: Prog,
    ahead: bool,
    positive: bool,
}

/// A fully compiled regular expression.
#[derive(Clone, Debug)]
pub(crate) struct Compiled {
    pub sets: Vec<CharSet>,
    pub nodes: Vec<Node>,
    pub root: usize,
    pub lacons: Vec<LaconProg>,
    pub nsub: usize,
    pub icase: bool,
    pub backref: bool,
    pub cm: ClassMap,
    /// matching needs a large stack (see DEEP_TREE)
    pub deep: bool,
}

impl Compiled {
    pub(crate) fn from_parsed(p: Parsed) -> Result<Compiled, i32> {
        // Pre-order traversal of the subre tree (iterative: trees can be
        // thousands of levels deep).
        let kids_of = |t: usize| -> Vec<usize> {
            let mut kids = Vec::new();
            let mut c = p.subs[t].child;
            while let Some(ci) = c {
                kids.push(ci);
                c = p.subs[ci].sibling;
            }
            kids
        };
        let mut order: Vec<usize> = Vec::new();
        let mut tree_depth = 0usize;
        let mut stack = vec![(p.tree, 1usize)];
        while let Some((t, d)) = stack.pop() {
            order.push(t);
            tree_depth = tree_depth.max(d);
            for &k in kids_of(t).iter().rev() {
                stack.push((k, d + 1));
            }
        }
        let idx_of: HashMap<usize, usize> = order.iter().enumerate().map(|(i, &t)| (t, i)).collect();
        let mut total_states = 0usize;
        let mut nodes = Vec::with_capacity(order.len());
        for &t in &order {
            let s = &p.subs[t];
            let prog = Prog::from_region(&p.graph, s.begin, s.end);
            total_states += prog.nstates;
            if total_states > MAX_TOTAL_STATES {
                return Err(compile::REG_ETOOBIG);
            }
            nodes.push(Node {
                op: s.op,
                flags: s.flags,
                capno: s.capno.max(0) as usize,
                backno: s.backno.max(0) as usize,
                min: s.min,
                max: s.max,
                children: kids_of(t).iter().map(|k| idx_of[k]).collect(),
                prog,
            });
        }
        let root = 0;
        let mut lacons: Vec<LaconProg> = Vec::with_capacity(p.lacons.len());
        let mut lacon_depth: Vec<usize> = Vec::with_capacity(p.lacons.len());
        for l in &p.lacons {
            let prog = Prog::from_region(&p.graph, l.begin, l.end);
            total_states += prog.nstates;
            if total_states > MAX_TOTAL_STATES {
                return Err(compile::REG_ETOOBIG);
            }
            // nested lookarounds are created before their parents
            let d = 1 + prog
                .all_eps()
                .iter()
                .filter_map(|a| if let EArc::Lacon(j, _) = *a { lacon_depth.get(j as usize).copied() } else { None })
                .max()
                .unwrap_or(0);
            lacon_depth.push(d);
            lacons.push(LaconProg {
                prog,
                ahead: l.latype == LATYPE_AHEAD_POS || l.latype == LATYPE_AHEAD_NEG,
                positive: l.latype == LATYPE_AHEAD_POS || l.latype == LATYPE_BEHIND_POS,
            });
        }
        let deep = tree_depth > DEEP_TREE || lacon_depth.iter().copied().max().unwrap_or(0) > DEEP_LACON;
        let mut used = Vec::new();
        for n in &nodes {
            n.prog.used_sets(&mut used);
        }
        for l in &lacons {
            l.prog.used_sets(&mut used);
        }
        used.sort_unstable();
        used.dedup();
        let cm = ClassMap::build(&p.sets, &used);
        for (i, n) in nodes.iter_mut().enumerate() {
            n.prog.build_dfas(&p.sets, &cm, true, i == root);
        }
        for l in lacons.iter_mut() {
            let ahead = l.ahead;
            l.prog.build_dfas(&p.sets, &cm, ahead, !ahead);
        }
        Ok(Compiled {
            sets: p.sets,
            nodes,
            root,
            lacons,
            nsub: p.nsub,
            icase: p.cflags & compile::REG_ICASE != 0,
            backref: p.uses_backref,
            cm,
            deep,
        })
    }
}

/// Upper bound on the total size of all per-node sub-NFAs (Spencer's design
/// keeps one per subexpression node, so e.g. k sequential capture groups
/// cost O(k^2) states). Beyond this we report "regular expression is too
/// complex" instead of exhausting memory (PostgreSQL itself can crash or be
/// OOM-killed on such patterns).
const MAX_TOTAL_STATES: usize = 4_000_000;
/// Matches on regexes with deeper subexpression trees / lookaround nesting
/// run on a dedicated thread with a large stack (dissection is recursive).
const DEEP_TREE: usize = 400;
const DEEP_LACON: usize = 100;

impl Compiled {
    /// Existence of a match using the root's search DFA directly over the
    /// UTF-8 string (no allocation). None if not applicable.
    pub(crate) fn dfa_is_match(&self, s: &str) -> Option<bool> {
        if self.backref || s.is_empty() {
            return None;
        }
        let prog = &self.nodes[self.root].prog;
        let (d, cm) = (prog.sdfa.as_ref()?, &self.cm);
        let mut st = d.init[d.nclasses];
        if s.is_ascii() {
            for &c in s.as_bytes() {
                let t = d.next(st, cm.class(c as u32));
                if t & ACC != 0 {
                    return Some(true);
                }
                st = t & STATE_MASK;
                if !d.live[st as usize] {
                    return Some(false);
                }
            }
        } else {
            for c in s.chars() {
                let t = d.next(st, cm.class(c as u32));
                if t & ACC != 0 {
                    return Some(true);
                }
                st = t & STATE_MASK;
                if !d.live[st as usize] {
                    return Some(false);
                }
            }
        }
        Some(d.accept_eos[st as usize])
    }
}

/// Bit set over program states.
struct Bits {
    w: Vec<u64>,
}

impl Bits {
    fn new(words: usize) -> Bits {
        Bits { w: vec![0; words] }
    }
    #[inline]
    fn clear(&mut self) {
        self.w.fill(0);
    }
    #[inline]
    fn has(&self, i: u32) -> bool {
        self.w[(i >> 6) as usize] & (1u64 << (i & 63)) != 0
    }
    #[inline]
    fn set(&mut self, i: u32) -> bool {
        let (wi, b) = ((i >> 6) as usize, 1u64 << (i & 63));
        if self.w[wi] & b != 0 {
            false
        } else {
            self.w[wi] |= b;
            true
        }
    }
    #[inline]
    fn is_empty(&self) -> bool {
        self.w.iter().all(|&x| x == 0)
    }
    fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        self.w.iter().enumerate().flat_map(|(wi, &word)| {
            let mut x = word;
            std::iter::from_fn(move || {
                if x == 0 {
                    None
                } else {
                    let t = x.trailing_zeros();
                    x &= x - 1;
                    Some((wi as u32) * 64 + t)
                }
            })
        })
    }
}

/// Error raised during execution (stack depth).
pub(crate) const REG_ETOOBIG: i32 = compile::REG_ETOOBIG;
const MAX_EXEC_DEPTH: usize = 100_000;

/// Per-execution state (Spencer's `struct vars` for regexec).
pub(crate) struct Matcher<'a> {
    re: &'a Compiled,
    s: &'a [u32],
    /// Capture positions in characters; (-1, -1) when unset.
    pub pmatch: Vec<(i64, i64)>,
    nmatch: usize,
    memo: HashMap<(u32, usize), bool>,
    depth: usize,
}

impl<'a> Matcher<'a> {
    pub(crate) fn new(re: &'a Compiled, s: &'a [u32]) -> Matcher<'a> {
        Matcher { re, s, pmatch: vec![(-1, -1); re.nsub + 1], nmatch: re.nsub + 1, memo: HashMap::default(), depth: 0 }
    }

    // ------------------------------------------------------------------
    // NFA simulation
    // ------------------------------------------------------------------

    /// Epsilon closure of `cur` at position `p` (states to expand are on
    /// `stack`).
    fn closure(&mut self, prog: &'a Prog, cur: &mut Bits, stack: &mut Vec<u32>, p: usize) {
        let n = self.s.len();
        while let Some(st) = stack.pop() {
            for arc in prog.eps(st) {
                let (ok, to) = match *arc {
                    EArc::Empty(to) => (true, to),
                    EArc::Bos(to) => (p == 0, to),
                    EArc::Eos(to) => (p == n, to),
                    EArc::Ahead(set, to) => (p < n && self.re.sets[set as usize].contains(self.s[p]), to),
                    EArc::Behind(set, to) => (p > 0 && self.re.sets[set as usize].contains(self.s[p - 1]), to),
                    EArc::Lacon(k, to) => {
                        if cur.has(to) {
                            continue;
                        }
                        (self.lacon(k, p), to)
                    }
                };
                if ok && cur.set(to) {
                    stack.push(to);
                }
            }
        }
    }

    /// Consume character at position `p` from `cur` into `next`; targets
    /// are pushed on `stack` for closure.
    #[inline]
    fn step(&self, prog: &Prog, cur: &Bits, next: &mut Bits, stack: &mut Vec<u32>, p: usize) {
        next.clear();
        let c = self.s[p];
        for st in cur.iter() {
            for &(set, to) in prog.chr(st) {
                if self.re.sets[set as usize].contains(c) && next.set(to) {
                    stack.push(to);
                }
            }
        }
    }

    /// Longest match of `prog` starting at `start` and ending at or before
    /// `stop` (Spencer's longest()).
    fn prog_longest(&mut self, prog: &'a Prog, start: usize, stop: usize) -> Option<usize> {
        if let Some(d) = &prog.adfa {
            let (cm, s) = (&self.re.cm, self.s);
            let mut st = d.init_at(cm, s, start);
            let mut last = None;
            let mut p = start;
            while p < stop {
                let t = d.next(st, cm.class(s[p]));
                if t & ACC != 0 {
                    last = Some(p);
                }
                st = t & STATE_MASK;
                p += 1;
                if !d.live[st as usize] {
                    return last;
                }
            }
            if d.accepts_at(cm, s, st, stop) {
                last = Some(stop);
            }
            return last;
        }
        let words = prog.words();
        let mut cur = Bits::new(words);
        let mut next = Bits::new(words);
        let mut stack = Vec::with_capacity(16);
        cur.set(prog.start);
        stack.push(prog.start);
        self.closure(prog, &mut cur, &mut stack, start);
        let mut last = if cur.has(prog.accept) { Some(start) } else { None };
        let mut p = start;
        while p < stop {
            self.step(prog, &cur, &mut next, &mut stack, p);
            p += 1;
            self.closure(prog, &mut next, &mut stack, p);
            if next.is_empty() {
                break;
            }
            if next.has(prog.accept) {
                last = Some(p);
            }
            std::mem::swap(&mut cur, &mut next);
        }
        last
    }

    /// Shortest match of `prog` starting at `start` and ending within
    /// [min, max] (Spencer's shortest()).
    fn prog_shortest(&mut self, prog: &'a Prog, start: usize, min: usize, max: usize) -> Option<usize> {
        if let Some(d) = &prog.adfa {
            let (cm, s) = (&self.re.cm, self.s);
            let n = s.len();
            let mut st = d.init_at(cm, s, start);
            let mut p = start;
            loop {
                if p == n {
                    return if p >= min && d.accept_eos[st as usize] { Some(p) } else { None };
                }
                let t = d.next(st, cm.class(s[p]));
                if p >= min && t & ACC != 0 {
                    return Some(p);
                }
                if p >= max {
                    return None;
                }
                st = t & STATE_MASK;
                p += 1;
                if !d.live[st as usize] {
                    return None;
                }
            }
        }
        let words = prog.words();
        let mut cur = Bits::new(words);
        let mut next = Bits::new(words);
        let mut stack = Vec::with_capacity(16);
        cur.set(prog.start);
        stack.push(prog.start);
        self.closure(prog, &mut cur, &mut stack, start);
        if start >= min && cur.has(prog.accept) {
            return Some(start);
        }
        let mut p = start;
        while p < max {
            self.step(prog, &cur, &mut next, &mut stack, p);
            p += 1;
            self.closure(prog, &mut next, &mut stack, p);
            if next.is_empty() {
                return None;
            }
            if p >= min && next.has(prog.accept) {
                return Some(p);
            }
            std::mem::swap(&mut cur, &mut next);
        }
        None
    }

    /// Unanchored search for the earliest end of any match of `prog`
    /// starting at or after `from` (the "search RE" with shortest()).
    fn search_earliest_end(&mut self, prog: &'a Prog, from: usize) -> Option<usize> {
        let n = self.s.len();
        if from > n {
            return None;
        }
        if let Some(d) = &prog.sdfa {
            let (cm, s) = (&self.re.cm, self.s);
            let mut st = d.init_at(cm, s, from);
            let mut p = from;
            loop {
                if p == n {
                    return if d.accept_eos[st as usize] { Some(n) } else { None };
                }
                let t = d.next(st, cm.class(s[p]));
                if t & ACC != 0 {
                    return Some(p);
                }
                st = t & STATE_MASK;
                p += 1;
                if !d.live[st as usize] {
                    return None;
                }
            }
        }
        let words = prog.words();
        let mut cur = Bits::new(words);
        let mut next = Bits::new(words);
        let mut stack = Vec::with_capacity(16);
        cur.set(prog.start);
        stack.push(prog.start);
        self.closure(prog, &mut cur, &mut stack, from);
        let mut p = from;
        loop {
            if cur.has(prog.accept) {
                return Some(p);
            }
            if p >= n {
                return None;
            }
            self.step(prog, &cur, &mut next, &mut stack, p);
            p += 1;
            if next.set(prog.start) {
                stack.push(prog.start);
            }
            self.closure(prog, &mut next, &mut stack, p);
            std::mem::swap(&mut cur, &mut next);
        }
    }

    /// Smallest begin position (>= from) of any match of `prog`, together
    /// with the earliest position at which a match was seen. Uses
    /// leftmost-start tracking per NFA state.
    fn search_leftmost_begin(&mut self, prog: &'a Prog, from: usize) -> Option<usize> {
        let n = self.s.len();
        if from > n {
            return None;
        }
        if let (Some(sd), Some(ad)) = (&prog.sdfa, &prog.adfa) {
            let (cm, s) = (&self.re.cm, self.s);
            // earliest end E, and the last position where all threads died
            let mut st = sd.init_at(cm, s, from);
            let mut cold = from;
            let mut p = from;
            let e = loop {
                if p == n {
                    if sd.accept_eos[st as usize] {
                        break n;
                    }
                    return None;
                }
                let t = sd.next(st, cm.class(s[p]));
                if t & ACC != 0 {
                    break p;
                }
                p += 1;
                if t & RESET != 0 {
                    cold = p;
                }
                st = t & STATE_MASK;
                if !sd.live[st as usize] {
                    return None;
                }
            };
            // the leftmost begin is the first position >= cold with a match
            for b in cold..=e {
                let mut st = ad.init_at(cm, s, b);
                let mut q = b;
                loop {
                    if q == n {
                        if ad.accept_eos[st as usize] {
                            return Some(b);
                        }
                        break;
                    }
                    let t = ad.next(st, cm.class(s[q]));
                    if t & ACC != 0 {
                        return Some(b);
                    }
                    st = t & STATE_MASK;
                    q += 1;
                    if !ad.live[st as usize] {
                        break;
                    }
                }
            }
            return None; // cannot happen
        }
        const NONE: usize = usize::MAX;
        let ns = prog.nstates;
        let mut cur = vec![NONE; ns];
        let mut next = vec![NONE; ns];
        let mut stack: Vec<u32> = Vec::with_capacity(16);
        let mut best = NONE;
        let mut p = from;
        cur[prog.start as usize] = p;
        stack.push(prog.start);
        loop {
            // closure with min-start propagation
            self.closure_starts(prog, &mut cur, &mut stack, p);
            let acc = cur[prog.accept as usize];
            if acc < best {
                best = acc;
            }
            if best != NONE {
                // prune threads that cannot yield an earlier begin
                for v in cur.iter_mut() {
                    if *v >= best {
                        *v = NONE;
                    }
                }
            }
            if p >= n {
                break;
            }
            // step
            for v in next.iter_mut() {
                *v = NONE;
            }
            let c = self.s[p];
            let mut any = false;
            for st in 0..ns {
                let sv = cur[st];
                if sv == NONE {
                    continue;
                }
                for &(set, to) in prog.chr(st as u32) {
                    if self.re.sets[set as usize].contains(c) && sv < next[to as usize] {
                        next[to as usize] = sv;
                        any = true;
                    }
                }
            }
            p += 1;
            std::mem::swap(&mut cur, &mut next);
            stack.clear();
            for st in 0..ns {
                if cur[st] != NONE {
                    stack.push(st as u32);
                }
            }
            if best == NONE {
                if p < cur[prog.start as usize] {
                    cur[prog.start as usize] = p;
                    stack.push(prog.start);
                }
            } else if !any {
                break;
            }
        }
        if best == NONE { None } else { Some(best) }
    }

    fn closure_starts(&mut self, prog: &'a Prog, cur: &mut [usize], stack: &mut Vec<u32>, p: usize) {
        let n = self.s.len();
        while let Some(st) = stack.pop() {
            let sv = cur[st as usize];
            for arc in prog.eps(st) {
                let (ok, to) = match *arc {
                    EArc::Empty(to) => (true, to),
                    EArc::Bos(to) => (p == 0, to),
                    EArc::Eos(to) => (p == n, to),
                    EArc::Ahead(set, to) => (p < n && self.re.sets[set as usize].contains(self.s[p]), to),
                    EArc::Behind(set, to) => (p > 0 && self.re.sets[set as usize].contains(self.s[p - 1]), to),
                    EArc::Lacon(k, to) => {
                        if sv >= cur[to as usize] {
                            continue;
                        }
                        (self.lacon(k, p), to)
                    }
                };
                if ok && sv < cur[to as usize] {
                    cur[to as usize] = sv;
                    stack.push(to);
                }
            }
        }
    }

    /// Evaluates lookaround constraint `k` at position `p` (Spencer's lacon()).
    fn lacon(&mut self, k: u32, p: usize) -> bool {
        if let Some(&r) = self.memo.get(&(k, p)) {
            return r;
        }
        let re = self.re;
        let la = &re.lacons[k as usize];
        let found = if la.ahead {
            self.prog_shortest(&la.prog, p, p, self.s.len()).is_some()
        } else {
            self.lookbehind_match(&la.prog, p)
        };
        let r = if la.positive { found } else { !found };
        self.memo.insert((k, p), r);
        r
    }

    /// Does some match of `prog` end exactly at `p` (starting anywhere at
    /// or after the string start)? (matchuntil() with a search NFA.)
    fn lookbehind_match(&mut self, prog: &'a Prog, p: usize) -> bool {
        if let Some(d) = &prog.sdfa {
            let (cm, s) = (&self.re.cm, self.s);
            let mut st = d.init_at(cm, s, 0);
            for &c in &s[..p] {
                st = d.next(st, cm.class(c)) & STATE_MASK;
            }
            return d.accepts_at(cm, s, st, p);
        }
        let words = prog.words();
        let mut cur = Bits::new(words);
        let mut next = Bits::new(words);
        let mut stack = Vec::with_capacity(16);
        cur.set(prog.start);
        stack.push(prog.start);
        self.closure(prog, &mut cur, &mut stack, 0);
        let mut q = 0;
        while q < p {
            self.step(prog, &cur, &mut next, &mut stack, q);
            q += 1;
            if next.set(prog.start) {
                stack.push(prog.start);
            }
            self.closure(prog, &mut next, &mut stack, q);
            std::mem::swap(&mut cur, &mut next);
        }
        cur.has(prog.accept)
    }

    // ------------------------------------------------------------------
    // node-level longest/shortest including backref DFAs
    // ------------------------------------------------------------------

    fn compare(&self, a: usize, b: usize, len: usize) -> bool {
        let s = self.s;
        if self.re.icase {
            (0..len).all(|i| s[a + i] == s[b + i] || wc_tolower(s[a + i]) == wc_tolower(s[b + i]))
        } else {
            s[a..a + len] == s[b..b + len]
        }
    }

    /// dfa_backref(): exact matching of a backref node when its target
    /// capture is known.
    fn dfa_backref(&self, node: &Node, start: usize, min: usize, max: usize, shortest: bool) -> Option<usize> {
        let (so, eo) = self.pmatch[node.backno];
        if so < 0 {
            return None;
        }
        let brstring = so as usize;
        let brlen = (eo - so) as usize;
        let (backmin, backmax) = (node.min as usize, node.max);
        if brlen == 0 {
            if min == start && node.min <= node.max {
                return Some(start);
            }
            return None;
        }
        let mut minreps = if min <= start { 0 } else { (min - start - 1) / brlen + 1 };
        let mut maxreps = (max - start) / brlen;
        if minreps < backmin {
            minreps = backmin;
        }
        if backmax != DUPINF && maxreps > backmax as usize {
            maxreps = backmax as usize;
        }
        if maxreps < minreps {
            return None;
        }
        if shortest && minreps == 0 {
            return Some(start);
        }
        let mut p = start;
        let mut numreps = 0;
        while numreps < maxreps {
            if !self.compare(brstring, p, brlen) {
                break;
            }
            p += brlen;
            numreps += 1;
            if shortest && numreps >= minreps {
                break;
            }
        }
        if numreps >= minreps { Some(p) } else { None }
    }

    fn longest(&mut self, t: usize, start: usize, stop: usize) -> Option<usize> {
        let re = self.re;
        let node = &re.nodes[t];
        if node.op == b'b' && self.pmatch[node.backno].0 >= 0 {
            return self.dfa_backref(node, start, start, stop, false);
        }
        self.prog_longest(&node.prog, start, stop)
    }

    fn shortest(&mut self, t: usize, start: usize, min: usize, max: usize) -> Option<usize> {
        let re = self.re;
        let node = &re.nodes[t];
        if node.op == b'b' && self.pmatch[node.backno].0 >= 0 {
            return self.dfa_backref(node, start, min, max, true);
        }
        self.prog_shortest(&node.prog, start, min, max)
    }

    // ------------------------------------------------------------------
    // find / cfind
    // ------------------------------------------------------------------

    /// pg_regexec(): searches from `search_start`; `nmatch` is the number of
    /// match slots the caller wants (0 = existence only). On success the
    /// overall match is in pmatch[0].
    pub(crate) fn exec(&mut self, search_start: usize, nmatch: usize) -> Result<bool, i32> {
        let n = self.s.len();
        if search_start > n {
            return Ok(false);
        }
        for m in self.pmatch.iter_mut() {
            *m = (-1, -1);
        }
        self.memo.clear();
        let backref = self.re.backref;
        self.nmatch = if backref && nmatch <= self.re.nsub { self.re.nsub + 1 } else { nmatch.min(self.re.nsub + 1) };
        let st = if backref { self.cfind(search_start) } else { self.find(search_start) };
        match st {
            REG_OKAY => {
                if nmatch < self.pmatch.len() {
                    // forget slots the caller did not ask for
                    for i in nmatch.max(1)..self.pmatch.len() {
                        self.pmatch[i] = (-1, -1);
                    }
                }
                Ok(true)
            }
            REG_NOMATCH => Ok(false),
            e => Err(e),
        }
    }

    fn find(&mut self, search_start: usize) -> i32 {
        let re = self.re;
        let root = re.root;
        let shorter = re.nodes[root].flags & SHORTER != 0;
        let rprog = &re.nodes[root].prog;
        if self.nmatch == 0 {
            return if self.search_earliest_end(rprog, search_start).is_some() { REG_OKAY } else { REG_NOMATCH };
        }
        let begin = match self.search_leftmost_begin(rprog, search_start) {
            Some(b) => b,
            None => return REG_NOMATCH,
        };
        let end = if shorter {
            self.prog_shortest(rprog, begin, begin, self.s.len())
        } else {
            self.prog_longest(rprog, begin, self.s.len())
        };
        let end = match end {
            Some(e) => e,
            None => return REG_NOMATCH, // cannot happen
        };
        self.pmatch[0] = (begin as i64, end as i64);
        if self.nmatch == 1 {
            return REG_OKAY;
        }
        self.cdissect(root, begin, end)
    }

    fn cfind(&mut self, search_start: usize) -> i32 {
        let re = self.re;
        let root = re.root;
        let rprog = &re.nodes[root].prog;
        let shorter = re.nodes[root].flags & SHORTER != 0;
        let n = self.s.len();
        let mut close = search_start;
        loop {
            // Search with the search RE for match range at/beyond "close"
            let c = match self.search_earliest_end(rprog, close) {
                Some(c) => c,
                None => break,
            };
            let open = close;
            close = c;
            let mut begin = open;
            while begin <= close {
                let mut estart = begin;
                let mut estop = n;
                loop {
                    let end = if shorter {
                        self.prog_shortest(rprog, begin, estart, estop)
                    } else {
                        self.prog_longest(rprog, begin, estop)
                    };
                    let end = match end {
                        Some(e) => e,
                        None => break,
                    };
                    let er = self.cdissect(root, begin, end);
                    if er == REG_OKAY {
                        if self.nmatch > 0 {
                            self.pmatch[0] = (begin as i64, end as i64);
                        }
                        return REG_OKAY;
                    }
                    if er != REG_NOMATCH {
                        return er;
                    }
                    if shorter {
                        if end == estop {
                            break;
                        }
                        estart = end + 1;
                    } else {
                        if end == begin {
                            break;
                        }
                        estop = end - 1;
                    }
                }
                begin += 1;
            }
            close += 1;
            if close >= n {
                break;
            }
        }
        REG_NOMATCH
    }

    // ------------------------------------------------------------------
    // dissection
    // ------------------------------------------------------------------

    fn zaptreesubs(&mut self, t: usize) {
        let re = self.re;
        let node = &re.nodes[t];
        if node.capno > 0 && node.capno < self.nmatch {
            self.pmatch[node.capno] = (-1, -1);
        }
        for &c in &node.children {
            self.zaptreesubs(c);
        }
    }

    fn subset(&mut self, t: usize, begin: usize, end: usize) {
        let n = self.re.nodes[t].capno;
        if n >= self.nmatch {
            return;
        }
        self.pmatch[n] = (begin as i64, end as i64);
    }

    fn cdissect(&mut self, t: usize, begin: usize, end: usize) -> i32 {
        self.depth += 1;
        if self.depth > MAX_EXEC_DEPTH {
            self.depth -= 1;
            return REG_ETOOBIG;
        }
        let re = self.re;
        let node = &re.nodes[t];
        let er = match node.op {
            b'=' => REG_OKAY,
            b'b' => self.cbrdissect(t, begin, end),
            b'.' => {
                if re.nodes[node.children[0]].flags & SHORTER != 0 {
                    self.crevcondissect(t, begin, end)
                } else {
                    self.ccondissect(t, begin, end)
                }
            }
            b'|' => self.caltdissect(t, begin, end),
            b'*' => {
                if re.nodes[node.children[0]].flags & SHORTER != 0 {
                    self.creviterdissect(t, begin, end)
                } else {
                    self.citerdissect(t, begin, end)
                }
            }
            b'(' => self.cdissect(node.children[0], begin, end),
            _ => compile::REG_ASSERT,
        };
        self.depth -= 1;
        if node.capno > 0 && er == REG_OKAY {
            self.subset(t, begin, end);
        }
        er
    }

    fn ccondissect(&mut self, t: usize, begin: usize, end: usize) -> i32 {
        let re = self.re;
        let left = re.nodes[t].children[0];
        let right = re.nodes[t].children[1];
        let mut mid = match self.longest(left, begin, end) {
            Some(m) => m,
            None => return REG_NOMATCH,
        };
        loop {
            if self.longest(right, mid, end) == Some(end) {
                let mut er = self.cdissect(left, begin, mid);
                if er == REG_OKAY {
                    er = self.cdissect(right, mid, end);
                    if er == REG_OKAY {
                        return REG_OKAY;
                    }
                    self.zaptreesubs(left);
                }
                if er != REG_NOMATCH {
                    return er;
                }
            }
            if mid == begin {
                return REG_NOMATCH;
            }
            mid = match self.longest(left, begin, mid - 1) {
                Some(m) => m,
                None => return REG_NOMATCH,
            };
        }
    }

    fn crevcondissect(&mut self, t: usize, begin: usize, end: usize) -> i32 {
        let re = self.re;
        let left = re.nodes[t].children[0];
        let right = re.nodes[t].children[1];
        let mut mid = match self.shortest(left, begin, begin, end) {
            Some(m) => m,
            None => return REG_NOMATCH,
        };
        loop {
            if self.longest(right, mid, end) == Some(end) {
                let mut er = self.cdissect(left, begin, mid);
                if er == REG_OKAY {
                    er = self.cdissect(right, mid, end);
                    if er == REG_OKAY {
                        return REG_OKAY;
                    }
                    self.zaptreesubs(left);
                }
                if er != REG_NOMATCH {
                    return er;
                }
            }
            if mid == end {
                return REG_NOMATCH;
            }
            mid = match self.shortest(left, begin, mid + 1, end) {
                Some(m) => m,
                None => return REG_NOMATCH,
            };
        }
    }

    fn cbrdissect(&mut self, t: usize, begin: usize, end: usize) -> i32 {
        let re = self.re;
        let node = &re.nodes[t];
        let n = node.backno;
        let (min, max) = (node.min, node.max);
        let (so, eo) = self.pmatch[n];
        if so == -1 {
            return REG_NOMATCH;
        }
        let brstring = so as usize;
        let brlen = (eo - so) as usize;
        if brlen == 0 {
            if begin == end && min <= max {
                return REG_OKAY;
            }
            return REG_NOMATCH;
        }
        if begin == end {
            if min == 0 {
                return REG_OKAY;
            }
            return REG_NOMATCH;
        }
        let tlen = end - begin;
        if !tlen.is_multiple_of(brlen) {
            return REG_NOMATCH;
        }
        let numreps = tlen / brlen;
        if numreps < min as usize || (numreps > max as usize && max != DUPINF) {
            return REG_NOMATCH;
        }
        let mut p = begin;
        for _ in 0..numreps {
            if !self.compare(brstring, p, brlen) {
                return REG_NOMATCH;
            }
            p += brlen;
        }
        REG_OKAY
    }

    fn caltdissect(&mut self, t: usize, begin: usize, end: usize) -> i32 {
        let re = self.re;
        for &c in &re.nodes[t].children {
            if self.longest(c, begin, end) == Some(end) {
                let er = self.cdissect(c, begin, end);
                if er != REG_NOMATCH {
                    return er;
                }
            }
        }
        REG_NOMATCH
    }

    fn citerdissect(&mut self, t: usize, begin: usize, end: usize) -> i32 {
        let re = self.re;
        let node = &re.nodes[t];
        let child = node.children[0];
        let mut min_matches = node.min as usize;
        if node.min <= 0 {
            min_matches = 1;
        }
        let mut max_matches = end - begin;
        if node.max != DUPINF && max_matches > node.max as usize {
            max_matches = node.max as usize;
        }
        if max_matches < min_matches {
            max_matches = min_matches;
        }
        let mut endpts: Vec<usize> = vec![0; max_matches + 1];
        endpts[0] = begin;

        let mut nverified = 0usize;
        let mut k: usize = 1;
        let mut limit = end;
        'outer: while k > 0 {
            // try to find an endpoint for the k'th sub-match
            let found = self.longest(child, endpts[k - 1], limit);
            let do_backtrack;
            match found {
                None => {
                    k -= 1;
                    do_backtrack = true;
                }
                Some(e) => {
                    endpts[k] = e;
                    if nverified >= k {
                        nverified = k - 1;
                    }
                    if e != end {
                        if k >= max_matches {
                            k -= 1;
                            do_backtrack = true;
                        } else if e == endpts[k - 1]
                            && (k >= min_matches || min_matches - k < end - endpts[k])
                        {
                            do_backtrack = true;
                        } else {
                            k += 1;
                            limit = end;
                            continue 'outer;
                        }
                    } else if k < min_matches {
                        do_backtrack = true;
                    } else {
                        // verify
                        let mut i = nverified + 1;
                        while i <= k {
                            self.zaptreesubs(child);
                            let er = self.cdissect(child, endpts[i - 1], endpts[i]);
                            if er == REG_OKAY {
                                nverified = i;
                                i += 1;
                                continue;
                            }
                            if er == REG_NOMATCH {
                                break;
                            }
                            return er;
                        }
                        if i > k {
                            return REG_OKAY;
                        }
                        k = i;
                        do_backtrack = true;
                    }
                }
            }
            if do_backtrack {
                while k > 0 {
                    let prev_end = endpts[k - 1];
                    if endpts[k] > prev_end {
                        limit = endpts[k] - 1;
                        if limit > prev_end || (k < min_matches && min_matches - k >= end - prev_end) {
                            break;
                        }
                    }
                    k -= 1;
                }
            }
        }
        if node.min == 0 && begin == end {
            return REG_OKAY;
        }
        REG_NOMATCH
    }

    fn creviterdissect(&mut self, t: usize, begin: usize, end: usize) -> i32 {
        let re = self.re;
        let node = &re.nodes[t];
        let child = node.children[0];
        let mut min_matches = node.min as usize;
        if node.min <= 0 {
            if begin == end {
                return REG_OKAY;
            }
            min_matches = 1;
        }
        let mut max_matches = end - begin;
        if node.max != DUPINF && max_matches > node.max as usize {
            max_matches = node.max as usize;
        }
        if max_matches < min_matches {
            max_matches = min_matches;
        }
        let mut endpts: Vec<usize> = vec![0; max_matches + 1];
        endpts[0] = begin;
        let mut nverified = 0usize;
        let mut k: usize = 1;
        let mut limit = begin;
        'outer: while k > 0 {
            if limit == endpts[k - 1] && limit != end && (k >= min_matches || min_matches - k < end - limit) {
                limit += 1;
            }
            if k >= max_matches {
                limit = end;
            }
            let found = self.shortest(child, endpts[k - 1], limit, end);
            let do_backtrack;
            match found {
                None => {
                    k -= 1;
                    do_backtrack = true;
                }
                Some(e) => {
                    endpts[k] = e;
                    if nverified >= k {
                        nverified = k - 1;
                    }
                    if e != end {
                        if k >= max_matches {
                            k -= 1;
                            do_backtrack = true;
                        } else {
                            k += 1;
                            limit = endpts[k - 1];
                            continue 'outer;
                        }
                    } else if k < min_matches {
                        do_backtrack = true;
                    } else {
                        let mut i = nverified + 1;
                        while i <= k {
                            self.zaptreesubs(child);
                            let er = self.cdissect(child, endpts[i - 1], endpts[i]);
                            if er == REG_OKAY {
                                nverified = i;
                                i += 1;
                                continue;
                            }
                            if er == REG_NOMATCH {
                                break;
                            }
                            return er;
                        }
                        if i > k {
                            return REG_OKAY;
                        }
                        k = i;
                        do_backtrack = true;
                    }
                }
            }
            if do_backtrack {
                while k > 0 {
                    if endpts[k] < end {
                        limit = endpts[k] + 1;
                        break;
                    }
                    k -= 1;
                }
            }
        }
        REG_NOMATCH
    }
}
