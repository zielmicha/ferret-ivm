//! Eagerly built (size-capped) DFAs for sub-NFAs, used to accelerate
//! longest()/shortest()/search.
//!
//! A program qualifies when it has no lookaround-subexpression (LACON) arcs.
//! Like Spencer's DFAs, the closure over constraint arcs at a position needs
//! the previous and the next character, so a DFA state is a pair (raw set of
//! NFA states before closure, key of the previous character), and the
//! transition on the next character first closes the set in that context
//! (which also tells whether the position before the character is a match
//! end), then consumes the character. Acceptance at the end of the string is
//! a per-state flag (closure with "next" = end of string).

use super::charset::CharSet;
use super::exec::{EArc, Prog};
use super::fxhash::FxHashMap as HashMap;

/// Flag in a transition entry: the position before the consumed character
/// is an accepting position.
pub(crate) const ACC: u32 = 1 << 30;
/// Flag in a transition entry: the NFA step produced no live state (search
/// mode only; the target then holds just the fresh start).
pub(crate) const RESET: u32 = 1 << 31;
pub(crate) const STATE_MASK: u32 = (1 << 30) - 1;
const MAX_DFA_STATES: usize = 3000;
/// Large sub-NFAs (e.g. one node of a regex with thousands of captures) are
/// simulated directly; building DFAs for them would cost quadratic time.
const MAX_NFA_STATES_FOR_DFA: usize = 2000;
const MAX_DFA_ENTRIES: usize = 300_000;

/// Partition of the character space into classes that no character set of
/// the regex distinguishes (Spencer's colors).
#[derive(Clone, Debug)]
pub(crate) struct ClassMap {
    ascii: [u16; 128],
    starts: Vec<u32>,
    classes: Vec<u16>,
    pub nclasses: usize,
    pub reps: Vec<u32>,
}

impl ClassMap {
    /// Builds the partition induced by the given character sets (all sets
    /// used by any program of a regex), with a sweep over range bounds.
    pub(crate) fn build(sets: &[CharSet], used: &[u32]) -> ClassMap {
        let mut b: Vec<u32> = (0..=128u32).collect();
        for &id in used {
            for &(lo, hi) in sets[id as usize].ranges() {
                b.push(lo);
                if hi < u32::MAX {
                    b.push(hi + 1);
                }
            }
        }
        b.sort_unstable();
        b.dedup();
        // signature bit j of interval i: interval [b[i], b[i+1]) is in set used[j]
        let nw = used.len().div_ceil(64).max(1);
        let mut sig = vec![0u64; b.len() * nw];
        for (j, &id) in used.iter().enumerate() {
            let (wj, bj) = (j / 64, 1u64 << (j % 64));
            let mut i = 0;
            for &(lo, hi) in sets[id as usize].ranges() {
                // b contains lo, and every interval starting in [lo, hi] lies
                // inside the range
                while b[i] < lo {
                    i += 1;
                }
                while i < b.len() && b[i] <= hi {
                    sig[i * nw + wj] |= bj;
                    i += 1;
                }
                if i >= b.len() {
                    break;
                }
            }
        }
        let mut sigmap: HashMap<&[u64], u16> = HashMap::default();
        let mut reps: Vec<u32> = Vec::new();
        let mut ascii = [0u16; 128];
        let mut starts = Vec::new();
        let mut classes = Vec::new();
        for (i, &lo) in b.iter().enumerate() {
            let key = &sig[i * nw..(i + 1) * nw];
            let next = sigmap.len() as u16;
            let cls = *sigmap.entry(key).or_insert_with(|| {
                reps.push(lo);
                next
            });
            if lo < 128 {
                ascii[lo as usize] = cls;
            } else if classes.last() != Some(&cls) {
                starts.push(lo);
                classes.push(cls);
            }
        }
        ClassMap { ascii, starts, classes, nclasses: reps.len(), reps }
    }

    #[inline]
    pub(crate) fn class(&self, c: u32) -> usize {
        if c < 128 {
            return self.ascii[c as usize] as usize;
        }
        let idx = self.starts.partition_point(|&s| s <= c);
        // starts[0] == 128, so idx >= 1 for c >= 128
        self.classes[idx - 1] as usize
    }
}

/// A DFA for one program in one mode (anchored or search).
#[derive(Clone, Debug)]
pub(crate) struct Dfa {
    pub nclasses: usize,
    /// target state | ACC | RESET, indexed by state * nclasses + class
    pub trans: Vec<u32>,
    /// accepting if the string ends at this state's position
    pub accept_eos: Vec<bool>,
    /// some accepting position is reachable from this state
    pub live: Vec<bool>,
    /// initial state when the previous character has class k (index
    /// nclasses = beginning of string)
    pub init: Vec<u32>,
}

impl Dfa {
    #[inline]
    pub(crate) fn next(&self, st: u32, class: usize) -> u32 {
        self.trans[st as usize * self.nclasses + class]
    }

    /// Initial state for a match attempt starting at position `p` of `s`.
    #[inline]
    pub(crate) fn init_at(&self, cm: &ClassMap, s: &[u32], p: usize) -> u32 {
        if p == 0 { self.init[self.nclasses] } else { self.init[cm.class(s[p - 1])] }
    }

    /// Is position `p` (with state `st` at that position) accepting?
    #[inline]
    pub(crate) fn accepts_at(&self, cm: &ClassMap, s: &[u32], st: u32, p: usize) -> bool {
        if p == s.len() { self.accept_eos[st as usize] } else { self.next(st, cm.class(s[p])) & ACC != 0 }
    }
}

/// Context of a closure: previous character (None = beginning of string)
/// and next character (None = end of string), as class representatives.
#[derive(Clone, Copy)]
struct Ctx {
    prev: Option<u32>,
    next: Option<u32>,
}

struct Builder<'a> {
    prog: &'a Prog,
    sets: &'a [CharSet],
    words: usize,
}

impl Builder<'_> {
    fn closure(&self, set: &mut [u64], ctx: Ctx) {
        let mut stack: Vec<u32> = Vec::new();
        for (wi, &w) in set.iter().enumerate() {
            let mut x = w;
            while x != 0 {
                let t = x.trailing_zeros();
                x &= x - 1;
                stack.push(wi as u32 * 64 + t);
            }
        }
        while let Some(st) = stack.pop() {
            for arc in self.prog.eps(st) {
                let (ok, to) = match *arc {
                    EArc::Empty(to) => (true, to),
                    EArc::Bos(to) => (ctx.prev.is_none(), to),
                    EArc::Eos(to) => (ctx.next.is_none(), to),
                    EArc::Ahead(s, to) => (ctx.next.is_some_and(|c| self.sets[s as usize].contains(c)), to),
                    EArc::Behind(s, to) => (ctx.prev.is_some_and(|c| self.sets[s as usize].contains(c)), to),
                    EArc::Lacon(..) => continue,
                };
                if !ok {
                    continue;
                }
                let (wi, bit) = ((to >> 6) as usize, 1u64 << (to & 63));
                if set[wi] & bit == 0 {
                    set[wi] |= bit;
                    stack.push(to);
                }
            }
        }
    }

    fn has(&self, set: &[u64], i: u32) -> bool {
        set[(i >> 6) as usize] & (1u64 << (i & 63)) != 0
    }

    fn step(&self, set: &[u64], rep: u32) -> Vec<u64> {
        let mut out = vec![0u64; self.words];
        for (wi, &w) in set.iter().enumerate() {
            let mut x = w;
            while x != 0 {
                let t = x.trailing_zeros();
                x &= x - 1;
                let st = wi as u32 * 64 + t;
                for &(s, to) in self.prog.chr(st) {
                    if self.sets[s as usize].contains(rep) {
                        out[(to >> 6) as usize] |= 1u64 << (to & 63);
                    }
                }
            }
        }
        out
    }
}

/// Builds the DFA, or returns None if the program is not eligible or the
/// DFA would be too large.
pub(crate) fn build(prog: &Prog, sets: &[CharSet], cm: &ClassMap, search: bool) -> Option<Dfa> {
    if prog.nstates > MAX_NFA_STATES_FOR_DFA || prog.all_eps().iter().any(|a| matches!(a, EArc::Lacon(..))) {
        return None;
    }
    let words = prog.nstates.div_ceil(64).max(1);
    let b = Builder { prog, sets, words };
    let nclasses = cm.nclasses.max(1);

    // The previous character only matters through BEHIND constraints and
    // BOS; classes with the same behind-signature share a key.
    let behind_sets: Vec<u32> = {
        let mut v: Vec<u32> =
            prog.all_eps().iter().filter_map(|a| if let EArc::Behind(s, _) = *a { Some(s) } else { None }).collect();
        v.sort_unstable();
        v.dedup();
        v
    };
    let has_bos = prog.all_eps().iter().any(|a| matches!(a, EArc::Bos(_)));
    let mut keymap: HashMap<Vec<bool>, u32> = HashMap::default();
    let mut key_rep: Vec<Option<u32>> = Vec::new(); // representative prev for each key
    let mut prevkey: Vec<u32> = Vec::with_capacity(nclasses + 1);
    for k in 0..=nclasses {
        let (sig, rep): (Vec<bool>, Option<u32>) = if k == nclasses {
            // beginning of string: distinct only if BOS arcs exist or BEHIND
            // sets would see "no character"
            let mut sig = vec![false; behind_sets.len()];
            sig.push(has_bos || !behind_sets.is_empty());
            (sig, None)
        } else {
            let rep = cm.reps.get(k).copied().unwrap_or(0);
            let mut sig: Vec<bool> = behind_sets.iter().map(|&s| sets[s as usize].contains(rep)).collect();
            sig.push(false);
            (sig, Some(rep))
        };
        let n = keymap.len() as u32;
        let id = *keymap.entry(sig).or_insert_with(|| {
            key_rep.push(rep);
            n
        });
        prevkey.push(id);
    }

    let mut ids: HashMap<(Vec<u64>, u32), u32> = HashMap::default();
    let mut list: Vec<(Vec<u64>, u32)> = Vec::new();
    let intern = |s: (Vec<u64>, u32), ids: &mut HashMap<(Vec<u64>, u32), u32>, list: &mut Vec<(Vec<u64>, u32)>| -> u32 {
        if let Some(&id) = ids.get(&s) {
            return id;
        }
        let id = list.len() as u32;
        ids.insert(s.clone(), id);
        list.push(s);
        id
    };
    let mut start = vec![0u64; words];
    start[(prog.start >> 6) as usize] |= 1u64 << (prog.start & 63);
    let mut init = Vec::with_capacity(nclasses + 1);
    for k in 0..=nclasses {
        init.push(intern((start.clone(), prevkey[k]), &mut ids, &mut list));
    }

    let mut trans: Vec<u32> = Vec::new();
    let mut i = 0;
    while i < list.len() {
        if list.len() > MAX_DFA_STATES || list.len() * nclasses > MAX_DFA_ENTRIES {
            return None;
        }
        let (raw, pk) = list[i].clone();
        let prev = key_rep[pk as usize];
        for k in 0..nclasses {
            let rep = cm.reps.get(k).copied().unwrap_or(0);
            let mut closed = raw.clone();
            b.closure(&mut closed, Ctx { prev, next: Some(rep) });
            let acc = b.has(&closed, prog.accept);
            let mut next = b.step(&closed, rep);
            let mut flags = if acc { ACC } else { 0 };
            if search {
                if next.iter().all(|&w| w == 0) {
                    flags |= RESET;
                }
                next[(prog.start >> 6) as usize] |= 1u64 << (prog.start & 63);
            }
            let id = intern((next, prevkey[k]), &mut ids, &mut list);
            trans.push(id | flags);
        }
        i += 1;
    }
    let mut accept_eos = Vec::with_capacity(list.len());
    for (raw, pk) in &list {
        let mut e = raw.clone();
        b.closure(&mut e, Ctx { prev: key_rep[*pk as usize], next: None });
        accept_eos.push(b.has(&e, prog.accept));
    }
    // liveness: can reach a state with an accepting transition or accept_eos
    let ns = list.len();
    let mut live = vec![false; ns];
    let mut rev: Vec<Vec<u32>> = vec![Vec::new(); ns];
    let mut work = Vec::new();
    for s in 0..ns {
        let mut acc_here = accept_eos[s];
        for k in 0..nclasses {
            let t = trans[s * nclasses + k];
            if t & ACC != 0 {
                acc_here = true;
            }
            rev[(t & STATE_MASK) as usize].push(s as u32);
        }
        if acc_here {
            live[s] = true;
            work.push(s as u32);
        }
    }
    while let Some(s) = work.pop() {
        for &p in &rev[s as usize] {
            if !live[p as usize] {
                live[p as usize] = true;
                work.push(p);
            }
        }
    }
    Some(Dfa { nclasses, trans, accept_eos, live, init })
}
