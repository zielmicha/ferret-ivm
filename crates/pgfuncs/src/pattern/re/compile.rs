//! Regular expression compilation: a faithful port of Henry Spencer's
//! lexer (regc_lex.c) and parser (regcomp.c) as found in PostgreSQL 17,
//! together with the NFA graph operations they use (regc_nfa.c).
//!
//! The NFA optimizer, colormap and compacted-NFA machinery are not ported;
//! instead each subexpression node's sub-NFA is extracted from the graph
//! and handed to our own NFA simulator (see exec.rs), which evaluates
//! constraints with full-string context exactly like Spencer's DFAs do.
//!
//! Portions Copyright (c) 1998, 1999 Henry Spencer. All rights reserved.
//! Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group.
//! Used under the PostgreSQL License / Henry Spencer's license (see
//! PostgreSQL's src/backend/regex/COPYRIGHT).

use super::charset::{self, CClass, CharSet, CLASS_NAMES, CNAMES};

// ---- compile flags (regex.h) ----
pub(crate) const REG_EXTENDED: u32 = 0o1;
pub(crate) const REG_ADVF: u32 = 0o2;
pub(crate) const REG_ADVANCED: u32 = 0o3;
pub(crate) const REG_QUOTE: u32 = 0o4;
pub(crate) const REG_ICASE: u32 = 0o10;
pub(crate) const REG_EXPANDED: u32 = 0o40;
pub(crate) const REG_NLSTOP: u32 = 0o100;
pub(crate) const REG_NLANCH: u32 = 0o200;
pub(crate) const REG_NEWLINE: u32 = 0o300;

// ---- error codes (regex.h) ----
pub(crate) const REG_BADPAT: i32 = 2;
pub(crate) const REG_ECOLLATE: i32 = 3;
pub(crate) const REG_ECTYPE: i32 = 4;
pub(crate) const REG_EESCAPE: i32 = 5;
pub(crate) const REG_ESUBREG: i32 = 6;
pub(crate) const REG_EBRACK: i32 = 7;
pub(crate) const REG_EPAREN: i32 = 8;
pub(crate) const REG_EBRACE: i32 = 9;
pub(crate) const REG_BADBR: i32 = 10;
pub(crate) const REG_ERANGE: i32 = 11;
pub(crate) const REG_BADRPT: i32 = 13;
pub(crate) const REG_ASSERT: i32 = 15;
pub(crate) const REG_BADOPT: i32 = 18;
pub(crate) const REG_ETOOBIG: i32 = 19;

pub(crate) fn reg_error_message(code: i32) -> &'static str {
    match code {
        2 => "invalid regexp (reg version 0.8)",
        3 => "invalid collating element",
        4 => "invalid character class",
        5 => "invalid escape \\ sequence",
        6 => "invalid backreference number",
        7 => "brackets [] not balanced",
        8 => "parentheses () not balanced",
        9 => "braces {} not balanced",
        10 => "invalid repetition count(s)",
        11 => "invalid character range",
        12 => "out of memory",
        13 => "quantifier operand invalid",
        15 => "\"cannot happen\" -- you found a bug",
        16 => "invalid argument to regex function",
        17 => "character widths of regex and string differ",
        18 => "invalid embedded option",
        19 => "regular expression is too complex",
        20 => "too many colors",
        _ => "*** unknown regex error code ***",
    }
}

// ---- token types (regcomp.c) ----
const EMPTY: i32 = b'n' as i32;
const EOS: i32 = b'e' as i32;
const PLAIN: i32 = b'p' as i32;
const DIGIT: i32 = b'd' as i32;
const BACKREF: i32 = b'b' as i32;
const COLLEL: i32 = b'I' as i32;
const ECLASS: i32 = b'E' as i32;
const CCLASS: i32 = b'C' as i32;
const END: i32 = b'X' as i32;
const CCLASSS: i32 = b's' as i32;
const CCLASSC: i32 = b'c' as i32;
const RANGE: i32 = b'R' as i32;
const LACON: i32 = b'L' as i32;
pub(crate) const AHEAD: u8 = b'a';
pub(crate) const BEHIND: u8 = b'r';
const WBDRY: i32 = b'w' as i32;
const NWBDRY: i32 = b'W' as i32;
const SBEGIN: i32 = b'A' as i32;
const SEND: i32 = b'Z' as i32;

const fn tok(c: u8) -> i32 {
    c as i32
}

// arc types in the graph
pub(crate) const ARC_PLAIN: u8 = b'p';
pub(crate) const ARC_EMPTY: u8 = b'n';
pub(crate) const ARC_BOS: u8 = b'^';
pub(crate) const ARC_EOS: u8 = b'$';
pub(crate) const ARC_LACON: u8 = b'L';

// lookaround types (regguts.h)
pub(crate) const LATYPE_AHEAD_POS: u32 = 0o3;
pub(crate) const LATYPE_AHEAD_NEG: u32 = 0o2;
pub(crate) const LATYPE_BEHIND_POS: u32 = 0o1;
pub(crate) const LATYPE_BEHIND_NEG: u32 = 0o0;

// lexical contexts
const L_ERE: u8 = 1;
const L_BRE: u8 = 2;
const L_Q: u8 = 3;
const L_EBND: u8 = 4;
const L_BBND: u8 = 5;
const L_BRACK: u8 = 6;
const L_CEL: u8 = 7;
const L_ECL: u8 = 8;
const L_CCL: u8 = 9;

pub(crate) const DUPMAX: i32 = 255;
pub(crate) const DUPINF: i32 = DUPMAX + 1;

// subre flags (regguts.h)
pub(crate) const LONGER: u32 = 0o1;
pub(crate) const SHORTER: u32 = 0o2;
pub(crate) const MIXED: u32 = 0o4;
pub(crate) const CAP: u32 = 0o10;
pub(crate) const BACKR: u32 = 0o20;
pub(crate) const BRUSE: u32 = 0o40;
const UPPROP: u32 = MIXED | CAP | BACKR;

#[inline]
fn up(f: u32) -> u32 {
    (f & UPPROP) | ((f << 2) & (f << 1) & MIXED)
}
#[inline]
fn messy(f: u32) -> bool {
    f & (MIXED | CAP | BACKR) != 0
}
#[inline]
fn pref(f: u32) -> u32 {
    f & (LONGER | SHORTER)
}
#[inline]
fn pref2(f1: u32, f2: u32) -> u32 {
    if pref(f1) != 0 { pref(f1) } else { pref(f2) }
}
#[inline]
fn combine(f1: u32, f2: u32) -> u32 {
    up(f1 | f2) | pref2(f1, f2)
}

// limits approximating REG_MAX_COMPILE_SPACE
const MAX_STATES: usize = 500_000;
const MAX_ARCS: usize = 2_000_000;
// PostgreSQL limits parser recursion by C stack usage (max_stack_depth =
// 2MB). We model it with per-frame weights fitted to measurements on PG 17:
// 8178 nested groups and 13084 sequential capturing groups are the largest
// that compile (in a top-level SELECT); one more raises "regular expression
// is too complex". This is necessarily approximate for other shapes.
const FRAME_PARSE: usize = 12;
const FRAME_BRANCH: usize = 10;
const FRAME_QATOM: usize = 10;
const STACK_BUDGET: usize = 261_720;

// ---------------------------------------------------------------------------
// NFA graph
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct GArc {
    pub t: u8,
    /// set id (PLAIN/AHEAD/BEHIND), color 0/1 ('^'/'$'), lacon number (LACON)
    pub co: u32,
    pub from: u32,
    pub to: u32,
    pub alive: bool,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct GState {
    pub outs: Vec<u32>,
    pub ins: Vec<u32>,
    pub alive: bool,
}

#[derive(Default)]
pub(crate) struct Graph {
    pub states: Vec<GState>,
    pub arcs: Vec<GArc>,
    live_states: usize,
    live_arcs: usize,
    pub err: i32,
}

impl Graph {
    fn newstate(&mut self) -> u32 {
        if self.live_states >= MAX_STATES
            && self.err == 0 {
                self.err = REG_ETOOBIG;
            }
        self.states.push(GState { outs: Vec::new(), ins: Vec::new(), alive: true });
        self.live_states += 1;
        (self.states.len() - 1) as u32
    }

    fn createarc(&mut self, t: u8, co: u32, from: u32, to: u32) {
        if self.live_arcs >= MAX_ARCS {
            if self.err == 0 {
                self.err = REG_ETOOBIG;
            }
            return;
        }
        let id = self.arcs.len() as u32;
        self.arcs.push(GArc { t, co, from, to, alive: true });
        self.states[from as usize].outs.push(id);
        self.states[to as usize].ins.push(id);
        self.live_arcs += 1;
    }

    pub(crate) fn newarc(&mut self, t: u8, co: u32, from: u32, to: u32) {
        let (fo, ti) = (&self.states[from as usize].outs, &self.states[to as usize].ins);
        let dup = if fo.len() <= ti.len() {
            fo.iter().any(|&a| {
                let a = &self.arcs[a as usize];
                a.to == to && a.co == co && a.t == t
            })
        } else {
            ti.iter().any(|&a| {
                let a = &self.arcs[a as usize];
                a.from == from && a.co == co && a.t == t
            })
        };
        if !dup {
            self.createarc(t, co, from, to);
        }
    }

    fn freearc(&mut self, a: u32) {
        let (from, to) = {
            let arc = &mut self.arcs[a as usize];
            if !arc.alive {
                return;
            }
            arc.alive = false;
            (arc.from, arc.to)
        };
        let outs = &mut self.states[from as usize].outs;
        if let Some(i) = outs.iter().position(|&x| x == a) {
            outs.swap_remove(i);
        }
        let ins = &mut self.states[to as usize].ins;
        if let Some(i) = ins.iter().position(|&x| x == a) {
            ins.swap_remove(i);
        }
        self.live_arcs -= 1;
    }

    fn freestate(&mut self, s: u32) {
        let st = &mut self.states[s as usize];
        if st.alive {
            st.alive = false;
            self.live_states -= 1;
        }
    }

    fn dropstate(&mut self, s: u32) {
        while let Some(&a) = self.states[s as usize].ins.first() {
            self.freearc(a);
        }
        while let Some(&a) = self.states[s as usize].outs.first() {
            self.freearc(a);
        }
        self.freestate(s);
    }

    fn cparc(&mut self, a: u32, from: u32, to: u32) {
        let (t, co) = {
            let arc = &self.arcs[a as usize];
            (arc.t, arc.co)
        };
        self.newarc(t, co, from, to);
    }

    fn moveins(&mut self, old: u32, new: u32) {
        debug_assert!(old != new);
        let ins = self.states[old as usize].ins.clone();
        for a in ins {
            let from = self.arcs[a as usize].from;
            self.cparc(a, from, new);
            self.freearc(a);
        }
    }

    fn moveouts(&mut self, old: u32, new: u32) {
        debug_assert!(old != new);
        let outs = self.states[old as usize].outs.clone();
        for a in outs {
            let to = self.arcs[a as usize].to;
            self.cparc(a, new, to);
            self.freearc(a);
        }
    }

    fn cloneouts(&mut self, old: u32, from: u32, to: u32, t: u8) {
        let outs = self.states[old as usize].outs.clone();
        for a in outs {
            let co = self.arcs[a as usize].co;
            self.newarc(t, co, from, to);
        }
    }

    /// States reachable from `start` without expanding `stop` (which is
    /// included if reached). Order: discovery order.
    fn region(&self, start: u32, stop: u32) -> Vec<u32> {
        let mut seen = std::collections::HashSet::new();
        let mut order = Vec::new();
        let mut stack = vec![start];
        seen.insert(start);
        while let Some(s) = stack.pop() {
            order.push(s);
            if s == stop {
                continue;
            }
            for &a in &self.states[s as usize].outs {
                let to = self.arcs[a as usize].to;
                if seen.insert(to) {
                    stack.push(to);
                }
            }
        }
        order
    }

    /// `delsub`: delete the sub-NFA between lp and rp (not inclusive).
    fn delsub(&mut self, lp: u32, rp: u32) {
        debug_assert!(lp != rp);
        if self.states[lp as usize].outs.is_empty() {
            return;
        }
        let reg = self.region(lp, rp);
        for &s in &reg {
            if s == rp {
                continue;
            }
            while let Some(&a) = self.states[s as usize].outs.first() {
                self.freearc(a);
            }
        }
        for &s in &reg {
            if s != lp && s != rp && self.states[s as usize].ins.is_empty() {
                self.freestate(s);
            }
        }
    }

    /// `dupnfa`: duplicate the sub-NFA from start to stop, stringing the
    /// duplicate from `from` to `to`.
    fn dupnfa(&mut self, start: u32, stop: u32, from: u32, to: u32) {
        if start == stop {
            self.newarc(ARC_EMPTY, 0, from, to);
            return;
        }
        let reg = self.region(start, stop);
        let mut map: super::fxhash::FxHashMap<u32, u32> = Default::default();
        for &s in &reg {
            let m = if s == stop {
                to
            } else if s == start {
                from
            } else {
                self.newstate()
            };
            map.insert(s, m);
        }
        // `stop` maps to `to` even if unreachable
        map.entry(stop).or_insert(to);
        for &s in &reg {
            if s == stop {
                continue;
            }
            let outs = self.states[s as usize].outs.clone();
            let sm = map[&s];
            for a in outs {
                let tgt = self.arcs[a as usize].to;
                let tm = map[&tgt];
                self.cparc(a, sm, tm);
            }
        }
    }

    /// `removeconstraints`: replace constraint arcs in the sub-NFA by EMPTY.
    fn removeconstraints(&mut self, start: u32, stop: u32) {
        if start == stop {
            return;
        }
        let reg = self.region(start, stop);
        for &s in &reg {
            if s == stop {
                continue;
            }
            let outs = self.states[s as usize].outs.clone();
            for a in outs {
                let (t, to) = {
                    let arc = &self.arcs[a as usize];
                    (arc.t, arc.to)
                };
                match t {
                    ARC_PLAIN | ARC_EMPTY => {}
                    _ => {
                        self.newarc(ARC_EMPTY, 0, s, to);
                        self.freearc(a);
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// subre tree
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct Sub {
    pub op: u8,
    pub flags: u32,
    pub capno: i32,
    pub backno: i32,
    pub min: i32,
    pub max: i32,
    pub child: Option<usize>,
    pub sibling: Option<usize>,
    pub begin: u32,
    pub end: u32,
}

pub(crate) struct LaconDef {
    pub begin: u32,
    pub end: u32,
    pub latype: u32,
}

/// Result of parsing.
pub(crate) struct Parsed {
    pub graph: Graph,
    pub sets: Vec<CharSet>,
    pub subs: Vec<Sub>,
    pub tree: usize,
    pub lacons: Vec<LaconDef>,
    pub nsub: usize,
    pub cflags: u32,
    pub uses_backref: bool,
}

struct Parser<'a> {
    pat: &'a [u32],
    now: usize,
    stop: usize,
    err: i32,
    cflags: u32,
    lasttype: i32,
    nexttype: i32,
    nextvalue: u32,
    lexcon: u8,
    nsubexp: i32,
    subs: Vec<Option<usize>>,
    g: Graph,
    sets: Vec<CharSet>,
    set_ids: super::fxhash::FxHashMap<CharSet, u32>,
    wordchrs: Option<u32>,
    t: Vec<Sub>,
    lacons: Vec<LaconDef>,
    uses_backref: bool,
    depth: usize,
}

/// Compiles `pattern` with `cflags`; returns the parse result or a regex
/// error code.
pub(crate) fn parse_regex(pattern: &[u32], cflags: u32) -> Result<Parsed, i32> {
    let mut v = Parser {
        pat: pattern,
        now: 0,
        stop: pattern.len(),
        err: 0,
        cflags,
        lasttype: EMPTY,
        nexttype: EMPTY,
        nextvalue: 0,
        lexcon: 0,
        nsubexp: 0,
        subs: vec![None; 10],
        g: Graph::default(),
        sets: Vec::new(),
        set_ids: Default::default(),
        wordchrs: None,
        t: Vec::new(),
        lacons: Vec::new(),
        uses_backref: false,
        depth: 0,
    };
    let init = v.g.newstate();
    let fin = v.g.newstate();
    v.lexstart();
    if v.iserr() {
        return Err(v.err);
    }
    let tree = v.parse(EOS, PLAIN, init, fin);
    if v.g.err != 0 && v.err == 0 {
        v.err = v.g.err;
    }
    if v.iserr() {
        return Err(v.err);
    }
    let tree = tree.ok_or(REG_ASSERT)?;
    Ok(Parsed {
        graph: v.g,
        sets: v.sets,
        subs: v.t,
        tree,
        lacons: v.lacons,
        nsub: v.nsubexp as usize,
        cflags: v.cflags,
        uses_backref: v.uses_backref,
    })
}

impl<'a> Parser<'a> {
    // ---- error handling ----
    #[inline]
    fn iserr(&self) -> bool {
        self.err != 0 || self.g.err != 0
    }

    fn seterr(&mut self, e: i32) {
        self.nexttype = EOS;
        if self.err == 0 {
            self.err = if self.g.err != 0 { self.g.err } else { e };
        }
    }

    #[inline]
    fn see(&self, t: i32) -> bool {
        self.nexttype == t
    }

    fn eat(&mut self, t: i32) -> bool {
        self.see(t) && self.next()
    }

    fn set_id(&mut self, cs: CharSet) -> u32 {
        if let Some(&id) = self.set_ids.get(&cs) {
            return id;
        }
        let id = self.sets.len() as u32;
        self.sets.push(cs.clone());
        self.set_ids.insert(cs, id);
        id
    }

    fn emptyarc(&mut self, from: u32, to: u32) {
        self.g.newarc(ARC_EMPTY, 0, from, to);
    }

    fn newstate(&mut self) -> u32 {
        self.g.newstate()
    }

    // ---- lexer (regc_lex.c) ----
    #[inline]
    fn ateos(&self) -> bool {
        self.now >= self.stop
    }
    #[inline]
    fn have(&self, n: usize) -> bool {
        self.stop - self.now >= n
    }
    #[inline]
    fn next1(&self, c: u8) -> bool {
        !self.ateos() && self.pat[self.now] == c as u32
    }
    #[inline]
    fn next2(&self, a: u8, b: u8) -> bool {
        self.have(2) && self.pat[self.now] == a as u32 && self.pat[self.now + 1] == b as u32
    }
    #[inline]
    fn next3(&self, a: u8, b: u8, c: u8) -> bool {
        self.have(3)
            && self.pat[self.now] == a as u32
            && self.pat[self.now + 1] == b as u32
            && self.pat[self.now + 2] == c as u32
    }
    #[inline]
    fn ret(&mut self, t: i32) -> bool {
        self.nexttype = t;
        true
    }
    #[inline]
    fn retv(&mut self, t: i32, v: u32) -> bool {
        self.nexttype = t;
        self.nextvalue = v;
        true
    }
    #[inline]
    fn failw(&mut self, e: i32) -> bool {
        self.seterr(e);
        false
    }

    fn lexstart(&mut self) {
        self.prefixes();
        if self.iserr() {
            return;
        }
        if self.cflags & REG_QUOTE != 0 {
            self.lexcon = L_Q;
        } else if self.cflags & REG_EXTENDED != 0 {
            self.lexcon = L_ERE;
        } else {
            self.lexcon = L_BRE;
        }
        self.nexttype = EMPTY;
        self.next();
    }

    fn prefixes(&mut self) {
        if self.cflags & REG_QUOTE != 0 {
            return;
        }
        if self.have(4) && self.next3(b'*', b'*', b'*') {
            match self.pat[self.now + 3] {
                c if c == b'?' as u32 => {
                    self.seterr(REG_BADPAT);
                    return;
                }
                c if c == b'=' as u32 => {
                    self.cflags |= REG_QUOTE;
                    self.cflags &= !(REG_ADVANCED | REG_EXPANDED | REG_NEWLINE);
                    self.now += 4;
                    return;
                }
                c if c == b':' as u32 => {
                    self.cflags |= REG_ADVANCED;
                    self.now += 4;
                }
                _ => {
                    self.seterr(REG_BADRPT);
                    return;
                }
            }
        }
        if (self.cflags & REG_ADVANCED) != REG_ADVANCED {
            return;
        }
        if self.have(3) && self.next2(b'(', b'?') && charset::wc_isalpha(self.pat[self.now + 2]) {
            self.now += 2;
            while !self.ateos() && charset::wc_isalpha(self.pat[self.now]) {
                let c = self.pat[self.now];
                match char::from_u32(c).unwrap_or('\0') {
                    'b' => self.cflags &= !(REG_ADVANCED | REG_QUOTE),
                    'c' => self.cflags &= !REG_ICASE,
                    'e' => {
                        self.cflags |= REG_EXTENDED;
                        self.cflags &= !(REG_ADVF | REG_QUOTE);
                    }
                    'i' => self.cflags |= REG_ICASE,
                    'm' | 'n' => self.cflags |= REG_NEWLINE,
                    'p' => {
                        self.cflags |= REG_NLSTOP;
                        self.cflags &= !REG_NLANCH;
                    }
                    'q' => {
                        self.cflags |= REG_QUOTE;
                        self.cflags &= !REG_ADVANCED;
                    }
                    's' => self.cflags &= !REG_NEWLINE,
                    't' => self.cflags &= !REG_EXPANDED,
                    'w' => {
                        self.cflags &= !REG_NLSTOP;
                        self.cflags |= REG_NLANCH;
                    }
                    'x' => self.cflags |= REG_EXPANDED,
                    _ => {
                        self.seterr(REG_BADOPT);
                        return;
                    }
                }
                self.now += 1;
            }
            if !self.next1(b')') {
                self.seterr(REG_BADOPT);
                return;
            }
            self.now += 1;
            if self.cflags & REG_QUOTE != 0 {
                self.cflags &= !(REG_EXPANDED | REG_NEWLINE);
            }
        }
    }

    fn next(&mut self) -> bool {
        loop {
            if self.iserr() {
                return false;
            }
            self.lasttype = self.nexttype;

            if self.cflags & REG_EXPANDED != 0 && matches!(self.lexcon, L_ERE | L_BRE | L_EBND | L_BBND) {
                self.skip();
            }

            if self.ateos() {
                return match self.lexcon {
                    L_ERE | L_BRE | L_Q => self.ret(EOS),
                    L_EBND | L_BBND => self.failw(REG_EBRACE),
                    _ => self.failw(REG_EBRACK),
                };
            }

            let c = self.pat[self.now];
            self.now += 1;

            match self.lexcon {
                L_BRE => return self.brenext(c),
                L_ERE => {}
                L_Q => return self.retv(PLAIN, c),
                L_BBND | L_EBND => {
                    if (b'0' as u32..=b'9' as u32).contains(&c) {
                        return self.retv(DIGIT, c - b'0' as u32);
                    }
                    if c == b',' as u32 {
                        return self.ret(tok(b','));
                    }
                    if c == b'}' as u32 {
                        if self.lexcon == L_EBND {
                            self.lexcon = L_ERE;
                            if self.cflags & REG_ADVF != 0 && self.next1(b'?') {
                                self.now += 1;
                                return self.retv(tok(b'}'), 0);
                            }
                            return self.retv(tok(b'}'), 1);
                        }
                        return self.failw(REG_BADBR);
                    }
                    if c == b'\\' as u32 {
                        if self.lexcon == L_BBND && self.next1(b'}') {
                            self.now += 1;
                            self.lexcon = L_BRE;
                            return self.retv(tok(b'}'), 1);
                        }
                        return self.failw(REG_BADBR);
                    }
                    return self.failw(REG_BADBR);
                }
                L_BRACK => {
                    if c == b']' as u32 {
                        if self.lasttype == tok(b'[') {
                            return self.retv(PLAIN, c);
                        }
                        self.lexcon = if self.cflags & REG_EXTENDED != 0 { L_ERE } else { L_BRE };
                        return self.ret(tok(b']'));
                    }
                    if c == b'\\' as u32 {
                        if self.cflags & REG_ADVF == 0 {
                            return self.retv(PLAIN, c);
                        }
                        if self.ateos() {
                            return self.failw(REG_EESCAPE);
                        }
                        if !self.lexescape() {
                            return false;
                        }
                        if matches!(self.nexttype, PLAIN | CCLASSS | CCLASSC) {
                            return true;
                        }
                        return self.failw(REG_EESCAPE);
                    }
                    if c == b'-' as u32 {
                        if self.lasttype == tok(b'[') || self.next1(b']') {
                            return self.retv(PLAIN, c);
                        }
                        return self.retv(RANGE, c);
                    }
                    if c == b'[' as u32 {
                        if self.ateos() {
                            return self.failw(REG_EBRACK);
                        }
                        let c2 = self.pat[self.now];
                        self.now += 1;
                        if c2 == b'.' as u32 {
                            self.lexcon = L_CEL;
                            return self.ret(COLLEL);
                        } else if c2 == b'=' as u32 {
                            self.lexcon = L_ECL;
                            return self.ret(ECLASS);
                        } else if c2 == b':' as u32 {
                            self.lexcon = L_CCL;
                            return self.ret(CCLASS);
                        } else {
                            self.now -= 1;
                            return self.retv(PLAIN, c);
                        }
                    }
                    return self.retv(PLAIN, c);
                }
                L_CEL => {
                    if c == b'.' as u32 && self.next1(b']') {
                        self.now += 1;
                        self.lexcon = L_BRACK;
                        return self.retv(END, b'.' as u32);
                    }
                    return self.retv(PLAIN, c);
                }
                L_ECL => {
                    if c == b'=' as u32 && self.next1(b']') {
                        self.now += 1;
                        self.lexcon = L_BRACK;
                        return self.retv(END, b'=' as u32);
                    }
                    return self.retv(PLAIN, c);
                }
                L_CCL => {
                    if c == b':' as u32 && self.next1(b']') {
                        self.now += 1;
                        self.lexcon = L_BRACK;
                        return self.retv(END, b':' as u32);
                    }
                    return self.retv(PLAIN, c);
                }
                _ => {
                    self.seterr(REG_ASSERT);
                    return false;
                }
            }

            // ERE/ARE, except backslashes
            let ch = char::from_u32(c).unwrap_or('\u{0}');
            match ch {
                '|' => return self.ret(tok(b'|')),
                '*' | '+' | '?' => {
                    let t = tok(ch as u8);
                    if self.cflags & REG_ADVF != 0 && self.next1(b'?') {
                        self.now += 1;
                        return self.retv(t, 0);
                    }
                    return self.retv(t, 1);
                }
                '{' => {
                    if self.cflags & REG_EXPANDED != 0 {
                        self.skip();
                    }
                    if self.ateos() || !charset::wc_isdigit(self.pat[self.now]) {
                        return self.retv(PLAIN, c);
                    }
                    self.lexcon = L_EBND;
                    return self.ret(tok(b'{'));
                }
                '(' => {
                    if self.cflags & REG_ADVF != 0 && self.next1(b'?') {
                        self.now += 1;
                        if self.ateos() {
                            return self.failw(REG_BADRPT);
                        }
                        let c2 = self.pat[self.now];
                        self.now += 1;
                        match char::from_u32(c2).unwrap_or('\0') {
                            ':' => return self.retv(tok(b'('), 0),
                            '#' => {
                                while !self.ateos() && self.pat[self.now] != b')' as u32 {
                                    self.now += 1;
                                }
                                if !self.ateos() {
                                    self.now += 1;
                                }
                                // restore lasttype so the restart sees the
                                // same "previous token"
                                self.nexttype = self.lasttype;
                                continue;
                            }
                            '=' => return self.retv(LACON, LATYPE_AHEAD_POS),
                            '!' => return self.retv(LACON, LATYPE_AHEAD_NEG),
                            '<' => {
                                if self.ateos() {
                                    return self.failw(REG_BADRPT);
                                }
                                let c3 = self.pat[self.now];
                                self.now += 1;
                                return match char::from_u32(c3).unwrap_or('\0') {
                                    '=' => self.retv(LACON, LATYPE_BEHIND_POS),
                                    '!' => self.retv(LACON, LATYPE_BEHIND_NEG),
                                    _ => self.failw(REG_BADRPT),
                                };
                            }
                            _ => return self.failw(REG_BADRPT),
                        }
                    }
                    return self.retv(tok(b'('), 1);
                }
                ')' => return self.retv(tok(b')'), c),
                '[' => return self.lexbracket(),
                '.' => return self.ret(tok(b'.')),
                '^' => return self.ret(tok(b'^')),
                '$' => return self.ret(tok(b'$')),
                '\\' => {
                    if self.ateos() {
                        return self.failw(REG_EESCAPE);
                    }
                }
                _ => return self.retv(PLAIN, c),
            }

            // ERE/ARE backslash handling; backslash already eaten
            if self.cflags & REG_ADVF == 0 {
                let c2 = self.pat[self.now];
                self.now += 1;
                return self.retv(PLAIN, c2);
            }
            return self.lexescape();
        }
    }

    /// Bracket opening shared by the basic and extended regex lexers.
    fn lexbracket(&mut self) -> bool {
        if self.have(6)
            && self.pat[self.now] == b'[' as u32
            && self.pat[self.now + 1] == b':' as u32
            && (self.pat[self.now + 2] == b'<' as u32 || self.pat[self.now + 2] == b'>' as u32)
            && self.pat[self.now + 3] == b':' as u32
            && self.pat[self.now + 4] == b']' as u32
            && self.pat[self.now + 5] == b']' as u32
        {
            let c2 = self.pat[self.now + 2];
            self.now += 6;
            return self.ret(if c2 == b'<' as u32 { tok(b'<') } else { tok(b'>') });
        }
        self.lexcon = L_BRACK;
        if self.next1(b'^') {
            self.now += 1;
            return self.retv(tok(b'['), 0);
        }
        self.retv(tok(b'['), 1)
    }

    fn lexescape(&mut self) -> bool {
        let c = self.pat[self.now];
        self.now += 1;
        let ch = char::from_u32(c).unwrap_or('\0');
        if !ch.is_ascii_alphanumeric() {
            return self.retv(PLAIN, c);
        }
        match ch {
            'a' => self.retv(PLAIN, 0o7),
            'A' => self.ret(SBEGIN),
            'b' => self.retv(PLAIN, 8),
            'B' => self.retv(PLAIN, b'\\' as u32),
            'c' => {
                if self.ateos() {
                    return self.failw(REG_EESCAPE);
                }
                let c2 = self.pat[self.now];
                self.now += 1;
                self.retv(PLAIN, c2 & 0o37)
            }
            'd' => self.retv(CCLASSS, CClass::Digit as u32),
            'D' => self.retv(CCLASSC, CClass::Digit as u32),
            'e' => self.retv(PLAIN, 0o33),
            'f' => self.retv(PLAIN, 0x0c),
            'm' => self.ret(tok(b'<')),
            'M' => self.ret(tok(b'>')),
            'n' => self.retv(PLAIN, b'\n' as u32),
            'r' => self.retv(PLAIN, b'\r' as u32),
            's' => self.retv(CCLASSS, CClass::Space as u32),
            'S' => self.retv(CCLASSC, CClass::Space as u32),
            't' => self.retv(PLAIN, b'\t' as u32),
            'u' => {
                let c2 = self.lexdigits(16, 4, 4);
                if self.iserr() || c2 > charset::CHR_MAX {
                    return self.failw(REG_EESCAPE);
                }
                self.retv(PLAIN, c2)
            }
            'U' => {
                let c2 = self.lexdigits(16, 8, 8);
                if self.iserr() || c2 > charset::CHR_MAX {
                    return self.failw(REG_EESCAPE);
                }
                self.retv(PLAIN, c2)
            }
            'v' => self.retv(PLAIN, 0x0b),
            'w' => self.retv(CCLASSS, CClass::Word as u32),
            'W' => self.retv(CCLASSC, CClass::Word as u32),
            'x' => {
                let c2 = self.lexdigits(16, 1, 255);
                if self.iserr() || c2 > charset::CHR_MAX {
                    return self.failw(REG_EESCAPE);
                }
                self.retv(PLAIN, c2)
            }
            'y' => self.ret(WBDRY),
            'Y' => self.ret(NWBDRY),
            'Z' => self.ret(SEND),
            '1'..='9' => {
                let save = self.now;
                self.now -= 1; // put first digit back
                let c2 = self.lexdigits(10, 1, 255);
                if self.iserr() {
                    return self.failw(REG_EESCAPE);
                }
                // ugly heuristic (first test is "exactly 1 digit?")
                if self.now == save || ((c2 as i32) > 0 && (c2 as i32) <= self.nsubexp) {
                    self.uses_backref = true;
                    return self.retv(BACKREF, c2);
                }
                // not a backref after all; fall through into octal number
                self.now = save;
                self.lex_octal()
            }
            '0' => self.lex_octal(),
            _ => self.failw(REG_EESCAPE),
        }
    }

    fn lex_octal(&mut self) -> bool {
        self.now -= 1; // put first digit back
        let mut c = self.lexdigits(8, 1, 3);
        if self.iserr() {
            return self.failw(REG_EESCAPE);
        }
        if c > 0xff {
            // out of range, so we handled one digit too much
            self.now -= 1;
            c >>= 3;
        }
        self.retv(PLAIN, c)
    }

    fn lexdigits(&mut self, base: u32, minlen: usize, maxlen: usize) -> u32 {
        let mut n: u32 = 0;
        let mut len = 0;
        while len < maxlen && !self.ateos() {
            let c = self.pat[self.now];
            self.now += 1;
            let mut d: i32 = match char::from_u32(c).unwrap_or('\0') {
                ch @ '0'..='9' => ch as i32 - '0' as i32,
                'a' | 'A' => 10,
                'b' | 'B' => 11,
                'c' | 'C' => 12,
                'd' | 'D' => 13,
                'e' | 'E' => 14,
                'f' | 'F' => 15,
                _ => {
                    self.now -= 1;
                    -1
                }
            };
            if d >= base as i32 {
                self.now -= 1;
                d = -1;
            }
            if d < 0 {
                break;
            }
            n = n.wrapping_mul(base).wrapping_add(d as u32);
            len += 1;
        }
        if len < minlen {
            self.seterr(REG_EESCAPE);
        }
        n
    }

    fn brenext(&mut self, c: u32) -> bool {
        let ch = char::from_u32(c).unwrap_or('\0');
        match ch {
            '*' => {
                if self.lasttype == EMPTY || self.lasttype == tok(b'(') || self.lasttype == tok(b'^') {
                    return self.retv(PLAIN, c);
                }
                return self.retv(tok(b'*'), 1);
            }
            '[' => return self.lexbracket(),
            '.' => return self.ret(tok(b'.')),
            '^' => {
                if self.lasttype == EMPTY {
                    return self.ret(tok(b'^'));
                }
                if self.lasttype == tok(b'(') {
                    return self.ret(tok(b'^'));
                }
                return self.retv(PLAIN, c);
            }
            '$' => {
                if self.cflags & REG_EXPANDED != 0 {
                    self.skip();
                }
                if self.ateos() {
                    return self.ret(tok(b'$'));
                }
                if self.next2(b'\\', b')') {
                    return self.ret(tok(b'$'));
                }
                return self.retv(PLAIN, c);
            }
            '\\' => {}
            _ => return self.retv(PLAIN, c),
        }
        // backslash
        if self.ateos() {
            return self.failw(REG_EESCAPE);
        }
        let c2 = self.pat[self.now];
        self.now += 1;
        match char::from_u32(c2).unwrap_or('\0') {
            '{' => {
                self.lexcon = L_BBND;
                self.ret(tok(b'{'))
            }
            '(' => self.retv(tok(b'('), 1),
            ')' => self.retv(tok(b')'), c2),
            '<' => self.ret(tok(b'<')),
            '>' => self.ret(tok(b'>')),
            d @ '1'..='9' => {
                self.uses_backref = true;
                self.retv(BACKREF, d as u32 - '0' as u32)
            }
            _ => self.retv(PLAIN, c2),
        }
    }

    fn skip(&mut self) {
        loop {
            while !self.ateos() && charset::wc_isspace(self.pat[self.now]) {
                self.now += 1;
            }
            if self.ateos() || self.pat[self.now] != b'#' as u32 {
                break;
            }
            while !self.ateos() && self.pat[self.now] != b'\n' as u32 {
                self.now += 1;
            }
        }
    }

    // ---- parser (regcomp.c) ----

    fn subre(&mut self, op: u8, flags: u32, begin: u32, end: u32) -> Option<usize> {
        // PostgreSQL checks for stack overflow here (STACK_TOO_DEEP).
        if self.depth > STACK_BUDGET {
            self.seterr(REG_ETOOBIG);
            return None;
        }
        self.t.push(Sub {
            op,
            flags,
            capno: 0,
            backno: 0,
            min: 1,
            max: 1,
            child: None,
            sibling: None,
            begin,
            end,
        });
        Some(self.t.len() - 1)
    }

    fn parse(&mut self, stopper: i32, typ: i32, init: u32, fin: u32) -> Option<usize> {
        self.depth += FRAME_PARSE;
        let r = self.parse_inner(stopper, typ, init, fin);
        self.depth -= FRAME_PARSE;
        r
    }

    fn parse_inner(&mut self, stopper: i32, typ: i32, init: u32, fin: u32) -> Option<usize> {
        let mut branches = self.subre(b'|', LONGER, init, fin)?;
        let mut lastbranch: Option<usize> = None;
        loop {
            let left = self.newstate();
            let right = self.newstate();
            if self.iserr() {
                return None;
            }
            self.emptyarc(init, left);
            self.emptyarc(right, fin);
            if self.iserr() {
                return None;
            }
            let branch = self.parsebranch(stopper, typ, left, right, false)?;
            if self.iserr() {
                return None;
            }
            match lastbranch {
                Some(lb) => self.t[lb].sibling = Some(branch),
                None => self.t[branches].child = Some(branch),
            }
            let f = self.t[branches].flags | self.t[branch].flags;
            self.t[branches].flags |= up(f);
            lastbranch = Some(branch);
            if !self.eat(tok(b'|')) {
                break;
            }
        }
        if !self.see(stopper) {
            self.seterr(REG_EPAREN);
        }
        // optimize out simple cases
        if lastbranch == self.t[branches].child {
            branches = lastbranch.unwrap();
        } else if !messy(self.t[branches].flags) {
            self.t[branches].child = None;
            self.t[branches].op = b'=';
        }
        Some(branches)
    }

    fn parsebranch(&mut self, stopper: i32, typ: i32, left: u32, right: u32, partial: bool) -> Option<usize> {
        self.depth += FRAME_BRANCH;
        let r = self.parsebranch_inner(stopper, typ, left, right, partial);
        self.depth -= FRAME_BRANCH;
        r
    }

    fn parsebranch_inner(&mut self, stopper: i32, typ: i32, left: u32, right: u32, partial: bool) -> Option<usize> {
        let mut lp = left;
        let mut seencontent = false;
        let mut t = self.subre(b'=', 0, left, right)?;
        while !self.see(tok(b'|')) && !self.see(stopper) && !self.see(EOS) {
            if seencontent {
                lp = self.newstate();
                if self.iserr() {
                    return None;
                }
                self.g.moveins(right, lp);
            }
            seencontent = true;
            t = self.parseqatom(stopper, typ, lp, right, t)?;
            if self.iserr() {
                return None;
            }
        }
        if !seencontent {
            let _ = partial;
            self.emptyarc(left, right);
        }
        Some(t)
    }

    fn arcv(&mut self, t: u8, co: u32, lp: u32, rp: u32) {
        self.g.newarc(t, co, lp, rp);
    }

    fn nl_set(&mut self) -> u32 {
        self.set_id(CharSet::single(b'\n' as u32))
    }

    fn parseqatom(&mut self, stopper: i32, typ: i32, lp: u32, rp: u32, mut top: usize) -> Option<usize> {
        self.depth += FRAME_QATOM;
        let r = self.parseqatom_inner(stopper, typ, lp, rp, &mut top);
        self.depth -= FRAME_QATOM;
        r.map(|_| top)
    }

    fn parseqatom_inner(&mut self, stopper: i32, typ: i32, lp: u32, rp: u32, topref: &mut usize) -> Option<()> {
        let mut top = *topref;
        let mut atom: Option<usize> = None;
        let mut subno: i32 = 0;
        let mut atomtype = self.nexttype;

        match atomtype {
            t if t == tok(b'^') => {
                self.arcv(ARC_BOS, 1, lp, rp);
                if self.cflags & REG_NLANCH != 0 {
                    let nl = self.nl_set();
                    self.arcv(BEHIND, nl, lp, rp);
                }
                self.next();
                return Some(());
            }
            t if t == tok(b'$') => {
                self.arcv(ARC_EOS, 1, lp, rp);
                if self.cflags & REG_NLANCH != 0 {
                    let nl = self.nl_set();
                    self.arcv(AHEAD, nl, lp, rp);
                }
                self.next();
                return Some(());
            }
            SBEGIN => {
                self.arcv(ARC_BOS, 1, lp, rp);
                self.arcv(ARC_BOS, 0, lp, rp);
                self.next();
                return Some(());
            }
            SEND => {
                self.arcv(ARC_EOS, 1, lp, rp);
                self.arcv(ARC_EOS, 0, lp, rp);
                self.next();
                return Some(());
            }
            t if t == tok(b'<') => {
                self.wordchrs();
                let s = self.newstate();
                if self.iserr() {
                    return None;
                }
                self.nonword(BEHIND, lp, s);
                self.word(AHEAD, s, rp);
                self.next();
                return Some(());
            }
            t if t == tok(b'>') => {
                self.wordchrs();
                let s = self.newstate();
                if self.iserr() {
                    return None;
                }
                self.word(BEHIND, lp, s);
                self.nonword(AHEAD, s, rp);
                self.next();
                return Some(());
            }
            WBDRY => {
                self.wordchrs();
                let s = self.newstate();
                if self.iserr() {
                    return None;
                }
                self.nonword(BEHIND, lp, s);
                self.word(AHEAD, s, rp);
                let s = self.newstate();
                if self.iserr() {
                    return None;
                }
                self.word(BEHIND, lp, s);
                self.nonword(AHEAD, s, rp);
                self.next();
                return Some(());
            }
            NWBDRY => {
                self.wordchrs();
                let s = self.newstate();
                if self.iserr() {
                    return None;
                }
                self.word(BEHIND, lp, s);
                self.word(AHEAD, s, rp);
                let s = self.newstate();
                if self.iserr() {
                    return None;
                }
                self.nonword(BEHIND, lp, s);
                self.nonword(AHEAD, s, rp);
                self.next();
                return Some(());
            }
            LACON => {
                let latype = self.nextvalue;
                self.next();
                let s = self.newstate();
                let s2 = self.newstate();
                if self.iserr() {
                    return None;
                }
                let _t = self.parse(tok(b')'), LACON, s, s2);
                if self.iserr() {
                    return None;
                }
                debug_assert!(self.see(tok(b')')));
                self.next();
                self.processlacon(s, s2, latype, lp, rp);
                return Some(());
            }
            t if t == tok(b'*') || t == tok(b'+') || t == tok(b'?') || t == tok(b'{') => {
                self.seterr(REG_BADRPT);
                return Some(());
            }
            t if t == tok(b')') || t == PLAIN => {
                if t == tok(b')') && (self.cflags & REG_ADVANCED) != REG_EXTENDED {
                    self.seterr(REG_EPAREN);
                    return Some(());
                }
                let c = self.nextvalue;
                self.onechr(c, lp, rp);
                if self.iserr() {
                    return None;
                }
                self.next();
            }
            t if t == tok(b'[') => {
                if self.nextvalue == 1 {
                    self.bracket(lp, rp);
                } else {
                    self.cbracket(lp, rp);
                }
                self.next();
            }
            CCLASSS => {
                let cls = CClass::from_index(self.nextvalue as usize);
                self.charclass(cls, lp, rp);
                self.next();
            }
            CCLASSC => {
                let cls = CClass::from_index(self.nextvalue as usize);
                self.charclasscomplement(cls, lp, rp);
                self.next();
            }
            t if t == tok(b'.') => {
                let set = if self.cflags & REG_NLSTOP != 0 {
                    CharSet::single(b'\n' as u32).complement()
                } else {
                    CharSet::full()
                };
                let id = self.set_id(set);
                self.g.newarc(ARC_PLAIN, id, lp, rp);
                self.next();
            }
            t if t == tok(b'(') => {
                let cap = if typ == LACON { 0 } else { self.nextvalue };
                if cap != 0 {
                    self.nsubexp += 1;
                    subno = self.nsubexp;
                    if subno as usize >= self.subs.len() {
                        let n = subno as usize * 3 / 2 + 1;
                        self.subs.resize(n, None);
                    }
                } else {
                    atomtype = PLAIN; // something that's not '('
                }
                self.next();
                let s = self.newstate();
                let s2 = self.newstate();
                if self.iserr() {
                    return None;
                }
                self.emptyarc(lp, s);
                self.emptyarc(s2, rp);
                if self.iserr() {
                    return None;
                }
                let a = self.parse(tok(b')'), typ, s, s2);
                self.next();
                if self.iserr() {
                    return None;
                }
                let mut a = a?;
                if cap != 0 {
                    if self.t[a].capno == 0 {
                        self.t[a].flags |= CAP;
                        self.t[a].capno = subno;
                    } else {
                        let f = self.t[a].flags | CAP;
                        let t = self.subre(b'(', f, s, s2)?;
                        self.t[t].capno = subno;
                        self.t[t].child = Some(a);
                        a = t;
                    }
                    self.subs[subno as usize] = Some(a);
                }
                atom = Some(a);
            }
            BACKREF => {
                if typ == LACON {
                    self.seterr(REG_ESUBREG);
                }
                subno = self.nextvalue as i32;
                if (subno as usize) >= self.subs.len() {
                    self.seterr(REG_ESUBREG);
                }
                if self.iserr() {
                    return None;
                }
                if self.subs[subno as usize].is_none() {
                    self.seterr(REG_ESUBREG);
                }
                if self.iserr() {
                    return None;
                }
                let a = self.subre(b'b', BACKR, lp, rp)?;
                self.t[a].backno = subno;
                let target = self.subs[subno as usize].unwrap();
                self.t[target].flags |= BRUSE;
                self.emptyarc(lp, rp);
                self.next();
                atom = Some(a);
            }
            _ => {
                self.seterr(REG_ASSERT);
                return Some(());
            }
        }

        // ...and an atom may be followed by a quantifier
        let (m, n, qprefer);
        match self.nexttype {
            t if t == tok(b'*') => {
                m = 0;
                n = DUPINF;
                qprefer = if self.nextvalue != 0 { LONGER } else { SHORTER };
                self.next();
            }
            t if t == tok(b'+') => {
                m = 1;
                n = DUPINF;
                qprefer = if self.nextvalue != 0 { LONGER } else { SHORTER };
                self.next();
            }
            t if t == tok(b'?') => {
                m = 0;
                n = 1;
                qprefer = if self.nextvalue != 0 { LONGER } else { SHORTER };
                self.next();
            }
            t if t == tok(b'{') => {
                self.next();
                m = self.scannum();
                let nn;
                let qp;
                if self.eat(tok(b',')) {
                    if self.see(DIGIT) {
                        nn = self.scannum();
                    } else {
                        nn = DUPINF;
                    }
                    if m > nn {
                        self.seterr(REG_BADBR);
                        return Some(());
                    }
                    qp = if self.nextvalue != 0 { LONGER } else { SHORTER };
                } else {
                    nn = m;
                    qp = 0;
                }
                if !self.see(tok(b'}')) {
                    self.seterr(REG_BADBR);
                    return Some(());
                }
                self.next();
                n = nn;
                qprefer = qp;
            }
            _ => {
                m = 1;
                n = 1;
                qprefer = 0;
            }
        }
        if self.iserr() {
            return None;
        }

        // annoying special case:  {0} or {0,0} cancels everything
        if m == 0 && n == 0 {
            if let Some(a) = atom
                && self.t[a].flags & CAP != 0
            {
                let (ab, ae) = (self.t[a].begin, self.t[a].end);
                self.g.delsub(lp, ab);
                self.g.delsub(ae, rp);
            } else {
                self.g.delsub(lp, rp);
            }
            self.emptyarc(lp, rp);
            return Some(());
        }

        // if not a messy case, avoid hard part
        let f = self.t[top].flags | qprefer | atom.map_or(0, |a| self.t[a].flags);
        if atomtype != tok(b'(') && atomtype != BACKREF && !messy(up(f)) {
            if !(m == 1 && n == 1) {
                self.repeat(lp, rp, m, n);
            }
            self.t[top].flags = f;
            return Some(());
        }

        // hard part: something messy
        let atom = match atom {
            Some(a) => a,
            None => self.subre(b'=', 0, lp, rp)?,
        };

        if self.t[atom].begin == lp || self.t[atom].end == rp {
            let s = self.newstate();
            let s2 = self.newstate();
            if self.iserr() {
                return None;
            }
            self.g.moveouts(lp, s);
            self.g.moveins(rp, s2);
            self.t[atom].begin = s;
            self.t[atom].end = s2;
        } else {
            let (ab, ae) = (self.t[atom].begin, self.t[atom].end);
            self.g.delsub(lp, ab);
            self.g.delsub(ae, rp);
        }

        let s = self.newstate();
        if self.iserr() {
            return None;
        }
        self.emptyarc(lp, s);
        if self.iserr() {
            return None;
        }

        // break remaining subRE into x{...} and what follows
        let atomflags = self.t[atom].flags;
        let t = self.subre(b'.', combine(qprefer, atomflags), lp, rp)?;
        self.t[t].child = Some(atom);

        debug_assert!(self.t[top].op == b'=' && self.t[top].child.is_none());
        let (tf, tb) = (self.t[top].flags, self.t[top].begin);
        let pre = self.subre(b'=', tf, tb, lp)?;
        self.t[top].child = Some(pre);
        self.t[top].op = b'.';
        self.t[pre].sibling = Some(t);

        if atomtype == BACKREF {
            let (ab, ae) = (self.t[atom].begin, self.t[atom].end);
            self.g.delsub(ab, ae);
            let target = self.subs[subno as usize].unwrap();
            let (sb, se) = (self.t[target].begin, self.t[target].end);
            self.g.dupnfa(sb, se, ab, ae);
            if self.iserr() {
                return None;
            }
            self.g.removeconstraints(ab, ae);
            if self.iserr() {
                return None;
            }
        }

        
        let s2 = if atomtype == BACKREF {
            let (ab, ae) = (self.t[atom].begin, self.t[atom].end);
            self.emptyarc(s, ab);
            self.repeat(ab, ae, m, n);
            self.t[atom].min = m;
            self.t[atom].max = n;
            let af = self.t[atom].flags;
            self.t[atom].flags |= combine(qprefer, af);
            self.t[atom].end
        } else if m == 1
            && n == 1
            && (qprefer == 0
                || (atomflags & (LONGER | SHORTER | MIXED)) == 0
                || qprefer == (atomflags & (LONGER | SHORTER | MIXED)))
        {
            let ab = self.t[atom].begin;
            self.emptyarc(s, ab);
            self.t[atom].end
        } else if atomflags & (CAP | BACKR) == 0 {
            let (ab, ae) = (self.t[atom].begin, self.t[atom].end);
            self.emptyarc(s, ab);
            self.repeat(ab, ae, m, n);
            let f = combine(qprefer, atomflags);
            let t2 = self.subre(b'=', f, ab, ae)?;
            self.t[t].child = Some(t2);
            ae
        } else if m > 0 && atomflags & BACKR == 0 {
            let (ab, ae) = (self.t[atom].begin, self.t[atom].end);
            self.g.dupnfa(ab, ae, s, ab);
            self.repeat(s, ab, m - 1, if n == DUPINF { n } else { n - 1 });
            let f = combine(qprefer, atomflags);
            let t2 = self.subre(b'.', f, s, ae)?;
            let c = self.subre(b'=', pref(f), s, ab)?;
            self.t[t2].child = Some(c);
            self.t[c].sibling = Some(atom);
            self.t[t].child = Some(t2);
            ae
        } else {
            let ns2 = self.newstate();
            if self.iserr() {
                return None;
            }
            let (ab, ae) = (self.t[atom].begin, self.t[atom].end);
            self.g.moveouts(ae, ns2);
            self.g.dupnfa(ab, ae, s, ns2);
            self.repeat(s, ns2, m, n);
            let f = combine(qprefer, atomflags);
            let t2 = self.subre(b'*', f, s, ns2)?;
            self.t[t2].min = m;
            self.t[t2].max = n;
            self.t[t2].child = Some(atom);
            self.t[t].child = Some(t2);
            ns2
        };
        if self.iserr() {
            return None;
        }

        // and finally, look after that postponed recursion
        let t = self.t[self.t[top].child.unwrap()].sibling.unwrap();
        if !(self.see(tok(b'|')) || self.see(stopper) || self.see(EOS)) {
            let rest = self.parsebranch(stopper, typ, s2, rp, true)?;
            if self.iserr() {
                return None;
            }
            let tc = self.t[t].child.unwrap();
            self.t[tc].sibling = Some(rest);
            let (tfl, rfl) = (self.t[t].flags, self.t[rest].flags);
            self.t[t].flags |= combine(tfl, rfl);
            let (topfl, tfl) = (self.t[top].flags, self.t[t].flags);
            self.t[top].flags |= combine(topfl, tfl);

            let topchild = self.t[top].child.unwrap();
            if self.t[topchild].begin == self.t[topchild].end {
                self.t[top].child = self.t[t].child;
            } else {
                let tc = self.t[t].child.unwrap();
                let tcs = self.t[tc].sibling.unwrap();
                if self.t[tc].op == b'='
                    && self.t[tcs].op == b'='
                    && !messy(up(self.t[tc].flags | self.t[tcs].flags))
                {
                    self.t[t].op = b'=';
                    self.t[t].flags = combine(self.t[tc].flags, self.t[tcs].flags);
                    self.t[t].child = None;
                }
            }
        } else {
            self.emptyarc(s2, rp);
            let tc = self.t[t].child;
            let topchild = self.t[top].child.unwrap();
            self.t[topchild].sibling = tc;
            let (topfl, cfl) = (self.t[top].flags, self.t[tc.unwrap()].flags);
            self.t[top].flags |= combine(topfl, cfl);
            if self.t[topchild].begin == self.t[topchild].end {
                let nt = self.t[topchild].sibling.unwrap();
                self.t[topchild].sibling = None;
                top = nt;
            }
        }
        *topref = top;
        Some(())
    }

    fn scannum(&mut self) -> i32 {
        let mut n: i32 = 0;
        while self.see(DIGIT) && n < DUPMAX {
            n = n * 10 + self.nextvalue as i32;
            self.next();
        }
        if self.see(DIGIT) || n > DUPMAX {
            self.seterr(REG_BADBR);
            return 0;
        }
        n
    }

    fn repeat(&mut self, lp: u32, rp: u32, m: i32, n: i32) {
        const SOME: i32 = 2;
        const INF: i32 = 3;
        let reduce = |x: i32| -> i32 {
            if x == DUPINF {
                INF
            } else if x > 1 {
                SOME
            } else {
                x
            }
        };
        if self.iserr() {
            return;
        }
        let (rm, rn) = (reduce(m), reduce(n));
        match (rm, rn) {
            (0, 0) => {
                self.g.delsub(lp, rp);
                self.emptyarc(lp, rp);
            }
            (0, 1) => self.emptyarc(lp, rp),
            (0, SOME) => {
                self.repeat(lp, rp, 1, n);
                self.emptyarc(lp, rp);
            }
            (0, INF) => {
                let s = self.newstate();
                if self.iserr() {
                    return;
                }
                self.g.moveouts(lp, s);
                self.g.moveins(rp, s);
                self.emptyarc(lp, s);
                self.emptyarc(s, rp);
            }
            (1, 1) => {}
            (1, SOME) => {
                let s = self.newstate();
                if self.iserr() {
                    return;
                }
                self.g.moveouts(lp, s);
                self.g.dupnfa(s, rp, lp, s);
                self.repeat(lp, s, 1, n - 1);
                self.emptyarc(lp, s);
            }
            (1, INF) => {
                let s = self.newstate();
                let s2 = self.newstate();
                if self.iserr() {
                    return;
                }
                self.g.moveouts(lp, s);
                self.g.moveins(rp, s2);
                self.emptyarc(lp, s);
                self.emptyarc(s2, rp);
                self.emptyarc(s2, s);
            }
            (SOME, SOME) => {
                let s = self.newstate();
                if self.iserr() {
                    return;
                }
                self.g.moveouts(lp, s);
                self.g.dupnfa(s, rp, lp, s);
                self.repeat(lp, s, m - 1, n - 1);
            }
            (SOME, INF) => {
                let s = self.newstate();
                if self.iserr() {
                    return;
                }
                self.g.moveouts(lp, s);
                self.g.dupnfa(s, rp, lp, s);
                self.repeat(lp, s, m - 1, n);
            }
            _ => self.seterr(REG_ASSERT),
        }
    }

    fn icase(&self) -> bool {
        self.cflags & REG_ICASE != 0
    }

    fn add_set_arc(&mut self, set: CharSet, lp: u32, rp: u32) {
        if set.is_empty() {
            return;
        }
        let id = self.set_id(set);
        self.g.newarc(ARC_PLAIN, id, lp, rp);
    }

    fn onechr(&mut self, c: u32, lp: u32, rp: u32) {
        if !self.icase() {
            self.add_set_arc(CharSet::single(c), lp, rp);
            return;
        }
        self.add_set_arc(charset::allcases(c), lp, rp);
    }

    fn charclass(&mut self, cls: CClass, lp: u32, rp: u32) {
        let set = charset::class_set(cls, self.icase());
        self.add_set_arc(set, lp, rp);
    }

    fn charclasscomplement(&mut self, cls: CClass, lp: u32, rp: u32) {
        let set = charset::class_set(cls, self.icase()).complement();
        self.add_set_arc(set, lp, rp);
    }

    /// Union of the PLAIN arc sets leaving `s`.
    fn outset_union(&self, s: u32) -> CharSet {
        let mut u = CharSet::empty();
        for &a in &self.g.states[s as usize].outs {
            let arc = &self.g.arcs[a as usize];
            if arc.t == ARC_PLAIN {
                u = u.union(&self.sets[arc.co as usize]);
            }
        }
        u
    }

    fn colorcomplement(&mut self, t: u8, of: u32, lp: u32, rp: u32) {
        let set = self.outset_union(of).complement();
        if set.is_empty() {
            return;
        }
        let id = self.set_id(set);
        self.g.newarc(t, id, lp, rp);
    }

    fn wordchrs(&mut self) {
        if self.wordchrs.is_some() {
            return;
        }
        let cstate = self.newstate();
        if self.iserr() {
            return;
        }
        let set = charset::class_set(CClass::Word, self.icase());
        self.add_set_arc(set, cstate, cstate);
        self.wordchrs = Some(cstate);
    }

    fn nonword(&mut self, dir: u8, lp: u32, rp: u32) {
        let anchor = if dir == AHEAD { ARC_EOS } else { ARC_BOS };
        self.g.newarc(anchor, 1, lp, rp);
        self.g.newarc(anchor, 0, lp, rp);
        let w = self.wordchrs.unwrap();
        self.colorcomplement(dir, w, lp, rp);
    }

    fn word(&mut self, dir: u8, lp: u32, rp: u32) {
        let w = self.wordchrs.unwrap();
        self.g.cloneouts(w, lp, rp, dir);
    }

    fn processlacon(&mut self, begin: u32, end: u32, latype: u32, lp: u32, rp: u32) {
        let n = self.lacons.len() as u32;
        self.lacons.push(LaconDef { begin, end, latype });
        self.g.newarc(ARC_LACON, n, lp, rp);
    }

    fn bracket(&mut self, lp: u32, rp: u32) {
        let mut have_cclassc = [false; 14];
        debug_assert!(self.see(tok(b'[')));
        self.next();
        while !self.see(tok(b']')) && !self.see(EOS) {
            self.brackpart(lp, rp, &mut have_cclassc);
        }
        if self.iserr() {
            return;
        }
        for (i, &h) in have_cclassc.iter().enumerate() {
            if h {
                self.charclasscomplement(CClass::from_index(i), lp, rp);
                if self.iserr() {
                    return;
                }
            }
        }
    }

    fn cbracket(&mut self, lp: u32, rp: u32) {
        let left = self.newstate();
        let right = self.newstate();
        if self.iserr() {
            return;
        }
        self.bracket(left, right);
        if self.cflags & REG_NLSTOP != 0 {
            let nl = self.nl_set();
            self.g.newarc(ARC_PLAIN, nl, left, right);
        }
        if self.iserr() {
            return;
        }
        self.colorcomplement(ARC_PLAIN, left, lp, rp);
        if self.iserr() {
            return;
        }
        self.g.dropstate(left);
        self.g.dropstate(right);
    }

    fn brackpart(&mut self, lp: u32, rp: u32, have_cclassc: &mut [bool; 14]) {
        let startc;
        match self.nexttype {
            RANGE => {
                self.seterr(REG_ERANGE);
                return;
            }
            PLAIN => {
                startc = self.nextvalue;
                self.next();
                if !self.see(RANGE) {
                    self.onechr(startc, lp, rp);
                    return;
                }
                if self.iserr() {
                    return;
                }
            }
            COLLEL => {
                let startp = self.now;
                let endp = self.scanplain();
                if startp >= endp {
                    self.seterr(REG_ECOLLATE);
                }
                if self.iserr() {
                    return;
                }
                startc = self.element(startp, endp);
                if self.iserr() {
                    return;
                }
            }
            ECLASS => {
                let startp = self.now;
                let endp = self.scanplain();
                if startp >= endp {
                    self.seterr(REG_ECOLLATE);
                }
                if self.iserr() {
                    return;
                }
                let c = self.element(startp, endp);
                if self.iserr() {
                    return;
                }
                let set = if self.icase() { charset::allcases(c) } else { CharSet::single(c) };
                self.add_set_arc(set, lp, rp);
                return;
            }
            CCLASS => {
                let startp = self.now;
                let endp = self.scanplain();
                if startp >= endp {
                    self.seterr(REG_ECTYPE);
                }
                if self.iserr() {
                    return;
                }
                let cls = self.lookupcclass(startp, endp);
                if self.iserr() {
                    return;
                }
                self.charclass(cls, lp, rp);
                return;
            }
            CCLASSS => {
                let cls = CClass::from_index(self.nextvalue as usize);
                self.charclass(cls, lp, rp);
                self.next();
                return;
            }
            CCLASSC => {
                have_cclassc[self.nextvalue as usize] = true;
                self.next();
                return;
            }
            _ => {
                self.seterr(REG_ASSERT);
                return;
            }
        }

        let endc;
        if self.see(RANGE) {
            self.next();
            match self.nexttype {
                PLAIN | RANGE => {
                    endc = self.nextvalue;
                    self.next();
                    if self.iserr() {
                        return;
                    }
                }
                COLLEL => {
                    let startp = self.now;
                    let endp = self.scanplain();
                    if startp >= endp {
                        self.seterr(REG_ECOLLATE);
                    }
                    if self.iserr() {
                        return;
                    }
                    endc = self.element(startp, endp);
                    if self.iserr() {
                        return;
                    }
                }
                _ => {
                    self.seterr(REG_ERANGE);
                    return;
                }
            }
        } else {
            endc = startc;
        }
        let set = match self.range(startc, endc) {
            Some(s) => s,
            None => return,
        };
        self.add_set_arc(set, lp, rp);
    }

    fn scanplain(&mut self) -> usize {
        self.next();
        let mut endp = self.now;
        while self.see(PLAIN) {
            endp = self.now;
            self.next();
        }
        self.next();
        endp
    }

    /// `element`: collating element name (startp..endp is in the pattern;
    /// note endp points just after the last PLAIN char lexed).
    fn element(&mut self, startp: usize, endp: usize) -> u32 {
        let len = endp - startp;
        if len == 1 {
            return self.pat[startp];
        }
        let name = &self.pat[startp..endp];
        for &(n, code) in CNAMES {
            if n.len() == len && n.chars().zip(name.iter()).all(|(a, &b)| a as u32 == b) {
                return code;
            }
        }
        self.seterr(REG_ECOLLATE);
        0
    }

    fn lookupcclass(&mut self, startp: usize, endp: usize) -> CClass {
        let len = endp - startp;
        let name = &self.pat[startp..endp];
        for (i, n) in CLASS_NAMES.iter().enumerate() {
            if n.len() == len && n.chars().zip(name.iter()).all(|(a, &b)| a as u32 == b) {
                return CClass::from_index(i);
            }
        }
        self.seterr(REG_ECTYPE);
        CClass::Alnum
    }

    /// `range` (regc_locale.c)
    fn range(&mut self, a: u32, b: u32) -> Option<CharSet> {
        if a > b {
            self.seterr(REG_ERANGE);
            return None;
        }
        if !self.icase() {
            return Some(CharSet::from_ranges(vec![(a, b)]));
        }
        let span = (b as u64) - (a as u64) + 1;
        let chrspace: u64 = if span > 100_000 { 100_000 } else { span.max(100) };
        let mut extra: Vec<(u32, u32)> = Vec::new();
        let mut nchrs: u64 = 0;
        let hi = b.min(0x10ffff);
        let mut c = a;
        while c <= hi {
            let lc = charset::wc_tolower(c);
            if lc != c && (lc < a || lc > b) {
                if nchrs >= chrspace {
                    self.seterr(REG_ETOOBIG);
                    return None;
                }
                extra.push((lc, lc));
                nchrs += 1;
            }
            let uc = charset::wc_toupper(c);
            if uc != c && (uc < a || uc > b) {
                if nchrs >= chrspace {
                    self.seterr(REG_ETOOBIG);
                    return None;
                }
                extra.push((uc, uc));
                nchrs += 1;
            }
            if c == hi {
                break;
            }
            c += 1;
        }
        extra.push((a, b));
        Some(CharSet::from_ranges(extra))
    }
}
