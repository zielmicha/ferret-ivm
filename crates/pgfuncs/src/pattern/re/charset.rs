//! Character sets for the regex engine (replacing Spencer's colormap/cvec).
//!
//! Character classes follow PostgreSQL 17's regc_locale.c / regc_pg_locale.c
//! for the builtin collation provider (PG_REGEX_BUILTIN strategy).

use crate::unicode;
use std::sync::OnceLock;

/// Largest regex character value (Spencer's CHR_MAX).
pub(crate) const CHR_MAX: u32 = 0x7fff_fffe;

/// A set of character codes, as sorted, merged, inclusive ranges, plus an
/// ASCII bitmap for fast membership tests.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct CharSet {
    ranges: Vec<(u32, u32)>,
    ascii: [u64; 2],
}

impl CharSet {
    pub(crate) fn empty() -> CharSet {
        CharSet { ranges: Vec::new(), ascii: [0, 0] }
    }

    pub(crate) fn full() -> CharSet {
        CharSet::from_ranges(vec![(0, CHR_MAX)])
    }

    pub(crate) fn single(c: u32) -> CharSet {
        CharSet::from_ranges(vec![(c, c)])
    }

    pub(crate) fn from_ranges(mut r: Vec<(u32, u32)>) -> CharSet {
        r.retain(|&(a, b)| a <= b);
        r.sort_unstable();
        let mut out: Vec<(u32, u32)> = Vec::with_capacity(r.len());
        for (a, b) in r {
            if let Some(last) = out.last_mut()
                && a <= last.1.saturating_add(1)
            {
                if b > last.1 {
                    last.1 = b;
                }
                continue;
            }
            out.push((a, b));
        }
        let mut ascii = [0u64; 2];
        for &(a, b) in &out {
            if a > 127 {
                break;
            }
            for c in a..=b.min(127) {
                ascii[(c >> 6) as usize] |= 1u64 << (c & 63);
            }
        }
        CharSet { ranges: out, ascii }
    }

    #[inline]
    pub(crate) fn contains(&self, c: u32) -> bool {
        if c < 128 {
            return self.ascii[(c >> 6) as usize] & (1u64 << (c & 63)) != 0;
        }
        let r = &self.ranges;
        // binary search for the last range with start <= c
        let idx = r.partition_point(|&(a, _)| a <= c);
        idx > 0 && r[idx - 1].1 >= c
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    pub(crate) fn ranges(&self) -> &[(u32, u32)] {
        &self.ranges
    }

    pub(crate) fn union(&self, other: &CharSet) -> CharSet {
        let mut v = self.ranges.clone();
        v.extend_from_slice(&other.ranges);
        CharSet::from_ranges(v)
    }

    /// Complement over [0, CHR_MAX].
    pub(crate) fn complement(&self) -> CharSet {
        let mut out = Vec::with_capacity(self.ranges.len() + 1);
        let mut next = 0u32;
        let mut done = false;
        for &(a, b) in &self.ranges {
            if a > next {
                out.push((next, a - 1));
            }
            if b >= CHR_MAX {
                done = true;
                break;
            }
            next = b + 1;
        }
        if !done && next <= CHR_MAX {
            out.push((next, CHR_MAX));
        }
        CharSet::from_ranges(out)
    }
}

/// Spencer's `enum char_classes`, in classNames order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CClass {
    Alnum = 0,
    Alpha,
    Ascii,
    Blank,
    Cntrl,
    Digit,
    Graph,
    Lower,
    Print,
    Punct,
    Space,
    Upper,
    Xdigit,
    Word,
}

pub(crate) const CLASS_NAMES: [&str; 14] = [
    "alnum", "alpha", "ascii", "blank", "cntrl", "digit", "graph", "lower", "print", "punct", "space", "upper",
    "xdigit", "word",
];

impl CClass {
    pub(crate) fn from_index(i: usize) -> CClass {
        use CClass::*;
        [Alnum, Alpha, Ascii, Blank, Cntrl, Digit, Graph, Lower, Print, Punct, Space, Upper, Xdigit, Word][i]
    }
}

/// Builds the set of all code points satisfying `pred`, evaluating it once
/// per segment of constant Unicode properties.
fn build_from_pred(pred: impl Fn(char) -> bool) -> CharSet {
    let b = unicode::property_boundaries();
    let mut ranges = Vec::new();
    for w in b.windows(2) {
        let (lo, hi) = (w[0], w[1]);
        if (0xD800..0xE000).contains(&lo) {
            continue;
        }
        if let Some(c) = char::from_u32(lo)
            && pred(c)
        {
            ranges.push((lo, hi - 1));
        }
    }
    CharSet::from_ranges(ranges)
}

macro_rules! cached {
    ($name:ident, $e:expr) => {
        fn $name() -> &'static CharSet {
            static S: OnceLock<CharSet> = OnceLock::new();
            S.get_or_init(|| $e)
        }
    };
}

cached!(set_alpha, build_from_pred(unicode::is_alpha));
cached!(set_alnum, build_from_pred(|c| unicode::is_alnum(c, true)));
cached!(set_word, build_from_pred(|c| c == '_' || unicode::is_alnum(c, true)));
cached!(set_upper, build_from_pred(unicode::is_upper));
cached!(set_lower, build_from_pred(unicode::is_lower));
cached!(set_space, build_from_pred(unicode::is_space));
cached!(set_punct, build_from_pred(|c| unicode::is_punct(c, true)));
cached!(set_print, build_from_pred(unicode::is_print));
cached!(set_graph, build_from_pred(unicode::is_graph));

/// `cclasscvec`: the character set of a class. With `cases` (REG_ICASE),
/// upper and lower are remapped to alpha.
pub(crate) fn class_set(cls: CClass, cases: bool) -> CharSet {
    let cls = if cases && matches!(cls, CClass::Lower | CClass::Upper) { CClass::Alpha } else { cls };
    match cls {
        CClass::Print => set_print().clone(),
        CClass::Alnum => set_alnum().clone(),
        CClass::Alpha => set_alpha().clone(),
        CClass::Word => set_word().clone(),
        CClass::Ascii => CharSet::from_ranges(vec![(0, 0x7f)]),
        CClass::Blank => CharSet::from_ranges(vec![(b'\t' as u32, b'\t' as u32), (b' ' as u32, b' ' as u32)]),
        CClass::Cntrl => CharSet::from_ranges(vec![(0, 0x1f), (0x7f, 0x9f)]),
        CClass::Digit => CharSet::from_ranges(vec![(b'0' as u32, b'9' as u32)]),
        CClass::Punct => set_punct().clone(),
        CClass::Xdigit => CharSet::from_ranges(vec![
            (b'0' as u32, b'9' as u32),
            (b'a' as u32, b'f' as u32),
            (b'A' as u32, b'F' as u32),
        ]),
        CClass::Space => set_space().clone(),
        CClass::Lower => set_lower().clone(),
        CClass::Upper => set_upper().clone(),
        CClass::Graph => set_graph().clone(),
    }
}

/// `pg_wc_tolower` for the builtin provider (identity beyond Unicode).
#[inline]
pub(crate) fn wc_tolower(c: u32) -> u32 {
    match char::from_u32(c) {
        Some(ch) => unicode::to_lower(ch) as u32,
        None => c,
    }
}

/// `pg_wc_toupper` for the builtin provider (identity beyond Unicode).
#[inline]
pub(crate) fn wc_toupper(c: u32) -> u32 {
    match char::from_u32(c) {
        Some(ch) => unicode::to_upper(ch) as u32,
        None => c,
    }
}

/// `pg_wc_isalpha` etc. used by the lexer.
#[inline]
pub(crate) fn wc_isalpha(c: u32) -> bool {
    char::from_u32(c).is_some_and(unicode::is_alpha)
}

#[inline]
pub(crate) fn wc_isdigit(c: u32) -> bool {
    (b'0' as u32..=b'9' as u32).contains(&c)
}

#[inline]
pub(crate) fn wc_isspace(c: u32) -> bool {
    char::from_u32(c).is_some_and(unicode::is_space)
}

/// `allcases`: lower and upper case versions of `c` (note: not `c` itself).
pub(crate) fn allcases(c: u32) -> CharSet {
    let lc = wc_tolower(c);
    let uc = wc_toupper(c);
    CharSet::from_ranges(vec![(lc, lc), (uc, uc)])
}

/// Character names usable in `[.name.]` (regc_locale.c cnames).
pub(crate) static CNAMES: &[(&str, u32)] = &[
    ("NUL", 0),
    ("SOH", 1),
    ("STX", 2),
    ("ETX", 3),
    ("EOT", 4),
    ("ENQ", 5),
    ("ACK", 6),
    ("BEL", 7),
    ("alert", 7),
    ("BS", 8),
    ("backspace", 8),
    ("HT", 9),
    ("tab", 9),
    ("LF", 10),
    ("newline", 10),
    ("VT", 11),
    ("vertical-tab", 11),
    ("FF", 12),
    ("form-feed", 12),
    ("CR", 13),
    ("carriage-return", 13),
    ("SO", 14),
    ("SI", 15),
    ("DLE", 16),
    ("DC1", 17),
    ("DC2", 18),
    ("DC3", 19),
    ("DC4", 20),
    ("NAK", 21),
    ("SYN", 22),
    ("ETB", 23),
    ("CAN", 24),
    ("EM", 25),
    ("SUB", 26),
    ("ESC", 27),
    ("IS4", 28),
    ("FS", 28),
    ("IS3", 29),
    ("GS", 29),
    ("IS2", 30),
    ("RS", 30),
    ("IS1", 31),
    ("US", 31),
    ("space", 32),
    ("exclamation-mark", 33),
    ("quotation-mark", 34),
    ("number-sign", 35),
    ("dollar-sign", 36),
    ("percent-sign", 37),
    ("ampersand", 38),
    ("apostrophe", 39),
    ("left-parenthesis", 40),
    ("right-parenthesis", 41),
    ("asterisk", 42),
    ("plus-sign", 43),
    ("comma", 44),
    ("hyphen", 45),
    ("hyphen-minus", 45),
    ("period", 46),
    ("full-stop", 46),
    ("slash", 47),
    ("solidus", 47),
    ("zero", 48),
    ("one", 49),
    ("two", 50),
    ("three", 51),
    ("four", 52),
    ("five", 53),
    ("six", 54),
    ("seven", 55),
    ("eight", 56),
    ("nine", 57),
    ("colon", 58),
    ("semicolon", 59),
    ("less-than-sign", 60),
    ("equals-sign", 61),
    ("greater-than-sign", 62),
    ("question-mark", 63),
    ("commercial-at", 64),
    ("left-square-bracket", 91),
    ("backslash", 92),
    ("reverse-solidus", 92),
    ("right-square-bracket", 93),
    ("circumflex", 94),
    ("circumflex-accent", 94),
    ("underscore", 95),
    ("low-line", 95),
    ("grave-accent", 96),
    ("left-brace", 123),
    ("left-curly-bracket", 123),
    ("vertical-line", 124),
    ("right-brace", 125),
    ("right-curly-bracket", 125),
    ("tilde", 126),
    ("DEL", 127),
];
