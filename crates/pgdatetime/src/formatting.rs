//! to_char(), to_date() and to_timestamp(): port of the DCH (date/time)
//! parts of PostgreSQL's src/backend/utils/adt/formatting.c (PostgreSQL
//! License).  LC_TIME = C (English month/day names for the TM prefix).

use crate::core::*;
use crate::datetime::{c_isdigit, c_isspace, c_strtol};
use crate::error::*;
use crate::tz::{self, AbbrevKind, ZoneRef};
use crate::{Date, Error, Interval, Result, TimeZone, Timestamp, TimestampTz};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[allow(non_camel_case_types, clippy::upper_case_acronyms)]
enum Dch {
    A_D,
    A_M,
    AD,
    AM,
    B_C,
    BC,
    CC,
    DAY,
    DDD,
    DD,
    DY,
    Day,
    Dy,
    D,
    FF1,
    FF2,
    FF3,
    FF4,
    FF5,
    FF6,
    FX,
    HH24,
    HH12,
    HH,
    IDDD,
    ID,
    IW,
    IYYY,
    IYY,
    IY,
    I,
    J,
    MI,
    MM,
    MONTH,
    MON,
    MS,
    Month,
    Mon,
    OF,
    P_M,
    PM,
    Q,
    RM,
    SSSS,
    SS,
    TZH,
    TZM,
    TZ,
    US,
    WW,
    W,
    Y_YYY,
    YYYY,
    YYY,
    YY,
    Y,
    a_d,
    a_m,
    ad,
    am,
    b_c,
    bc,
    day,
    dy,
    month,
    mon,
    p_m,
    pm,
    rm,
    tz,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum DateMode {
    None,
    Gregorian,
    IsoWeek,
}

#[derive(Debug)]
struct KeyWord {
    name: &'static str,
    id: Dch,
    is_digit: bool,
    date_mode: DateMode,
}

macro_rules! kw {
    ($n:expr, $id:ident, $dig:expr, $m:ident) => {
        KeyWord {
            name: $n,
            id: Dch::$id,
            is_digit: $dig,
            date_mode: DateMode::$m,
        }
    };
}

/// DCH_keywords[] in the same order as PostgreSQL (order matters for the
/// sequential search).
static KEYWORDS: &[KeyWord] = &[
    kw!("A.D.", A_D, false, None),
    kw!("A.M.", A_M, false, None),
    kw!("AD", AD, false, None),
    kw!("AM", AM, false, None),
    kw!("B.C.", B_C, false, None),
    kw!("BC", BC, false, None),
    kw!("CC", CC, true, None),
    kw!("DAY", DAY, false, None),
    kw!("DDD", DDD, true, Gregorian),
    kw!("DD", DD, true, Gregorian),
    kw!("DY", DY, false, None),
    kw!("Day", Day, false, None),
    kw!("Dy", Dy, false, None),
    kw!("D", D, true, Gregorian),
    kw!("FF1", FF1, true, None),
    kw!("FF2", FF2, true, None),
    kw!("FF3", FF3, true, None),
    kw!("FF4", FF4, true, None),
    kw!("FF5", FF5, true, None),
    kw!("FF6", FF6, true, None),
    kw!("FX", FX, false, None),
    kw!("HH24", HH24, true, None),
    kw!("HH12", HH12, true, None),
    kw!("HH", HH, true, None),
    kw!("IDDD", IDDD, true, IsoWeek),
    kw!("ID", ID, true, IsoWeek),
    kw!("IW", IW, true, IsoWeek),
    kw!("IYYY", IYYY, true, IsoWeek),
    kw!("IYY", IYY, true, IsoWeek),
    kw!("IY", IY, true, IsoWeek),
    kw!("I", I, true, IsoWeek),
    kw!("J", J, true, None),
    kw!("MI", MI, true, None),
    kw!("MM", MM, true, Gregorian),
    kw!("MONTH", MONTH, false, Gregorian),
    kw!("MON", MON, false, Gregorian),
    kw!("MS", MS, true, None),
    kw!("Month", Month, false, Gregorian),
    kw!("Mon", Mon, false, Gregorian),
    kw!("OF", OF, false, None),
    kw!("P.M.", P_M, false, None),
    kw!("PM", PM, false, None),
    kw!("Q", Q, true, None),
    kw!("RM", RM, false, Gregorian),
    kw!("SSSSS", SSSS, true, None),
    kw!("SSSS", SSSS, true, None),
    kw!("SS", SS, true, None),
    kw!("TZH", TZH, false, None),
    kw!("TZM", TZM, true, None),
    kw!("TZ", TZ, false, None),
    kw!("US", US, true, None),
    kw!("WW", WW, true, Gregorian),
    kw!("W", W, true, Gregorian),
    kw!("Y,YYY", Y_YYY, true, Gregorian),
    kw!("YYYY", YYYY, true, Gregorian),
    kw!("YYY", YYY, true, Gregorian),
    kw!("YY", YY, true, Gregorian),
    kw!("Y", Y, true, Gregorian),
    kw!("a.d.", a_d, false, None),
    kw!("a.m.", a_m, false, None),
    kw!("ad", ad, false, None),
    kw!("am", am, false, None),
    kw!("b.c.", b_c, false, None),
    kw!("bc", bc, false, None),
    kw!("cc", CC, true, None),
    kw!("day", day, false, None),
    kw!("ddd", DDD, true, Gregorian),
    kw!("dd", DD, true, Gregorian),
    kw!("dy", dy, false, None),
    kw!("d", D, true, Gregorian),
    kw!("ff1", FF1, true, None),
    kw!("ff2", FF2, true, None),
    kw!("ff3", FF3, true, None),
    kw!("ff4", FF4, true, None),
    kw!("ff5", FF5, true, None),
    kw!("ff6", FF6, true, None),
    kw!("fx", FX, false, None),
    kw!("hh24", HH24, true, None),
    kw!("hh12", HH12, true, None),
    kw!("hh", HH, true, None),
    kw!("iddd", IDDD, true, IsoWeek),
    kw!("id", ID, true, IsoWeek),
    kw!("iw", IW, true, IsoWeek),
    kw!("iyyy", IYYY, true, IsoWeek),
    kw!("iyy", IYY, true, IsoWeek),
    kw!("iy", IY, true, IsoWeek),
    kw!("i", I, true, IsoWeek),
    kw!("j", J, true, None),
    kw!("mi", MI, true, None),
    kw!("mm", MM, true, Gregorian),
    kw!("month", month, false, Gregorian),
    kw!("mon", mon, false, Gregorian),
    kw!("ms", MS, true, None),
    kw!("of", OF, false, None),
    kw!("p.m.", p_m, false, None),
    kw!("pm", pm, false, None),
    kw!("q", Q, true, None),
    kw!("rm", rm, false, Gregorian),
    kw!("sssss", SSSS, true, None),
    kw!("ssss", SSSS, true, None),
    kw!("ss", SS, true, None),
    kw!("tzh", TZH, false, None),
    kw!("tzm", TZM, true, None),
    kw!("tz", tz, false, None),
    kw!("us", US, true, None),
    kw!("ww", WW, true, Gregorian),
    kw!("w", W, true, Gregorian),
    kw!("y,yyy", Y_YYY, true, Gregorian),
    kw!("yyyy", YYYY, true, Gregorian),
    kw!("yyy", YYY, true, Gregorian),
    kw!("yy", YY, true, Gregorian),
    kw!("y", Y, true, Gregorian),
];

fn index_seq_search(s: &[u8]) -> Option<&'static KeyWord> {
    let c = *s.first()?;
    let start = KEYWORDS.iter().position(|k| k.name.as_bytes()[0] == c)?;
    KEYWORDS[start..]
        .iter()
        .take_while(|k| k.name.as_bytes()[0] == c)
        .find(|k| s.starts_with(k.name.as_bytes()))
}

const S_FM: u8 = 0x01;
const S_TH: u8 = 0x02;
const S_TH_LOWER: u8 = 0x04;
const S_SP: u8 = 0x08;
const S_TM: u8 = 0x10;

#[derive(Clone, Debug)]
enum Node {
    Action(&'static KeyWord, u8),
    Char(String),
    Separator(String),
    Space(String),
}

#[inline]
fn utf8_len(b: u8) -> usize {
    if b < 0x80 {
        1
    } else if b >> 5 == 0x6 {
        2
    } else if b >> 4 == 0xE {
        3
    } else if b >> 3 == 0x1E {
        4
    } else {
        1
    }
}

fn mb_char(s: &[u8], i: usize) -> (String, usize) {
    let l = utf8_len(s[i]).min(s.len() - i);
    (String::from_utf8_lossy(&s[i..i + l]).into_owned(), l)
}

/// is_separator_char(): ASCII printable, not letter or digit
#[inline]
fn is_separator_char(c: u8) -> bool {
    c > 0x20 && c < 0x7F && !c.is_ascii_alphanumeric()
}

/// Parsed-format cache (PostgreSQL keeps a similar DCH cache).
fn cached_format(fmt: &str) -> std::rc::Rc<Vec<Node>> {
    use std::cell::RefCell;
    use std::rc::Rc;
    const ENTRIES: usize = 16;
    thread_local! {
        static CACHE: RefCell<Vec<(String, Rc<Vec<Node>>)>> = const { RefCell::new(Vec::new()) };
    }
    CACHE.with(|c| {
        let mut c = c.borrow_mut();
        if let Some(pos) = c.iter().position(|(f, _)| f == fmt) {
            let e = c.remove(pos);
            let nodes = e.1.clone();
            c.insert(0, e);
            return nodes;
        }
        let nodes = Rc::new(parse_format(fmt, false).expect("permissive format"));
        if fmt.len() <= 256 {
            if c.len() >= ENTRIES {
                c.pop();
            }
            c.insert(0, (fmt.to_string(), nodes.clone()));
        }
        nodes
    })
}

/// Parse DCH fields, optionally requiring standard datetime separators.
fn parse_format(fmt: &str, strict: bool) -> Result<Vec<Node>> {
    let s = fmt.as_bytes();
    let mut nodes = Vec::new();
    let mut i = 0;
    while i < s.len() {
        let mut suffix: u8 = 0;
        let rest = &s[i..];
        if rest.starts_with(b"FM") || rest.starts_with(b"fm") {
            suffix |= S_FM;
            i += 2;
        } else if rest.starts_with(b"TM") || rest.starts_with(b"tm") {
            suffix |= S_TM;
            i += 2;
        }
        if i < s.len()
            && let Some(k) = index_seq_search(&s[i..])
        {
            i += k.name.len();
            let rest = &s[i.min(s.len())..];
            if i < s.len() {
                if rest.starts_with(b"TH") {
                    suffix |= S_TH;
                    i += 2;
                } else if rest.starts_with(b"th") {
                    suffix |= S_TH_LOWER;
                    i += 2;
                } else if rest.starts_with(b"SP") {
                    suffix |= S_SP;
                    i += 2;
                }
            }
            nodes.push(Node::Action(k, suffix));
        } else if i < s.len() {
            if s[i] == b'"' {
                i += 1;
                while i < s.len() {
                    if s[i] == b'"' {
                        i += 1;
                        break;
                    }
                    if s[i] == b'\\' && i + 1 < s.len() {
                        i += 1;
                    }
                    let (ch, l) = mb_char(s, i);
                    nodes.push(Node::Char(ch));
                    i += l;
                }
            } else {
                if strict && !b"-./,':; ".contains(&s[i]) {
                    return Err(Error::new(
                        INVALID_DATETIME_FORMAT,
                        format!("invalid datetime format separator: \"{}\"", mb_char(s, i).0),
                    ));
                }
                if s[i] == b'\\' && i + 1 < s.len() && s[i + 1] == b'"' {
                    i += 1;
                }
                let (ch, l) = mb_char(s, i);
                let c = s[i];
                if is_separator_char(c) {
                    nodes.push(Node::Separator(ch));
                } else if c_isspace(c) {
                    nodes.push(Node::Space(ch));
                } else {
                    nodes.push(Node::Char(ch));
                }
                i += l;
            }
        }
    }
    Ok(nodes)
}

// ---------------------------------------------------------------------------
// to_char
// ---------------------------------------------------------------------------

const MONTHS_FULL: [&str; 12] = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];
const DAYS_SHORT: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
const RM_MONTHS_UPPER: [&str; 12] = [
    "XII", "XI", "X", "IX", "VIII", "VII", "VI", "V", "IV", "III", "II", "I",
];
const RM_MONTHS_LOWER: [&str; 12] = [
    "xii", "xi", "x", "ix", "viii", "vii", "vi", "v", "iv", "iii", "ii", "i",
];

/// struct fmt_tm + fsec + tzn
struct TmToChar {
    sec: i32,
    min: i32,
    hour: i64,
    mday: i32,
    mon: i32,
    year: i32,
    wday: i32,
    yday: i32,
    gmtoff: i64,
    fsec: i32,
    tzn: Option<String>,
}

fn invalid_for_interval() -> Error {
    Error::new(
        INVALID_DATETIME_FORMAT,
        "invalid format specification for an interval value",
    )
}

/// get_th() / str_numth()
fn append_th(s: &mut String, start: usize, upper: bool) -> Result<()> {
    let num = &s[start..];
    let b = num.as_bytes();
    let len = b.len();
    let last = *b.last().unwrap_or(&b' ');
    if !c_isdigit(last) {
        return Err(Error::new(
            INVALID_TEXT_REPRESENTATION,
            format!("\"{}\" is not a number", num),
        ));
    }
    let last = if len > 1 && b[len - 2] == b'1' {
        b'0'
    } else {
        last
    };
    let suf = match last {
        b'1' => "st",
        b'2' => "nd",
        b'3' => "rd",
        _ => "th",
    };
    if upper {
        s.push_str(&suf.to_ascii_uppercase());
    } else {
        s.push_str(suf);
    }
    Ok(())
}

/// asc_initcap
fn initcap(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut wasalnum = false;
    for c in s.chars() {
        let c2 = if wasalnum {
            c.to_ascii_lowercase()
        } else {
            c.to_ascii_uppercase()
        };
        wasalnum = c2.is_ascii_alphanumeric();
        out.push(c2);
    }
    out
}

#[inline]
fn adjust_year(year: i32, is_interval: bool) -> i32 {
    if is_interval {
        year
    } else if year <= 0 {
        -(year - 1)
    } else {
        year
    }
}

/// "%0*d"-style zero padded integer; width 0 means no padding.
fn pad(v: i64, width: usize) -> String {
    if width == 0 {
        v.to_string()
    } else {
        format!("{:0width$}", v, width = width)
    }
}

/// "%*s" with negative width (left justify)
fn ljust(s: &str, width: usize) -> String {
    if width == 0 || s.len() >= width {
        s.to_string()
    } else {
        format!("{:<width$}", s, width = width)
    }
}

/// DCH_to_char()
fn dch_to_char(nodes: &[Node], is_interval: bool, tm: &TmToChar) -> Result<String> {
    let mut s = String::new();
    for n in nodes {
        let (k, suffix) = match n {
            Node::Action(k, suf) => (*k, *suf),
            Node::Char(c) | Node::Separator(c) | Node::Space(c) => {
                s.push_str(c);
                continue;
            }
        };
        let fm = suffix & S_FM != 0;
        let thth = suffix & (S_TH | S_TH_LOWER) != 0;
        let th_upper = suffix & S_TH != 0;
        let tmsuf = suffix & S_TM != 0;
        let start = s.len();
        let numth = |s: &mut String| -> Result<()> {
            if thth {
                append_th(s, start, th_upper)
            } else {
                Ok(())
            }
        };
        let pm = tm.hour % HOURS_PER_DAY as i64 >= (HOURS_PER_DAY / 2) as i64;
        match k.id {
            Dch::A_M | Dch::P_M => s.push_str(if pm { "P.M." } else { "A.M." }),
            Dch::AM | Dch::PM => s.push_str(if pm { "PM" } else { "AM" }),
            Dch::a_m | Dch::p_m => s.push_str(if pm { "p.m." } else { "a.m." }),
            Dch::am | Dch::pm => s.push_str(if pm { "pm" } else { "am" }),
            Dch::HH | Dch::HH12 => {
                let w = if fm {
                    0
                } else if tm.hour >= 0 {
                    2
                } else {
                    3
                };
                let v = if tm.hour % 12 == 0 { 12 } else { tm.hour % 12 };
                s.push_str(&pad(v, w));
                numth(&mut s)?;
            }
            Dch::HH24 => {
                let w = if fm {
                    0
                } else if tm.hour >= 0 {
                    2
                } else {
                    3
                };
                s.push_str(&pad(tm.hour, w));
                numth(&mut s)?;
            }
            Dch::MI => {
                let w = if fm {
                    0
                } else if tm.min >= 0 {
                    2
                } else {
                    3
                };
                s.push_str(&pad(tm.min as i64, w));
                numth(&mut s)?;
            }
            Dch::SS => {
                let w = if fm {
                    0
                } else if tm.sec >= 0 {
                    2
                } else {
                    3
                };
                s.push_str(&pad(tm.sec as i64, w));
                numth(&mut s)?;
            }
            Dch::FF1 => {
                s.push_str(&pad((tm.fsec / 100000) as i64, 1));
                numth(&mut s)?;
            }
            Dch::FF2 => {
                s.push_str(&pad((tm.fsec / 10000) as i64, 2));
                numth(&mut s)?;
            }
            Dch::FF3 | Dch::MS => {
                s.push_str(&pad((tm.fsec / 1000) as i64, 3));
                numth(&mut s)?;
            }
            Dch::FF4 => {
                s.push_str(&pad((tm.fsec / 100) as i64, 4));
                numth(&mut s)?;
            }
            Dch::FF5 => {
                s.push_str(&pad((tm.fsec / 10) as i64, 5));
                numth(&mut s)?;
            }
            Dch::FF6 | Dch::US => {
                s.push_str(&pad(tm.fsec as i64, 6));
                numth(&mut s)?;
            }
            Dch::SSSS => {
                s.push_str(&(tm.hour * 3600 + tm.min as i64 * 60 + tm.sec as i64).to_string());
                numth(&mut s)?;
            }
            Dch::tz => {
                if is_interval {
                    return Err(invalid_for_interval());
                }
                if let Some(z) = &tm.tzn {
                    let p = z.to_ascii_lowercase();
                    if p.len() > 2 * 12 {
                        return Err(Error::new(
                            DATETIME_VALUE_OUT_OF_RANGE,
                            "time zone format value too long",
                        ));
                    }
                    s.push_str(&p);
                }
            }
            Dch::TZ => {
                if is_interval {
                    return Err(invalid_for_interval());
                }
                if let Some(z) = &tm.tzn {
                    if z.len() > 2 * 12 {
                        return Err(Error::new(
                            DATETIME_VALUE_OUT_OF_RANGE,
                            "time zone format value too long",
                        ));
                    }
                    s.push_str(z);
                }
            }
            Dch::TZH => {
                if is_interval {
                    return Err(invalid_for_interval());
                }
                let g = tm.gmtoff as i32;
                s.push(if tm.gmtoff >= 0 { '+' } else { '-' });
                s.push_str(&format!("{:02}", g.wrapping_abs() / 3600));
            }
            Dch::TZM => {
                if is_interval {
                    return Err(invalid_for_interval());
                }
                let g = tm.gmtoff as i32;
                s.push_str(&format!("{:02}", (g.wrapping_abs() % 3600) / 60));
            }
            Dch::OF => {
                if is_interval {
                    return Err(invalid_for_interval());
                }
                let g = tm.gmtoff as i32;
                s.push(if tm.gmtoff >= 0 { '+' } else { '-' });
                s.push_str(&pad(
                    (g.wrapping_abs() / 3600) as i64,
                    if fm { 0 } else { 2 },
                ));
                if g.wrapping_abs() % 3600 != 0 {
                    s.push_str(&format!(":{:02}", (g.wrapping_abs() % 3600) / 60));
                }
            }
            Dch::A_D | Dch::B_C => {
                if is_interval {
                    return Err(invalid_for_interval());
                }
                s.push_str(if tm.year <= 0 { "B.C." } else { "A.D." });
            }
            Dch::AD | Dch::BC => {
                if is_interval {
                    return Err(invalid_for_interval());
                }
                s.push_str(if tm.year <= 0 { "BC" } else { "AD" });
            }
            Dch::a_d | Dch::b_c => {
                if is_interval {
                    return Err(invalid_for_interval());
                }
                s.push_str(if tm.year <= 0 { "b.c." } else { "a.d." });
            }
            Dch::ad | Dch::bc => {
                if is_interval {
                    return Err(invalid_for_interval());
                }
                s.push_str(if tm.year <= 0 { "bc" } else { "ad" });
            }
            Dch::MONTH | Dch::Month | Dch::month => {
                if is_interval {
                    return Err(invalid_for_interval());
                }
                if tm.mon == 0 {
                    continue;
                }
                let name = MONTHS_FULL[(tm.mon - 1) as usize];
                let v = match k.id {
                    Dch::MONTH => name.to_ascii_uppercase(),
                    Dch::Month => {
                        if tmsuf {
                            initcap(name)
                        } else {
                            name.to_string()
                        }
                    }
                    _ => name.to_ascii_lowercase(),
                };
                if tmsuf {
                    s.push_str(&v);
                } else {
                    s.push_str(&ljust(&v, if fm { 0 } else { 9 }));
                }
            }
            Dch::MON | Dch::Mon | Dch::mon => {
                if is_interval {
                    return Err(invalid_for_interval());
                }
                if tm.mon == 0 {
                    continue;
                }
                let name = MONTHS[(tm.mon - 1) as usize];
                let v = match k.id {
                    Dch::MON => name.to_ascii_uppercase(),
                    Dch::Mon => {
                        if tmsuf {
                            initcap(name)
                        } else {
                            name.to_string()
                        }
                    }
                    _ => name.to_ascii_lowercase(),
                };
                s.push_str(&v);
            }
            Dch::MM => {
                let w = if fm {
                    0
                } else if tm.mon >= 0 {
                    2
                } else {
                    3
                };
                s.push_str(&pad(tm.mon as i64, w));
                numth(&mut s)?;
            }
            Dch::DAY | Dch::Day | Dch::day => {
                if is_interval {
                    return Err(invalid_for_interval());
                }
                let name = DAYS[tm.wday.rem_euclid(7) as usize];
                let v = match k.id {
                    Dch::DAY => name.to_ascii_uppercase(),
                    Dch::Day => {
                        if tmsuf {
                            initcap(name)
                        } else {
                            name.to_string()
                        }
                    }
                    _ => name.to_ascii_lowercase(),
                };
                if tmsuf {
                    s.push_str(&v);
                } else {
                    s.push_str(&ljust(&v, if fm { 0 } else { 9 }));
                }
            }
            Dch::DY | Dch::Dy | Dch::dy => {
                if is_interval {
                    return Err(invalid_for_interval());
                }
                let name = DAYS_SHORT[tm.wday.rem_euclid(7) as usize];
                let v = match k.id {
                    Dch::DY => name.to_ascii_uppercase(),
                    Dch::Dy => {
                        if tmsuf {
                            initcap(name)
                        } else {
                            name.to_string()
                        }
                    }
                    _ => name.to_ascii_lowercase(),
                };
                s.push_str(&v);
            }
            Dch::DDD | Dch::IDDD => {
                let v = if k.id == Dch::DDD {
                    tm.yday
                } else {
                    date2isoyearday(tm.year, tm.mon, tm.mday)
                };
                s.push_str(&pad(v as i64, if fm { 0 } else { 3 }));
                numth(&mut s)?;
            }
            Dch::DD => {
                s.push_str(&pad(tm.mday as i64, if fm { 0 } else { 2 }));
                numth(&mut s)?;
            }
            Dch::D => {
                if is_interval {
                    return Err(invalid_for_interval());
                }
                s.push_str(&(tm.wday + 1).to_string());
                numth(&mut s)?;
            }
            Dch::ID => {
                if is_interval {
                    return Err(invalid_for_interval());
                }
                s.push_str(&(if tm.wday == 0 { 7 } else { tm.wday }).to_string());
                numth(&mut s)?;
            }
            Dch::WW => {
                s.push_str(&pad(
                    ((tm.yday.wrapping_sub(1)) / 7 + 1) as i64,
                    if fm { 0 } else { 2 },
                ));
                numth(&mut s)?;
            }
            Dch::IW => {
                s.push_str(&pad(
                    date2isoweek(tm.year, tm.mon, tm.mday) as i64,
                    if fm { 0 } else { 2 },
                ));
                numth(&mut s)?;
            }
            Dch::Q => {
                if tm.mon == 0 {
                    continue;
                }
                s.push_str(&((tm.mon - 1) / 3 + 1).to_string());
                numth(&mut s)?;
            }
            Dch::CC => {
                let i = if is_interval {
                    tm.year / 100
                } else if tm.year > 0 {
                    (tm.year - 1) / 100 + 1
                } else {
                    tm.year / 100 - 1
                };
                if (-99..=99).contains(&i) {
                    s.push_str(&pad(
                        i as i64,
                        if fm {
                            0
                        } else if i >= 0 {
                            2
                        } else {
                            3
                        },
                    ));
                } else {
                    s.push_str(&i.to_string());
                }
                numth(&mut s)?;
            }
            Dch::Y_YYY => {
                let y = adjust_year(tm.year, is_interval);
                let i = y / 1000;
                s.push_str(&format!("{},{:03}", i, y - i * 1000));
                numth(&mut s)?;
            }
            Dch::YYYY | Dch::IYYY => {
                let y = adjust_year(tm.year, is_interval);
                let w = if fm {
                    0
                } else if y >= 0 {
                    4
                } else {
                    5
                };
                let v = if k.id == Dch::YYYY {
                    y
                } else {
                    adjust_year(date2isoyear(tm.year, tm.mon, tm.mday), is_interval)
                };
                s.push_str(&pad(v as i64, w));
                numth(&mut s)?;
            }
            Dch::YYY | Dch::IYY => {
                let y = adjust_year(tm.year, is_interval);
                let w = if fm {
                    0
                } else if y >= 0 {
                    3
                } else {
                    4
                };
                let v = if k.id == Dch::YYY {
                    y
                } else {
                    adjust_year(date2isoyear(tm.year, tm.mon, tm.mday), is_interval)
                };
                s.push_str(&pad((v % 1000) as i64, w));
                numth(&mut s)?;
            }
            Dch::YY | Dch::IY => {
                let y = adjust_year(tm.year, is_interval);
                let w = if fm {
                    0
                } else if y >= 0 {
                    2
                } else {
                    3
                };
                let v = if k.id == Dch::YY {
                    y
                } else {
                    adjust_year(date2isoyear(tm.year, tm.mon, tm.mday), is_interval)
                };
                s.push_str(&pad((v % 100) as i64, w));
                numth(&mut s)?;
            }
            Dch::Y | Dch::I => {
                let v = if k.id == Dch::Y {
                    adjust_year(tm.year, is_interval)
                } else {
                    adjust_year(date2isoyear(tm.year, tm.mon, tm.mday), is_interval)
                };
                s.push_str(&pad((v % 10) as i64, 1));
                numth(&mut s)?;
            }
            Dch::RM | Dch::rm => {
                if tm.mon == 0 && tm.year == 0 {
                    continue;
                }
                let months = if k.id == Dch::RM {
                    &RM_MONTHS_UPPER
                } else {
                    &RM_MONTHS_LOWER
                };
                let mon: usize = if tm.mon == 0 {
                    if tm.year >= 0 {
                        0
                    } else {
                        (MONTHS_PER_YEAR - 1) as usize
                    }
                } else if tm.mon < 0 {
                    (-(tm.mon + 1)) as usize
                } else {
                    (MONTHS_PER_YEAR - tm.mon) as usize
                };
                s.push_str(&ljust(months[mon.min(11)], if fm { 0 } else { 4 }));
            }
            Dch::W => {
                s.push_str(&((tm.mday.wrapping_sub(1)) / 7 + 1).to_string());
                numth(&mut s)?;
            }
            Dch::J => {
                s.push_str(&date2j(tm.year, tm.mon, tm.mday).to_string());
                numth(&mut s)?;
            }
            Dch::FX => {}
        }
    }
    Ok(s)
}

fn tm_to_char_from_tm(tm: &Tm, fsec: i32, tzn: Option<String>) -> TmToChar {
    let thisdate = date2j(tm.year, tm.mon, tm.mday);
    TmToChar {
        sec: tm.sec,
        min: tm.min,
        hour: tm.hour as i64,
        mday: tm.mday,
        mon: tm.mon,
        year: tm.year,
        wday: (thisdate + 1) % 7,
        yday: thisdate - date2j(tm.year, 1, 1) + 1,
        gmtoff: tm.gmtoff,
        fsec,
        tzn,
    }
}

/// to_char(timestamp, text); None where PostgreSQL returns NULL (infinite
/// input or empty format).
pub fn to_char_timestamp_opt(ts: Timestamp, fmt: &str) -> Result<Option<String>> {
    if fmt.is_empty() || timestamp_not_finite(ts) {
        return Ok(None);
    }
    let (mut tm, fsec) = timestamp2tm(ts)?;
    tm.gmtoff = 0;
    let t = tm_to_char_from_tm(&tm, fsec, None);
    Ok(Some(dch_to_char(&cached_format(fmt), false, &t)?))
}

pub fn to_char_timestamp(ts: Timestamp, fmt: &str) -> Result<String> {
    to_char_timestamp_opt(ts, fmt)?.ok_or_else(null_result)
}

pub fn to_char_timestamptz_opt(t: TimestampTz, fmt: &str, tz: &TimeZone) -> Result<Option<String>> {
    if fmt.is_empty() || timestamp_not_finite(t) {
        return Ok(None);
    }
    let tt = timestamp2tm_tz(t, &tz.zone_ref())?;
    let tc = tm_to_char_from_tm(&tt.tm, tt.fsec, tt.tzn.map(|z| z.as_string()));
    Ok(Some(dch_to_char(&cached_format(fmt), false, &tc)?))
}

pub fn to_char_timestamptz(t: TimestampTz, fmt: &str, tz: &TimeZone) -> Result<String> {
    to_char_timestamptz_opt(t, fmt, tz)?.ok_or_else(null_result)
}

pub fn to_char_interval_opt(iv: Interval, fmt: &str) -> Result<Option<String>> {
    if fmt.is_empty() || interval_not_finite(&iv) {
        return Ok(None);
    }
    let itm = interval2itm(iv);
    let t = TmToChar {
        sec: itm.sec,
        min: itm.min,
        hour: itm.hour,
        mday: itm.mday,
        mon: itm.mon,
        year: itm.year,
        wday: 0,
        yday: (itm.year.wrapping_mul(MONTHS_PER_YEAR).wrapping_add(itm.mon))
            .wrapping_mul(DAYS_PER_MONTH)
            .wrapping_add(itm.mday),
        gmtoff: 0,
        fsec: itm.usec,
        tzn: None,
    };
    Ok(Some(dch_to_char(&cached_format(fmt), true, &t)?))
}

pub fn to_char_interval(iv: Interval, fmt: &str) -> Result<String> {
    to_char_interval_opt(iv, fmt)?.ok_or_else(null_result)
}

// ---------------------------------------------------------------------------
// to_timestamp / to_date
// ---------------------------------------------------------------------------

#[derive(Default)]
struct TmFromChar {
    mode: Option<DateMode>,
    hh: i32,
    pm: i32,
    mi: i32,
    ss: i32,
    ssss: i32,
    d: i32,
    dd: i32,
    ddd: i32,
    mm: i32,
    ms: i32,
    year: i32,
    bc: i32,
    ww: i32,
    w: i32,
    cc: i32,
    j: i32,
    us: i32,
    yysz: i32,
    clock12: bool,
    tzsign: i32,
    tzh: i32,
    tzm: i32,
    ff: i32,
    has_tz: bool,
    gmtoffset: i32,
    tzp: Option<ZoneRef>,
    abbrev: String,
}

fn set_mode(tmfc: &mut TmFromChar, mode: DateMode) -> Result<()> {
    if mode != DateMode::None {
        match tmfc.mode {
            None | Some(DateMode::None) => tmfc.mode = Some(mode),
            Some(m) if m != mode => {
                return Err(Error::new(
                    INVALID_DATETIME_FORMAT,
                    "invalid combination of date conventions",
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

fn set_int(dest: &mut i32, value: i32, k: &KeyWord) -> Result<()> {
    if *dest != 0 && *dest != value {
        return Err(Error::new(
            INVALID_DATETIME_FORMAT,
            format!(
                "conflicting values for \"{}\" field in formatting string",
                k.name
            ),
        )
        .with_detail("This value contradicts a previous setting for the same field type."));
    }
    *dest = value;
    Ok(())
}

/// is_next_separator()
fn is_next_separator(nodes: &[Node], idx: usize) -> bool {
    if let Node::Action(_, suf) = &nodes[idx]
        && suf & (S_TH | S_TH_LOWER) != 0
    {
        return true;
    }
    let Some(next) = nodes.get(idx + 1) else {
        return true;
    };
    match next {
        Node::Action(k, _) => !k.is_digit,
        Node::Char(c) | Node::Separator(c) | Node::Space(c) => {
            let b = c.as_bytes();
            !(b.len() == 1 && c_isdigit(b[0]))
        }
    }
}

/// from_char_parse_int_len(); returns characters consumed
fn parse_int_len(
    dest: Option<&mut i32>,
    src: &[u8],
    pos: &mut usize,
    len: usize,
    nodes: &[Node],
    idx: usize,
) -> Result<usize> {
    let (k, suffix) = match &nodes[idx] {
        Node::Action(k, s) => (*k, *s),
        _ => unreachable!(),
    };
    let init = *pos;
    while *pos < src.len() && c_isspace(src[*pos]) {
        *pos += 1;
    }
    let avail = &src[*pos..];
    let copy_len = len.min(avail.len());
    let copy = &avail[..copy_len];
    let copy_str = String::from_utf8_lossy(copy);
    let result: i64;
    let erange: bool;
    if suffix & S_FM != 0 || is_next_separator(nodes, idx) {
        let (v, end, er) = c_strtol(&src[init..]);
        result = v;
        erange = er;
        *pos = init + end;
        if end == 0 {
            *pos = init;
        }
    } else {
        if copy_len < len {
            return Err(Error::new(
                INVALID_DATETIME_FORMAT,
                format!(
                    "source string too short for \"{}\" formatting field",
                    k.name
                ),
            )
            .with_detail(format!(
                "Field requires {} characters, but only {} remain.",
                len, copy_len
            )));
        }
        let (v, used, er) = c_strtol(copy);
        result = v;
        erange = er;
        if used > 0 && used < len {
            return Err(Error::new(
                INVALID_DATETIME_FORMAT,
                format!("invalid value \"{}\" for \"{}\"", copy_str, k.name),
            )
            .with_detail(format!(
                "Field requires {} characters, but only {} could be parsed.",
                len, used
            )));
        }
        *pos += used;
    }
    if *pos == init {
        return Err(Error::new(
            INVALID_DATETIME_FORMAT,
            format!("invalid value \"{}\" for \"{}\"", copy_str, k.name),
        )
        .with_detail("Value must be an integer."));
    }
    if erange || result < i32::MIN as i64 || result > i32::MAX as i64 {
        return Err(Error::new(
            DATETIME_VALUE_OUT_OF_RANGE,
            format!("value for \"{}\" in source string is out of range", k.name),
        )
        .with_detail(format!(
            "Value must be in the range {} to {}.",
            i32::MIN,
            i32::MAX
        )));
    }
    if let Some(d) = dest {
        set_int(d, result as i32, k)?;
    }
    Ok(*pos - init)
}

/// seq_search_ascii(): case-insensitive prefix match; returns (index, len)
fn seq_search(name: &[u8], array: &[&str]) -> Option<(usize, usize)> {
    if name.is_empty() {
        return None;
    }
    let firstc = name[0].to_ascii_lowercase();
    for (i, a) in array.iter().enumerate() {
        let ab = a.as_bytes();
        if ab[0].to_ascii_lowercase() != firstc {
            continue;
        }
        let mut p = 1;
        loop {
            if p == ab.len() {
                return Some((i, p));
            }
            if p >= name.len() {
                break;
            }
            if !ab[p].eq_ignore_ascii_case(&name[p]) {
                break;
            }
            p += 1;
        }
    }
    None
}

fn seq_search_field(src: &[u8], pos: &mut usize, array: &[&str], k: &KeyWord) -> Result<usize> {
    match seq_search(&src[*pos..], array) {
        Some((i, len)) => {
            *pos += len;
            Ok(i)
        }
        None => {
            let rest = &src[*pos..];
            let end = rest
                .iter()
                .position(|&c| matches!(c, b' ' | b'\t' | b'\n' | b'\r' | 0x0c))
                .unwrap_or(rest.len());
            let copy = String::from_utf8_lossy(&rest[..end]).into_owned();
            Err(Error::new(
                INVALID_DATETIME_FORMAT,
                format!("invalid value \"{}\" for \"{}\"", copy, k.name),
            )
            .with_detail("The given value did not match any of the allowed values for this field."))
        }
    }
}

fn skip_th(src: &[u8], pos: &mut usize, suffix: u8) {
    if suffix & (S_TH | S_TH_LOWER) != 0 {
        for _ in 0..2 {
            if *pos < src.len() {
                *pos += utf8_len(src[*pos]).min(src.len() - *pos);
            }
        }
    }
}

/// adjust_partial_year_to_2020()
fn adjust_partial_year_to_2020(year: i32) -> i32 {
    if year < 70 {
        year + 2000
    } else if year < 100 {
        year + 1900
    } else if year < 520 {
        year + 2000
    } else if year < 1000 {
        year + 1000
    } else {
        year
    }
}

const AMPM_STRINGS: [&str; 4] = ["am", "pm", "AM", "PM"];
const AMPM_STRINGS_LONG: [&str; 4] = ["a.m.", "p.m.", "A.M.", "P.M."];
const ADBC_STRINGS: [&str; 4] = ["ad", "bc", "AD", "BC"];
const ADBC_STRINGS_LONG: [&str; 4] = ["a.d.", "b.c.", "A.D.", "B.C."];

/// sscanf(s, "%d,%03d%n") for Y,YYY
fn scan_y_yyy(s: &[u8]) -> Option<(i32, i32, usize)> {
    let (m, e1, _) = c_strtol(s);
    if e1 == 0 {
        return None;
    }
    if s.get(e1) != Some(&b',') {
        return None;
    }
    let mut p = e1 + 1;
    // %03d: skip whitespace, then at most 3 characters (incl. sign)
    while p < s.len() && c_isspace(s[p]) {
        p += 1;
    }
    let start = p;
    let mut q = p;
    let mut width = 0;
    if q < s.len() && (s[q] == b'+' || s[q] == b'-') && width < 3 {
        q += 1;
        width += 1;
    }
    let dstart = q;
    while q < s.len() && width < 3 && c_isdigit(s[q]) {
        q += 1;
        width += 1;
    }
    if q == dstart {
        return None;
    }
    let txt = std::str::from_utf8(&s[start..q]).ok()?;
    let y: i32 = txt.parse().ok()?;
    Some((m as i32, y, q))
}

/// DCH_from_char(), with exact separators and complete input required in strict mode.
fn dch_from_char(nodes: &[Node], src: &[u8], out: &mut TmFromChar, strict: bool) -> Result<()> {
    let mut fx_mode = strict;
    let mut extra_skip: i32 = 0;
    let mut pos = 0usize;
    let mut idx = 0usize;
    while idx < nodes.len() && pos < src.len() {
        let n = &nodes[idx];
        let is_action = matches!(n, Node::Action(..));
        let is_fx = matches!(n, Node::Action(k, _) if k.id == Dch::FX);
        if !fx_mode && !is_fx && (is_action || idx == 0) {
            while pos < src.len() && c_isspace(src[pos]) {
                pos += 1;
                extra_skip += 1;
            }
        }
        match n {
            Node::Space(c) | Node::Separator(c) => {
                if strict && !src[pos..].starts_with(c.as_bytes()) {
                    return Err(Error::new(
                        INVALID_DATETIME_FORMAT,
                        format!("unmatched format separator \"{c}\""),
                    ));
                }
                if !fx_mode {
                    extra_skip -= 1;
                    if pos < src.len() && (c_isspace(src[pos]) || is_separator_char(src[pos])) {
                        pos += 1;
                        extra_skip += 1;
                    }
                } else if pos < src.len() {
                    pos += utf8_len(src[pos]).min(src.len() - pos);
                }
                idx += 1;
                continue;
            }
            Node::Char(c) => {
                if strict && !src[pos..].starts_with(c.as_bytes()) {
                    return Err(Error::new(
                        INVALID_DATETIME_FORMAT,
                        format!("unmatched format character \"{c}\""),
                    ));
                }
                if !fx_mode {
                    if extra_skip > 0 {
                        extra_skip -= 1;
                    } else if pos < src.len() {
                        pos += utf8_len(src[pos]).min(src.len() - pos);
                    }
                } else if pos < src.len() {
                    pos += utf8_len(src[pos]).min(src.len() - pos);
                }
                idx += 1;
                continue;
            }
            Node::Action(..) => {}
        }
        let (k, suffix) = match n {
            Node::Action(k, s) => (*k, *s),
            _ => unreachable!(),
        };
        set_mode(out, k.date_mode)?;
        match k.id {
            Dch::FX => fx_mode = true,
            Dch::A_M | Dch::P_M | Dch::a_m | Dch::p_m => {
                let v = seq_search_field(src, &mut pos, &AMPM_STRINGS_LONG, k)?;
                set_int(&mut out.pm, (v % 2) as i32, k)?;
                out.clock12 = true;
            }
            Dch::AM | Dch::PM | Dch::am | Dch::pm => {
                let v = seq_search_field(src, &mut pos, &AMPM_STRINGS, k)?;
                set_int(&mut out.pm, (v % 2) as i32, k)?;
                out.clock12 = true;
            }
            Dch::HH | Dch::HH12 => {
                parse_int_len(Some(&mut out.hh), src, &mut pos, 2, nodes, idx)?;
                out.clock12 = true;
                skip_th(src, &mut pos, suffix);
            }
            Dch::HH24 => {
                parse_int_len(Some(&mut out.hh), src, &mut pos, 2, nodes, idx)?;
                skip_th(src, &mut pos, suffix);
            }
            Dch::MI => {
                parse_int_len(Some(&mut out.mi), src, &mut pos, k.name.len(), nodes, idx)?;
                skip_th(src, &mut pos, suffix);
            }
            Dch::SS => {
                parse_int_len(Some(&mut out.ss), src, &mut pos, k.name.len(), nodes, idx)?;
                skip_th(src, &mut pos, suffix);
            }
            Dch::MS => {
                let len = parse_int_len(Some(&mut out.ms), src, &mut pos, 3, nodes, idx)?;
                out.ms = out.ms.wrapping_mul(match len {
                    1 => 100,
                    2 => 10,
                    _ => 1,
                });
                skip_th(src, &mut pos, suffix);
            }
            Dch::FF1 | Dch::FF2 | Dch::FF3 | Dch::FF4 | Dch::FF5 | Dch::FF6 | Dch::US => {
                let flen = match k.id {
                    Dch::FF1 => 1,
                    Dch::FF2 => 2,
                    Dch::FF3 => 3,
                    Dch::FF4 => 4,
                    Dch::FF5 => 5,
                    Dch::FF6 => 6,
                    _ => 0,
                };
                if flen > 0 {
                    out.ff = flen;
                }
                let want = if k.id == Dch::US { 6 } else { out.ff as usize };
                let len = parse_int_len(Some(&mut out.us), src, &mut pos, want, nodes, idx)?;
                out.us = out.us.wrapping_mul(match len {
                    1 => 100000,
                    2 => 10000,
                    3 => 1000,
                    4 => 100,
                    5 => 10,
                    _ => 1,
                });
                skip_th(src, &mut pos, suffix);
            }
            Dch::SSSS => {
                parse_int_len(Some(&mut out.ssss), src, &mut pos, k.name.len(), nodes, idx)?;
                skip_th(src, &mut pos, suffix);
            }
            Dch::tz | Dch::TZ | Dch::OF => {
                let mut parse_of = k.id == Dch::OF;
                if !parse_of {
                    match decode_timezone_abbrev_prefix(&src[pos..]) {
                        Some((len, kind)) => {
                            out.has_tz = true;
                            match kind {
                                PrefixAbbrev::Fixed(off) => {
                                    out.gmtoffset = off;
                                    out.tzp = None;
                                }
                                PrefixAbbrev::Dyn(z) => {
                                    out.tzp = Some(z);
                                    out.abbrev =
                                        String::from_utf8_lossy(&src[pos..pos + len]).into_owned();
                                }
                            }
                            out.tzsign = 0;
                            pos += len;
                        }
                        None => {
                            if pos < src.len() && src[pos].is_ascii_alphabetic() {
                                return Err(Error::new(
                                    INVALID_DATETIME_FORMAT,
                                    format!(
                                        "invalid value \"{}\" for \"{}\"",
                                        String::from_utf8_lossy(&src[pos..]),
                                        k.name
                                    ),
                                )
                                .with_detail("Time zone abbreviation is not recognized."));
                            }
                            parse_of = true;
                        }
                    }
                }
                if parse_of {
                    if pos < src.len() && (src[pos] == b'+' || src[pos] == b'-' || src[pos] == b' ')
                    {
                        out.tzsign = if src[pos] == b'-' { -1 } else { 1 };
                        pos += 1;
                    } else if extra_skip > 0 && pos > 0 && src[pos - 1] == b'-' {
                        out.tzsign = -1;
                    } else {
                        out.tzsign = 1;
                    }
                    parse_int_len(Some(&mut out.tzh), src, &mut pos, 2, nodes, idx)?;
                    if pos < src.len() && src[pos] == b':' {
                        pos += 1;
                        parse_int_len(Some(&mut out.tzm), src, &mut pos, 2, nodes, idx)?;
                    }
                }
            }
            Dch::TZH => {
                if pos < src.len() && (src[pos] == b'+' || src[pos] == b'-' || src[pos] == b' ') {
                    out.tzsign = if src[pos] == b'-' { -1 } else { 1 };
                    pos += 1;
                } else if extra_skip > 0 && pos > 0 && src[pos - 1] == b'-' {
                    out.tzsign = -1;
                } else {
                    out.tzsign = 1;
                }
                parse_int_len(Some(&mut out.tzh), src, &mut pos, 2, nodes, idx)?;
            }
            Dch::TZM => {
                if out.tzsign == 0 {
                    out.tzsign = 1;
                }
                parse_int_len(Some(&mut out.tzm), src, &mut pos, 2, nodes, idx)?;
            }
            Dch::A_D | Dch::B_C | Dch::a_d | Dch::b_c => {
                let v = seq_search_field(src, &mut pos, &ADBC_STRINGS_LONG, k)?;
                set_int(&mut out.bc, (v % 2) as i32, k)?;
            }
            Dch::AD | Dch::BC | Dch::ad | Dch::bc => {
                let v = seq_search_field(src, &mut pos, &ADBC_STRINGS, k)?;
                set_int(&mut out.bc, (v % 2) as i32, k)?;
            }
            Dch::MONTH | Dch::Month | Dch::month => {
                let v = seq_search_field(src, &mut pos, &MONTHS_FULL, k)?;
                set_int(&mut out.mm, v as i32 + 1, k)?;
            }
            Dch::MON | Dch::Mon | Dch::mon => {
                let v = seq_search_field(src, &mut pos, &MONTHS, k)?;
                set_int(&mut out.mm, v as i32 + 1, k)?;
            }
            Dch::MM => {
                parse_int_len(Some(&mut out.mm), src, &mut pos, k.name.len(), nodes, idx)?;
                skip_th(src, &mut pos, suffix);
            }
            Dch::DAY | Dch::Day | Dch::day => {
                let v = seq_search_field(src, &mut pos, &DAYS, k)?;
                set_int(&mut out.d, v as i32, k)?;
                out.d = out.d.wrapping_add(1);
            }
            Dch::DY | Dch::Dy | Dch::dy => {
                let v = seq_search_field(src, &mut pos, &DAYS_SHORT, k)?;
                set_int(&mut out.d, v as i32, k)?;
                out.d = out.d.wrapping_add(1);
            }
            Dch::DDD => {
                parse_int_len(Some(&mut out.ddd), src, &mut pos, k.name.len(), nodes, idx)?;
                skip_th(src, &mut pos, suffix);
            }
            Dch::IDDD => {
                parse_int_len(Some(&mut out.ddd), src, &mut pos, 3, nodes, idx)?;
                skip_th(src, &mut pos, suffix);
            }
            Dch::DD => {
                parse_int_len(Some(&mut out.dd), src, &mut pos, k.name.len(), nodes, idx)?;
                skip_th(src, &mut pos, suffix);
            }
            Dch::D => {
                parse_int_len(Some(&mut out.d), src, &mut pos, k.name.len(), nodes, idx)?;
                skip_th(src, &mut pos, suffix);
            }
            Dch::ID => {
                parse_int_len(Some(&mut out.d), src, &mut pos, 1, nodes, idx)?;
                out.d = out.d.wrapping_add(1);
                if out.d > 7 {
                    out.d = 1;
                }
                skip_th(src, &mut pos, suffix);
            }
            Dch::WW | Dch::IW => {
                parse_int_len(Some(&mut out.ww), src, &mut pos, k.name.len(), nodes, idx)?;
                skip_th(src, &mut pos, suffix);
            }
            Dch::Q => {
                parse_int_len(None, src, &mut pos, k.name.len(), nodes, idx)?;
                skip_th(src, &mut pos, suffix);
            }
            Dch::CC => {
                parse_int_len(Some(&mut out.cc), src, &mut pos, k.name.len(), nodes, idx)?;
                skip_th(src, &mut pos, suffix);
            }
            Dch::Y_YYY => {
                let (millennia, years, nch) = scan_y_yyy(&src[pos..]).ok_or_else(|| {
                    Error::new(
                        INVALID_DATETIME_FORMAT,
                        "invalid input string for \"Y,YYY\"",
                    )
                })?;
                let years = years.wrapping_add(millennia.wrapping_mul(1000));
                set_int(&mut out.year, years, k)?;
                out.yysz = 4;
                pos += nch;
                skip_th(src, &mut pos, suffix);
            }
            Dch::YYYY | Dch::IYYY => {
                parse_int_len(Some(&mut out.year), src, &mut pos, k.name.len(), nodes, idx)?;
                out.yysz = 4;
                skip_th(src, &mut pos, suffix);
            }
            Dch::YYY | Dch::IYY | Dch::YY | Dch::IY | Dch::Y | Dch::I => {
                let len =
                    parse_int_len(Some(&mut out.year), src, &mut pos, k.name.len(), nodes, idx)?;
                if len < 4 {
                    out.year = adjust_partial_year_to_2020(out.year);
                }
                out.yysz = k.name.len() as i32;
                skip_th(src, &mut pos, suffix);
            }
            Dch::RM | Dch::rm => {
                let v = seq_search_field(src, &mut pos, &RM_MONTHS_LOWER, k)?;
                set_int(&mut out.mm, MONTHS_PER_YEAR - v as i32, k)?;
            }
            Dch::W => {
                parse_int_len(Some(&mut out.w), src, &mut pos, k.name.len(), nodes, idx)?;
                skip_th(src, &mut pos, suffix);
            }
            Dch::J => {
                parse_int_len(Some(&mut out.j), src, &mut pos, k.name.len(), nodes, idx)?;
                skip_th(src, &mut pos, suffix);
            }
        }
        if !fx_mode {
            extra_skip = 0;
            while pos < src.len() && c_isspace(src[pos]) {
                pos += 1;
                extra_skip += 1;
            }
        }
        idx += 1;
    }
    if strict {
        if idx < nodes.len() {
            return Err(Error::new(
                INVALID_DATETIME_FORMAT,
                "input string is too short for datetime format",
            ));
        }
        if src[pos..].iter().any(|&c| !c_isspace(c)) {
            return Err(Error::new(
                INVALID_DATETIME_FORMAT,
                "trailing characters remain in input string after datetime format",
            ));
        }
    }
    Ok(())
}

enum PrefixAbbrev {
    Fixed(i32),
    Dyn(ZoneRef),
}

/// DecodeTimezoneAbbrevPrefix(): longest prefix (of up to 10 letters)
/// matching a known abbreviation.
fn decode_timezone_abbrev_prefix(s: &[u8]) -> Option<(usize, PrefixAbbrev)> {
    let mut low = String::new();
    for &c in s.iter().take(10) {
        if !c.is_ascii_alphabetic() {
            break;
        }
        low.push(c.to_ascii_lowercase() as char);
    }
    let mut len = low.len();
    while len > 0 {
        match tz::lookup_abbrev(&low[..len]) {
            Some(AbbrevKind::Tz(off)) | Some(AbbrevKind::Dtz(off)) => {
                return Some((len, PrefixAbbrev::Fixed(off)));
            }
            Some(AbbrevKind::Dyn(zone)) => {
                if let Some(z) = tz::fetch_dynamic_zone(zone) {
                    return Some((len, PrefixAbbrev::Dyn(z)));
                }
            }
            None => {}
        }
        len -= 1;
    }
    None
}

/// Result of do_to_timestamp()
struct ToTs {
    tm: Tm,
    fsec: i32,
    has_tz: bool,
    /// PG tz (seconds west)
    gmtoffset: i32,
    fprec: i32,
}

fn field_overflow(date_str: &str) -> Error {
    datetime_parse_error(
        DTERR_FIELD_OVERFLOW,
        &DtErrExtra::default(),
        date_str,
        "timestamp",
    )
}

/// do_to_timestamp()
fn do_to_timestamp(date_str: &str, nodes: &[Node], strict: bool, empty: bool) -> Result<ToTs> {
    let mut tmfc = TmFromChar::default();
    let mut tm = Tm {
        mday: 1,
        mon: 1,
        ..Default::default()
    };
    let mut fsec: i32 = 0;
    let mut fmask: i32 = 0;
    if !empty {
        dch_from_char(nodes, date_str.as_bytes(), &mut tmfc, strict)?;
    }
    if tmfc.ssss != 0 {
        let mut x = tmfc.ssss;
        tm.hour = x / 3600;
        x %= 3600;
        tm.min = x / 60;
        x %= 60;
        tm.sec = x;
    }
    if tmfc.ss != 0 {
        tm.sec = tmfc.ss;
    }
    if tmfc.mi != 0 {
        tm.min = tmfc.mi;
    }
    if tmfc.hh != 0 {
        tm.hour = tmfc.hh;
    }
    if tmfc.clock12 {
        if tm.hour < 1 || tm.hour > HOURS_PER_DAY / 2 {
            return Err(Error::new(
                INVALID_DATETIME_FORMAT,
                format!("hour \"{}\" is invalid for the 12-hour clock", tm.hour),
            ));
        }
        if tmfc.pm != 0 && tm.hour < HOURS_PER_DAY / 2 {
            tm.hour += HOURS_PER_DAY / 2;
        } else if tmfc.pm == 0 && tm.hour == HOURS_PER_DAY / 2 {
            tm.hour = 0;
        }
    }
    use crate::datetime::{DAY, MONTH, YEAR, dtk_m};
    if tmfc.year != 0 {
        if tmfc.cc != 0 && tmfc.yysz <= 2 {
            if tmfc.bc != 0 {
                tmfc.cc = tmfc.cc.wrapping_neg();
            }
            tm.year = tmfc.year % 100;
            if tm.year != 0 {
                if tmfc.cc >= 0 {
                    tm.year = tm
                        .year
                        .wrapping_add(tmfc.cc.wrapping_sub(1).wrapping_mul(100));
                } else {
                    tm.year = tmfc
                        .cc
                        .wrapping_add(1)
                        .wrapping_mul(100)
                        .wrapping_sub(tm.year)
                        .wrapping_add(1);
                }
            } else {
                tm.year = tmfc
                    .cc
                    .wrapping_mul(100)
                    .wrapping_add(if tmfc.cc >= 0 { 0 } else { 1 });
            }
        } else {
            tm.year = tmfc.year;
            if tmfc.bc != 0 {
                tm.year = tm.year.wrapping_neg();
            }
            if tm.year < 0 {
                tm.year = tm.year.wrapping_add(1);
            }
        }
        fmask |= dtk_m(YEAR);
    } else if tmfc.cc != 0 {
        if tmfc.bc != 0 {
            tmfc.cc = tmfc.cc.wrapping_neg();
        }
        if tmfc.cc >= 0 {
            tm.year = tmfc.cc.wrapping_sub(1).wrapping_mul(100).wrapping_add(1);
        } else {
            tm.year = tmfc.cc.wrapping_mul(100).wrapping_add(1);
        }
        fmask |= dtk_m(YEAR);
    }
    if tmfc.j != 0 {
        let (y, m, d) = j2date(tmfc.j);
        tm.year = y;
        tm.mon = m;
        tm.mday = d;
        fmask |= crate::datetime::DTK_DATE_M;
    }
    if tmfc.ww != 0 {
        if tmfc.mode == Some(DateMode::IsoWeek) {
            let (y, m, d) = if tmfc.d != 0 {
                isoweekdate2date(tmfc.ww, tmfc.d, tm.year)
            } else {
                isoweek2date(tmfc.ww, tm.year)
            };
            tm.year = y;
            tm.mon = m;
            tm.mday = d;
            fmask |= crate::datetime::DTK_DATE_M;
        } else {
            tmfc.ddd = tmfc.ww.wrapping_sub(1).wrapping_mul(7).wrapping_add(1);
        }
    }
    if tmfc.w != 0 {
        tmfc.dd = tmfc.w.wrapping_sub(1).wrapping_mul(7).wrapping_add(1);
    }
    if tmfc.dd != 0 {
        tm.mday = tmfc.dd;
        fmask |= dtk_m(DAY);
    }
    if tmfc.mm != 0 {
        tm.mon = tmfc.mm;
        fmask |= dtk_m(MONTH);
    }
    if tmfc.ddd != 0 && (tm.mon <= 1 || tm.mday <= 1) {
        if tm.year == 0 && tmfc.bc == 0 {
            return Err(Error::new(
                INVALID_DATETIME_FORMAT,
                "cannot calculate day of year without year information",
            ));
        }
        if tmfc.mode == Some(DateMode::IsoWeek) {
            let j0 = isoweek2j(tm.year, 1).wrapping_sub(1);
            let (y, m, d) = j2date(j0.wrapping_add(tmfc.ddd));
            tm.year = y;
            tm.mon = m;
            tm.mday = d;
            fmask |= crate::datetime::DTK_DATE_M;
        } else {
            const YSUM: [[i32; 13]; 2] = [
                [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365],
                [0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366],
            ];
            let y = &YSUM[isleap(tm.year) as usize];
            let mut i = 1;
            while i <= MONTHS_PER_YEAR as usize {
                if tmfc.ddd <= y[i] {
                    break;
                }
                i += 1;
            }
            if tm.mon <= 1 {
                tm.mon = i as i32;
            }
            if tm.mday <= 1 {
                let yi = y[(i - 1).min(12)];
                tm.mday = tmfc.ddd - yi;
            }
            fmask |= dtk_m(MONTH) | dtk_m(DAY);
        }
    }
    if tmfc.ms != 0 {
        fsec = fsec.wrapping_add(tmfc.ms.wrapping_mul(1000));
    }
    if tmfc.us != 0 {
        fsec = fsec.wrapping_add(tmfc.us);
    }
    let fprec = tmfc.ff;
    if fmask != 0 && crate::datetime::validate_date(fmask, true, false, false, &mut tm, 0).is_err()
    {
        return Err(field_overflow(date_str));
    }
    if tm.hour < 0
        || tm.hour >= HOURS_PER_DAY
        || tm.min < 0
        || tm.min >= 60
        || tm.sec < 0
        || tm.sec >= 60
        || fsec < 0
        || fsec as i64 >= USECS_PER_SEC
    {
        return Err(field_overflow(date_str));
    }
    let mut has_tz = false;
    let mut gmtoffset = 0;
    if tmfc.tzsign != 0 {
        if tmfc.tzh < 0 || tmfc.tzh > MAX_TZDISP_HOUR || tmfc.tzm < 0 || tmfc.tzm >= 60 {
            return Err(datetime_parse_error(
                DTERR_TZDISP_OVERFLOW,
                &DtErrExtra::default(),
                date_str,
                "timestamp",
            ));
        }
        has_tz = true;
        gmtoffset = (tmfc.tzh * 60 + tmfc.tzm) * 60;
        if tmfc.tzsign > 0 {
            gmtoffset = -gmtoffset;
        }
    } else if tmfc.has_tz {
        has_tz = true;
        match &tmfc.tzp {
            None => gmtoffset = -tmfc.gmtoffset,
            Some(z) => {
                gmtoffset = tz::determine_abbrev_offset(&mut tm, &tmfc.abbrev, z);
            }
        }
    }
    Ok(ToTs {
        tm,
        fsec,
        has_tz,
        gmtoffset,
        fprec,
    })
}

/// to_date()
pub fn to_date(s: &str, fmt: &str) -> Result<Date> {
    let r = do_to_timestamp(s, &cached_format(fmt), false, fmt.is_empty())?;
    date_from_tm(&r.tm, s)
}

fn date_from_tm(tm: &Tm, s: &str) -> Result<Date> {
    let oor = || {
        Error::new(
            DATETIME_VALUE_OUT_OF_RANGE,
            format!("date out of range: \"{}\"", s),
        )
    };
    if !is_valid_julian(tm.year, tm.mon, tm.mday) {
        return Err(oor());
    }
    let result = (date2j(tm.year, tm.mon, tm.mday) - POSTGRES_EPOCH_JDATE) as i64;
    if !is_valid_date(result) {
        return Err(oor());
    }
    Ok(result as i32)
}

/// to_timestamp(text, text): fields without an explicit zone are local
/// time in `tz` (the session time zone).
pub fn to_timestamp(s: &str, fmt: &str, tz: &TimeZone) -> Result<TimestampTz> {
    let r = do_to_timestamp(s, &cached_format(fmt), false, fmt.is_empty())?;
    let mut tm = r.tm;
    let tzo = if r.has_tz {
        r.gmtoffset
    } else {
        tz::determine_offset(&mut tm, &tz.zone_ref())
    };
    let mut result = tm2timestamp(&tm, r.fsec, Some(tzo))?;
    if r.fprec != 0 {
        result = crate::io::adjust_timestamp(result, r.fprec)?;
    }
    Ok(result)
}

/// A datetime value parsed from a standard template. Zone offsets are seconds west of UTC.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParsedDateTime {
    Date(Date),
    Time(crate::Time),
    TimeTz(crate::Time, i32),
    Timestamp(Timestamp),
    TimestampTz(TimestampTz, i32),
}

/// A compiled, strict datetime template, as used by SQL/JSON path methods.
pub struct DateTimeTemplate {
    nodes: Vec<Node>,
    empty: bool,
}

impl DateTimeTemplate {
    /// Compile a template, rejecting unquoted nonstandard separators.
    pub fn parse(fmt: &str) -> Result<Self> {
        Ok(Self {
            nodes: parse_format(fmt, true)?,
            empty: fmt.is_empty(),
        })
    }

    /// Parse the complete input and return its value and fractional precision (-1 if unspecified).
    pub fn parse_value(&self, input: &str) -> Result<(ParsedDateTime, i32)> {
        let r = do_to_timestamp(input, &self.nodes, true, self.empty)?;
        let mut dated = false;
        let mut timed = false;
        let mut zoned = false;
        for node in &self.nodes {
            let Node::Action(k, _) = node else { continue };
            use Dch::*;
            match k.id {
                // PostgreSQL does not count IW as a date field here.
                FX | IW => {}
                A_M | P_M | a_m | p_m | AM | PM | am | pm | HH | HH12 | HH24 | MI | SS | MS
                | US | FF1 | FF2 | FF3 | FF4 | FF5 | FF6 | SSSS => timed = true,
                tz | TZ | OF | TZH | TZM => zoned = true,
                _ => dated = true,
            }
        }
        let typmod = if r.fprec == 0 { -1 } else { r.fprec };
        let zone = || {
            if r.has_tz {
                Ok(r.gmtoffset)
            } else {
                Err(Error::new(
                    INVALID_DATETIME_FORMAT,
                    format!(
                        "missing time zone in input string for type {}",
                        if dated { "timestamptz" } else { "timetz" }
                    ),
                ))
            }
        };
        let value = if dated && timed {
            let offset = if zoned { Some(zone()?) } else { None };
            let ts = tm2timestamp(&r.tm, r.fsec, offset).map_err(|_| {
                Error::new(
                    DATETIME_VALUE_OUT_OF_RANGE,
                    if zoned {
                        "timestamptz out of range"
                    } else {
                        "timestamp out of range"
                    },
                )
            })?;
            let ts = crate::adjust_timestamp(ts, typmod)?;
            match offset {
                Some(z) => ParsedDateTime::TimestampTz(ts, z),
                None => ParsedDateTime::Timestamp(ts),
            }
        } else if dated {
            if zoned {
                return Err(Error::new(
                    INVALID_DATETIME_FORMAT,
                    "datetime format is zoned but not timed",
                ));
            }
            ParsedDateTime::Date(date_from_tm(&r.tm, input)?)
        } else if timed {
            let time = crate::adjust_time(time2t(r.tm.hour, r.tm.min, r.tm.sec, r.fsec), typmod);
            if zoned {
                ParsedDateTime::TimeTz(time, zone()?)
            } else {
                ParsedDateTime::Time(time)
            }
        } else {
            return Err(Error::new(
                INVALID_DATETIME_FORMAT,
                "datetime format is not dated and not timed",
            ));
        };
        Ok((value, typmod))
    }
}
