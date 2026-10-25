//! Datetime template parsing for jsonpath: parse_datetime() in standard
//! (strict) mode, ported from src/backend/utils/adt/formatting.c of
//! PostgreSQL 17 (parse_format(), DCH_from_char(), do_to_timestamp(),
//! parse_datetime()) and the helpers it uses from datetime.c, date.c and
//! timestamp.c (Copyright (c) 1996-2024, PostgreSQL Global Development
//! Group; PostgreSQL License). Month and day names are those of
//! `lc_time = 'C'`.

use super::tzabbrev::{ABBREVS, Abbrev};
use crate::{Error, Result, sqlstate};

pub(super) const USECS_PER_SEC: i64 = 1_000_000;
pub(super) const USECS_PER_DAY: i64 = 86_400_000_000;
pub(super) const POSTGRES_EPOCH_JDATE: i32 = 2_451_545;
const MAX_TZDISP_HOUR: i32 = 15;
const MIN_TIMESTAMP: i64 = -211_813_488_000_000_000;
const END_TIMESTAMP: i64 = 9_223_371_331_200_000_000;

/// A data error while parsing a datetime (PostgreSQL reports these softly,
/// so they can be suppressed); template errors are reported by
/// [`Template::parse`].
type DtResult<T> = std::result::Result<T, Error>;

fn soft(code: &'static str, msg: impl Into<String>) -> Error {
    Error::new(code, msg)
}

fn soft_detail(code: &'static str, msg: impl Into<String>, detail: impl Into<String>) -> Error {
    Error::with_detail(code, msg, detail)
}

// ------------------------------------------------------------- calendar

/// date2j() (with PostgreSQL's wrapping int arithmetic)
pub(super) fn date2j(year: i32, month: i32, day: i32) -> i32 {
    let (mut y, mut m) = (year, month);
    if m > 2 {
        m = m.wrapping_add(1);
        y = y.wrapping_add(4800);
    } else {
        m = m.wrapping_add(13);
        y = y.wrapping_add(4799);
    }
    let century = y / 100;
    let mut julian = y.wrapping_mul(365).wrapping_sub(32167);
    julian = julian
        .wrapping_add(y / 4)
        .wrapping_sub(century)
        .wrapping_add(century / 4);
    julian
        .wrapping_add(7834i32.wrapping_mul(m) / 256)
        .wrapping_add(day)
}

/// j2date()
pub(super) fn j2date(jd: i32) -> (i32, i32, i32) {
    let mut julian = jd as u32;
    julian = julian.wrapping_add(32044);
    let mut quad = julian / 146097;
    let extra = (julian.wrapping_sub(quad.wrapping_mul(146097)))
        .wrapping_mul(4)
        .wrapping_add(3);
    julian = julian
        .wrapping_add(60)
        .wrapping_add(quad.wrapping_mul(3))
        .wrapping_add(extra / 146097);
    quad = julian / 1461;
    julian = julian.wrapping_sub(quad.wrapping_mul(1461));
    let mut y = (julian.wrapping_mul(4) / 1461) as i32;
    julian = if y != 0 {
        (julian.wrapping_add(305)) % 365
    } else {
        (julian.wrapping_add(306)) % 366
    }
    .wrapping_add(123);
    y = y.wrapping_add((quad.wrapping_mul(4)) as i32);
    let year = y.wrapping_sub(4800);
    let quad = julian.wrapping_mul(2141) / 65536;
    let day = julian.wrapping_sub(7834u32.wrapping_mul(quad) / 256) as i32;
    let month = ((quad.wrapping_add(10)) % 12 + 1) as i32;
    (year, month, day)
}

/// j2day(): day of week, 0 = Sunday
fn j2day(date: i32) -> i32 {
    let d = date.wrapping_add(1) % 7;
    if d < 0 { d + 7 } else { d }
}

fn isleap(y: i32) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}

const DAY_TAB: [[i32; 12]; 2] = [
    [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
    [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
];

/// isoweek2j(): Julian day of Monday of ISO week 1 of `year`, plus weeks
fn isoweek2j(year: i32, week: i32) -> i32 {
    // fourth day of current year
    let day4 = date2j(year, 1, 4);
    // day0 == offset to first day of week (Monday)
    let day0 = j2day(day4.wrapping_sub(1));
    week.wrapping_sub(1)
        .wrapping_mul(7)
        .wrapping_add(day4.wrapping_sub(day0))
}

/// isoweek2date()
fn isoweek2date(woy: i32, year: &mut i32, mon: &mut i32, mday: &mut i32) {
    let (y, m, d) = j2date(isoweek2j(*year, woy));
    (*year, *mon, *mday) = (y, m, d);
}

/// isoweekdate2date(): `wday` is 1 (Sunday) .. 7
fn isoweekdate2date(isoweek: i32, wday: i32, year: &mut i32, mon: &mut i32, mday: &mut i32) {
    let mut jday = isoweek2j(*year, isoweek);
    // convert Gregorian week start (Sunday=1) to ISO week start (Monday=1)
    if wday > 1 {
        jday = jday.wrapping_add(wday.wrapping_sub(2));
    } else {
        jday = jday.wrapping_add(6);
    }
    let (y, m, d) = j2date(jday);
    (*year, *mon, *mday) = (y, m, d);
}

/// IS_VALID_JULIAN()
pub(super) fn is_valid_julian(y: i32, m: i32, _d: i32) -> bool {
    (y > -4713 || (y == -4713 && m >= 11)) && (y < 5874898 || (y == 5874898 && m < 6))
}

/// IS_VALID_DATE()
pub(super) fn is_valid_date(d: i32) -> bool {
    let d = i64::from(d);
    -i64::from(POSTGRES_EPOCH_JDATE) <= d && d < 2_147_483_494 - i64::from(POSTGRES_EPOCH_JDATE)
}

/// IS_VALID_TIMESTAMP()
pub(super) fn is_valid_timestamp(t: i64) -> bool {
    (MIN_TIMESTAMP..END_TIMESTAMP).contains(&t)
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Tm {
    pub year: i32,
    pub mon: i32,
    pub mday: i32,
    pub hour: i32,
    pub min: i32,
    pub sec: i32,
}

/// tm2timestamp(): None if out of range. `tz` is in seconds west.
pub(super) fn tm2timestamp(tm: &Tm, fsec: i64, tz: Option<i32>) -> Option<i64> {
    if !is_valid_julian(tm.year, tm.mon, tm.mday) {
        return None;
    }
    let date = i64::from(date2j(tm.year, tm.mon, tm.mday) - POSTGRES_EPOCH_JDATE);
    let time = ((i64::from(tm.hour) * 60 + i64::from(tm.min)) * 60 + i64::from(tm.sec))
        * USECS_PER_SEC
        + fsec;
    let mut result = date.wrapping_mul(USECS_PER_DAY).wrapping_add(time);
    // check for major overflow
    if (result.wrapping_sub(time)) / USECS_PER_DAY != date {
        return None;
    }
    // check for just-barely overflow (okay except time-of-day wraps)
    if (result < 0 && date > 0) || (result > 0 && date < -1) {
        return None;
    }
    if let Some(tz) = tz {
        result = result.wrapping_sub(-i64::from(tz) * USECS_PER_SEC);
    }
    if !is_valid_timestamp(result) {
        return None;
    }
    Some(result)
}

/// Rounds microseconds to `precision` fractional digits
/// (AdjustTimeForTypmod / AdjustTimestampForTypmod for 0..=6).
pub(super) fn adjust_precision(t: i64, precision: i32) -> i64 {
    const SCALES: [i64; 7] = [1_000_000, 100_000, 10_000, 1_000, 100, 10, 1];
    const OFFSETS: [i64; 7] = [500_000, 50_000, 5_000, 500, 50, 5, 0];
    if !(0..=6).contains(&precision) {
        return t;
    }
    let (s, o) = (SCALES[precision as usize], OFFSETS[precision as usize]);
    if t >= 0 {
        ((t + o) / s) * s
    } else {
        -(((-t + o) / s) * s)
    }
}

// ------------------------------------------------------------- template

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

/// DCH_keywords[] (order matters for the sequential search)
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

/// index_seq_search(): keywords are searched from the first entry starting
/// with the same character (DCH_index).
fn index_seq_search(s: &[u8]) -> Option<&'static KeyWord> {
    let c = *s.first()?;
    if c <= b' ' || c >= b'~' {
        return None;
    }
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
enum FNode {
    Action(&'static KeyWord, u8),
    /// quoted literal character (bytes of one character)
    Char(Vec<u8>),
    Separator(u8),
    Space,
}

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

/// parse_format() with DCH_FLAG | STD_FLAG.
fn parse_format(fmt: &[u8]) -> Result<Vec<FNode>> {
    let s = fmt;
    let mut nodes = Vec::new();
    let mut i = 0;
    while i < s.len() {
        let mut suffix: u8 = 0;
        // prefix
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
            if i < s.len() {
                let rest = &s[i..];
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
            nodes.push(FNode::Action(k, suffix));
        } else if i < s.len() {
            let c = s[i];
            if c != b'"' {
                // Standard mode allows only the separators "-./,':; ".
                if !b"-./,':; ".contains(&c) {
                    let l = utf8_len(c).min(s.len() - i);
                    return Err(Error::new(
                        sqlstate::INVALID_DATETIME_FORMAT,
                        format!(
                            "invalid datetime format separator: \"{}\"",
                            String::from_utf8_lossy(&s[i..i + l])
                        ),
                    ));
                }
                nodes.push(if c == b' ' {
                    FNode::Space
                } else {
                    FNode::Separator(c)
                });
                i += 1;
            } else {
                // double-quoted literal
                i += 1;
                while i < s.len() {
                    if s[i] == b'"' {
                        i += 1;
                        break;
                    }
                    if s[i] == b'\\' && i + 1 < s.len() {
                        i += 1;
                    }
                    let l = utf8_len(s[i]).min(s.len() - i);
                    nodes.push(FNode::Char(s[i..i + l].to_vec()));
                    i += l;
                }
            }
        }
    }
    Ok(nodes)
}

/// DCH_datetime_type() flags
const DCH_DATED: u32 = 0x01;
const DCH_TIMED: u32 = 0x02;
const DCH_ZONED: u32 = 0x04;

fn datetime_type(nodes: &[FNode]) -> u32 {
    let mut flags = 0;
    for n in nodes {
        let FNode::Action(k, _) = n else { continue };
        use Dch::*;
        flags |= match k.id {
            // IW is not among PostgreSQL's date fields here
            FX | IW => 0,
            A_M | P_M | a_m | p_m | AM | PM | am | pm | HH | HH12 | HH24 | MI | SS | MS | US
            | FF1 | FF2 | FF3 | FF4 | FF5 | FF6 | SSSS => DCH_TIMED,
            tz | TZ | OF | TZH | TZM => DCH_ZONED,
            _ => DCH_DATED,
        };
    }
    flags
}

// ------------------------------------------------------------- from_char

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
    /// dynamic abbreviation: (abbreviation as written, zone)
    dyn_abbrev: Option<String>,
}

fn is_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c)
}

/// scanner_isspace()
fn scanner_isspace(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | b'\r' | 0x0c)
}

/// C strtol(s, &end, 10) clamped to i64: (value, bytes used, overflowed).
/// `used` is 0 when no digits were found.
fn c_strtol(s: &[u8]) -> (i64, usize, bool) {
    let mut i = 0;
    while i < s.len() && is_space(s[i]) {
        i += 1;
    }
    let mut neg = false;
    if i < s.len() && (s[i] == b'+' || s[i] == b'-') {
        neg = s[i] == b'-';
        i += 1;
    }
    let start = i;
    let mut v: i128 = 0;
    let mut overflow = false;
    while i < s.len() && s[i].is_ascii_digit() {
        v = v * 10 + i128::from(s[i] - b'0');
        if v > i128::from(i64::MAX) + 1 {
            overflow = true;
            v = i128::from(i64::MAX) + 1;
        }
        i += 1;
    }
    if i == start {
        return (0, 0, false);
    }
    let v = if neg { -v } else { v };
    if v > i128::from(i64::MAX) {
        return (i64::MAX, i, true);
    }
    if v < i128::from(i64::MIN) {
        return (i64::MIN, i, true);
    }
    (v as i64, i, overflow)
}

fn set_mode(tmfc: &mut TmFromChar, mode: DateMode) -> DtResult<()> {
    if mode != DateMode::None {
        match tmfc.mode {
            None | Some(DateMode::None) => tmfc.mode = Some(mode),
            Some(m) if m != mode => {
                return Err(soft(
                    sqlstate::INVALID_DATETIME_FORMAT,
                    "invalid combination of date conventions",
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

/// from_char_set_int()
fn set_int(dest: &mut i32, value: i32, k: &KeyWord) -> DtResult<()> {
    if *dest != 0 && *dest != value {
        return Err(soft_detail(
            sqlstate::INVALID_DATETIME_FORMAT,
            format!(
                "conflicting values for \"{}\" field in formatting string",
                k.name
            ),
            "This value contradicts a previous setting for the same field type.",
        ));
    }
    *dest = value;
    Ok(())
}

/// is_next_separator()
fn is_next_separator(nodes: &[FNode], idx: usize) -> bool {
    if let FNode::Action(_, suf) = &nodes[idx]
        && suf & (S_TH | S_TH_LOWER) != 0
    {
        return true;
    }
    match nodes.get(idx + 1) {
        None => true,
        Some(FNode::Action(k, _)) => !k.is_digit,
        Some(FNode::Char(c)) => !(c.len() == 1 && c[0].is_ascii_digit()),
        Some(FNode::Separator(_)) | Some(FNode::Space) => true,
    }
}

/// from_char_parse_int_len(); returns the number of bytes consumed.
fn parse_int_len(
    dest: Option<&mut i32>,
    src: &[u8],
    pos: &mut usize,
    len: usize,
    nodes: &[FNode],
    idx: usize,
) -> DtResult<usize> {
    let (k, suffix) = match &nodes[idx] {
        FNode::Action(k, s) => (*k, *s),
        _ => unreachable!(),
    };
    let init = *pos;
    while *pos < src.len() && is_space(src[*pos]) {
        *pos += 1;
    }
    let avail = &src[*pos..];
    let copy_len = len.min(avail.len());
    let copy = &avail[..copy_len];
    let copy_str = String::from_utf8_lossy(copy).into_owned();
    let result: i64;
    let erange: bool;
    if suffix & S_FM != 0 || is_next_separator(nodes, idx) {
        let (v, end, er) = c_strtol(&src[init..]);
        result = v;
        erange = er;
        *pos = init + end;
    } else {
        if copy_len < len {
            return Err(soft_detail(
                sqlstate::INVALID_DATETIME_FORMAT,
                format!(
                    "source string too short for \"{}\" formatting field",
                    k.name
                ),
                format!("Field requires {len} characters, but only {copy_len} remain."),
            ));
        }
        let (v, used, er) = c_strtol(copy);
        result = v;
        erange = er;
        if used > 0 && used < len {
            return Err(soft_detail(
                sqlstate::INVALID_DATETIME_FORMAT,
                format!("invalid value \"{copy_str}\" for \"{}\"", k.name),
                format!("Field requires {len} characters, but only {used} could be parsed."),
            ));
        }
        *pos += used;
    }
    if *pos == init {
        return Err(soft_detail(
            sqlstate::INVALID_DATETIME_FORMAT,
            format!("invalid value \"{copy_str}\" for \"{}\"", k.name),
            "Value must be an integer.",
        ));
    }
    if erange || result < i64::from(i32::MIN) || result > i64::from(i32::MAX) {
        return Err(soft_detail(
            sqlstate::DATETIME_VALUE_OUT_OF_RANGE,
            format!("value for \"{}\" in source string is out of range", k.name),
            format!("Value must be in the range {} to {}.", i32::MIN, i32::MAX),
        ));
    }
    if let Some(d) = dest {
        set_int(d, result as i32, k)?;
    }
    Ok(*pos - init)
}

/// seq_search_ascii(): (index, length) of the first array entry that is a
/// case-insensitive prefix of `name`.
fn seq_search(name: &[u8], array: &[&str]) -> Option<(usize, usize)> {
    let firstc = name.first()?.to_ascii_lowercase();
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
            if p >= name.len() || !ab[p].eq_ignore_ascii_case(&name[p]) {
                break;
            }
            p += 1;
        }
    }
    None
}

/// from_char_seq_search()
fn seq_search_field(src: &[u8], pos: &mut usize, array: &[&str], k: &KeyWord) -> DtResult<usize> {
    match seq_search(&src[*pos..], array) {
        Some((i, len)) => {
            *pos += len;
            Ok(i)
        }
        None => {
            let rest = &src[*pos..];
            let end = rest
                .iter()
                .position(|&c| scanner_isspace(c))
                .unwrap_or(rest.len());
            Err(soft_detail(
                sqlstate::INVALID_DATETIME_FORMAT,
                format!(
                    "invalid value \"{}\" for \"{}\"",
                    String::from_utf8_lossy(&rest[..end]),
                    k.name
                ),
                "The given value did not match any of the allowed values for this field.",
            ))
        }
    }
}

/// SKIP_THth()
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

const MONTHS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];
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
const DAYS: [&str; 7] = [
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
];
const DAYS_SHORT: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
const RM_MONTHS_LOWER: [&str; 12] = [
    "xii", "xi", "x", "ix", "viii", "vii", "vi", "v", "iv", "iii", "ii", "i",
];
const AMPM_STRINGS: [&str; 4] = ["am", "pm", "AM", "PM"];
const AMPM_STRINGS_LONG: [&str; 4] = ["a.m.", "p.m.", "A.M.", "P.M."];
const ADBC_STRINGS: [&str; 4] = ["ad", "bc", "AD", "BC"];
const ADBC_STRINGS_LONG: [&str; 4] = ["a.d.", "b.c.", "A.D.", "B.C."];

/// sscanf(s, "%d,%03d%n") for Y,YYY
fn scan_y_yyy(s: &[u8]) -> Option<(i32, i32, usize)> {
    let (m, e1, _) = c_strtol(s);
    if e1 == 0 || s.get(e1) != Some(&b',') {
        return None;
    }
    let mut p = e1 + 1;
    while p < s.len() && is_space(s[p]) {
        p += 1;
    }
    let start = p;
    let mut q = p;
    let mut width = 0;
    if q < s.len() && (s[q] == b'+' || s[q] == b'-') {
        q += 1;
        width += 1;
    }
    let dstart = q;
    while q < s.len() && width < 3 && s[q].is_ascii_digit() {
        q += 1;
        width += 1;
    }
    if q == dstart {
        return None;
    }
    let y: i32 = std::str::from_utf8(&s[start..q]).ok()?.parse().ok()?;
    Some((m as i32, y, q))
}

/// DecodeTimezoneAbbrevPrefix(): longest prefix (up to 10 letters) that is
/// a known abbreviation.
fn decode_timezone_abbrev_prefix(s: &[u8]) -> Option<(usize, Abbrev)> {
    let low: Vec<u8> = s
        .iter()
        .take(10)
        .take_while(|c| c.is_ascii_alphabetic())
        .map(|c| c.to_ascii_lowercase())
        .collect();
    let mut len = low.len();
    while len > 0 {
        let key = std::str::from_utf8(&low[..len]).unwrap_or("");
        if let Ok(i) = ABBREVS.binary_search_by(|(t, _)| (*t).cmp(key)) {
            return Some((len, ABBREVS[i].1));
        }
        len -= 1;
    }
    None
}

/// DCH_from_char() in standard mode.
fn from_char(nodes: &[FNode], src: &[u8], out: &mut TmFromChar) -> DtResult<()> {
    let mut pos = 0usize;
    let mut idx = 0usize;
    while idx < nodes.len() && pos < src.len() {
        let (k, suffix) = match &nodes[idx] {
            FNode::Space => {
                if src[pos] != b' ' {
                    return Err(soft(
                        sqlstate::INVALID_DATETIME_FORMAT,
                        "unmatched format separator \" \"",
                    ));
                }
                pos += 1;
                idx += 1;
                continue;
            }
            FNode::Separator(c) => {
                if src[pos] != *c {
                    return Err(soft(
                        sqlstate::INVALID_DATETIME_FORMAT,
                        format!("unmatched format separator \"{}\"", *c as char),
                    ));
                }
                pos += 1;
                idx += 1;
                continue;
            }
            FNode::Char(c) => {
                let chlen = utf8_len(src[pos]);
                // strncmp(s, n->character, chlen)
                let a = &src[pos..(pos + chlen).min(src.len())];
                let mut eq = true;
                for k in 0..chlen {
                    let x = a.get(k).copied().unwrap_or(0);
                    let y = c.get(k).copied().unwrap_or(0);
                    if x != y {
                        eq = false;
                        break;
                    }
                    if x == 0 {
                        break;
                    }
                }
                if !eq {
                    return Err(soft(
                        sqlstate::INVALID_DATETIME_FORMAT,
                        format!(
                            "unmatched format character \"{}\"",
                            String::from_utf8_lossy(c)
                        ),
                    ));
                }
                pos = (pos + chlen).min(src.len());
                idx += 1;
                continue;
            }
            FNode::Action(k, s) => (*k, *s),
        };
        set_mode(out, k.date_mode)?;
        use Dch::*;
        match k.id {
            FX => {}
            A_M | P_M | a_m | p_m => {
                let v = seq_search_field(src, &mut pos, &AMPM_STRINGS_LONG, k)?;
                set_int(&mut out.pm, (v % 2) as i32, k)?;
                out.clock12 = true;
            }
            AM | PM | am | pm => {
                let v = seq_search_field(src, &mut pos, &AMPM_STRINGS, k)?;
                set_int(&mut out.pm, (v % 2) as i32, k)?;
                out.clock12 = true;
            }
            HH | HH12 => {
                parse_int_len(Some(&mut out.hh), src, &mut pos, 2, nodes, idx)?;
                out.clock12 = true;
                skip_th(src, &mut pos, suffix);
            }
            HH24 => {
                parse_int_len(Some(&mut out.hh), src, &mut pos, 2, nodes, idx)?;
                skip_th(src, &mut pos, suffix);
            }
            MI => {
                parse_int_len(Some(&mut out.mi), src, &mut pos, k.name.len(), nodes, idx)?;
                skip_th(src, &mut pos, suffix);
            }
            SS => {
                parse_int_len(Some(&mut out.ss), src, &mut pos, k.name.len(), nodes, idx)?;
                skip_th(src, &mut pos, suffix);
            }
            MS => {
                let len = parse_int_len(Some(&mut out.ms), src, &mut pos, 3, nodes, idx)?;
                out.ms = out.ms.wrapping_mul(match len {
                    1 => 100,
                    2 => 10,
                    _ => 1,
                });
                skip_th(src, &mut pos, suffix);
            }
            FF1 | FF2 | FF3 | FF4 | FF5 | FF6 | US => {
                let flen = match k.id {
                    FF1 => 1,
                    FF2 => 2,
                    FF3 => 3,
                    FF4 => 4,
                    FF5 => 5,
                    FF6 => 6,
                    _ => 0,
                };
                if flen > 0 {
                    out.ff = flen;
                }
                let want = if k.id == US { 6 } else { out.ff as usize };
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
            SSSS => {
                parse_int_len(Some(&mut out.ssss), src, &mut pos, k.name.len(), nodes, idx)?;
                skip_th(src, &mut pos, suffix);
            }
            tz | TZ | OF => {
                let mut parse_of = k.id == OF;
                if !parse_of {
                    match decode_timezone_abbrev_prefix(&src[pos..]) {
                        Some((len, kind)) => {
                            out.has_tz = true;
                            match kind {
                                Abbrev::Fixed(off) => {
                                    out.gmtoffset = off;
                                    out.dyn_abbrev = None;
                                }
                                Abbrev::Dynamic(_) => {
                                    out.dyn_abbrev = Some(
                                        String::from_utf8_lossy(&src[pos..pos + len]).into_owned(),
                                    );
                                }
                            }
                            out.tzsign = 0;
                            pos += len;
                        }
                        None => {
                            if pos < src.len() && src[pos].is_ascii_alphabetic() {
                                return Err(soft_detail(
                                    sqlstate::INVALID_DATETIME_FORMAT,
                                    format!(
                                        "invalid value \"{}\" for \"{}\"",
                                        String::from_utf8_lossy(&src[pos..]),
                                        k.name
                                    ),
                                    "Time zone abbreviation is not recognized.",
                                ));
                            }
                            parse_of = true;
                        }
                    }
                }
                if parse_of {
                    // In standard mode no characters are skipped, so the
                    // sign must be present here (extra_skip is always 0).
                    if pos < src.len() && (src[pos] == b'+' || src[pos] == b'-' || src[pos] == b' ')
                    {
                        out.tzsign = if src[pos] == b'-' { -1 } else { 1 };
                        pos += 1;
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
            TZH => {
                if pos < src.len() && (src[pos] == b'+' || src[pos] == b'-' || src[pos] == b' ') {
                    out.tzsign = if src[pos] == b'-' { -1 } else { 1 };
                    pos += 1;
                } else {
                    out.tzsign = 1;
                }
                parse_int_len(Some(&mut out.tzh), src, &mut pos, 2, nodes, idx)?;
            }
            TZM => {
                if out.tzsign == 0 {
                    out.tzsign = 1;
                }
                parse_int_len(Some(&mut out.tzm), src, &mut pos, 2, nodes, idx)?;
            }
            A_D | B_C | a_d | b_c => {
                let v = seq_search_field(src, &mut pos, &ADBC_STRINGS_LONG, k)?;
                set_int(&mut out.bc, (v % 2) as i32, k)?;
            }
            AD | BC | ad | bc => {
                let v = seq_search_field(src, &mut pos, &ADBC_STRINGS, k)?;
                set_int(&mut out.bc, (v % 2) as i32, k)?;
            }
            MONTH | Month | month => {
                let v = seq_search_field(src, &mut pos, &MONTHS_FULL, k)?;
                set_int(&mut out.mm, v as i32 + 1, k)?;
            }
            MON | Mon | mon => {
                let v = seq_search_field(src, &mut pos, &MONTHS, k)?;
                set_int(&mut out.mm, v as i32 + 1, k)?;
            }
            MM => {
                parse_int_len(Some(&mut out.mm), src, &mut pos, k.name.len(), nodes, idx)?;
                skip_th(src, &mut pos, suffix);
            }
            DAY | Day | day => {
                let v = seq_search_field(src, &mut pos, &DAYS, k)?;
                set_int(&mut out.d, v as i32, k)?;
                out.d += 1;
            }
            DY | Dy | dy => {
                let v = seq_search_field(src, &mut pos, &DAYS_SHORT, k)?;
                set_int(&mut out.d, v as i32, k)?;
                out.d += 1;
            }
            DDD => {
                parse_int_len(Some(&mut out.ddd), src, &mut pos, k.name.len(), nodes, idx)?;
                skip_th(src, &mut pos, suffix);
            }
            IDDD => {
                parse_int_len(Some(&mut out.ddd), src, &mut pos, 3, nodes, idx)?;
                skip_th(src, &mut pos, suffix);
            }
            DD => {
                parse_int_len(Some(&mut out.dd), src, &mut pos, k.name.len(), nodes, idx)?;
                skip_th(src, &mut pos, suffix);
            }
            D => {
                parse_int_len(Some(&mut out.d), src, &mut pos, k.name.len(), nodes, idx)?;
                skip_th(src, &mut pos, suffix);
            }
            ID => {
                parse_int_len(Some(&mut out.d), src, &mut pos, 1, nodes, idx)?;
                // Shift numbering to match Gregorian where Sunday = 1
                out.d = out.d.wrapping_add(1);
                if out.d > 7 {
                    out.d = 1;
                }
                skip_th(src, &mut pos, suffix);
            }
            WW | IW => {
                parse_int_len(Some(&mut out.ww), src, &mut pos, k.name.len(), nodes, idx)?;
                skip_th(src, &mut pos, suffix);
            }
            Q => {
                parse_int_len(None, src, &mut pos, k.name.len(), nodes, idx)?;
                skip_th(src, &mut pos, suffix);
            }
            CC => {
                parse_int_len(Some(&mut out.cc), src, &mut pos, k.name.len(), nodes, idx)?;
                skip_th(src, &mut pos, suffix);
            }
            Y_YYY => {
                let Some((millennia, years, nch)) = scan_y_yyy(&src[pos..]) else {
                    return Err(soft(
                        sqlstate::INVALID_DATETIME_FORMAT,
                        "invalid input string for \"Y,YYY\"",
                    ));
                };
                let years = years.wrapping_add(millennia.wrapping_mul(1000));
                set_int(&mut out.year, years, k)?;
                out.yysz = 4;
                pos += nch;
                skip_th(src, &mut pos, suffix);
            }
            YYYY | IYYY => {
                parse_int_len(Some(&mut out.year), src, &mut pos, k.name.len(), nodes, idx)?;
                out.yysz = 4;
                skip_th(src, &mut pos, suffix);
            }
            YYY | IYY | YY | IY | Y | I => {
                let len =
                    parse_int_len(Some(&mut out.year), src, &mut pos, k.name.len(), nodes, idx)?;
                if len < 4 {
                    out.year = adjust_partial_year_to_2020(out.year);
                }
                out.yysz = match k.id {
                    YYY | IYY => 3,
                    YY | IY => 2,
                    _ => 1,
                };
                skip_th(src, &mut pos, suffix);
            }
            RM | rm => {
                let v = seq_search_field(src, &mut pos, &RM_MONTHS_LOWER, k)?;
                set_int(&mut out.mm, 12 - v as i32, k)?;
            }
            W => {
                parse_int_len(Some(&mut out.w), src, &mut pos, k.name.len(), nodes, idx)?;
                skip_th(src, &mut pos, suffix);
            }
            J => {
                parse_int_len(Some(&mut out.j), src, &mut pos, k.name.len(), nodes, idx)?;
                skip_th(src, &mut pos, suffix);
            }
        }
        idx += 1;
    }
    // Standard parsing mode doesn't allow unmatched format patterns or
    // trailing characters in the input string.
    if idx < nodes.len() {
        return Err(soft(
            sqlstate::INVALID_DATETIME_FORMAT,
            "input string is too short for datetime format",
        ));
    }
    while pos < src.len() && is_space(src[pos]) {
        pos += 1;
    }
    if pos < src.len() {
        return Err(soft(
            sqlstate::INVALID_DATETIME_FORMAT,
            "trailing characters remain in input string after datetime format",
        ));
    }
    let _ = S_SP;
    let _ = S_TM;
    Ok(())
}

// ------------------------------------------------------- do_to_timestamp

/// A parsed datetime value, as parse_datetime() returns it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Parsed {
    Date(i32),
    Time(i64),
    /// time, zone (seconds west of UTC)
    TimeTz(i64, i32),
    Timestamp(i64),
    /// value, offset of the input (seconds west of UTC)
    TimestampTz(i64, i32),
}

fn field_overflow(date_str: &str) -> Error {
    soft(
        sqlstate::DATETIME_FIELD_OVERFLOW,
        format!("date/time field value out of range: \"{date_str}\""),
    )
}

const DTK_YEAR: u32 = 1 << 2;
const DTK_MONTH: u32 = 1 << 1;
const DTK_DAY: u32 = 1 << 3;
const DTK_DATE_M: u32 = DTK_YEAR | DTK_MONTH | DTK_DAY;

/// ValidateDate() with isjulian = true, is2digits = false, bc = false.
fn validate_date(fmask: u32, tm: &Tm) -> bool {
    if fmask & DTK_MONTH != 0 && (tm.mon < 1 || tm.mon > 12) {
        return false;
    }
    if fmask & DTK_DAY != 0 && (tm.mday < 1 || tm.mday > 31) {
        return false;
    }
    if fmask & DTK_DATE_M == DTK_DATE_M
        && tm.mday > DAY_TAB[isleap(tm.year) as usize][(tm.mon - 1) as usize]
    {
        return false;
    }
    true
}

/// A datetime template parsed in standard mode.
pub(super) struct Template {
    nodes: Vec<FNode>,
    empty: bool,
}

impl Template {
    /// parse_format() in standard mode; errors here are always raised.
    pub fn parse(fmt: &str) -> Result<Template> {
        Ok(Template {
            nodes: parse_format(fmt.as_bytes())?,
            empty: fmt.is_empty(),
        })
    }
}

/// do_to_timestamp() in standard mode: (tm, fsec, tz west, fprec, flags).
#[allow(clippy::type_complexity)]
fn do_to_timestamp(date_str: &str, fmt: &Template) -> DtResult<(Tm, i64, Option<i32>, i32, u32)> {
    let mut tmfc = TmFromChar::default();
    let mut tm = Tm {
        mday: 1,
        mon: 1,
        ..Tm::default()
    };
    // fsec_t is int32 in PostgreSQL
    let mut fsec: i32 = 0;
    let mut flags = 0;
    let mut fmask = 0u32;
    if !fmt.empty {
        from_char(&fmt.nodes, date_str.as_bytes(), &mut tmfc)?;
        flags = datetime_type(&fmt.nodes);
    }
    if tmfc.ssss != 0 {
        let x = tmfc.ssss;
        tm.hour = x / 3600;
        tm.min = (x % 3600) / 60;
        tm.sec = x % 60;
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
        if tm.hour < 1 || tm.hour > 12 {
            return Err(soft(
                sqlstate::INVALID_DATETIME_FORMAT,
                format!("hour \"{}\" is invalid for the 12-hour clock", tm.hour),
            ));
        }
        if tmfc.pm != 0 && tm.hour < 12 {
            tm.hour += 12;
        } else if tmfc.pm == 0 && tm.hour == 12 {
            tm.hour = 0;
        }
    }
    if tmfc.year != 0 {
        // If CC and YY (or Y) are provided, use YY as 2 low-order digits for
        // the year in the given century.
        if tmfc.cc != 0 && tmfc.yysz <= 2 {
            if tmfc.bc != 0 {
                tmfc.cc = tmfc.cc.wrapping_neg();
            }
            tm.year = tmfc.year % 100;
            if tm.year != 0 {
                if tmfc.cc >= 0 {
                    tm.year = tm
                        .year
                        .wrapping_add((tmfc.cc.wrapping_sub(1)).wrapping_mul(100));
                } else {
                    tm.year = (tmfc.cc.wrapping_add(1))
                        .wrapping_mul(100)
                        .wrapping_sub(tm.year)
                        .wrapping_add(1);
                }
            } else {
                // find century year for dates ending in "00"
                tm.year = tmfc
                    .cc
                    .wrapping_mul(100)
                    .wrapping_add(if tmfc.cc >= 0 { 0 } else { 1 });
            }
        } else {
            // If a 4-digit year is provided, we use that and ignore CC.
            tm.year = tmfc.year;
            if tmfc.bc != 0 {
                tm.year = tm.year.wrapping_neg();
            }
            // correct for our representation of BC years
            if tm.year < 0 {
                tm.year += 1;
            }
        }
        fmask |= DTK_YEAR;
    } else if tmfc.cc != 0 {
        // use first year of century
        if tmfc.bc != 0 {
            tmfc.cc = tmfc.cc.wrapping_neg();
        }
        tm.year = if tmfc.cc >= 0 {
            // +1 because 21st century started in 2001
            (tmfc.cc.wrapping_sub(1)).wrapping_mul(100).wrapping_add(1)
        } else {
            // +1 because year == 599 is 600 BC
            tmfc.cc.wrapping_mul(100).wrapping_add(1)
        };
        fmask |= DTK_YEAR;
    }
    if tmfc.j != 0 {
        let (y, m, d) = j2date(tmfc.j);
        (tm.year, tm.mon, tm.mday) = (y, m, d);
        fmask |= DTK_DATE_M;
    }
    if tmfc.ww != 0 {
        if tmfc.mode == Some(DateMode::IsoWeek) {
            // If tmfc.d is not set, then the date is left at the beginning of
            // the ISO week (Monday).
            if tmfc.d != 0 {
                isoweekdate2date(tmfc.ww, tmfc.d, &mut tm.year, &mut tm.mon, &mut tm.mday);
            } else {
                isoweek2date(tmfc.ww, &mut tm.year, &mut tm.mon, &mut tm.mday);
            }
            fmask |= DTK_DATE_M;
        } else {
            tmfc.ddd = (tmfc.ww.wrapping_sub(1)).wrapping_mul(7).wrapping_add(1);
        }
    }
    if tmfc.w != 0 {
        tmfc.dd = (tmfc.w.wrapping_sub(1)).wrapping_mul(7).wrapping_add(1);
    }
    if tmfc.dd != 0 {
        tm.mday = tmfc.dd;
        fmask |= DTK_DAY;
    }
    if tmfc.mm != 0 {
        tm.mon = tmfc.mm;
        fmask |= DTK_MONTH;
    }
    if tmfc.ddd != 0 && (tm.mon <= 1 || tm.mday <= 1) {
        // The month and day field have not been set, so we use the
        // day-of-year field to populate them.
        if tm.year == 0 && tmfc.bc == 0 {
            return Err(soft(
                sqlstate::INVALID_DATETIME_FORMAT,
                "cannot calculate day of year without year information",
            ));
        }
        if tmfc.mode == Some(DateMode::IsoWeek) {
            // zeroth day of the ISO year, in Julian
            let j0 = isoweek2j(tm.year, 1).wrapping_sub(1);
            let (y, m, d) = j2date(j0.wrapping_add(tmfc.ddd));
            (tm.year, tm.mon, tm.mday) = (y, m, d);
            fmask |= DTK_DATE_M;
        } else {
            const YSUM: [[i32; 13]; 2] = [
                [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365],
                [0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366],
            ];
            let y = &YSUM[isleap(tm.year) as usize];
            let mut i = 1;
            while i <= 12 {
                if tmfc.ddd <= y[i] {
                    break;
                }
                i += 1;
            }
            if tm.mon <= 1 {
                tm.mon = i as i32;
            }
            if tm.mday <= 1 {
                tm.mday = tmfc.ddd.wrapping_sub(y[i - 1]);
            }
            fmask |= DTK_MONTH | DTK_DAY;
        }
    }
    if tmfc.ms != 0 {
        fsec = fsec.wrapping_add(tmfc.ms.wrapping_mul(1000));
    }
    if tmfc.us != 0 {
        fsec = fsec.wrapping_add(tmfc.us);
    }
    let fprec = tmfc.ff;
    // Range-check date fields according to bit mask computed above
    if fmask != 0 && !validate_date(fmask, &tm) {
        return Err(field_overflow(date_str));
    }
    // Range-check time fields too
    if tm.hour < 0
        || tm.hour >= 24
        || tm.min < 0
        || tm.min >= 60
        || tm.sec < 0
        || tm.sec >= 60
        || !(0..USECS_PER_SEC).contains(&i64::from(fsec))
    {
        return Err(field_overflow(date_str));
    }
    // If timezone info was present, reduce it to a GMT offset.
    let mut tz = None;
    if tmfc.tzsign != 0 {
        if tmfc.tzh < 0 || tmfc.tzh > MAX_TZDISP_HOUR || tmfc.tzm < 0 || tmfc.tzm >= 60 {
            return Err(soft(
                sqlstate::INVALID_TIME_ZONE_DISPLACEMENT_VALUE,
                format!("time zone displacement out of range: \"{date_str}\""),
            ));
        }
        let mut off = (tmfc.tzh * 60 + tmfc.tzm) * 60;
        // note we are flipping the sign convention here
        if tmfc.tzsign > 0 {
            off = -off;
        }
        tz = Some(off);
    } else if tmfc.has_tz {
        tz = Some(match &tmfc.dyn_abbrev {
            None => -tmfc.gmtoffset,
            Some(abbrev) => dynamic_abbrev_offset(&tm, abbrev),
        });
    }
    Ok((tm, i64::from(fsec), tz, fprec, flags))
}

/// DetermineTimeZoneAbbrevOffset() for a dynamic abbreviation: the offset
/// (seconds west) the abbreviation meant at local time `tm`.
fn dynamic_abbrev_offset(tm: &Tm, abbrev: &str) -> i32 {
    let local = tm2timestamp(tm, 0, None).unwrap_or(0);
    match pgdatetime::timezone_ts_by_name(abbrev, local) {
        Ok(utc) => ((utc - local) / USECS_PER_SEC) as i32,
        Err(_) => 0,
    }
}

/// parse_datetime() in strict mode: the value, its typmod (the fractional
/// precision given by FFn, or -1) and, for zoned types, the offset.
pub(super) fn parse_datetime(date_str: &str, fmt: &Template) -> DtResult<(Parsed, i32)> {
    let (tm, fsec, tz, fprec, flags) = do_to_timestamp(date_str, fmt)?;
    let typmod = if fprec != 0 { fprec } else { -1 };
    if flags & DCH_DATED != 0 {
        if flags & DCH_TIMED != 0 {
            if flags & DCH_ZONED != 0 {
                let Some(tz) = tz else {
                    return Err(soft(
                        sqlstate::INVALID_DATETIME_FORMAT,
                        "missing time zone in input string for type timestamptz",
                    ));
                };
                let Some(r) = tm2timestamp(&tm, fsec, Some(tz)) else {
                    return Err(soft(
                        sqlstate::DATETIME_VALUE_OUT_OF_RANGE,
                        "timestamptz out of range",
                    ));
                };
                return Ok((Parsed::TimestampTz(adjust_timestamp(r, typmod), tz), typmod));
            }
            let Some(r) = tm2timestamp(&tm, fsec, None) else {
                return Err(soft(
                    sqlstate::DATETIME_VALUE_OUT_OF_RANGE,
                    "timestamp out of range",
                ));
            };
            return Ok((Parsed::Timestamp(adjust_timestamp(r, typmod)), typmod));
        }
        if flags & DCH_ZONED != 0 {
            return Err(soft(
                sqlstate::INVALID_DATETIME_FORMAT,
                "datetime format is zoned but not timed",
            ));
        }
        // Prevent overflow in Julian-day routines
        if !is_valid_julian(tm.year, tm.mon, tm.mday) {
            return Err(soft(
                sqlstate::DATETIME_VALUE_OUT_OF_RANGE,
                format!("date out of range: \"{date_str}\""),
            ));
        }
        let result = date2j(tm.year, tm.mon, tm.mday).wrapping_sub(POSTGRES_EPOCH_JDATE);
        // Now check for just-out-of-range dates
        if !is_valid_date(result) {
            return Err(soft(
                sqlstate::DATETIME_VALUE_OUT_OF_RANGE,
                format!("date out of range: \"{date_str}\""),
            ));
        }
        return Ok((Parsed::Date(result), typmod));
    }
    if flags & DCH_TIMED != 0 {
        let time = ((i64::from(tm.hour) * 60 + i64::from(tm.min)) * 60 + i64::from(tm.sec))
            * USECS_PER_SEC
            + fsec;
        if flags & DCH_ZONED != 0 {
            let Some(tz) = tz else {
                return Err(soft(
                    sqlstate::INVALID_DATETIME_FORMAT,
                    "missing time zone in input string for type timetz",
                ));
            };
            return Ok((Parsed::TimeTz(adjust_time(time, typmod), tz), typmod));
        }
        return Ok((Parsed::Time(adjust_time(time, typmod)), typmod));
    }
    Err(soft(
        sqlstate::INVALID_DATETIME_FORMAT,
        "datetime format is not dated and not timed",
    ))
}

/// AdjustTimeForTypmod()
pub(super) fn adjust_time(t: i64, typmod: i32) -> i64 {
    if (0..=6).contains(&typmod) {
        adjust_precision(t, typmod)
    } else {
        t
    }
}

/// AdjustTimestampForTypmod() (typmod is -1 or 1..=6 here)
pub(super) fn adjust_timestamp(t: i64, typmod: i32) -> i64 {
    if typmod != -1 && typmod != 6 && (0..=6).contains(&typmod) {
        adjust_precision(t, typmod)
    } else {
        t
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn julian() {
        assert_eq!(date2j(2000, 1, 1), POSTGRES_EPOCH_JDATE);
        assert_eq!(j2date(POSTGRES_EPOCH_JDATE), (2000, 1, 1));
        for jd in [0, 1, 1721426, 2440588, 2451545, 2459945, 5373484] {
            let (y, m, d) = j2date(jd);
            assert_eq!(date2j(y, m, d), jd);
        }
    }

    #[test]
    fn iso_formats() {
        let p = |s: &str, f: &str| {
            parse_datetime(s, &Template::parse(f).unwrap())
                .ok()
                .map(|r| r.0)
        };
        assert_eq!(p("2017-03-10", "yyyy-mm-dd"), Some(Parsed::Date(6278)));
        assert_eq!(
            p("12:34:56", "HH24:MI:SS"),
            Some(Parsed::Time(45_296_000_000))
        );
        assert_eq!(
            p("12:34:56+05:30", "HH24:MI:SSTZ"),
            Some(Parsed::TimeTz(45_296_000_000, -19800))
        );
        assert!(p("12:34:56", "HH24:MI:SS.US").is_none());
        assert!(p("2017-03-10x", "yyyy-mm-dd").is_none());
    }
}
