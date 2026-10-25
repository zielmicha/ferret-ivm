//! Time zone support: zone lookup (pg_tzset), abbreviations, and the
//! DetermineTimeZoneOffset() family from datetime.c.
//!
//! Zone data: the tz database (public domain) compiled TZif files from the
//! system that PostgreSQL 17 uses (`--with-system-tzdata`), embedded at build
//! time (see tools/gen_tzdata.py), so results do not depend on the runtime
//! machine.

mod abbrevs;
pub(crate) mod zone;

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Mutex, OnceLock};

use crate::core::*;
use crate::error::*;
use crate::{Error, Result, TimeZone};
use zone::ZoneState;

static TZDATA: &[u8] = include_bytes!("tzdata.bin");

#[derive(Clone, Copy, Debug)]
pub(crate) enum AbbrevKind {
    /// fixed offset, standard time (seconds east of UTC)
    Tz(i32),
    /// fixed offset, daylight time (seconds east of UTC)
    Dtz(i32),
    /// dynamic abbreviation: meaning depends on the given zone's history
    Dyn(&'static str),
}

/// Time zone abbreviation for display.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum TzName {
    Static(&'static str),
    /// abbreviation of a fixed-offset zone made by pg_tzset_offset(), e.g. "+05:30"
    Fixed(i32),
}

impl TzName {
    pub fn as_string(self) -> String {
        match self {
            TzName::Static(s) => s.to_string(),
            TzName::Fixed(utoff) => fixed_offset_abbrev(utoff),
        }
    }
}

/// Abbreviation of the zone pg_tzset_offset() makes for `utoff` seconds east.
pub(crate) fn fixed_offset_abbrev(utoff: i32) -> String {
    let gmtoffset = -(utoff as i64);
    let mut absoffset = gmtoffset.abs();
    let mut s = String::new();
    s.push(if gmtoffset > 0 { '-' } else { '+' });
    s.push_str(&format!("{:02}", absoffset / 3600));
    absoffset %= 3600;
    if absoffset != 0 {
        s.push_str(&format!(":{:02}", absoffset / 60));
        absoffset %= 60;
        if absoffset != 0 {
            s.push_str(&format!(":{:02}", absoffset));
        }
    }
    s
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct LocalTime {
    /// seconds east of UTC
    pub utoff: i32,
    pub isdst: bool,
    pub abbrev: TzName,
}

/// A resolved zone.
#[derive(Clone, Copy, Debug)]
pub(crate) enum ZoneRef {
    Utc,
    /// seconds east of UTC
    Fixed(i32),
    State(&'static ZoneState),
}

impl ZoneRef {
    /// pg_localtime(): offset/isdst/abbrev in effect at Unix time `t`.
    pub fn localtime(&self, t: i64) -> LocalTime {
        match *self {
            ZoneRef::Utc => LocalTime {
                utoff: 0,
                isdst: false,
                abbrev: TzName::Static("UTC"),
            },
            ZoneRef::Fixed(off) => LocalTime {
                utoff: off,
                isdst: false,
                abbrev: TzName::Fixed(off),
            },
            ZoneRef::State(sp) => {
                let i = sp.type_at(t);
                let tt = &sp.ttis[i];
                LocalTime {
                    utoff: tt.utoff,
                    isdst: tt.isdst,
                    abbrev: TzName::Static(static_abbrev(sp, tt.desigidx)),
                }
            }
        }
    }

    /// pg_get_timezone_offset(): Some(seconds east) if the zone has a single offset.
    pub fn single_offset(&self) -> Option<i32> {
        match *self {
            ZoneRef::Utc => Some(0),
            ZoneRef::Fixed(off) => Some(off),
            ZoneRef::State(sp) => sp.single_offset(),
        }
    }

    fn next_dst_boundary(&self, t: i64) -> (i32, i32, bool, i64, i32, bool) {
        match *self {
            ZoneRef::Utc => (0, 0, false, 0, 0, false),
            ZoneRef::Fixed(off) => (0, off, false, 0, 0, false),
            ZoneRef::State(sp) => sp.next_dst_boundary(t),
        }
    }
}

fn static_abbrev(sp: &'static ZoneState, idx: usize) -> &'static str {
    sp.abbrev_at(idx)
}

/// DetermineTimeZoneOffsetInternal(): returns (tz (seconds west), isdst, t).
pub(crate) fn determine_offset_internal(tm: &Tm, zone: &ZoneRef) -> (i32, i32, i64) {
    if !is_valid_julian(tm.year, tm.mon, tm.mday) {
        return (0, 0, 0);
    }
    let date = (date2j(tm.year, tm.mon, tm.mday) - UNIX_EPOCH_JDATE) as i64;
    let day = date * SECS_PER_DAY;
    let sec = tm.sec as i64 + (tm.min as i64 + tm.hour as i64 * MINS_PER_HOUR) * SECS_PER_MINUTE;
    let mytime = day + sec;
    let prevtime = mytime - SECS_PER_DAY;
    let (res, before_gmtoff, before_isdst, boundary, after_gmtoff, after_isdst) =
        zone.next_dst_boundary(prevtime);
    if res < 0 {
        return (0, 0, 0);
    }
    let before_gmtoff = before_gmtoff as i64;
    let after_gmtoff = after_gmtoff as i64;
    let neg = |g: i64| (g as i32).wrapping_neg();
    if res == 0 {
        return (
            neg(before_gmtoff),
            before_isdst as i32,
            mytime - before_gmtoff,
        );
    }
    let beforetime = mytime - before_gmtoff;
    let aftertime = mytime - after_gmtoff;
    if beforetime < boundary && aftertime < boundary {
        return (neg(before_gmtoff), before_isdst as i32, beforetime);
    }
    if beforetime > boundary && aftertime >= boundary {
        return (neg(after_gmtoff), after_isdst as i32, aftertime);
    }
    if beforetime > aftertime {
        return (neg(before_gmtoff), before_isdst as i32, beforetime);
    }
    (neg(after_gmtoff), after_isdst as i32, aftertime)
}

/// DetermineTimeZoneOffset(): seconds WEST of UTC; also sets tm.isdst.
pub(crate) fn determine_offset(tm: &mut Tm, zone: &ZoneRef) -> i32 {
    let (tz, isdst, _) = determine_offset_internal(tm, zone);
    tm.isdst = isdst;
    tz
}

/// DetermineTimeZoneAbbrevOffset()
pub(crate) fn determine_abbrev_offset(tm: &mut Tm, abbr: &str, zone: &ZoneRef) -> i32 {
    let (zone_offset, isdst, t) = determine_offset_internal(tm, zone);
    if let ZoneRef::State(sp) = zone
        && let Some((utoff, abbr_isdst)) = sp.interpret_abbrev(&abbr.to_ascii_uppercase(), t)
    {
        tm.isdst = abbr_isdst as i32;
        return -utoff;
    }
    tm.isdst = isdst;
    zone_offset
}

/// DetermineTimeZoneAbbrevOffsetTS(): returns (tz west, isdst)
pub(crate) fn determine_abbrev_offset_ts(
    ts: i64,
    abbr: &str,
    zone: &ZoneRef,
) -> Result<(i32, i32)> {
    let t = ts / USECS_PER_SEC + (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) as i64 * SECS_PER_DAY;
    if let ZoneRef::State(sp) = zone
        && let Some((utoff, isdst)) = sp.interpret_abbrev(&abbr.to_ascii_uppercase(), t)
    {
        return Ok((-utoff, isdst as i32));
    }
    let tt = timestamp2tm_tz(ts, zone)?;
    let mut tm = tt.tm;
    let off = determine_offset(&mut tm, zone);
    Ok((off, tm.isdst))
}

// ---------------------------------------------------------------------------
// Zone database
// ---------------------------------------------------------------------------

struct TzIndex {
    version: &'static str,
    /// upper-case name -> (canonical name, blob index, name index)
    names: HashMap<String, (&'static str, usize, usize)>,
    blobs: Vec<&'static [u8]>,
    /// names sharing each blob (links)
    blob_names: Vec<Vec<&'static str>>,
    /// lazily loaded zone state per name (lock-free after first use)
    loaded: Vec<OnceLock<Option<&'static ZoneState>>>,
}

fn tz_index() -> &'static TzIndex {
    static INDEX: OnceLock<TzIndex> = OnceLock::new();
    INDEX.get_or_init(|| {
        let d = TZDATA;
        let mut p = 8usize; // "PGTZDATA"
        let vlen = d[p] as usize;
        p += 1;
        let version = std::str::from_utf8(&d[p..p + vlen]).unwrap_or("unknown");
        p += vlen;
        let rd_u32 = |p: usize| u32::from_le_bytes([d[p], d[p + 1], d[p + 2], d[p + 3]]) as usize;
        let nnames = rd_u32(p);
        p += 4;
        let mut names = HashMap::with_capacity(nnames);
        for i in 0..nnames {
            let l = d[p] as usize;
            p += 1;
            let name = std::str::from_utf8(&d[p..p + l]).unwrap_or("");
            p += l;
            let idx = u16::from_le_bytes([d[p], d[p + 1]]) as usize;
            p += 2;
            names.insert(name.to_ascii_uppercase(), (name, idx, i));
        }
        let nblobs = rd_u32(p);
        p += 4;
        let mut blobs = Vec::with_capacity(nblobs);
        for _ in 0..nblobs {
            let l = rd_u32(p);
            p += 4;
            blobs.push(&d[p..p + l]);
            p += l;
        }
        let mut blob_names: Vec<Vec<&'static str>> = vec![Vec::new(); nblobs];
        for (canon, idx, _) in names.values() {
            blob_names[*idx].push(*canon);
        }
        for v in blob_names.iter_mut() {
            v.sort_unstable();
        }
        let loaded = (0..nnames).map(|_| OnceLock::new()).collect();
        TzIndex {
            version,
            names,
            blobs,
            blob_names,
            loaded,
        }
    })
}

/// Version of the embedded tz database (e.g. "2026e").
pub fn tzdata_version() -> &'static str {
    tz_index().version
}

fn zone_cache() -> &'static Mutex<HashMap<String, Option<&'static ZoneState>>> {
    static CACHE: OnceLock<Mutex<HashMap<String, Option<&'static ZoneState>>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Load a zone from the embedded database by (case-insensitive) name.
fn load_tzdata_zone(upper: &str) -> Option<&'static ZoneState> {
    let idx = tz_index();
    let (canon, blob, i) = *idx.names.get(upper)?;
    *idx.loaded[i].get_or_init(|| {
        ZoneState::from_tzif(canon, idx.blobs[blob]).map(|z| &*Box::leak(Box::new(z)))
    })
}

/// A zone found by pg_tzset().
#[derive(Clone, Copy, Debug)]
pub(crate) enum TzsetResult {
    /// zone from the tz database, with canonical name
    File(&'static ZoneState),
    /// POSIX TZ string with DST rules (or "GMT")
    Posix(&'static ZoneState),
    /// POSIX TZ string without DST: fixed offset (seconds east)
    PosixFixed(i32),
}

impl TzsetResult {
    pub fn zone_ref(self) -> ZoneRef {
        match self {
            TzsetResult::File(z) | TzsetResult::Posix(z) => ZoneRef::State(z),
            TzsetResult::PosixFixed(off) => ZoneRef::Fixed(off),
        }
    }
}

/// Upper bound on cached (leaked) POSIX-with-DST zone states.
const MAX_POSIX_ZONES: usize = 4096;

/// pg_tzset(): look up a zone name (case-insensitively), falling back to
/// parsing it as a POSIX TZ string.
pub(crate) fn pg_tzset(name: &str) -> Option<TzsetResult> {
    if name.len() > 255 {
        return None;
    }
    let upper = name.to_ascii_uppercase();
    if upper == "GMT" {
        let mut cache = zone_cache().lock().unwrap();
        let key = "P:GMT".to_string();
        if let Some(z) = cache.get(&key) {
            return z.map(TzsetResult::Posix);
        }
        let z = zone::tzparse("GMT", true).map(|mut z| {
            z.name = "GMT".to_string();
            &*Box::leak(Box::new(z))
        });
        cache.insert(key, z);
        return z.map(TzsetResult::Posix);
    }
    if let Some(z) = load_tzdata_zone(&upper) {
        return Some(TzsetResult::File(z));
    }
    if upper.starts_with(':') {
        return None;
    }
    let key = format!("P:{}", upper);
    if let Some(z) = zone_cache().lock().unwrap().get(&key) {
        return z.map(TzsetResult::Posix);
    }
    let mut z = zone::tzparse(&upper, false)?;
    if z.ats.is_empty()
        && let Some(off) = z.single_offset()
    {
        return Some(TzsetResult::PosixFixed(off));
    }
    z.name = upper;
    let mut cache = zone_cache().lock().unwrap();
    if cache.len() >= MAX_POSIX_ZONES {
        return None;
    }
    let z: &'static ZoneState = Box::leak(Box::new(z));
    cache.insert(key, Some(z));
    Some(TzsetResult::Posix(z))
}

/// DecodeTimezoneAbbrev(): look up a lower-case token in the abbreviation table.
pub(crate) fn lookup_abbrev(lowtoken: &str) -> Option<AbbrevKind> {
    let key = truncate_token(lowtoken);
    abbrevs::ABBREVS
        .binary_search_by(|(t, _)| (*t).cmp(key))
        .ok()
        .map(|i| abbrevs::ABBREVS[i].1)
}

/// Truncate to TOKMAXLEN (10) bytes, emulating strncmp(.., TOKMAXLEN) lookups.
pub(crate) fn truncate_token(s: &str) -> &str {
    if s.len() <= 10 {
        s
    } else {
        let mut end = 10;
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        &s[..end]
    }
}

/// Resolve a dynamic abbreviation's zone (FetchDynamicTimeZone()).
pub(crate) fn fetch_dynamic_zone(zone: &'static str) -> Option<ZoneRef> {
    pg_tzset(zone).map(|r| r.zone_ref())
}

/// downcase_truncate_identifier() for zone names (ASCII downcasing,
/// truncation to NAMEDATALEN-1 bytes).
pub(crate) fn downcase_truncate(s: &str) -> String {
    let mut out = s.to_ascii_lowercase();
    if out.len() > 63 {
        let mut end = 63;
        while !out.is_char_boundary(end) {
            end -= 1;
        }
        out.truncate(end);
    }
    out
}

// ---------------------------------------------------------------------------
// TimeZone (public)
// ---------------------------------------------------------------------------

/// Result of DecodeTimezoneName().
#[derive(Clone, Copy, Debug)]
pub(crate) enum DecodedTzName {
    /// fixed offset, seconds EAST of UTC
    Fixed(i32),
    /// dynamic abbreviation with its underlying zone
    Dyn(ZoneRef),
    Zone(ZoneRef),
}

/// DecodeTimezoneName(): abbreviation table first, then zone database /
/// POSIX TZ string.
pub(crate) fn decode_timezone_name(tzname: &str) -> Result<DecodedTzName> {
    let lowzone = downcase_truncate(tzname);
    match lookup_abbrev(&lowzone) {
        Some(AbbrevKind::Tz(off)) | Some(AbbrevKind::Dtz(off)) => Ok(DecodedTzName::Fixed(off)),
        Some(AbbrevKind::Dyn(zone)) => match fetch_dynamic_zone(zone) {
            Some(z) => Ok(DecodedTzName::Dyn(z)),
            None => {
                let extra = DtErrExtra {
                    timezone: zone.to_string(),
                    abbrev: lowzone.clone(),
                };
                Err(datetime_parse_error(DTERR_BAD_ZONE_ABBREV, &extra, "", ""))
            }
        },
        None => match pg_tzset(tzname) {
            Some(r) => Ok(DecodedTzName::Zone(r.zone_ref())),
            None => Err(Error::new(
                INVALID_PARAMETER_VALUE,
                format!("time zone \"{}\" not recognized", tzname),
            )),
        },
    }
}

fn tz_variant_index(tz: chrono_tz::Tz) -> usize {
    tz as usize
}

fn named_zone_ref(tz: chrono_tz::Tz) -> ZoneRef {
    static BY_TZ: OnceLock<Vec<OnceLock<ZoneRef>>> = OnceLock::new();
    let table = BY_TZ.get_or_init(|| {
        (0..chrono_tz::TZ_VARIANTS.len() + 1)
            .map(|_| OnceLock::new())
            .collect()
    });
    let idx = tz_variant_index(tz);
    let resolve = || -> ZoneRef {
        let name = tz.name();
        if let Some(z) = load_tzdata_zone(&name.to_ascii_uppercase()) {
            return ZoneRef::State(z);
        }
        // Not in the embedded database: build a state from chrono-tz data.
        ZoneRef::State(Box::leak(Box::new(chrono_fallback_state(tz))))
    };
    match table.get(idx) {
        Some(cell) => *cell.get_or_init(resolve),
        None => resolve(),
    }
}

/// Fallback for zones unknown to the embedded database: approximate the
/// transition table from chrono-tz by scanning.
fn chrono_fallback_state(tz: chrono_tz::Tz) -> ZoneState {
    use chrono::{DateTime, Offset, TimeZone as _};
    use chrono_tz::{OffsetComponents, OffsetName};
    let info = |t: i64| -> (i32, bool, String) {
        let dt = DateTime::from_timestamp(t, 0)
            .unwrap_or_default()
            .naive_utc();
        let off = tz.offset_from_utc_datetime(&dt);
        let utoff = off.fix().local_minus_utc();
        let isdst = off.dst_offset().num_seconds() != 0;
        // numeric abbreviations ("+1030") have no name in chrono-tz; its
        // Display renders them like the tz database does
        (
            utoff,
            isdst,
            off.abbreviation()
                .map(str::to_string)
                .unwrap_or_else(|| off.to_string()),
        )
    };
    let start = -5_364_662_400i64; // 1800-01-01
    let end = 4_102_444_800i64; // 2100-01-01
    let mut types: Vec<(i32, bool, String)> = Vec::new();
    let mut ats = Vec::new();
    let mut tidx = Vec::new();
    let type_of = |x: (i32, bool, String), types: &mut Vec<(i32, bool, String)>| -> u8 {
        if let Some(i) = types.iter().position(|y| *y == x) {
            i as u8
        } else {
            types.push(x);
            (types.len() - 1) as u8
        }
    };
    let first = info(start);
    type_of(first.clone(), &mut types);
    let mut prev = first;
    let mut t = start;
    while t < end {
        let next = t + 86400;
        let cur = info(next);
        if cur != prev {
            let (mut lo, mut hi) = (t, next);
            while hi - lo > 1 {
                let mid = lo + (hi - lo) / 2;
                if info(mid) == prev {
                    lo = mid
                } else {
                    hi = mid
                }
            }
            let ty = type_of(cur.clone(), &mut types);
            ats.push(hi);
            tidx.push(ty);
            prev = cur;
        }
        t = next;
    }
    let mut chars = Vec::new();
    let mut ttis = Vec::new();
    for (utoff, isdst, abbr) in types {
        let desigidx = chars.len();
        chars.extend_from_slice(abbr.as_bytes());
        chars.push(0);
        ttis.push(zone::TtInfo {
            utoff,
            isdst,
            desigidx,
            ttisstd: false,
            ttisut: false,
        });
    }
    ZoneState {
        name: tz.name().to_string(),
        ats,
        types: tidx,
        ttis,
        chars,
        goback: false,
        goahead: false,
        defaulttype: 0,
    }
}

impl TimeZone {
    /// Parse a zone the way `AT TIME ZONE` / `timezone(zone, ...)` do
    /// (DecodeTimezoneName): the abbreviation table first ('PST', 'CET', 'UTC'
    /// are fixed offsets), then a zone name ('Europe/Warsaw', 'EST5EDT',
    /// case-insensitive), then a POSIX TZ string ('+02:00' and '-5' use the
    /// POSIX sign convention: positive is WEST of Greenwich).
    ///
    /// Dynamic abbreviations (e.g. 'MSK') resolve to their underlying zone;
    /// POSIX strings with DST rules that are not zone names are rejected.
    pub fn parse(name: &str) -> Result<TimeZone> {
        if name.eq_ignore_ascii_case("utc") {
            return Ok(TimeZone::Utc);
        }
        let lowzone = downcase_truncate(name);
        match lookup_abbrev(&lowzone) {
            Some(AbbrevKind::Tz(off)) | Some(AbbrevKind::Dtz(off)) => Ok(TimeZone::Fixed(off)),
            Some(AbbrevKind::Dyn(zone)) => match pg_tzset(zone) {
                Some(r) => tzset_to_timezone(r, name),
                None => {
                    let extra = DtErrExtra {
                        timezone: zone.to_string(),
                        abbrev: lowzone.clone(),
                    };
                    Err(datetime_parse_error(DTERR_BAD_ZONE_ABBREV, &extra, "", ""))
                }
            },
            None => match pg_tzset(name) {
                Some(r) => tzset_to_timezone(r, name),
                None => Err(Error::new(
                    INVALID_PARAMETER_VALUE,
                    format!("time zone \"{}\" not recognized", name),
                )),
            },
        }
    }

    /// Parse a zone the way `SET TIME ZONE` / the TimeZone setting does
    /// (check_timezone): `INTERVAL '...'`, a plain number of hours (SQL sign
    /// convention: positive is EAST), or a zone name / POSIX TZ string.  The
    /// abbreviation table is not consulted.
    pub fn parse_session(name: &str) -> Result<TimeZone> {
        let bad = || {
            Error::new(
                INVALID_PARAMETER_VALUE,
                format!("invalid value for parameter \"TimeZone\": \"{}\"", name),
            )
        };
        if name.len() >= 8 && name.as_bytes()[..8].eq_ignore_ascii_case(b"interval") {
            let rest = name[8..].trim_start_matches(|c: char| c.is_ascii_whitespace());
            let rest = rest.strip_prefix('\'').ok_or_else(bad)?;
            let end = rest.find('\'').ok_or_else(bad)?;
            if end + 1 != rest.len() {
                return Err(bad());
            }
            let iv = crate::parse_interval(&rest[..end])?;
            if iv.months != 0 || iv.days != 0 {
                return Err(bad());
            }
            let gmtoffset = -(iv.micros / USECS_PER_SEC);
            return fixed_from_gmtoffset(gmtoffset).ok_or_else(bad);
        }
        if let Some(hours) = crate::datetime::c_strtod_full(name) {
            let gmtoffset = (-hours * SECS_PER_HOUR as f64) as i64;
            return fixed_from_gmtoffset(gmtoffset).ok_or_else(bad);
        }
        match pg_tzset(name) {
            Some(TzsetResult::File(z)) if z.name == "UTC" => Ok(TimeZone::Utc),
            Some(r) => tzset_to_timezone(r, name).map_err(|_| bad()),
            None => Err(bad()),
        }
    }

    /// Canonical name, as `SHOW timezone` would print for a session zone.
    pub fn name(&self) -> String {
        match self {
            TimeZone::Utc => "UTC".to_string(),
            TimeZone::Fixed(off) => {
                let gmtoffset = -(*off as i64);
                let a = fixed_offset_abbrev(*off);
                let mut b = String::new();
                let mut absoffset = gmtoffset.abs();
                b.push_str(&format!("{:02}", absoffset / 3600));
                absoffset %= 3600;
                if absoffset != 0 {
                    b.push_str(&format!(":{:02}", absoffset / 60));
                    absoffset %= 60;
                    if absoffset != 0 {
                        b.push_str(&format!(":{:02}", absoffset));
                    }
                }
                format!("<{}>{}{}", a, if gmtoffset > 0 { '+' } else { '-' }, b)
            }
            TimeZone::Named(tz) => tz.name().to_string(),
        }
    }

    pub(crate) fn zone_ref(&self) -> ZoneRef {
        match self {
            TimeZone::Utc => ZoneRef::Utc,
            TimeZone::Fixed(off) => ZoneRef::Fixed(*off),
            TimeZone::Named(tz) => named_zone_ref(*tz),
        }
    }
}

/// pg_tzset_offset() equivalent; gmtoffset is seconds WEST (POSIX).
fn fixed_from_gmtoffset(gmtoffset: i64) -> Option<TimeZone> {
    // pg_tzset_offset() goes through tzparse(), which limits hours to 167
    let abs = gmtoffset.unsigned_abs();
    let hours = abs / 3600;
    if hours > 167 {
        return None;
    }
    Some(TimeZone::Fixed((-gmtoffset) as i32))
}

fn tzset_to_timezone(r: TzsetResult, name: &str) -> Result<TimeZone> {
    match r {
        TzsetResult::File(z) => {
            if z.name == "UTC" {
                return Ok(TimeZone::Utc);
            }
            if let Ok(tz) = chrono_tz::Tz::from_str(&z.name) {
                return Ok(TimeZone::Named(tz));
            }
            // A name chrono-tz does not know (e.g. "posixrules"): use another
            // name for identical data, or a fixed offset.
            let idx = tz_index();
            if let Some((_, blob, _)) = idx.names.get(&z.name.to_ascii_uppercase()) {
                for other in &idx.blob_names[*blob] {
                    if let Ok(tz) = chrono_tz::Tz::from_str(other) {
                        return Ok(TimeZone::Named(tz));
                    }
                }
            }
            match z.single_offset() {
                Some(off) => Ok(TimeZone::Fixed(off)),
                None => Err(Error::new(
                    INVALID_PARAMETER_VALUE,
                    format!("time zone \"{}\" not recognized", name),
                )),
            }
        }
        TzsetResult::PosixFixed(off) => Ok(TimeZone::Fixed(off)),
        TzsetResult::Posix(z) => {
            if z.name == "GMT" {
                return Ok(TimeZone::Named(chrono_tz::Tz::GMT));
            }
            Err(Error::new(
                FEATURE_NOT_SUPPORTED,
                format!(
                    "time zone \"{}\" with daylight-saving rules is not supported",
                    name
                ),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The chrono-tz fallback (only used for zones missing from the embedded
    /// database) must agree with the embedded data where both exist
    /// (1900..2099; chrono-tz has no rules beyond its table).
    #[test]
    fn chrono_fallback_matches_embedded() {
        for tz in [
            chrono_tz::Tz::Europe__Warsaw,
            chrono_tz::Tz::America__New_York,
            chrono_tz::Tz::Australia__Lord_Howe,
        ] {
            let fallback = chrono_fallback_state(tz);
            let embedded = load_tzdata_zone(&tz.name().to_ascii_uppercase()).unwrap();
            let mut t = -2_208_988_800i64; // 1900
            while t < 4_000_000_000 {
                let a = &fallback.ttis[fallback.type_at(t)];
                let b = &embedded.ttis[embedded.type_at(t)];
                assert_eq!(
                    (a.utoff, a.isdst),
                    (b.utoff, b.isdst),
                    "{} at {}",
                    tz.name(),
                    t
                );
                assert_eq!(
                    fallback.abbrev_at(a.desigidx),
                    embedded.abbrev_at(b.desigidx),
                    "{} at {}",
                    tz.name(),
                    t
                );
                t += 7 * 3600 + 13;
            }
        }
    }

    #[test]
    fn posix_rules() {
        // DST rule from a POSIX string, evaluated far in the future
        let z = zone::tzparse("CET-1CEST,M3.5.0,M10.5.0/3", false).unwrap();
        let summer_2300 = 10_413_792_000i64; // 2300-01-01 + ~0.5 year (approx)
        let i = z.type_at(summer_2300 + 182 * 86400);
        assert_eq!(z.ttis[i].utoff, 7200);
        assert!(zone::tzparse("<+0330>-3:30", false).is_some());
        assert!(zone::tzparse("-5", false).is_some());
        assert!(zone::tzparse("", false).is_none());
    }
}
