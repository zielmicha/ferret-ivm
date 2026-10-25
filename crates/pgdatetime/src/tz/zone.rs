//! Time zone state: a port of PostgreSQL's src/timezone/localtime.c
//! (public domain, derived from the IANA tz code) limited to what the
//! date/time functions need.

const TZ_MAX_TIMES: usize = 2000;
const TZ_MAX_TYPES: usize = 256;
const TZ_MAX_CHARS: usize = 50;
const YEARSPERREPEAT: i64 = 400;
const AVGSECSPERYEAR: i64 = 31556952;
const SECSPERREPEAT: i64 = YEARSPERREPEAT * AVGSECSPERYEAR;
const SECSPERDAY: i32 = 86400;
const SECSPERHOUR: i32 = 3600;
const SECSPERMIN: i32 = 60;
const EPOCH_YEAR: i32 = 1970;
const DAYSPERNYEAR: i32 = 365;
const DAYSPERLYEAR: i32 = 366;
const DAYSPERWEEK: i32 = 7;
const MONSPERYEAR: i32 = 12;
const HOURSPERDAY: i32 = 24;
const MINSPERHOUR: i32 = 60;
const TZDEFRULESTRING: &str = ",M3.2.0,M11.1.0";

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct TtInfo {
    pub utoff: i32,
    pub isdst: bool,
    pub desigidx: usize,
    pub ttisstd: bool,
    pub ttisut: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct ZoneState {
    /// canonical name (TZname)
    pub name: String,
    pub ats: Vec<i64>,
    pub types: Vec<u8>,
    pub ttis: Vec<TtInfo>,
    /// abbreviation characters, NUL separated
    pub chars: Vec<u8>,
    pub goback: bool,
    pub goahead: bool,
    pub defaulttype: usize,
}

#[inline]
fn isleap(y: i32) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}

const MON_LENGTHS: [[i32; 12]; 2] = [
    [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
    [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
];
const YEAR_LENGTHS: [i32; 2] = [DAYSPERNYEAR, DAYSPERLYEAR];

impl ZoneState {
    fn empty(name: &str) -> ZoneState {
        ZoneState {
            name: name.to_string(),
            ats: Vec::new(),
            types: Vec::new(),
            ttis: Vec::new(),
            chars: Vec::new(),
            goback: false,
            goahead: false,
            defaulttype: 0,
        }
    }

    /// The abbreviation starting at byte index `idx` of `chars`.
    pub fn abbrev_at(&self, idx: usize) -> &str {
        let bytes = &self.chars[idx.min(self.chars.len())..];
        let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
        std::str::from_utf8(&bytes[..end]).unwrap_or("")
    }

    fn typesequiv(&self, a: usize, b: usize) -> bool {
        if a >= self.ttis.len() || b >= self.ttis.len() {
            return false;
        }
        let ap = &self.ttis[a];
        let bp = &self.ttis[b];
        ap.utoff == bp.utoff
            && ap.isdst == bp.isdst
            && ap.ttisstd == bp.ttisstd
            && ap.ttisut == bp.ttisut
            && self.abbrev_at(ap.desigidx) == self.abbrev_at(bp.desigidx)
    }

    /// Port of tzloadbody() for an in-memory TZif image (we only embed the
    /// v2+ section, or the v1 section of old files).
    pub fn from_tzif(name: &str, data: &[u8]) -> Option<ZoneState> {
        let mut sp = ZoneState::empty(name);
        if data.len() < 44 || &data[0..4] != b"TZif" {
            return None;
        }
        let version = data[4];
        let stored: usize = if version == 0 { 4 } else { 8 };
        let rd32 = |p: usize| -> Option<i32> {
            data.get(p..p + 4)
                .map(|b| i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
        };
        let rd64 = |p: usize| -> Option<i64> {
            data.get(p..p + 8)
                .map(|b| i64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
        };
        let ttisutcnt = rd32(20)? as usize;
        let ttisstdcnt = rd32(24)? as usize;
        let leapcnt = rd32(28)? as usize;
        let timecnt = rd32(32)? as usize;
        let typecnt = rd32(36)? as usize;
        let charcnt = rd32(40)? as usize;
        if !(typecnt < TZ_MAX_TYPES
            && timecnt < TZ_MAX_TIMES
            && charcnt < TZ_MAX_CHARS
            && (ttisstdcnt == typecnt || ttisstdcnt == 0)
            && (ttisutcnt == typecnt || ttisutcnt == 0))
        {
            return None;
        }
        let mut p = 44usize;
        // Transitions.  As in PG, a transition followed by another one at the
        // same instant is dropped (the later one wins); decreasing times are
        // rejected.
        let mut raw_times: Vec<i64> = Vec::with_capacity(timecnt);
        for _ in 0..timecnt {
            raw_times.push(if stored == 4 {
                rd32(p)? as i64
            } else {
                rd64(p)?
            });
            p += stored;
        }
        for i in 1..timecnt {
            if raw_times[i] < raw_times[i - 1] {
                return None;
            }
        }
        let keep: Vec<bool> = (0..timecnt)
            .map(|i| !(i + 1 < timecnt && raw_times[i + 1] == raw_times[i]))
            .collect();
        let ats: Vec<i64> = raw_times
            .iter()
            .zip(keep.iter())
            .filter(|(_, k)| **k)
            .map(|(t, _)| *t)
            .collect();
        let mut types: Vec<u8> = Vec::with_capacity(ats.len());
        for k in keep.iter().take(timecnt) {
            let typ = *data.get(p)?;
            p += 1;
            if typecnt <= typ as usize {
                return None;
            }
            if *k {
                types.push(typ);
            }
        }
        if types.len() != ats.len() {
            return None;
        }
        let mut ttis: Vec<TtInfo> = Vec::with_capacity(typecnt);
        for _ in 0..typecnt {
            let utoff = rd32(p)?;
            p += 4;
            let isdst = *data.get(p)?;
            p += 1;
            if isdst >= 2 {
                return None;
            }
            let desigidx = *data.get(p)? as usize;
            p += 1;
            if desigidx >= charcnt {
                return None;
            }
            ttis.push(TtInfo {
                utoff,
                isdst: isdst != 0,
                desigidx,
                ttisstd: false,
                ttisut: false,
            });
        }
        let mut chars: Vec<u8> = data.get(p..p + charcnt)?.to_vec();
        chars.push(0);
        p += charcnt;
        // leap seconds: not supported (zones with leap seconds are not embedded)
        p += leapcnt * (stored + 4);
        for t in ttis.iter_mut() {
            if ttisstdcnt != 0 {
                let v = *data.get(p)?;
                if v > 1 {
                    return None;
                }
                t.ttisstd = v != 0;
                p += 1;
            }
        }
        for t in ttis.iter_mut() {
            if ttisutcnt != 0 {
                let v = *data.get(p)?;
                if v > 1 {
                    return None;
                }
                t.ttisut = v != 0;
                p += 1;
            }
        }
        sp.ats = ats;
        sp.types = types;
        sp.ttis = ttis;
        sp.chars = chars;

        // footer
        let rest = &data[p.min(data.len())..];
        if version != 0
            && rest.len() > 2
            && rest[0] == b'\n'
            && rest[rest.len() - 1] == b'\n'
            && sp.ttis.len() + 2 <= TZ_MAX_TYPES
            && let Ok(tzstr) = std::str::from_utf8(&rest[1..rest.len() - 1])
            && let Some(ts) = tzparse(tzstr, false)
        {
            sp.extend_with(&ts);
        }
        if sp.ttis.is_empty() {
            return None;
        }
        sp.finish();
        Some(sp)
    }

    /// The footer-extension part of tzloadbody().
    fn extend_with(&mut self, ts: &ZoneState) {
        let mut ts = ts.clone();
        let mut gotabbr = 0usize;
        // charcnt includes the terminating NUL of the last string
        let mut charcnt = self.chars.len();
        let mut newchars = self.chars.clone();
        // ensure a buffer large enough (PG's chars[] is 512 bytes)
        for i in 0..ts.ttis.len() {
            let tsabbr = ts.abbrev_at(ts.ttis[i].desigidx).to_string();
            let mut found = None;
            for j in 0..charcnt {
                let s = cstr_at(&newchars, j);
                if s == tsabbr.as_bytes() {
                    found = Some(j);
                    break;
                }
            }
            match found {
                Some(j) => {
                    ts.ttis[i].desigidx = j;
                    gotabbr += 1;
                }
                None => {
                    let j = charcnt;
                    let tsabbrlen = tsabbr.len();
                    if j + tsabbrlen < TZ_MAX_CHARS {
                        newchars.truncate(j);
                        newchars.extend_from_slice(tsabbr.as_bytes());
                        newchars.push(0);
                        charcnt = j + tsabbrlen + 1;
                        ts.ttis[i].desigidx = j;
                        gotabbr += 1;
                    }
                }
            }
        }
        if gotabbr == ts.ttis.len() {
            newchars.truncate(charcnt);
            self.chars = newchars;
            while self.ats.len() > 1
                && self.types[self.ats.len() - 1] == self.types[self.ats.len() - 2]
            {
                self.ats.pop();
                self.types.pop();
            }
            let mut i = 0;
            while i < ts.ats.len() {
                if self.ats.is_empty() || self.ats[self.ats.len() - 1] < ts.ats[i] {
                    break;
                }
                i += 1;
            }
            let base = self.ttis.len();
            while i < ts.ats.len() && self.ats.len() < TZ_MAX_TIMES {
                self.ats.push(ts.ats[i]);
                self.types.push((base + ts.types[i] as usize) as u8);
                i += 1;
            }
            for t in ts.ttis.iter() {
                self.ttis.push(*t);
            }
        }
    }

    /// goback/goahead and defaulttype inference (end of tzloadbody()).
    fn finish(&mut self) {
        let timecnt = self.ats.len();
        if timecnt > 1 {
            for i in 1..timecnt {
                if self.typesequiv(self.types[i] as usize, self.types[0] as usize)
                    && self.ats[i].wrapping_sub(self.ats[0]) == SECSPERREPEAT
                {
                    self.goback = true;
                    break;
                }
            }
            let mut i = timecnt as i64 - 2;
            while i >= 0 {
                let iu = i as usize;
                if self.typesequiv(self.types[timecnt - 1] as usize, self.types[iu] as usize)
                    && self.ats[timecnt - 1].wrapping_sub(self.ats[iu]) == SECSPERREPEAT
                {
                    self.goahead = true;
                    break;
                }
                i -= 1;
            }
        }
        let mut i: i64 = 0;
        for k in 0..timecnt {
            if self.types[k] == 0 {
                i = -1;
                break;
            }
        }
        if i < 0 && timecnt > 0 && self.ttis[self.types[0] as usize].isdst {
            i = self.types[0] as i64;
            loop {
                i -= 1;
                if i < 0 {
                    break;
                }
                if !self.ttis[i as usize].isdst {
                    break;
                }
            }
        }
        if i < 0 {
            i = 0;
            while self.ttis[i as usize].isdst {
                i += 1;
                if i as usize >= self.ttis.len() {
                    i = 0;
                    break;
                }
            }
        }
        self.defaulttype = i as usize;
    }

    /// Port of localsub(): find the time type in effect at UTC time t.
    pub fn type_at(&self, t: i64) -> usize {
        let timecnt = self.ats.len();
        if (self.goback && t < self.ats[0]) || (self.goahead && t > self.ats[timecnt - 1]) {
            let mut seconds = if t < self.ats[0] {
                self.ats[0] - t
            } else {
                t - self.ats[timecnt - 1]
            };
            seconds -= 1;
            let years = (seconds / SECSPERREPEAT + 1) * YEARSPERREPEAT;
            let seconds = years * AVGSECSPERYEAR;
            let newt = if t < self.ats[0] {
                t + seconds
            } else {
                t - seconds
            };
            if newt < self.ats[0] || newt > self.ats[timecnt - 1] {
                return self.defaulttype; // "cannot happen"
            }
            return self.type_at(newt);
        }
        if timecnt == 0 || t < self.ats[0] {
            self.defaulttype
        } else {
            let mut lo = 1usize;
            let mut hi = timecnt;
            while lo < hi {
                let mid = (lo + hi) >> 1;
                if t < self.ats[mid] {
                    hi = mid;
                } else {
                    lo = mid + 1;
                }
            }
            self.types[lo - 1] as usize
        }
    }

    /// Port of pg_next_dst_boundary().  Returns (result, before_gmtoff,
    /// before_isdst, boundary, after_gmtoff, after_isdst).
    pub fn next_dst_boundary(&self, t: i64) -> (i32, i32, bool, i64, i32, bool) {
        let timecnt = self.ats.len();
        if timecnt == 0 {
            let mut i = 0;
            while self.ttis[i].isdst {
                i += 1;
                if i >= self.ttis.len() {
                    i = 0;
                    break;
                }
            }
            let tt = &self.ttis[i];
            return (0, tt.utoff, tt.isdst, 0, 0, false);
        }
        if (self.goback && t < self.ats[0]) || (self.goahead && t > self.ats[timecnt - 1]) {
            let mut seconds = if t < self.ats[0] {
                self.ats[0] - t
            } else {
                t - self.ats[timecnt - 1]
            };
            seconds -= 1;
            let tcycles = seconds / YEARSPERREPEAT / AVGSECSPERYEAR + 1;
            let seconds = tcycles * YEARSPERREPEAT * AVGSECSPERYEAR;
            let newt = if t < self.ats[0] {
                t + seconds
            } else {
                t - seconds
            };
            if newt < self.ats[0] || newt > self.ats[timecnt - 1] {
                return (-1, 0, false, 0, 0, false);
            }
            let (res, bg, bd, mut boundary, ag, ad) = self.next_dst_boundary(newt);
            if t < self.ats[0] {
                boundary -= seconds;
            } else {
                boundary += seconds;
            }
            return (res, bg, bd, boundary, ag, ad);
        }
        if t >= self.ats[timecnt - 1] {
            let tt = &self.ttis[self.types[timecnt - 1] as usize];
            return (0, tt.utoff, tt.isdst, 0, 0, false);
        }
        if t < self.ats[0] {
            let mut i = 0;
            while self.ttis[i].isdst {
                i += 1;
                if i >= self.ttis.len() {
                    i = 0;
                    break;
                }
            }
            let b = &self.ttis[i];
            let a = &self.ttis[self.types[0] as usize];
            return (1, b.utoff, b.isdst, self.ats[0], a.utoff, a.isdst);
        }
        let mut lo = 1usize;
        let mut hi = timecnt - 1;
        while lo < hi {
            let mid = (lo + hi) >> 1;
            if t < self.ats[mid] {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        let i = lo;
        let b = &self.ttis[self.types[i - 1] as usize];
        let a = &self.ttis[self.types[i] as usize];
        (1, b.utoff, b.isdst, self.ats[i], a.utoff, a.isdst)
    }

    /// Port of pg_interpret_timezone_abbrev(); `abbrev` must be upper case.
    pub fn interpret_abbrev(&self, abbrev: &str, t: i64) -> Option<(i32, bool)> {
        // locate abbreviation in the chars list (at string boundaries)
        let mut abbrind = 0usize;
        let charcnt = self.chars.len();
        while abbrind < charcnt {
            if cstr_at(&self.chars, abbrind) == abbrev.as_bytes() {
                break;
            }
            while abbrind < charcnt && self.chars[abbrind] != 0 {
                abbrind += 1;
            }
            abbrind += 1;
        }
        if abbrind >= charcnt {
            return None;
        }
        let timecnt = self.ats.len();
        let mut lo = 0usize;
        let mut hi = timecnt;
        while lo < hi {
            let mid = (lo + hi) >> 1;
            if t < self.ats[mid] {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        let cutoff = lo;
        for i in (0..cutoff).rev() {
            let tt = &self.ttis[self.types[i] as usize];
            if tt.desigidx == abbrind {
                return Some((tt.utoff, tt.isdst));
            }
        }
        for i in cutoff..timecnt {
            let tt = &self.ttis[self.types[i] as usize];
            if tt.desigidx == abbrind {
                return Some((tt.utoff, tt.isdst));
            }
        }
        None
    }

    /// Port of pg_get_timezone_offset(): Some(utoff) if the zone only ever
    /// uses one UTC offset.
    pub fn single_offset(&self) -> Option<i32> {
        let first = self.ttis[0].utoff;
        if self.ttis.iter().skip(1).all(|t| t.utoff == first) {
            Some(first)
        } else {
            None
        }
    }
}

fn cstr_at(chars: &[u8], idx: usize) -> &[u8] {
    let bytes = &chars[idx.min(chars.len())..];
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    &bytes[..end]
}

#[derive(Clone, Copy, PartialEq)]
enum RType {
    JulianDay,
    DayOfYear,
    MonthNthDayOfWeek,
}

#[derive(Clone, Copy)]
struct Rule {
    r_type: RType,
    r_day: i32,
    r_week: i32,
    r_mon: i32,
    r_time: i32,
}

struct Cursor<'a> {
    s: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn peek(&self) -> u8 {
        *self.s.get(self.pos).unwrap_or(&0)
    }
    fn at_end(&self) -> bool {
        self.pos >= self.s.len()
    }
}

fn getzname(c: &mut Cursor) {
    loop {
        let ch = c.peek();
        if ch == 0 || ch.is_ascii_digit() || ch == b',' || ch == b'-' || ch == b'+' {
            break;
        }
        c.pos += 1;
    }
}

fn getqzname(c: &mut Cursor, delim: u8) {
    loop {
        let ch = c.peek();
        if ch == 0 || ch == delim {
            break;
        }
        c.pos += 1;
    }
}

fn getnum(c: &mut Cursor, min: i32, max: i32) -> Option<i32> {
    let mut ch = c.peek();
    if !ch.is_ascii_digit() {
        return None;
    }
    let mut num: i32 = 0;
    loop {
        num = num * 10 + (ch - b'0') as i32;
        if num > max {
            return None;
        }
        c.pos += 1;
        ch = c.peek();
        if !ch.is_ascii_digit() {
            break;
        }
    }
    if num < min {
        return None;
    }
    Some(num)
}

fn getsecs(c: &mut Cursor) -> Option<i32> {
    let num = getnum(c, 0, HOURSPERDAY * DAYSPERWEEK - 1)?;
    let mut secs = num * SECSPERHOUR;
    if c.peek() == b':' {
        c.pos += 1;
        let num = getnum(c, 0, MINSPERHOUR - 1)?;
        secs += num * SECSPERMIN;
        if c.peek() == b':' {
            c.pos += 1;
            let num = getnum(c, 0, SECSPERMIN)?;
            secs += num;
        }
    }
    Some(secs)
}

fn getoffset(c: &mut Cursor) -> Option<i32> {
    let mut neg = false;
    if c.peek() == b'-' {
        neg = true;
        c.pos += 1;
    } else if c.peek() == b'+' {
        c.pos += 1;
    }
    let off = getsecs(c)?;
    Some(if neg { -off } else { off })
}

fn getrule(c: &mut Cursor) -> Option<Rule> {
    let mut r = Rule {
        r_type: RType::JulianDay,
        r_day: 0,
        r_week: 0,
        r_mon: 0,
        r_time: 0,
    };
    let ch = c.peek();
    if ch == b'J' {
        r.r_type = RType::JulianDay;
        c.pos += 1;
        r.r_day = getnum(c, 1, DAYSPERNYEAR)?;
    } else if ch == b'M' {
        r.r_type = RType::MonthNthDayOfWeek;
        c.pos += 1;
        r.r_mon = getnum(c, 1, MONSPERYEAR)?;
        if c.peek() != b'.' {
            return None;
        }
        c.pos += 1;
        r.r_week = getnum(c, 1, 5)?;
        if c.peek() != b'.' {
            return None;
        }
        c.pos += 1;
        r.r_day = getnum(c, 0, DAYSPERWEEK - 1)?;
    } else if ch.is_ascii_digit() {
        r.r_type = RType::DayOfYear;
        r.r_day = getnum(c, 0, DAYSPERLYEAR - 1)?;
    } else {
        return None;
    }
    if c.peek() == b'/' {
        c.pos += 1;
        r.r_time = getoffset(c)?;
    } else {
        r.r_time = 2 * SECSPERHOUR;
    }
    Some(r)
}

fn transtime(year: i32, rule: &Rule, offset: i32) -> i32 {
    let leapyear = isleap(year);
    let mut value: i32;
    match rule.r_type {
        RType::JulianDay => {
            value = (rule.r_day - 1) * SECSPERDAY;
            if leapyear && rule.r_day >= 60 {
                value += SECSPERDAY;
            }
        }
        RType::DayOfYear => {
            value = rule.r_day * SECSPERDAY;
        }
        RType::MonthNthDayOfWeek => {
            let m1 = (rule.r_mon + 9) % 12 + 1;
            let yy0 = if rule.r_mon <= 2 { year - 1 } else { year };
            let yy1 = yy0 / 100;
            let yy2 = yy0 % 100;
            let mut dow = ((26 * m1 - 2) / 10 + 1 + yy2 + yy2 / 4 + yy1 / 4 - 2 * yy1) % 7;
            if dow < 0 {
                dow += DAYSPERWEEK;
            }
            let mut d = rule.r_day - dow;
            if d < 0 {
                d += DAYSPERWEEK;
            }
            for _ in 1..rule.r_week {
                if d + DAYSPERWEEK >= MON_LENGTHS[leapyear as usize][(rule.r_mon - 1) as usize] {
                    break;
                }
                d += DAYSPERWEEK;
            }
            value = d * SECSPERDAY;
            for i in 0..(rule.r_mon - 1) {
                value += MON_LENGTHS[leapyear as usize][i as usize] * SECSPERDAY;
            }
        }
    }
    value + rule.r_time + offset
}

fn increment_overflow_time(tp: &mut i64, j: i32) -> bool {
    match tp.checked_add(j as i64) {
        Some(v) => {
            *tp = v;
            false
        }
        None => true,
    }
}

/// Port of tzparse().  Returns None on failure.
pub(crate) fn tzparse(name_in: &str, lastditch: bool) -> Option<ZoneState> {
    let bytes = name_in.as_bytes();
    let mut c = Cursor { s: bytes, pos: 0 };
    let stdname: &[u8];
    let stdoffset: i32;
    if lastditch {
        stdname = bytes;
        c.pos = bytes.len();
        stdoffset = 0;
    } else {
        if c.peek() == b'<' {
            c.pos += 1;
            let start = c.pos;
            getqzname(&mut c, b'>');
            if c.peek() != b'>' {
                return None;
            }
            stdname = &bytes[start..c.pos];
            c.pos += 1;
        } else {
            let start = c.pos;
            getzname(&mut c);
            stdname = &bytes[start..c.pos];
        }
        if c.at_end() {
            return None;
        }
        stdoffset = getoffset(&mut c)?;
    }
    let stdlen = stdname.len();
    let mut charcnt = stdlen + 1;
    if charcnt > 512 {
        return None;
    }
    let mut sp = ZoneState::empty(name_in);
    let mut dstname: &[u8] = &[];
    if !c.at_end() {
        if c.peek() == b'<' {
            c.pos += 1;
            let start = c.pos;
            getqzname(&mut c, b'>');
            if c.peek() != b'>' {
                return None;
            }
            dstname = &bytes[start..c.pos];
            c.pos += 1;
        } else {
            let start = c.pos;
            getzname(&mut c);
            dstname = &bytes[start..c.pos];
        }
        if dstname.is_empty() {
            return None;
        }
        charcnt += dstname.len() + 1;
        if charcnt > 512 {
            return None;
        }
        let dstoffset: i32;
        let ch = c.peek();
        if !c.at_end() && ch != b',' && ch != b';' {
            dstoffset = getoffset(&mut c)?;
        } else {
            dstoffset = stdoffset - SECSPERHOUR;
        }
        // rules
        let rulestr: Vec<u8> = if c.at_end() {
            TZDEFRULESTRING.as_bytes().to_vec()
        } else {
            bytes[c.pos..].to_vec()
        };
        let mut rc = Cursor {
            s: &rulestr,
            pos: 0,
        };
        if rc.peek() == b',' || rc.peek() == b';' {
            rc.pos += 1;
            let start = getrule(&mut rc)?;
            if rc.peek() != b',' {
                return None;
            }
            rc.pos += 1;
            let end = getrule(&mut rc)?;
            if !rc.at_end() {
                return None;
            }
            sp.ttis = vec![
                TtInfo {
                    utoff: -stdoffset,
                    isdst: false,
                    desigidx: 0,
                    ttisstd: false,
                    ttisut: false,
                },
                TtInfo {
                    utoff: -dstoffset,
                    isdst: true,
                    desigidx: stdlen + 1,
                    ttisstd: false,
                    ttisut: false,
                },
            ];
            sp.defaulttype = 0;
            let mut janfirst: i64 = 0;
            let mut janoffset: i32 = 0;
            let mut yearbeg = EPOCH_YEAR;
            loop {
                let yearsecs = YEAR_LENGTHS[isleap(yearbeg - 1) as usize] * SECSPERDAY;
                yearbeg -= 1;
                if increment_overflow_time(&mut janfirst, -yearsecs) {
                    janoffset = -yearsecs;
                    break;
                }
                if !(EPOCH_YEAR - (YEARSPERREPEAT as i32) / 2 < yearbeg) {
                    break;
                }
            }
            let mut yearlim = yearbeg + YEARSPERREPEAT as i32 + 1;
            let mut year = yearbeg;
            while year < yearlim {
                let mut starttime = transtime(year, &start, stdoffset);
                let mut endtime = transtime(year, &end, dstoffset);
                let yearsecs = YEAR_LENGTHS[isleap(year) as usize] * SECSPERDAY;
                let reversed = endtime < starttime;
                if reversed {
                    std::mem::swap(&mut starttime, &mut endtime);
                }
                if reversed
                    || (starttime < endtime
                        && (endtime - starttime < (yearsecs + (stdoffset - dstoffset))))
                {
                    if TZ_MAX_TIMES - 2 < sp.ats.len() {
                        break;
                    }
                    let mut at = janfirst;
                    if !increment_overflow_time(&mut at, janoffset + starttime) {
                        sp.ats.push(at);
                        sp.types.push((!reversed) as u8);
                    }
                    let mut at = janfirst;
                    if !increment_overflow_time(&mut at, janoffset + endtime) {
                        sp.ats.push(at);
                        sp.types.push(reversed as u8);
                        yearlim = year + YEARSPERREPEAT as i32 + 1;
                    }
                }
                if increment_overflow_time(&mut janfirst, janoffset + yearsecs) {
                    break;
                }
                janoffset = 0;
                year += 1;
            }
            if sp.ats.is_empty() {
                sp.ttis[0] = sp.ttis[1];
                sp.ttis.truncate(1);
            } else if (YEARSPERREPEAT as i32) < year - yearbeg {
                sp.goback = true;
                sp.goahead = true;
            }
        } else {
            // POSIX TZ with DST name but no rules and not the default rules:
            // unreachable in PostgreSQL (load_ok is always false).
            return None;
        }
    } else {
        sp.ttis = vec![TtInfo {
            utoff: -stdoffset,
            isdst: false,
            desigidx: 0,
            ttisstd: false,
            ttisut: false,
        }];
        sp.defaulttype = 0;
    }
    let mut chars = Vec::with_capacity(charcnt);
    chars.extend_from_slice(stdname);
    chars.push(0);
    if !dstname.is_empty() {
        chars.extend_from_slice(dstname);
        chars.push(0);
    }
    sp.chars = chars;
    Some(sp)
}
