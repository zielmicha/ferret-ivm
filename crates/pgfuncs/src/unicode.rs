//! Unicode character properties and simple case mapping exactly as used by
//! PostgreSQL 17 with the builtin collation provider (Unicode 15.1).
//!
//! Ported from PostgreSQL's src/common/unicode_case.c and
//! src/common/unicode_category.c (PostgreSQL License, Copyright (c) 1996-2024,
//! PostgreSQL Global Development Group). The tables in `unicode_tables.rs` are
//! generated from PostgreSQL's headers by `tests/gen/gen_unicode_tables.py`.

#[path = "unicode_tables.rs"]
mod tables;

/// Unicode general category, numbered like PostgreSQL's `pg_unicode_category`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Category {
    Unassigned = 0,
    UppercaseLetter = 1,
    LowercaseLetter = 2,
    TitlecaseLetter = 3,
    ModifierLetter = 4,
    OtherLetter = 5,
    NonspacingMark = 6,
    EnclosingMark = 7,
    SpacingMark = 8,
    DecimalNumber = 9,
    LetterNumber = 10,
    OtherNumber = 11,
    SpaceSeparator = 12,
    LineSeparator = 13,
    ParagraphSeparator = 14,
    Control = 15,
    Format = 16,
    PrivateUse = 17,
    Surrogate = 18,
    DashPunctuation = 19,
    OpenPunctuation = 20,
    ClosePunctuation = 21,
    ConnectorPunctuation = 22,
    OtherPunctuation = 23,
    MathSymbol = 24,
    CurrencySymbol = 25,
    ModifierSymbol = 26,
    OtherSymbol = 27,
    InitialPunctuation = 28,
    FinalPunctuation = 29,
}

const CATEGORY_BY_NUM: [Category; 30] = [
    Category::Unassigned,
    Category::UppercaseLetter,
    Category::LowercaseLetter,
    Category::TitlecaseLetter,
    Category::ModifierLetter,
    Category::OtherLetter,
    Category::NonspacingMark,
    Category::EnclosingMark,
    Category::SpacingMark,
    Category::DecimalNumber,
    Category::LetterNumber,
    Category::OtherNumber,
    Category::SpaceSeparator,
    Category::LineSeparator,
    Category::ParagraphSeparator,
    Category::Control,
    Category::Format,
    Category::PrivateUse,
    Category::Surrogate,
    Category::DashPunctuation,
    Category::OpenPunctuation,
    Category::ClosePunctuation,
    Category::ConnectorPunctuation,
    Category::OtherPunctuation,
    Category::MathSymbol,
    Category::CurrencySymbol,
    Category::ModifierSymbol,
    Category::OtherSymbol,
    Category::InitialPunctuation,
    Category::FinalPunctuation,
];

const PROP_ALPHABETIC: u8 = 1 << 0;
const PROP_LOWERCASE: u8 = 1 << 1;
const PROP_UPPERCASE: u8 = 1 << 2;
const PROP_CASED: u8 = 1 << 3;
const PROP_CASE_IGNORABLE: u8 = 1 << 4;
const PROP_WHITE_SPACE: u8 = 1 << 5;
const PROP_JOIN_CONTROL: u8 = 1 << 6;
const PROP_HEX_DIGIT: u8 = 1 << 7;

#[inline]
fn case_entry(c: u32) -> Option<&'static (u32, u32, u32, u32)> {
    tables::CASE_MAP.binary_search_by_key(&c, |e| e.0).ok().map(|i| &tables::CASE_MAP[i])
}

#[inline]
fn to_char(c: u32) -> char {
    // Case mappings always produce valid scalar values.
    char::from_u32(c).unwrap_or('\u{FFFD}')
}

/// `unicode_lowercase_simple`
#[inline]
pub fn to_lower(c: char) -> char {
    if c.is_ascii() {
        return c.to_ascii_lowercase();
    }
    match case_entry(c as u32) {
        Some(e) => to_char(e.1),
        None => c,
    }
}

/// `unicode_titlecase_simple`
#[inline]
pub fn to_title(c: char) -> char {
    if c.is_ascii() {
        return c.to_ascii_uppercase();
    }
    match case_entry(c as u32) {
        Some(e) => to_char(e.2),
        None => c,
    }
}

/// `unicode_uppercase_simple`
#[inline]
pub fn to_upper(c: char) -> char {
    if c.is_ascii() {
        return c.to_ascii_uppercase();
    }
    match case_entry(c as u32) {
        Some(e) => to_char(e.3),
        None => c,
    }
}

/// `unicode_category`
pub fn category(c: char) -> Category {
    let c = c as u32;
    if c < 0x80 {
        return CATEGORY_BY_NUM[tables::ASCII_PROPS[c as usize].0 as usize];
    }
    let t = &tables::CATEGORIES;
    match t.binary_search_by(|&(first, last, _)| {
        if c > last {
            std::cmp::Ordering::Less
        } else if c < first {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Equal
        }
    }) {
        Ok(i) => CATEGORY_BY_NUM[t[i].2 as usize],
        Err(_) => Category::Unassigned,
    }
}

#[inline]
fn range_search(t: &[(u32, u32)], c: u32) -> bool {
    t.binary_search_by(|&(first, last)| {
        if c > last {
            std::cmp::Ordering::Less
        } else if c < first {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Equal
        }
    })
    .is_ok()
}

#[inline]
fn ascii_prop(c: u32, prop: u8) -> bool {
    tables::ASCII_PROPS[c as usize].1 & prop != 0
}

pub fn prop_alphabetic(c: char) -> bool {
    let c = c as u32;
    if c < 0x80 {
        return ascii_prop(c, PROP_ALPHABETIC);
    }
    range_search(&tables::ALPHABETIC, c)
}

pub fn prop_lowercase(c: char) -> bool {
    let c = c as u32;
    if c < 0x80 {
        return ascii_prop(c, PROP_LOWERCASE);
    }
    range_search(&tables::LOWERCASE, c)
}

pub fn prop_uppercase(c: char) -> bool {
    let c = c as u32;
    if c < 0x80 {
        return ascii_prop(c, PROP_UPPERCASE);
    }
    range_search(&tables::UPPERCASE, c)
}

pub fn prop_cased(c: char) -> bool {
    let cu = c as u32;
    if cu < 0x80 {
        return ascii_prop(cu, PROP_CASED);
    }
    category(c) == Category::TitlecaseLetter || prop_lowercase(c) || prop_uppercase(c)
}

pub fn prop_case_ignorable(c: char) -> bool {
    let c = c as u32;
    if c < 0x80 {
        return ascii_prop(c, PROP_CASE_IGNORABLE);
    }
    range_search(&tables::CASE_IGNORABLE, c)
}

pub fn prop_white_space(c: char) -> bool {
    let c = c as u32;
    if c < 0x80 {
        return ascii_prop(c, PROP_WHITE_SPACE);
    }
    range_search(&tables::WHITE_SPACE, c)
}

pub fn prop_hex_digit(c: char) -> bool {
    let c = c as u32;
    if c < 0x80 {
        return ascii_prop(c, PROP_HEX_DIGIT);
    }
    range_search(&tables::HEX_DIGIT, c)
}

pub fn prop_join_control(c: char) -> bool {
    let c = c as u32;
    if c < 0x80 {
        return ascii_prop(c, PROP_JOIN_CONTROL);
    }
    range_search(&tables::JOIN_CONTROL, c)
}

/// `pg_u_isdigit`
pub fn is_digit(c: char, posix: bool) -> bool {
    if posix { c.is_ascii_digit() } else { category(c) == Category::DecimalNumber }
}

/// `pg_u_isalpha`
#[inline]
pub fn is_alpha(c: char) -> bool {
    prop_alphabetic(c)
}

/// `pg_u_isalnum`
#[inline]
pub fn is_alnum(c: char, posix: bool) -> bool {
    is_alpha(c) || is_digit(c, posix)
}

/// `pg_u_isword`
pub fn is_word(c: char) -> bool {
    matches!(
        category(c),
        Category::NonspacingMark
            | Category::SpacingMark
            | Category::EnclosingMark
            | Category::DecimalNumber
            | Category::ConnectorPunctuation
    ) || is_alpha(c)
        || prop_join_control(c)
}

/// `pg_u_isupper`
#[inline]
pub fn is_upper(c: char) -> bool {
    prop_uppercase(c)
}

/// `pg_u_islower`
#[inline]
pub fn is_lower(c: char) -> bool {
    prop_lowercase(c)
}

/// `pg_u_isblank`
pub fn is_blank(c: char) -> bool {
    c == '\t' || category(c) == Category::SpaceSeparator
}

/// `pg_u_iscntrl`
pub fn is_cntrl(c: char) -> bool {
    category(c) == Category::Control
}

/// `pg_u_isgraph`
pub fn is_graph(c: char) -> bool {
    !matches!(category(c), Category::Control | Category::Surrogate | Category::Unassigned) && !is_space(c)
}

/// `pg_u_isprint`
pub fn is_print(c: char) -> bool {
    if category(c) == Category::Control {
        return false;
    }
    is_graph(c) || is_blank(c)
}

/// `pg_u_ispunct`
pub fn is_punct(c: char, posix: bool) -> bool {
    let cat = category(c);
    let is_p = matches!(
        cat,
        Category::ConnectorPunctuation
            | Category::DashPunctuation
            | Category::OpenPunctuation
            | Category::ClosePunctuation
            | Category::InitialPunctuation
            | Category::FinalPunctuation
            | Category::OtherPunctuation
    );
    if posix {
        if is_alpha(c) {
            return false;
        }
        is_p || matches!(
            cat,
            Category::MathSymbol | Category::CurrencySymbol | Category::ModifierSymbol | Category::OtherSymbol
        )
    } else {
        is_p
    }
}

/// `pg_u_isspace`
#[inline]
pub fn is_space(c: char) -> bool {
    prop_white_space(c)
}

/// `pg_u_isxdigit`
pub fn is_xdigit(c: char, posix: bool) -> bool {
    if posix { c.is_ascii_hexdigit() } else { category(c) == Category::DecimalNumber || prop_hex_digit(c) }
}

/// All code points at which some property or category used by this module
/// may change value (sorted, deduplicated, includes every ASCII code point,
/// the surrogate block bounds and 0x110000). Between two consecutive
/// boundaries every predicate in this module is constant.
pub(crate) fn property_boundaries() -> Vec<u32> {
    let mut b: Vec<u32> = (0..=0x80).collect();
    for &(f, l, _) in tables::CATEGORIES.iter() {
        b.push(f);
        b.push(l + 1);
    }
    for t in [
        &tables::ALPHABETIC[..],
        &tables::LOWERCASE[..],
        &tables::UPPERCASE[..],
        &tables::CASE_IGNORABLE[..],
        &tables::WHITE_SPACE[..],
        &tables::HEX_DIGIT[..],
        &tables::JOIN_CONTROL[..],
    ] {
        for &(f, l) in t {
            b.push(f);
            b.push(l + 1);
        }
    }
    b.extend([0xD800, 0xE000, 0x110000]);
    b.sort_unstable();
    b.dedup();
    b
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn case_mapping() {
        assert_eq!(to_upper('é'), 'É');
        assert_eq!(to_lower('É'), 'é');
        assert_eq!(to_upper('ß'), 'ß'); // no simple uppercase mapping
        assert_eq!(to_lower('\u{130}'), 'i'); // simple lowercase of İ
        assert_eq!(to_upper('ǆ'), 'Ǆ');
        assert_eq!(to_title('ǆ'), 'ǅ');
        assert_eq!(to_upper('\u{1F80}'), '\u{1F88}');
        assert_eq!(to_lower('\u{212A}'), 'k');
        assert_eq!(to_upper('k'), 'K');
    }

    #[test]
    fn properties() {
        assert!(is_alpha('a') && is_alpha('é') && !is_alpha('1'));
        assert!(is_alnum('1', true) && !is_alnum('٣', true) && is_digit('٣', false));
        assert!(is_space('\u{3000}') && is_space('\n') && !is_space('a'));
        assert!(is_punct('$', true) && !is_punct('$', false));
        assert_eq!(category('\u{10FFFF}'), Category::Unassigned);
        assert_eq!(category('A'), Category::UppercaseLetter);
    }
}
