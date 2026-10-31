//! Finite PostgreSQL numerics at the JSON boundary. Numeric owns parsing,
//! comparison, hashing and arithmetic; this wrapper excludes NaN/infinities
//! and lazily caches numeric_out for the borrowed text API.

use crate::{Error, Result, sqlstate};
use pgnumeric::Numeric;
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::sync::OnceLock;

/// A finite PostgreSQL numeric, preserving display scale. Equality and
/// ordering are by value (`1.50 == 1.5`).
#[derive(Debug, Clone)]
pub struct JsonNumber {
    value: Numeric,
    text: OnceLock<Box<str>>,
}

impl JsonNumber {
    /// Accepts PostgreSQL numeric syntax (including surrounding whitespace,
    /// exponents, underscores and nondecimal integers). JSON's stricter
    /// number grammar is enforced by the JSON lexer before this conversion.
    /// Like PostgreSQL's C input function, input ends at the first NUL.
    pub fn parse(s: &str) -> Result<Self> {
        let s = s.split('\0').next().unwrap();
        let value = Numeric::parse(s).map_err(|e| Error {
            sqlstate: e.sqlstate,
            message: e.message,
            detail: e.detail,
        })?;
        Self::from_numeric(value).ok_or_else(|| {
            Error::new(
                sqlstate::INVALID_TEXT_REPRESENTATION,
                format!(
                    "cannot represent numeric value \"{}\" as a JSON number",
                    s.trim()
                ),
            )
        })
    }

    /// A token whose grammar has already been checked by the JSON lexer.
    pub(crate) fn from_json_token(raw: &str) -> Result<Self> {
        Self::parse(raw)
    }

    /// Wraps a finite, storable numeric without a text conversion. Returns
    /// None for NaN/infinities or unbounded arithmetic results that exceed
    /// PostgreSQL's numeric storage range.
    pub fn from_numeric(value: Numeric) -> Option<Self> {
        if !value.is_finite() {
            return None;
        }
        Some(Self {
            value: value.into_checked().ok()?,
            text: OnceLock::new(),
        })
    }

    pub fn as_numeric(&self) -> &Numeric {
        &self.value
    }

    /// numeric_uminus: zero keeps its positive sign and display scale.
    pub(crate) fn negate(&self) -> Self {
        Self::from_numeric(self.value.neg()).unwrap()
    }

    /// The numeric_out text, cached on first use for the borrowed text API.
    pub fn as_str(&self) -> &str {
        self.text
            .get_or_init(|| self.value.to_string().into_boxed_str())
    }

    pub fn from_i64(v: i64) -> Self {
        Self::from_numeric(Numeric::from_i64(v)).unwrap()
    }

    /// float8 -> jsonb uses the shortest round-trip decimal, unlike the
    /// float8 -> numeric cast's 15 significant digits.
    pub fn from_f64(v: f64) -> Option<Self> {
        if !v.is_finite() {
            return None;
        }
        Self::parse(&format!("{v:e}")).ok()
    }

    pub fn is_zero(&self) -> bool {
        self.value.is_zero()
    }

    /// -1, 0 or 1.
    pub fn signum(&self) -> i32 {
        if self.is_zero() {
            0
        } else if self.value.is_negative() {
            -1
        } else {
            1
        }
    }
}

impl PartialEq for JsonNumber {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}
impl Eq for JsonNumber {}
impl PartialOrd for JsonNumber {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for JsonNumber {
    fn cmp(&self, other: &Self) -> Ordering {
        self.value.cmp(&other.value)
    }
}
impl Hash for JsonNumber {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.value.hash(state);
    }
}
impl std::fmt::Display for JsonNumber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(s: &str) -> String {
        JsonNumber::parse(s)
            .map(|n| n.as_str().to_owned())
            .unwrap_or_else(|e| format!("ERR {} {}", e.sqlstate, e.message))
    }

    #[test]
    fn normalization() {
        assert_eq!(p("1e2"), "100");
        assert_eq!(p("1.50"), "1.50");
        assert_eq!(p("1.5e-3"), "0.0015");
        assert_eq!(p("-0"), "0");
        assert_eq!(p("-0.00"), "0.00");
        assert_eq!(p("0e-5"), "0.00000");
        assert_eq!(p(" 12.5e1 "), "125");
        assert_eq!(p("1.55e1"), "15.5");
        assert_eq!(p(".5"), "0.5");
        assert_eq!(p("5."), "5");
        assert_eq!(p("0x1F"), "31");
        assert_eq!(p("-0x1F"), "-31");
        assert_eq!(p("0x_1"), "1");
        assert_eq!(p("1_000.5"), "1000.5");
        assert_eq!(p("0b101"), "5");
        assert_eq!(p("0o17"), "15");
        assert_eq!(p("007"), "7");
        assert_eq!(p("-000.0100"), "-0.0100");
        assert!(p("1e").starts_with("ERR 22P02"));
        assert!(p("1__0").starts_with("ERR 22P02"));
        assert!(p("1e9999999999").starts_with("ERR 22003"));
        assert!(p("1e200000").starts_with("ERR 22003"));
        assert!(p("1e-20000").starts_with("ERR 22003"));
        assert!(p("NaN").starts_with("ERR 22P02"));
    }

    #[test]
    fn json_boundary_and_cached_text() {
        for text in ["+1", ".5", "1_0", "0x10"] {
            assert!(JsonNumber::parse(text).is_ok());
            assert!(crate::json::Jsonb::parse(text).is_err());
        }
        assert_eq!(p("1.50\0invalid"), "1.50");
        assert_eq!(
            p("  -iNf \0invalid"),
            "ERR 22P02 cannot represent numeric value \"-iNf\" as a JSON number"
        );
        assert_eq!(
            p("+NaN"),
            "ERR 22P02 invalid input syntax for type numeric: \"+NaN\""
        );
        for value in [Numeric::nan(), Numeric::infinity(), Numeric::neg_infinity()] {
            assert!(JsonNumber::from_numeric(value).is_none());
        }
        let number = JsonNumber::from_numeric(Numeric::parse("1.500").unwrap()).unwrap();
        let equivalent = JsonNumber::parse("1.5").unwrap();
        let hash = |n: &JsonNumber| {
            let mut state = std::collections::hash_map::DefaultHasher::new();
            n.hash(&mut state);
            state.finish()
        };
        assert!(number.text.get().is_none());
        assert_eq!(number, equivalent);
        assert_eq!(hash(&number), hash(&equivalent));
        assert_eq!(number.as_str(), "1.500");
        assert_eq!(number.as_numeric().dscale(), 3);
        assert_eq!(number, equivalent);
        assert_eq!(hash(&number), hash(&equivalent));
        assert_eq!(number.negate().as_str(), "-1.500");
    }

    #[test]
    fn unbounded_numeric_cannot_escape_through_json() {
        let high = Numeric::parse("1e131071").unwrap();
        assert!(JsonNumber::from_numeric(high.clone()).is_some());
        let overflow = high.mul_unbounded(&Numeric::from_i64(10));
        assert!(overflow.is_finite());
        assert!(JsonNumber::from_numeric(overflow.neg()).is_none());
        assert!(JsonNumber::from_numeric(overflow).is_none());
        let tiny = Numeric::parse("1e-16383").unwrap();
        assert!(JsonNumber::from_numeric(tiny.clone()).is_some());
        // Unbounded multiplication still applies PostgreSQL's scale limit.
        let rounded = tiny.mul_unbounded(&Numeric::parse("0.1").unwrap());
        assert!(JsonNumber::from_numeric(rounded).unwrap().is_zero());
    }

    #[test]
    fn ordering() {
        let n = |s: &str| JsonNumber::parse(s).unwrap();
        assert_eq!(n("1.50"), n("1.5"));
        assert!(n("0.01") < n("0.1"));
        assert!(n("-0.01") > n("-0.1"));
        assert!(n("-1") < n("0"));
        assert!(n("10") > n("9.99"));
        assert_eq!(n("0.000"), n("0"));
        assert!(n("100") > n("99.5"));
    }

    #[test]
    fn floats() {
        let f = |v: f64| JsonNumber::from_f64(v).unwrap().as_str().to_owned();
        assert_eq!(f(0.1), "0.1");
        assert_eq!(f(1e20), "100000000000000000000");
        assert_eq!(f(1.5e-7), "0.00000015");
        assert_eq!(f(-0.0), "0");
        assert_eq!(f(123.456), "123.456");
        assert!(JsonNumber::from_f64(f64::NAN).is_none());
    }
}
