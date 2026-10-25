//! Byte offsets of containers in PostgreSQL's binary jsonb format, needed
//! for the `id` field that `.keyvalue()` reports (the offset of the object
//! within its base object, see executeKeyValueMethod()).
//!
//! The layout follows convertToJsonb() (src/backend/utils/adt/jsonb_util.c
//! of PostgreSQL 17): a varlena header, then the root container. A
//! container is a 4-byte header and one 4-byte JEntry per child (2 per
//! object pair), followed by the children's data (for objects all keys
//! first, then all values). Strings are stored unpadded; numerics (with a
//! 4-byte varlena header, a 2- or 4-byte numeric header and 2 bytes per
//! base-10000 digit) and nested containers are aligned to 4 bytes.

use crate::json::{JsonNumber, Jsonb};
use std::collections::HashMap;

#[inline]
fn align4(p: usize) -> usize {
    (p + 3) & !3
}

/// Size of the stored numeric (make_result()).
pub(super) fn numeric_size(n: &JsonNumber) -> usize {
    let s = n.as_str();
    let s = s.strip_prefix('-').unwrap_or(s);
    let (int, frac) = match s.find('.') {
        Some(p) => (&s[..p], &s[p + 1..]),
        None => (s, ""),
    };
    let dscale = frac.len();
    let int = if int.bytes().all(|c| c == b'0') {
        ""
    } else {
        int
    };
    // base-10000 digits: the integer part grouped from the decimal point to
    // the left, the fraction to the right
    let int_groups = int.len().div_ceil(4);
    let frac_groups = frac.len().div_ceil(4);
    let group = |i: usize| -> u32 {
        // digit group i of int_groups + frac_groups
        let mut v = 0u32;
        for k in 0..4 {
            let d = if i < int_groups {
                // position within the left-padded integer part
                let pos = (i * 4 + k) as isize - (int_groups * 4 - int.len()) as isize;
                if pos >= 0 {
                    int.as_bytes()[pos as usize] - b'0'
                } else {
                    0
                }
            } else {
                let pos = (i - int_groups) * 4 + k;
                frac.as_bytes().get(pos).map_or(0, |c| c - b'0')
            };
            v = v * 10 + u32::from(d);
        }
        v
    };
    let total = int_groups + frac_groups;
    let mut first = 0;
    while first < total && group(first) == 0 {
        first += 1;
    }
    let (weight, ndigits) = if first == total {
        (0i64, 0usize)
    } else {
        let mut last = total;
        while group(last - 1) == 0 {
            last -= 1;
        }
        (int_groups as i64 - 1 - first as i64, last - first)
    };
    let short = dscale <= 63 && (-64..=63).contains(&weight);
    4 + if short { 2 } else { 4 } + 2 * ndigits
}

/// Size of the data of a scalar child (without alignment).
fn scalar_size(v: &Jsonb) -> Option<(usize, bool)> {
    match v {
        Jsonb::Null | Jsonb::Bool(_) => Some((0, false)),
        Jsonb::String(s) => Some((s.len(), false)),
        Jsonb::Number(n) => Some((numeric_size(n), true)),
        Jsonb::Array(_) | Jsonb::Object(_) => None,
    }
}

enum Iter<'a> {
    Arr(std::slice::Iter<'a, Jsonb>),
    Obj(std::slice::Iter<'a, (String, Jsonb)>),
}

/// Header and keys of a container starting at `pos`; returns the position
/// of its first value and the iterator over its values.
fn open(v: &Jsonb, pos: usize) -> (usize, Iter<'_>) {
    match v {
        Jsonb::Array(a) => (pos + 4 + 4 * a.len(), Iter::Arr(a.iter())),
        Jsonb::Object(p) => {
            let keys: usize = p.iter().map(|(k, _)| k.len()).sum();
            (pos + 4 + 8 * p.len() + keys, Iter::Obj(p.iter()))
        }
        _ => unreachable!(),
    }
}

/// Positions of all containers below `root` (a container starting at
/// position `start`), keyed by address, in serialization order. Positions
/// count from the start of the varlena, so alignment is computed on them.
fn positions(root: &Jsonb, start: usize, out: &mut HashMap<usize, usize>) {
    if !matches!(root, Jsonb::Array(_) | Jsonb::Object(_)) {
        return;
    }
    let (mut pos, it) = open(root, start);
    let mut stack = vec![it];
    while let Some(top) = stack.last_mut() {
        let next = match top {
            Iter::Arr(it) => it.next(),
            Iter::Obj(it) => it.next().map(|(_, v)| v),
        };
        let Some(v) = next else {
            stack.pop();
            continue;
        };
        match scalar_size(v) {
            Some((size, aligned)) => {
                if aligned {
                    pos = align4(pos);
                }
                pos += size;
            }
            None => {
                pos = align4(pos);
                out.insert(v as *const Jsonb as usize, pos);
                let (p, it) = open(v, pos);
                pos = p;
                stack.push(it);
            }
        }
    }
}

/// Offsets of containers within the binary jsonb of a base object, built
/// once per base object (PostgreSQL gets them by pointer arithmetic).
#[derive(Default)]
pub(super) struct OffsetCache {
    /// root address -> (container address -> offset from the root)
    maps: Vec<(usize, HashMap<usize, i64>)>,
}

impl OffsetCache {
    /// Offset of the container `target` from the root container `root`
    /// (as stored in a jsonb datum, after the 4-byte varlena header).
    pub fn offset(&mut self, root: &Jsonb, target: &Jsonb) -> Option<i64> {
        if std::ptr::eq(root, target) {
            return Some(0);
        }
        let key = root as *const Jsonb as usize;
        let i = match self.maps.iter().position(|(k, _)| *k == key) {
            Some(i) => i,
            None => {
                let mut abs = HashMap::new();
                positions(root, 4, &mut abs);
                let rel = abs.into_iter().map(|(k, p)| (k, p as i64 - 4)).collect();
                self.maps.push((key, rel));
                self.maps.len() - 1
            }
        };
        self.maps[i]
            .1
            .get(&(target as *const Jsonb as usize))
            .copied()
    }

    /// Same for the object `{"id": id, "key": key, "value": value}`
    /// generated by `.keyvalue()`, where `target` is `value` or a container
    /// inside it.
    pub fn kv_offset(
        &mut self,
        key: &str,
        value: &super::exec::Val<'_>,
        id: i64,
        target: &Jsonb,
    ) -> Option<i64> {
        let super::exec::Val::Container(v) = value else {
            return None;
        };
        // root container at 4: header, 6 JEntries, keys "id" "key" "value"
        let mut pos = 4 + 4 + 6 * 4 + 2 + 3 + 5;
        pos = align4(pos) + numeric_size(&JsonNumber::from_i64(id));
        pos += key.len();
        pos = align4(pos);
        // offsets inside `value` do not depend on where it starts, as long
        // as the start is aligned
        let rel = self.offset(v, target)?;
        Some(pos as i64 - 4 + rel)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn size(s: &str) -> usize {
        numeric_size(&JsonNumber::parse(s).unwrap())
    }

    #[test]
    fn offsets() {
        let doc = Jsonb::parse(r#"{"a": {"b": 1}, "c": [1.5, {"d": "x"}]}"#).unwrap();
        let mut cache = OffsetCache::default();
        let a = doc.get_key("a").unwrap();
        let d = doc.get_key("c").unwrap().get_index(1).unwrap();
        assert_eq!(cache.offset(&doc, &doc), Some(0));
        // root: header 4 + 4 JEntries; keys "a" "c"; then {"b": 1} at 4+4+16+2=26 -> 28
        assert_eq!(cache.offset(&doc, a), Some(24));
        assert!(cache.offset(&doc, d).is_some());
    }

    #[test]
    fn numeric_sizes() {
        assert_eq!(size("0"), 6);
        assert_eq!(size("0.00"), 6);
        assert_eq!(size("1"), 8);
        assert_eq!(size("1.50"), 10);
        assert_eq!(size("12345.678"), 12);
        assert_eq!(size("100000000"), 8);
        assert_eq!(size("0.0001"), 8);
        assert_eq!(size("0.00001"), 8);
        assert_eq!(size("-7"), 8);
        // dscale > 63 needs the long header
        assert_eq!(size(&format!("0.{}1", "0".repeat(63))), 10);
    }
}
