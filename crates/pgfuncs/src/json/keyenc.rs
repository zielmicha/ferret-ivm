//! Order-preserving, self-delimiting binary encoding of jsonb values.
//!
//! `memcmp(enc(a), enc(b))` has the same sign as `a.cmp(b)` (PostgreSQL's
//! compareJsonbContainers ordering), equal values encode to identical bytes,
//! and no encoding is a prefix of another, so encodings can be concatenated
//! into composite keys.
//!
//! Layout:
//! ```text
//! top    := 0x01                          empty array (sorts first)
//!         | 0x02 scalar                   raw scalar
//!         | 0x03 count elem*              non-empty array
//!         | 0x04 count (string elem)*     object
//! elem   := scalar
//!         | 0x15 count elem*              array
//!         | 0x16 count (string elem)*     object
//! scalar := 0x10 | 0x11 string | 0x12 number | 0x13 (false) | 0x14 (true)
//! count  := u32 big endian
//! string := bytes with 0x00 escaped as 0x00 0xFF, terminated by 0x00 0x01
//! number := 0x01 !mag (negative) | 0x02 (zero) | 0x03 mag (positive)
//! mag    := (exponent as u32 ^ 0x8000_0000, big endian)
//!           (1 + 10*d1 + d2 per pair of significant digits)* 0x00
//! ```
//! where value = 0.d1d2d3... x 10^exponent with d1 != 0, and `!mag` is the
//! bitwise complement of every byte of `mag`.

use super::{JsonNumber, Jsonb};
use crate::{Error, Result};

const T_EMPTY_ARRAY: u8 = 0x01;
const T_RAW_SCALAR: u8 = 0x02;
const T_TOP_ARRAY: u8 = 0x03;
const T_TOP_OBJECT: u8 = 0x04;
const T_NULL: u8 = 0x10;
const T_STRING: u8 = 0x11;
const T_NUMBER: u8 = 0x12;
const T_FALSE: u8 = 0x13;
const T_TRUE: u8 = 0x14;
const T_ARRAY: u8 = 0x15;
const T_OBJECT: u8 = 0x16;

const N_NEG: u8 = 0x01;
const N_ZERO: u8 = 0x02;
const N_POS: u8 = 0x03;

fn put_count(out: &mut Vec<u8>, n: usize) {
    out.extend_from_slice(&(n as u32).to_be_bytes());
}

fn put_string(out: &mut Vec<u8>, s: &str) {
    for &b in s.as_bytes() {
        if b == 0 {
            out.extend_from_slice(&[0x00, 0xFF]);
        } else {
            out.push(b);
        }
    }
    out.extend_from_slice(&[0x00, 0x01]);
}

fn put_number(out: &mut Vec<u8>, n: &JsonNumber) {
    if n.is_zero() {
        out.push(N_ZERO);
        return;
    }
    // Keep the durable JSON key format: its decimal exponent and digit
    // pairs differ from Numeric::encode_key's base-100 exponent format.
    let text = n.as_str();
    let neg = n.as_numeric().is_negative();
    let text = text.strip_prefix('-').unwrap_or(text);
    let (int, frac) = text.split_once('.').unwrap_or((text, ""));
    let digit = |i: usize| {
        (if i < int.len() {
            int.as_bytes()[i]
        } else {
            frac.as_bytes()[i - int.len()]
        }) - b'0'
    };
    let mut first = 0;
    let mut end = int.len() + frac.len();
    while digit(first) == 0 {
        first += 1;
    }
    while digit(end - 1) == 0 {
        end -= 1;
    }
    let exp = int.len() as i32 - first as i32;
    out.push(if neg { N_NEG } else { N_POS });
    let start = out.len();
    out.extend_from_slice(&((exp as u32) ^ 0x8000_0000).to_be_bytes());
    for i in (first..end).step_by(2) {
        let lo = if i + 1 < end { digit(i + 1) } else { 0 };
        out.push(1 + 10 * digit(i) + lo);
    }
    out.push(0);
    if neg {
        for b in &mut out[start..] {
            *b = !*b;
        }
    }
}

fn put_scalar(out: &mut Vec<u8>, v: &Jsonb) {
    match v {
        Jsonb::Null => out.push(T_NULL),
        Jsonb::String(s) => {
            out.push(T_STRING);
            put_string(out, s);
        }
        Jsonb::Number(n) => {
            out.push(T_NUMBER);
            put_number(out, n);
        }
        Jsonb::Bool(false) => out.push(T_FALSE),
        Jsonb::Bool(true) => out.push(T_TRUE),
        Jsonb::Array(_) | Jsonb::Object(_) => unreachable!(),
    }
}

enum EncIter<'a> {
    Arr(std::slice::Iter<'a, Jsonb>),
    Obj(std::slice::Iter<'a, (String, Jsonb)>),
}

/// Writes `count` and the children of container `v` (iteratively).
fn put_children<'a>(out: &mut Vec<u8>, v: &'a Jsonb) {
    let open = |out: &mut Vec<u8>, v: &'a Jsonb| -> EncIter<'a> {
        match v {
            Jsonb::Array(a) => {
                put_count(out, a.len());
                EncIter::Arr(a.iter())
            }
            Jsonb::Object(p) => {
                put_count(out, p.len());
                EncIter::Obj(p.iter())
            }
            _ => unreachable!(),
        }
    };
    let mut stack = vec![open(out, v)];
    while let Some(top) = stack.last_mut() {
        let child = match top {
            EncIter::Arr(it) => it.next(),
            EncIter::Obj(it) => it.next().map(|(k, c)| {
                put_string(out, k);
                c
            }),
        };
        match child {
            None => {
                stack.pop();
            }
            Some(c @ Jsonb::Array(_)) => {
                out.push(T_ARRAY);
                let it = open(out, c);
                stack.push(it);
            }
            Some(c @ Jsonb::Object(_)) => {
                out.push(T_OBJECT);
                let it = open(out, c);
                stack.push(it);
            }
            Some(c) => put_scalar(out, c),
        }
    }
}

pub(crate) fn encode_top(v: &Jsonb, out: &mut Vec<u8>) {
    match v {
        Jsonb::Array(a) if a.is_empty() => out.push(T_EMPTY_ARRAY),
        Jsonb::Array(_) => {
            out.push(T_TOP_ARRAY);
            put_children(out, v);
        }
        Jsonb::Object(_) => {
            out.push(T_TOP_OBJECT);
            put_children(out, v);
        }
        s => {
            out.push(T_RAW_SCALAR);
            put_scalar(out, s);
        }
    }
}

fn corrupt() -> Error {
    Error::new("XX001", "invalid jsonb key encoding")
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl Reader<'_> {
    fn byte(&mut self) -> Result<u8> {
        let b = *self.buf.get(self.pos).ok_or_else(corrupt)?;
        self.pos += 1;
        Ok(b)
    }

    fn count(&mut self) -> Result<usize> {
        let end = self.pos + 4;
        let bytes: [u8; 4] = self
            .buf
            .get(self.pos..end)
            .ok_or_else(corrupt)?
            .try_into()
            .unwrap();
        self.pos = end;
        let n = u32::from_be_bytes(bytes) as usize;
        // every element needs at least one byte
        if n > self.buf.len() - self.pos {
            return Err(corrupt());
        }
        Ok(n)
    }

    fn string(&mut self) -> Result<String> {
        let mut bytes = Vec::new();
        loop {
            let b = self.byte()?;
            if b == 0 {
                match self.byte()? {
                    0x01 => break,
                    0xFF => bytes.push(0),
                    _ => return Err(corrupt()),
                }
            } else {
                bytes.push(b);
            }
        }
        String::from_utf8(bytes).map_err(|_| corrupt())
    }

    fn number(&mut self) -> Result<JsonNumber> {
        let sign = self.byte()?;
        if sign == N_ZERO {
            return Ok(JsonNumber::from_i64(0));
        }
        let neg = match sign {
            N_NEG => true,
            N_POS => false,
            _ => return Err(corrupt()),
        };
        let fix = |b: u8| if neg { !b } else { b };
        let mut e = [0u8; 4];
        for x in &mut e {
            *x = fix(self.byte()?);
        }
        let exp = i64::from((u32::from_be_bytes(e) ^ 0x8000_0000) as i32);
        let mut digits = Vec::new();
        loop {
            let b = fix(self.byte()?);
            if b == 0 {
                break;
            }
            if b > 100 {
                return Err(corrupt());
            }
            let v = b - 1;
            digits.push(v / 10);
            digits.push(v % 10);
        }
        if digits.last() == Some(&0) {
            digits.pop();
        }
        if digits.is_empty() || digits[0] == 0 || digits.last() == Some(&0) {
            return Err(corrupt());
        }
        // Scientific input reconstructs the minimal display scale without
        // duplicating Numeric's range checks or decimal normalization.
        let mut text = String::with_capacity(digits.len() + 16);
        if neg {
            text.push('-');
        }
        text.extend(digits.iter().map(|d| (b'0' + d) as char));
        text.push('e');
        text.push_str(&(exp - digits.len() as i64).to_string());
        JsonNumber::parse(&text).map_err(|_| corrupt())
    }

    fn scalar(&mut self, tag: u8) -> Result<Jsonb> {
        Ok(match tag {
            T_NULL => Jsonb::Null,
            T_STRING => Jsonb::String(self.string()?),
            T_NUMBER => Jsonb::Number(self.number()?),
            T_FALSE => Jsonb::Bool(false),
            T_TRUE => Jsonb::Bool(true),
            _ => return Err(corrupt()),
        })
    }

    /// Reads the children of a container (after its tag), iteratively.
    fn container(&mut self, is_array: bool) -> Result<Jsonb> {
        struct Frame {
            arr: Option<Vec<Jsonb>>,
            obj: Option<Vec<(String, Jsonb)>>,
            remaining: usize,
            key: String,
        }
        let open = |r: &mut Self, is_array: bool, key: String| -> Result<Frame> {
            let n = r.count()?;
            Ok(if is_array {
                Frame {
                    arr: Some(Vec::with_capacity(n)),
                    obj: None,
                    remaining: n,
                    key,
                }
            } else {
                Frame {
                    arr: None,
                    obj: Some(Vec::with_capacity(n)),
                    remaining: n,
                    key,
                }
            })
        };
        let mut stack = vec![open(self, is_array, String::new())?];
        loop {
            let top = stack.last_mut().unwrap();
            if top.remaining == 0 {
                let f = stack.pop().unwrap();
                let v = match (f.arr, f.obj) {
                    (Some(a), _) => Jsonb::Array(a),
                    (_, Some(p)) => Jsonb::Object(p),
                    _ => unreachable!(),
                };
                match stack.last_mut() {
                    None => return Ok(v),
                    Some(parent) => match (&mut parent.arr, &mut parent.obj) {
                        (Some(a), _) => a.push(v),
                        (_, Some(p)) => p.push((f.key, v)),
                        _ => unreachable!(),
                    },
                }
                continue;
            }
            top.remaining -= 1;
            let key = if let Some(p) = &top.obj {
                let k = self.string()?;
                if let Some((prev, _)) = p.last()
                    && super::key_cmp(prev, &k) != std::cmp::Ordering::Less
                {
                    return Err(corrupt());
                }
                k
            } else {
                String::new()
            };
            let tag = self.byte()?;
            match tag {
                T_ARRAY | T_OBJECT => {
                    let f = open(self, tag == T_ARRAY, key)?;
                    stack.push(f);
                }
                t => {
                    let v = self.scalar(t)?;
                    let top = stack.last_mut().unwrap();
                    match (&mut top.arr, &mut top.obj) {
                        (Some(a), _) => a.push(v),
                        (_, Some(p)) => p.push((key, v)),
                        _ => unreachable!(),
                    }
                }
            }
        }
    }
}

pub(crate) fn decode_top(buf: &[u8]) -> Result<(Jsonb, usize)> {
    let mut r = Reader { buf, pos: 0 };
    let v = match r.byte()? {
        T_EMPTY_ARRAY => Jsonb::Array(Vec::new()),
        T_RAW_SCALAR => {
            let t = r.byte()?;
            r.scalar(t)?
        }
        T_TOP_ARRAY => {
            let a = r.container(true)?;
            if matches!(&a, Jsonb::Array(v) if v.is_empty()) {
                return Err(corrupt());
            }
            a
        }
        T_TOP_OBJECT => r.container(false)?,
        _ => return Err(corrupt()),
    };
    Ok((v, r.pos))
}
