//! Binary encodings of `Numeric`:
//!
//! * an order-preserving ("memcomparable") key encoding,
//! * a compact exact value encoding preserving dscale,
//! * PostgreSQL's binary wire format (`numeric_send` / `numeric_recv`).

use smallvec::SmallVec;

use crate::var::*;
use crate::{Error, Numeric, Result};

// ----------------------------------------------------------------------
// Key encoding
// ----------------------------------------------------------------------
//
// Layout:
//   tag byte: 0x01 -Infinity, 0x02 negative, 0x03 zero, 0x04 positive,
//             0x05 +Infinity, 0x06 NaN
//   for nonzero finite values, followed by the magnitude encoding (bitwise
//   complemented for negative values):
//     base-100 exponent E (value = 0.c1c2c3... * 100^E, c1 != 0):
//        E in [-64, 63]       -> one byte 0x40 + (E + 64)
//        E >= 64              -> 0xC0, then E as 3 bytes big-endian
//        E <= -65             -> 0x3F, then (E + 2^23) as 3 bytes big-endian
//     base-100 digits c1..cn (trailing zero digits removed, so cn != 0):
//        each digit as one byte 2*c + 1, except the last which is 2*c.
//
// The magnitude encoding is prefix-free and orders by magnitude, so
// complementing it reverses the order for negative values. Equal values
// always produce identical bytes regardless of display scale.

const TAG_NINF: u8 = 0x01;
const TAG_NEG: u8 = 0x02;
const TAG_ZERO: u8 = 0x03;
const TAG_POS: u8 = 0x04;
const TAG_PINF: u8 = 0x05;
const TAG_NAN: u8 = 0x06;

fn key_error(msg: &str) -> Error {
    err("22P03", format!("invalid numeric key encoding: {}", msg))
}

pub(crate) fn encode_key(n: &Numeric, out: &mut Vec<u8>) {
    match n.sign {
        NUMERIC_NAN => {
            out.push(TAG_NAN);
            return;
        }
        NUMERIC_PINF => {
            out.push(TAG_PINF);
            return;
        }
        NUMERIC_NINF => {
            out.push(TAG_NINF);
            return;
        }
        _ => {}
    }
    if n.digits.is_empty() {
        out.push(TAG_ZERO);
        return;
    }
    let neg = n.sign == NUMERIC_NEG;
    out.push(if neg { TAG_NEG } else { TAG_POS });
    let start = out.len();

    // Convert base-10000 digits to base-100 digits.
    let first = n.digits[0];
    let mut exp100: i32 = 2 * n.weight + 2;
    let mut cent: SmallVec<[u8; 24]> = SmallVec::new();
    for &d in n.digits.iter() {
        cent.push((d / 100) as u8);
        cent.push((d % 100) as u8);
    }
    let mut lead = 0;
    if first < 100 {
        lead = 1;
        exp100 -= 1;
    }
    while cent.last() == Some(&0) {
        cent.pop();
    }
    let cent = &cent[lead..];

    if (-64..=63).contains(&exp100) {
        out.push((0x40 + (exp100 + 64)) as u8);
    } else if exp100 >= 64 {
        out.push(0xC0);
        let v = exp100 as u32;
        out.extend_from_slice(&[(v >> 16) as u8, (v >> 8) as u8, v as u8]);
    } else {
        out.push(0x3F);
        let v = (exp100 + (1 << 23)) as u32;
        out.extend_from_slice(&[(v >> 16) as u8, (v >> 8) as u8, v as u8]);
    }
    let last = cent.len() - 1;
    for (i, &c) in cent.iter().enumerate() {
        out.push(if i == last { 2 * c } else { 2 * c + 1 });
    }

    if neg {
        for b in &mut out[start..] {
            *b = !*b;
        }
    }
}

pub(crate) fn decode_key(buf: &[u8]) -> Result<(Numeric, usize)> {
    let tag = *buf.first().ok_or_else(|| key_error("empty input"))?;
    let neg = match tag {
        TAG_NAN => return Ok((Numeric::nan(), 1)),
        TAG_PINF => return Ok((Numeric::infinity(), 1)),
        TAG_NINF => return Ok((Numeric::neg_infinity(), 1)),
        TAG_ZERO => return Ok((Numeric::zero(), 1)),
        TAG_POS => false,
        TAG_NEG => true,
        _ => return Err(key_error("bad tag")),
    };
    let mask: u8 = if neg { 0xFF } else { 0x00 };
    let mut pos = 1;
    let next = |pos: &mut usize| -> Result<u8> {
        let b = *buf.get(*pos).ok_or_else(|| key_error("truncated input"))?;
        *pos += 1;
        Ok(b ^ mask)
    };

    let b0 = next(&mut pos)?;
    let exp100: i32 = if (0x40..=0xBF).contains(&b0) {
        b0 as i32 - 0x40 - 64
    } else if b0 == 0xC0 {
        let v = ((next(&mut pos)? as u32) << 16)
            | ((next(&mut pos)? as u32) << 8)
            | next(&mut pos)? as u32;
        v as i32
    } else if b0 == 0x3F {
        let v = ((next(&mut pos)? as u32) << 16)
            | ((next(&mut pos)? as u32) << 8)
            | next(&mut pos)? as u32;
        v as i32 - (1 << 23)
    } else {
        return Err(key_error("bad exponent"));
    };

    let mut cent: SmallVec<[u8; 24]> = SmallVec::new();
    loop {
        let b = next(&mut pos)?;
        let c = b >> 1;
        if c > 99 {
            return Err(key_error("bad digit"));
        }
        cent.push(c);
        if b & 1 == 0 {
            break;
        }
    }
    if cent[0] == 0 || *cent.last().unwrap() == 0 {
        return Err(key_error("non-normalized digits"));
    }

    // value = 0.c1c2... * 100^exp100. Align to base-10000 digits: the
    // base-10000 weight w satisfies 2*w + 2 == exp100 (if exp100 even) or
    // 2*w + 2 == exp100 + 1 (prepend a zero centimal digit).
    let mut cents: SmallVec<[u8; 26]> = SmallVec::new();
    let weight = if exp100 % 2 == 0 {
        exp100 / 2 - 1
    } else {
        cents.push(0);
        (exp100 + 1) / 2 - 1
    };
    cents.extend_from_slice(&cent);
    if cents.len() % 2 == 1 {
        cents.push(0);
    }
    let mut digits: Digits = SmallVec::with_capacity(cents.len() / 2);
    for pair in cents.chunks(2) {
        digits.push(pair[0] as i16 * 100 + pair[1] as i16);
    }
    let mut v = Numeric {
        sign: if neg { NUMERIC_NEG } else { NUMERIC_POS },
        weight,
        dscale: 0,
        digits,
    };
    if v.weight > NUMERIC_WEIGHT_MAX || v.weight < NUMERIC_WEIGHT_MIN {
        return Err(key_error("weight out of range"));
    }
    v.dscale = min_scale_of(&v);
    if v.dscale > NUMERIC_DSCALE_MAX {
        return Err(key_error("scale out of range"));
    }
    Ok((v, pos))
}

fn min_scale_of(v: &Numeric) -> i32 {
    // digits are stripped, so the last digit is nonzero
    let last = v.nd() - 1;
    let mut min_scale = (last - v.weight) * DEC_DIGITS;
    if min_scale > 0 {
        let mut d = v.dig(last);
        while d % 10 == 0 {
            min_scale -= 1;
            d /= 10;
        }
        min_scale
    } else {
        0
    }
}

// ----------------------------------------------------------------------
// Compact value encoding
// ----------------------------------------------------------------------
//
// Layout:
//   header byte: 0xF0 NaN, 0xF1 +Infinity, 0xF2 -Infinity; otherwise
//     bit 6 = negative, bits 0..5 = number of base-10000 digits, or 63 if
//     the count follows as a LEB128 varint.
//   dscale: LEB128 varint
//   if ndigits > 0: weight as zigzag LEB128 varint, then each digit as a
//     little-endian u16.

const BIN_NAN: u8 = 0xF0;
const BIN_PINF: u8 = 0xF1;
const BIN_NINF: u8 = 0xF2;

fn bin_error(msg: &str) -> Error {
    err("22P03", format!("invalid numeric binary encoding: {}", msg))
}

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn get_varint(buf: &[u8], pos: &mut usize) -> Result<u64> {
    let mut v: u64 = 0;
    let mut shift = 0;
    loop {
        let b = *buf.get(*pos).ok_or_else(|| bin_error("truncated input"))?;
        *pos += 1;
        if shift >= 63 && b > 1 {
            return Err(bin_error("varint overflow"));
        }
        v |= ((b & 0x7F) as u64) << shift;
        if b & 0x80 == 0 {
            return Ok(v);
        }
        shift += 7;
        if shift > 63 {
            return Err(bin_error("varint overflow"));
        }
    }
}

pub(crate) fn encode_binary(n: &Numeric, out: &mut Vec<u8>) {
    match n.sign {
        NUMERIC_NAN => {
            out.push(BIN_NAN);
            return;
        }
        NUMERIC_PINF => {
            out.push(BIN_PINF);
            return;
        }
        NUMERIC_NINF => {
            out.push(BIN_NINF);
            return;
        }
        _ => {}
    }
    let nd = n.digits.len();
    let neg = if n.sign == NUMERIC_NEG && nd > 0 {
        0x40
    } else {
        0
    };
    if nd < 63 {
        out.push(neg | nd as u8);
    } else {
        out.push(neg | 63);
        put_varint(out, nd as u64);
    }
    put_varint(out, n.dscale as u64);
    if nd > 0 {
        let w = n.weight as i64;
        put_varint(out, ((w << 1) ^ (w >> 63)) as u64);
        out.reserve(nd * 2);
        for &d in n.digits.iter() {
            out.extend_from_slice(&(d as u16).to_le_bytes());
        }
    }
}

pub(crate) fn decode_binary(buf: &[u8]) -> Result<(Numeric, usize)> {
    let h = *buf.first().ok_or_else(|| bin_error("empty input"))?;
    match h {
        BIN_NAN => return Ok((Numeric::nan(), 1)),
        BIN_PINF => return Ok((Numeric::infinity(), 1)),
        BIN_NINF => return Ok((Numeric::neg_infinity(), 1)),
        _ => {}
    }
    if h & 0x80 != 0 {
        return Err(bin_error("bad header"));
    }
    let mut pos = 1;
    let neg = h & 0x40 != 0;
    let mut nd = (h & 0x3F) as u64;
    if nd == 63 {
        nd = get_varint(buf, &mut pos)?;
    }
    let dscale = get_varint(buf, &mut pos)?;
    if dscale > NUMERIC_DSCALE_MAX as u64 {
        return Err(bin_error("scale out of range"));
    }
    let mut weight: i64 = 0;
    if nd > 0 {
        let z = get_varint(buf, &mut pos)?;
        weight = ((z >> 1) as i64) ^ -((z & 1) as i64);
    }
    let nd = nd as usize;
    if buf.len() < pos || (buf.len() - pos) / 2 < nd {
        return Err(bin_error("truncated input"));
    }
    let mut digits: Digits = SmallVec::with_capacity(nd);
    for i in 0..nd {
        let d = u16::from_le_bytes([buf[pos + 2 * i], buf[pos + 2 * i + 1]]);
        if d >= NBASE as u16 {
            return Err(bin_error("bad digit"));
        }
        digits.push(d as i16);
    }
    pos += 2 * nd;
    if weight.abs() > (1 << 30) {
        return Err(bin_error("weight out of range"));
    }
    let v = Numeric {
        sign: if neg { NUMERIC_NEG } else { NUMERIC_POS },
        weight: weight as i32,
        dscale: dscale as i32,
        digits,
    };
    let v = make_result(v).map_err(|_| bin_error("value out of range"))?;
    if !v.digits.is_empty() && min_scale_of(&v) > v.dscale {
        return Err(bin_error("scale hides digits"));
    }
    Ok((v, pos))
}

// ----------------------------------------------------------------------
// PostgreSQL binary format
// ----------------------------------------------------------------------

pub(crate) fn to_pg_binary(n: &Numeric) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + 2 * n.digits.len());
    let (ndigits, weight, sign, dscale): (u16, i16, u16, u16) = match n.sign {
        // numeric_send reads the special header as a short-format header,
        // which yields dscale 32 for infinities and 0 for NaN.
        NUMERIC_NAN => (0, 0, NUMERIC_NAN, 0),
        NUMERIC_PINF => (0, 0, NUMERIC_PINF, 32),
        NUMERIC_NINF => (0, 0, NUMERIC_NINF, 32),
        s => (n.digits.len() as u16, n.weight as i16, s, n.dscale as u16),
    };
    out.extend_from_slice(&ndigits.to_be_bytes());
    out.extend_from_slice(&weight.to_be_bytes());
    out.extend_from_slice(&sign.to_be_bytes());
    out.extend_from_slice(&dscale.to_be_bytes());
    if !n.is_special() {
        for &d in n.digits.iter() {
            out.extend_from_slice(&d.to_be_bytes());
        }
    }
    out
}

pub(crate) fn from_pg_binary(buf: &[u8], typmod: Option<(i32, i32)>) -> Result<Numeric> {
    let insufficient = || err("08P01", "insufficient data left in message");
    let get16 = |pos: usize| -> Result<u16> {
        if pos + 2 > buf.len() {
            return Err(insufficient());
        }
        Ok(u16::from_be_bytes([buf[pos], buf[pos + 1]]))
    };
    let len = get16(0)? as usize;
    let weight = get16(2)? as i16 as i32;
    let sign = get16(4)?;
    if !matches!(
        sign,
        NUMERIC_POS | NUMERIC_NEG | NUMERIC_NAN | NUMERIC_PINF | NUMERIC_NINF
    ) {
        return Err(err("22P03", "invalid sign in external \"numeric\" value"));
    }
    let dscale = get16(6)?;
    if (dscale as i32 & NUMERIC_DSCALE_MAX) != dscale as i32 {
        return Err(err("22P03", "invalid scale in external \"numeric\" value"));
    }
    let mut digits: Digits = SmallVec::with_capacity(len);
    for i in 0..len {
        let d = get16(8 + 2 * i)? as i16;
        if d < 0 || d as i32 >= NBASE {
            return Err(err("22P03", "invalid digit in external \"numeric\" value"));
        }
        digits.push(d);
    }
    if buf.len() != 8 + 2 * len {
        return Err(err("22P03", "incorrect binary data format"));
    }
    if sign == NUMERIC_POS || sign == NUMERIC_NEG {
        let mut value = Numeric {
            sign,
            weight,
            dscale: dscale as i32,
            digits,
        };
        trunc_var(&mut value, dscale as i32);
        if let Some((p, s)) = typmod {
            crate::apply_typmod_var(&mut value, p, s)?;
        }
        make_result(value)
    } else if let Some((p, s)) = typmod {
        crate::apply_typmod_special(var_special(sign), p, s)
    } else {
        Ok(var_special(sign))
    }
}
