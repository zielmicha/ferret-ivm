//! Crate-internal tests: every public operation must return values in the
//! normalized form (stripped digits, canonical zero, honest dscale), which
//! Hash/Eq/encoding rely on.

use crate::Numeric;
use crate::var::*;

fn assert_normalized(v: &Numeric, ctx: &str) {
    if v.is_special() {
        assert!(
            v.digits.is_empty() && v.weight == 0 && v.dscale == 0,
            "special not canonical: {ctx}"
        );
        return;
    }
    assert!(
        (0..=NUMERIC_DSCALE_MAX).contains(&v.dscale),
        "dscale out of range: {ctx}"
    );
    if v.digits.is_empty() {
        assert_eq!(v.weight, 0, "zero weight: {ctx}");
        assert_eq!(v.sign, NUMERIC_POS, "zero sign: {ctx}");
        return;
    }
    assert!(v.sign == NUMERIC_POS || v.sign == NUMERIC_NEG, "{ctx}");
    assert!(v.digits[0] != 0, "leading zero digit: {ctx}");
    assert!(*v.digits.last().unwrap() != 0, "trailing zero digit: {ctx}");
    assert!(
        v.digits.iter().all(|&d| (0..NBASE as i16).contains(&d)),
        "{ctx}"
    );
    assert!(
        (NUMERIC_WEIGHT_MIN..=NUMERIC_WEIGHT_MAX).contains(&v.weight),
        "weight out of range: {ctx}"
    );
    // dscale must not hide digits
    let mut min_scale = (v.nd() - 1 - v.weight) * DEC_DIGITS;
    if min_scale > 0 {
        let mut d = *v.digits.last().unwrap() as i32;
        while d % 10 == 0 {
            min_scale -= 1;
            d /= 10;
        }
        assert!(min_scale <= v.dscale, "dscale hides digits: {ctx}");
    }
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn random_numeric(r: &mut Rng) -> Numeric {
    match r.below(16) {
        0 => return Numeric::nan(),
        1 => return Numeric::infinity(),
        2 => return Numeric::neg_infinity(),
        3 => return Numeric::from_i64(r.next() as i64 >> r.below(64)),
        _ => {}
    }
    let mut s = String::new();
    if r.below(2) == 0 {
        s.push('-');
    }
    let ni = r.below(22);
    for _ in 0..ni {
        s.push((b'0' + r.below(10) as u8) as char);
    }
    if ni == 0 {
        s.push('0');
    }
    let nf = r.below(22);
    if nf > 0 {
        s.push('.');
        for _ in 0..nf {
            s.push((b'0' + r.below(10) as u8) as char);
        }
    }
    if r.below(5) == 0 {
        s.push_str(&format!("e{}", r.below(80) as i64 - 40));
    }
    Numeric::parse(&s).unwrap()
}

#[test]
fn public_results_are_normalized() {
    let mut r = Rng(0xdead_beef);
    for i in 0..8000 {
        let a = random_numeric(&mut r);
        let b = random_numeric(&mut r);
        let ctx = format!("#{i} a={a} b={b}");
        let check = |res: crate::Result<Numeric>, op: &str| {
            if let Ok(v) = res {
                assert_normalized(&v, &format!("{op} {ctx}"));
            }
        };
        assert_normalized(&a, &ctx);
        check(a.add(&b), "add");
        check(a.sub(&b), "sub");
        check(a.mul(&b), "mul");
        check(a.div(&b), "div");
        check(a.div_trunc(&b), "div_trunc");
        check(a.rem(&b), "rem");
        check(Ok(a.neg()), "neg");
        check(Ok(a.abs()), "abs");
        check(Ok(a.sign()), "sign");
        let s = r.below(30) as i32 - 10;
        check(a.round(s), "round");
        check(a.trunc(s), "trunc");
        check(Ok(a.ceil()), "ceil");
        check(Ok(a.floor()), "floor");
        check(Ok(a.normalize()), "normalize");
        check(Ok(a.with_dscale(r.below(30) as u32)), "with_dscale");
        check(a.abs().sqrt(), "sqrt");
        if i % 4 == 0 {
            check(a.trunc(2).and_then(|x| x.exp()), "exp");
            check(a.abs().ln(), "ln");
            check(a.abs().log10(), "log10");
            check(a.abs().log(&b.abs()), "log");
            check(a.pow(&b.trunc(1).unwrap()), "pow");
            check(
                a.pow(&Numeric::from_i64(r.below(20) as i64 - 10)),
                "pow int",
            );
            check(a.trunc(0).and_then(|x| x.gcd(&b.trunc(0).unwrap())), "gcd");
            check(a.lcm(&b), "lcm");
        }
        let p = 1 + r.below(30) as i32;
        let sc = r.below(30) as i32 - 5;
        check(a.apply_typmod(p, sc), "typmod");
        if let Ok(f) = a.to_f64() {
            check(Numeric::from_f64(f), "from_f64");
        }
        if let Ok(f) = a.to_f32() {
            check(Numeric::from_f32(f), "from_f32");
        }
        let mut k = Vec::new();
        a.encode_key(&mut k);
        check(Numeric::decode_key(&k).map(|x| x.0), "decode_key");
        let mut bb = Vec::new();
        a.encode_binary(&mut bb);
        check(Numeric::decode_binary(&bb).map(|x| x.0), "decode_binary");
        check(Numeric::from_pg_binary(&a.to_pg_binary()), "from_pg_binary");
    }
    for v in [0i64, 10000, -10000, 1_0000_0000, i64::MIN, i64::MAX] {
        assert_normalized(&Numeric::from_i64(v), "from_i64");
        assert_normalized(&Numeric::from_i128(v as i128 * 10000), "from_i128");
    }
    for n in [0, 1, 5, 25, 100] {
        assert_normalized(&Numeric::factorial(n).unwrap(), "factorial");
    }
}

#[test]
fn encoding_lengths_without_decoding() {
    let mut r = Rng(0x5EED_1E57);
    let mut vals: Vec<Numeric> = (0..5000).map(|_| random_numeric(&mut r)).collect();
    vals.extend([Numeric::nan(), Numeric::infinity(), Numeric::neg_infinity(), Numeric::zero()]);
    vals.push(Numeric::from_i128(i128::MAX));
    vals.push(Numeric::from_i128(i128::MIN));
    vals.push(Numeric::parse("1e1000").unwrap());
    vals.push(Numeric::parse("-1e-1000").unwrap());
    for a in &vals {
        let mut k = Vec::new();
        a.encode_key(&mut k);
        let n = k.len();
        // followed by other data, as inside a composite key
        k.extend_from_slice(&[0x00, 0x01, 0xFF]);
        assert_eq!(Numeric::key_len(&k, false).unwrap(), n, "key_len of {a}");
        let inv: Vec<u8> = k.iter().map(|b| !b).collect();
        assert_eq!(Numeric::key_len(&inv, true).unwrap(), n, "complemented key_len of {a}");
        let mut b = Vec::new();
        a.encode_binary(&mut b);
        let n = b.len();
        b.extend_from_slice(&[0x80, 0x00]);
        assert_eq!(Numeric::binary_len(&b).unwrap(), n, "binary_len of {a}");
        assert!(Numeric::binary_len(&b[..n - 1]).is_err() || n == 1);
    }
}
