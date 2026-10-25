//! Property tests (encodings, ordering, hashing) and unit tests for helpers.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use pgnumeric::Numeric;

/// Small deterministic PRNG (xorshift64*).
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
    fn digits(&mut self, n: usize) -> String {
        let mut s = String::with_capacity(n);
        for _ in 0..n {
            s.push((b'0' + self.below(10) as u8) as char);
        }
        s
    }
}

fn n(s: &str) -> Numeric {
    Numeric::parse(s).unwrap_or_else(|e| panic!("parse {s}: {e}"))
}

fn random_numeric(r: &mut Rng) -> Numeric {
    match r.below(20) {
        0 => return Numeric::nan(),
        1 => return Numeric::infinity(),
        2 => return Numeric::neg_infinity(),
        3 => {
            let z = r.below(6) as usize;
            return n(&format!("0.{}", "0".repeat(z)));
        }
        _ => {}
    }
    let sign = if r.below(2) == 0 { "-" } else { "" };
    let s = match r.below(4) {
        0 => {
            // scientific with wide exponent range
            let len = 1 + r.below(30) as usize;
            let mant = r.digits(len);
            let e = r.below(2000) as i64 - 1000;
            format!("{sign}{mant}e{e}")
        }
        1 => {
            // extreme exponents
            let len = 1 + r.below(8) as usize;
            let mant = r.digits(len);
            let e = if r.below(2) == 0 {
                131000 + r.below(60) as i64
            } else {
                -(16300 + r.below(70) as i64)
            };
            format!("{sign}{mant}e{e}")
        }
        _ => {
            let li = r.below(25) as usize;
            let ip = r.digits(li);
            let lf = r.below(25) as usize;
            let fp = r.digits(lf);
            let ip = if ip.is_empty() { "0".to_string() } else { ip };
            if fp.is_empty() {
                format!("{sign}{ip}")
            } else {
                format!("{sign}{ip}.{fp}")
            }
        }
    };
    match Numeric::parse(&s) {
        Ok(v) => v,
        Err(_) => Numeric::zero(), // out of range exponent etc.
    }
}

fn key(v: &Numeric) -> Vec<u8> {
    let mut out = Vec::new();
    v.encode_key(&mut out);
    out
}

fn hash_of(v: &Numeric) -> u64 {
    let mut h = DefaultHasher::new();
    v.hash(&mut h);
    h.finish()
}

#[test]
fn key_encoding_order_matches_cmp() {
    let mut r = Rng(0x1234_5678_9abc_def1);
    let mut vals: Vec<Numeric> = (0..4000).map(|_| random_numeric(&mut r)).collect();
    // boundary values around the 1-byte exponent range and digit splits
    for k in -300..300 {
        vals.push(n(&format!("1e{k}")));
        vals.push(n(&format!("-1e{k}")));
        vals.push(n(&format!("9.9e{k}")));
        vals.push(n(&format!("1.01e{k}")));
    }
    for s in [
        "0",
        "1",
        "-1",
        "0.01",
        "0.0100",
        "1.0",
        "99",
        "100",
        "101",
        "9999",
        "10000",
        "10001",
        "0.0099",
        "0.99",
        "0.999999",
        "1e131071",
        "9.999e131071",
        "-1e131071",
        "1e-16383",
        "-1e-16383",
        "12",
        "12.000001",
        "1200",
        "120000",
        "0.12",
        "0.1200",
    ] {
        vals.push(n(s));
    }
    let keys: Vec<Vec<u8>> = vals.iter().map(key).collect();
    for i in 0..vals.len() {
        // compare against a few random others plus the neighbour
        for _ in 0..8 {
            let j = r.below(vals.len() as u64) as usize;
            assert_eq!(
                vals[i].cmp(&vals[j]),
                keys[i].cmp(&keys[j]),
                "order mismatch: {} vs {}",
                vals[i],
                vals[j]
            );
        }
    }
    // full sort consistency
    let mut idx: Vec<usize> = (0..vals.len()).collect();
    idx.sort_by(|&a, &b| vals[a].cmp(&vals[b]));
    for w in idx.windows(2) {
        let (a, b) = (w[0], w[1]);
        assert_eq!(vals[a].cmp(&vals[b]), keys[a].cmp(&keys[b]));
    }
}

#[test]
fn key_encoding_scale_insensitive_and_roundtrip() {
    let mut r = Rng(42);
    for _ in 0..5000 {
        let v = random_numeric(&mut r);
        let k = key(&v);
        // equal values with different display scales encode identically
        if v.is_finite() {
            for extra in [1u32, 3, 4, 9] {
                let w = v.with_dscale(v.dscale() + extra);
                assert_eq!(w, v);
                assert_eq!(key(&w), k, "scale sensitivity for {v}");
                assert_eq!(hash_of(&w), hash_of(&v));
            }
            let norm = v.normalize();
            assert_eq!(key(&norm), k);
        }
        // roundtrip, self-delimiting with trailing garbage
        let mut buf = k.clone();
        buf.extend_from_slice(&[0x00, 0xFF, 0x42]);
        let (d, used) = Numeric::decode_key(&buf).unwrap();
        assert_eq!(used, k.len());
        assert_eq!(d, v);
        assert_eq!(d.dscale(), v.normalize().dscale(), "decoded scale of {v}");
        assert_eq!(d.to_string(), v.normalize().to_string());
    }
}

#[test]
fn key_encoding_concatenated_columns() {
    let mut r = Rng(7);
    for _ in 0..500 {
        let cols: Vec<Numeric> = (0..4).map(|_| random_numeric(&mut r)).collect();
        let mut buf = Vec::new();
        for c in &cols {
            c.encode_key(&mut buf);
        }
        let mut pos = 0;
        for c in &cols {
            let (d, used) = Numeric::decode_key(&buf[pos..]).unwrap();
            assert_eq!(&d, c);
            pos += used;
        }
        assert_eq!(pos, buf.len());
    }
    // tuple ordering: (a, b) vs (a', b') compares lexicographically
    for _ in 0..2000 {
        let a1 = random_numeric(&mut r);
        let b1 = random_numeric(&mut r);
        let a2 = if r.below(3) == 0 {
            a1.with_dscale(a1.dscale() + 2)
        } else {
            random_numeric(&mut r)
        };
        let b2 = random_numeric(&mut r);
        let mut k1 = Vec::new();
        a1.encode_key(&mut k1);
        b1.encode_key(&mut k1);
        let mut k2 = Vec::new();
        a2.encode_key(&mut k2);
        b2.encode_key(&mut k2);
        assert_eq!((&a1, &b1).cmp(&(&a2, &b2)), k1.cmp(&k2));
    }
}

#[test]
fn key_decode_rejects_garbage() {
    assert!(Numeric::decode_key(&[]).is_err());
    assert!(Numeric::decode_key(&[0x00]).is_err());
    assert!(Numeric::decode_key(&[0x04]).is_err());
    assert!(Numeric::decode_key(&[0x04, 0x41]).is_err());
    // digit byte > 2*99+1
    assert!(Numeric::decode_key(&[0x04, 0x41, 0xF0]).is_err());
    // non-normalized: last digit zero
    assert!(Numeric::decode_key(&[0x04, 0x41, 0x03, 0x00]).is_err());
}

#[test]
fn binary_encoding_roundtrip() {
    let mut r = Rng(99);
    for _ in 0..5000 {
        let v = random_numeric(&mut r);
        let mut buf = Vec::new();
        v.encode_binary(&mut buf);
        let len = buf.len();
        buf.extend_from_slice(b"tail");
        let (d, used) = Numeric::decode_binary(&buf).unwrap();
        assert_eq!(used, len);
        assert_eq!(d.to_string(), v.to_string());
        assert_eq!(d.dscale(), v.dscale());
        // pg wire format
        let p = v.to_pg_binary();
        let d2 = Numeric::from_pg_binary(&p).unwrap();
        assert_eq!(d2.to_string(), v.to_string());
    }
    // compactness of common values
    let mut buf = Vec::new();
    n("12.5").encode_binary(&mut buf);
    assert!(buf.len() <= 7, "{:?}", buf);
    assert!(Numeric::decode_binary(&[]).is_err());
    assert!(Numeric::decode_binary(&[0x02, 0x00, 0x00, 0x01]).is_err());
    assert!(Numeric::decode_binary(&[0x01, 0x00, 0x00, 0x10, 0x27]).is_err()); // digit 10000
    // 1 digit (5) at weight -1 => 0.0005 needs dscale 4; dscale 2 hides digits
    assert!(Numeric::decode_binary(&[0x01, 0x02, 0x01, 0x05, 0x00]).is_err());
    assert_eq!(
        Numeric::decode_binary(&[0x01, 0x04, 0x01, 0x05, 0x00])
            .unwrap()
            .0
            .to_string(),
        "0.0005"
    );
}

#[test]
fn eq_hash_ord_consistency() {
    let mut r = Rng(5);
    let vals: Vec<Numeric> = (0..2000).map(|_| random_numeric(&mut r)).collect();
    for i in 0..vals.len() {
        let j = r.below(vals.len() as u64) as usize;
        let (a, b) = (&vals[i], &vals[j]);
        if a == b {
            assert_eq!(hash_of(a), hash_of(b));
        }
        assert_eq!(a.cmp(b), b.cmp(a).reverse());
    }
    assert_eq!(n("1.0"), n("1.00"));
    assert_eq!(hash_of(&n("1.0")), hash_of(&n("1.00")));
    assert_eq!(hash_of(&n("0")), hash_of(&n("-0.000")));
    assert_eq!(Numeric::nan(), Numeric::nan());
    assert!(Numeric::nan() > Numeric::infinity());
    assert!(Numeric::infinity() > n("1e131071"));
    assert!(Numeric::neg_infinity() < n("-1e131071"));
}

#[test]
fn predicates_and_scales() {
    assert!(Numeric::nan().is_nan());
    assert!(!Numeric::nan().is_finite());
    assert!(Numeric::infinity().is_infinite());
    assert!(Numeric::neg_infinity().is_negative());
    assert!(!Numeric::infinity().is_negative());
    assert!(n("-0.000").is_zero());
    assert!(!n("-0.000").is_negative());
    assert!(n("-0.001").is_negative());
    assert_eq!(n("-0.000").dscale(), 3);
    assert_eq!(n("1.2300").scale(), Some(4));
    assert_eq!(n("1.2300").min_scale(), Some(2));
    assert_eq!(Numeric::nan().scale(), None);
    assert_eq!(n("1.2300").normalize().to_string(), "1.23");
    assert_eq!(n("1.25").with_dscale(1).to_string(), "1.3");
    assert_eq!(n("-1.25").with_dscale(1).to_string(), "-1.3");
    assert_eq!(n("1.2").with_dscale(4).to_string(), "1.2000");
    assert_eq!(n("9.99").with_dscale(0).to_string(), "10");
    assert_eq!(n("0.004").with_dscale(2).to_string(), "0.00");
    assert_eq!(Numeric::nan().with_dscale(3).to_string(), "NaN");
    assert_eq!(format!("{:?}", n("1.50")), "Numeric(1.50)");
    assert_eq!(Numeric::zero().to_string(), "0");
    assert_eq!(Numeric::default(), Numeric::zero());
}

#[test]
fn integer_conversions() {
    assert_eq!(
        Numeric::from_i64(i64::MIN).to_string(),
        "-9223372036854775808"
    );
    assert_eq!(Numeric::from_i64(i64::MAX).to_i64().unwrap(), i64::MAX);
    assert_eq!(Numeric::from_i64(i64::MIN).to_i64().unwrap(), i64::MIN);
    assert_eq!(
        Numeric::from_i128(i128::MIN).to_string(),
        "-170141183460469231731687303715884105728"
    );
    assert_eq!(
        Numeric::from_i128(i128::MIN).to_i128_exact(),
        Some(i128::MIN)
    );
    assert_eq!(
        Numeric::from_i128(i128::MAX).to_i128_exact(),
        Some(i128::MAX)
    );
    assert_eq!(
        n("170141183460469231731687303715884105728").to_i128_exact(),
        None
    );
    assert_eq!(n("12.000").to_i128_exact(), Some(12));
    assert_eq!(n("12.5").to_i128_exact(), None);
    assert_eq!(n("1e30").to_i128_exact(), Some(10i128.pow(30)));
    assert_eq!(Numeric::nan().to_i128_exact(), None);
    assert_eq!(n("-0").to_i128_exact(), Some(0));
    let e = Numeric::nan().to_i32().unwrap_err();
    assert_eq!(
        (e.sqlstate, e.message.as_str()),
        ("0A000", "cannot convert NaN to integer")
    );
    let e = Numeric::infinity().to_i16().unwrap_err();
    assert_eq!(e.message, "cannot convert infinity to smallint");
    assert_eq!(n("2.5").to_i64().unwrap(), 3);
    assert_eq!(n("-2.5").to_i64().unwrap(), -3);
    // from_i64/from_i128 produce normalized values (hash/binary equality)
    for v in [
        0i64,
        1,
        -1,
        10000,
        -10000,
        100000000,
        1_0000_0000_0000,
        12_0000,
        i64::MIN,
        i64::MAX,
    ] {
        let a = Numeric::from_i64(v);
        let b = n(&v.to_string());
        assert_eq!(hash_of(&a), hash_of(&b));
        assert_eq!(a.to_pg_binary(), b.to_pg_binary());
        let c = Numeric::from_i128(v as i128 * 10000);
        let d = n(&(v as i128 * 10000).to_string());
        assert_eq!(hash_of(&c), hash_of(&d));
        assert_eq!(c.to_pg_binary(), d.to_pg_binary());
    }
    let mut r = Rng(3);
    for _ in 0..2000 {
        let v = (r.next() as i128).wrapping_mul(r.next() as i128 >> (r.below(64) as u32));
        assert_eq!(Numeric::from_i128(v).to_i128_exact(), Some(v));
        assert_eq!(Numeric::from_i128(v).to_string(), v.to_string());
        let w = r.next() as i64;
        assert_eq!(Numeric::from_i64(w).to_i64().unwrap(), w);
    }
}

#[test]
fn display_parse_roundtrip() {
    let mut r = Rng(11);
    for _ in 0..3000 {
        let v = random_numeric(&mut r);
        let s = v.to_string();
        let w = n(&s);
        assert_eq!(w.to_string(), s);
        assert_eq!(w.dscale(), v.dscale());
        let fs: Numeric = s.parse().unwrap();
        assert_eq!(fs, v);
    }
}

#[test]
fn ceil_floor_saturation_helpers() {
    let huge = format!("{}.5", "9".repeat(131072));
    let v = n(&huge);
    assert!(v.try_ceil().is_err());
    assert_eq!(v.ceil(), Numeric::infinity());
    assert_eq!(v.floor().to_string().len(), 131072);
    let neg = v.neg();
    assert!(neg.try_floor().is_err());
    assert_eq!(neg.floor(), Numeric::neg_infinity());
    assert_eq!(n("1.5").ceil().to_string(), "2");
    assert_eq!(n("-1.5").floor().to_string(), "-2");
}

#[test]
fn stddev_helper_matches_postgres() {
    // Expected values from PostgreSQL 17.
    let check = |vals: &[&str], expect: [Option<&str>; 4]| {
        let mut sum = Numeric::zero();
        let mut sum2 = Numeric::zero();
        for v in vals {
            let x = n(v);
            sum = sum.add(&x).unwrap();
            sum2 = sum2.add(&x.mul(&x).unwrap()).unwrap();
        }
        let cnt = vals.len() as i64;
        let got = [
            Numeric::stddev_from_sums(cnt, &sum, &sum2, true, false).unwrap(),
            Numeric::stddev_from_sums(cnt, &sum, &sum2, true, true).unwrap(),
            Numeric::stddev_from_sums(cnt, &sum, &sum2, false, false).unwrap(),
            Numeric::stddev_from_sums(cnt, &sum, &sum2, false, true).unwrap(),
        ];
        for (g, e) in got.iter().zip(expect.iter()) {
            assert_eq!(g.as_ref().map(|x| x.to_string()).as_deref(), *e, "{vals:?}");
        }
    };
    check(
        &["1.5", "2.25", "3.125", "-4"],
        [
            Some("7.7529296875000000"),
            Some("10.3372395833333333"),
            Some("2.7844083191047968"),
            Some("3.2151577851379757"),
        ],
    );
    check(&["1"], [Some("0"), None, Some("0"), None]);
    check(&["1", "1.0"], [Some("0"), Some("0"), Some("0"), Some("0")]);
    check(
        &["1", "NaN"],
        [Some("NaN"), Some("NaN"), Some("NaN"), Some("NaN")],
    );
    check(
        &["1", "2", "3", "10", "1000000"],
        [
            Some("159998720012.56000000"),
            Some("199998400015.70000000"),
            Some("399998.40001250"),
            Some("447211.80665955"),
        ],
    );
    check(
        &["0.1", "0.2", "0.3", "123456789.987654321"],
        [
            Some("2857796052126200.301063100135619570"),
            Some("3810394736168267.068084133514159427"),
            Some("53458358.112891947488534050"),
            Some("61728394.893827160554000000"),
        ],
    );
    check(&[], [None, None, None, None]);
}

#[test]
fn float_formatting_roundtrips() {
    use pgnumeric::float::{format_f32, format_f64, parse_f32, parse_f64};
    let mut r = Rng(77);
    for _ in 0..200_000 {
        let x = f64::from_bits(r.next());
        let s = format_f64(x);
        let y = parse_f64(&s).unwrap();
        assert!(
            x.to_bits() == y.to_bits() || (x.is_nan() && y.is_nan()),
            "{x:e} -> {s}"
        );
        let xf = f32::from_bits(r.next() as u32);
        let sf = format_f32(xf);
        let yf = parse_f32(&sf).unwrap();
        assert!(
            xf.to_bits() == yf.to_bits() || (xf.is_nan() && yf.is_nan()),
            "{xf:e} -> {sf}"
        );
    }
    assert_eq!(format_f64(-0.0), "-0");
    assert_eq!(format_f64(f64::NAN), "NaN");
    assert_eq!(format_f64(f64::NEG_INFINITY), "-Infinity");
    assert_eq!(format_f64(1e15), "1e+15");
    assert_eq!(format_f64(123456789012345.0), "123456789012345");
    assert_eq!(format_f64(1e-5), "1e-05");
    assert_eq!(format_f64(0.0001), "0.0001");
    assert_eq!(format_f32(1e6), "1e+06");
    assert_eq!(format_f32(999999.0), "999999");
}
