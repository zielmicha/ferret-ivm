use super::*;
use proptest::prelude::*;

#[test]
fn float_sum_exact() {
    let mut s = FloatSum::default();
    s.add(1e100, 1);
    s.add(1.0, 1);
    s.add(-1e100, 1);
    assert_eq!(s.value(), 1.0);
    let mut s = FloatSum::default();
    for _ in 0..10 {
        s.add(0.1, 1);
    }
    assert_eq!(s.value(), 1.0); // exact sum of ten 0.1s rounds to 1.0
    s.add(0.1, -10);
    assert_eq!(s.value(), 0.0);
    let mut s = FloatSum::default();
    s.add(5e-324, 3);
    assert_eq!(s.value(), 3.0 * 5e-324);
    assert_eq!(s.div_value(3), 5e-324);
}

/// Averages and standard deviations whose sums (of squares) overflow a
/// double are computed from the exact sums: they overflow only if the
/// result does.
#[test]
fn float_aggregates_beyond_the_range_of_their_sums() {
    let of = |xs: &[f64]| {
        let (mut s, mut q) = (FloatSum::default(), FloatSum::default());
        for x in xs {
            s.add(*x, 1);
            q.add_square(*x, 1);
        }
        (s, q)
    };
    let (s, _) = of(&[1e308, 1e308]);
    assert_eq!(s.value(), f64::INFINITY);
    assert_eq!(s.div_value(2), 1e308);
    let (s, _) = of(&[f64::MAX, f64::MAX, f64::MAX]);
    assert_eq!(s.div_value(3), f64::MAX);
    let (s, _) = of(&[-f64::MAX, -f64::MAX, f64::MAX]);
    assert_eq!(s.div_value(-3), f64::MAX / 3.0);
    // the sum of squares (1e600) overflows, the variance may too
    let (s, q) = of(&[1e300, -1e300]);
    assert_eq!(s.variance(&q, 2, false), f64::INFINITY);
    assert_eq!(s.std_dev(&q, 2, false), 1e300);
    let (s, q) = of(&[1e300, -1e300, 0.0]);
    assert_eq!(s.variance(&q, 3, true), f64::INFINITY);
    assert_eq!(s.std_dev(&q, 3, true), 1e300);
    let (s, q) = of(&[f64::MAX, -f64::MAX]);
    assert_eq!(s.std_dev(&q, 2, false), f64::MAX);
    assert_eq!(s.std_dev(&q, 2, true), f64::INFINITY);
    // in range, the square root of the rounded variance (as before)
    let (s, q) = of(&[1.0, 2.0, 4.0]);
    assert_eq!(s.std_dev(&q, 3, true), s.variance(&q, 3, true).sqrt());
    // squares below 2^-1074 count as 0
    let (s, q) = of(&[5e-324, 0.0]);
    assert_eq!((s.div_value(2), s.variance(&q, 2, true), s.std_dev(&q, 2, true)), (0.0, 0.0, 0.0));
}

/// The float sum as it was before the dyadic representation: the finite
/// part as an integer multiple of 2^-1074. Results must stay bit-identical
/// (maintained views hold values computed with it, which later changes
/// retract by recomputing them).
#[derive(Clone, Debug, Default)]
struct FixedSum {
    fixed: BigInt,
    nan: i64,
    pos_inf: i64,
    neg_inf: i64,
}

impl FixedSum {
    fn add(&mut self, x: f64, w: i64) {
        if x.is_nan() {
            self.nan += w;
        } else if x == f64::INFINITY {
            self.pos_inf += w;
        } else if x == f64::NEG_INFINITY {
            self.neg_inf += w;
        } else if x != 0.0 {
            let (m, e) = decompose(x);
            self.fixed += (BigInt::from(m) << ((e + 1074) as usize)) * w;
        }
    }

    fn add_square(&mut self, x: f64, w: i64) {
        if !x.is_finite() {
            self.add(x * x, w);
            return;
        }
        if x == 0.0 {
            return;
        }
        let (m, e) = decompose(x);
        let m2 = BigInt::from(m) * BigInt::from(m);
        let exp = 2 * e + 1074;
        let v = if exp >= 0 { m2 << (exp as usize) } else { m2 >> ((-exp) as usize) };
        self.fixed += v * w;
    }

    fn value(&self) -> f64 {
        if self.nan > 0 || (self.pos_inf > 0 && self.neg_inf > 0) {
            return f64::NAN;
        }
        if self.pos_inf > 0 {
            return f64::INFINITY;
        }
        if self.neg_inf > 0 {
            return f64::NEG_INFINITY;
        }
        bigint_scaled_to_f64(&self.fixed, -1074)
    }

    /// (Corrected since: the average of finite values divides their exact
    /// sum also where it overflows a double, which gave an infinity.)
    fn div_value(&self, n: i64) -> f64 {
        if self.nan > 0 || self.pos_inf > 0 || self.neg_inf > 0 || n == 0 {
            return self.value() / n as f64;
        }
        let scaled: BigInt = &self.fixed << 128usize;
        bigint_scaled_to_f64(&(scaled / BigInt::from(n)), -1074 - 128)
    }

    fn variance(&self, q: &FixedSum, n: i64, sample: bool) -> f64 {
        let nb = BigInt::from(n);
        let num: BigInt = &nb * &q.fixed * (BigInt::from(1) << 1074usize) - &self.fixed * &self.fixed;
        if num.is_negative() || num == BigInt::from(0) {
            return 0.0;
        }
        let denom = if sample { &nb * BigInt::from(n - 1) } else { &nb * &nb };
        bigint_scaled_to_f64(&((num << 128usize) / denom), -2148 - 128)
    }

    /// Agg state tag 5.
    fn encode(&self, out: &mut Vec<u8>) {
        out.push(5);
        let (sign, bytes) = self.fixed.to_bytes_le();
        out.push(match sign {
            Sign::Minus => 2,
            Sign::NoSign => 0,
            Sign::Plus => 1,
        });
        codec::put_bytes(out, &bytes);
        put_ivarint(out, self.nan);
        put_ivarint(out, self.pos_inf);
        put_ivarint(out, self.neg_inf);
    }
}

fn same(a: f64, b: f64) -> bool {
    a.to_bits() == b.to_bits()
}

fn encode_state(s: &FloatSum) -> Vec<u8> {
    let mut out = Vec::new();
    AggState::SumFloat(Box::new(s.clone())).encode(&mut out);
    out
}

fn decode_state(mut buf: &[u8]) -> FloatSum {
    let st = AggState::decode(&mut buf).unwrap();
    assert!(buf.is_empty(), "trailing bytes");
    match st {
        AggState::SumFloat(f) => *f,
        s => panic!("not a float sum: {s:?}"),
    }
}

/// Doubles of every kind: edge values, any bit pattern, and ordinary data.
fn arb_f64() -> impl Strategy<Value = f64> {
    prop_oneof![
        3 => prop::sample::select(vec![
            0.0, -0.0, 1.0, -1.0, 0.1, -0.1, 0.5, 3.0, 1e308, -1e308, f64::MAX, -f64::MAX, f64::MIN_POSITIVE,
            -f64::MIN_POSITIVE, 5e-324, -5e-324, 1e-310, f64::NAN, f64::INFINITY, f64::NEG_INFINITY,
            f64::from_bits(0x000F_FFFF_FFFF_FFFF), f64::from_bits(0x0010_0000_0000_0001), 2f64.powi(-537),
            2f64.powi(-538), 1.5 * 2f64.powi(-540), 2f64.powi(1023), 9007199254740993.0, 1e16, 1e-16,
        ]),
        3 => any::<u64>().prop_map(f64::from_bits),
        3 => (-1_000_000i64..1_000_000, 0usize..6).prop_map(|(m, d)| m as f64 / 10f64.powi(d as i32)),
        1 => (any::<u64>(), -1100i32..-1000).prop_map(|(m, e)| (m >> 11) as f64 * 2f64.powi(e)),
    ]
}

fn arb_weight() -> impl Strategy<Value = i64> {
    prop_oneof![
        8 => prop::sample::select(vec![1i64, 1, 1, 2, -1, 3, -2]),
        1 => any::<i64>().prop_map(|w| w / 4),
    ]
}

fn divisors(count: i64) -> Vec<i64> {
    vec![1, 2, 3, 7, -3, 10, 1 << 20, (1 << 40) + 1, i64::MAX, i64::MIN, count]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(crate::testkit::proptest_cases(2048)))]

    /// Sums, averages and variances are bit-identical to the fixed-point
    /// representation, whatever the values and weights.
    #[test]
    fn float_sums_match_fixed_point(terms in prop::collection::vec((arb_f64(), arb_weight()), 0..24)) {
        let (mut s, mut q, mut fs, mut fq) = (FloatSum::default(), FloatSum::default(), FixedSum::default(), FixedSum::default());
        let mut count = 0i64;
        for (x, w) in &terms {
            s.add(*x, *w);
            q.add_square(*x, *w);
            fs.add(*x, *w);
            fq.add_square(*x, *w);
            count = count.wrapping_add(*w);
            prop_assert!(same(s.value(), fs.value()), "sum {:?} vs {:?}", s.value(), fs.value());
            prop_assert!(same(q.value(), fq.value()), "sum of squares");
        }
        for n in divisors(count) {
            prop_assert!(same(s.div_value(n), fs.div_value(n)), "avg / {n}: {:?} vs {:?}", s.div_value(n), fs.div_value(n));
            prop_assert!(same(q.div_value(n), fq.div_value(n)), "avg of squares / {n}");
        }
        let specials = s.nan > 0 || s.pos_inf > 0 || s.neg_inf > 0;
        for n in [count, 1, 2, 5, 1 << 40] {
            if n > 0 && !specials {
                for sample in [false, true] {
                    if !(sample && n == 1) {
                        prop_assert!(same(s.variance(&q, n, sample), fs.variance(&fq, n, sample)), "variance n={n} sample={sample}");
                    }
                }
            }
        }
        // the earlier encoding decodes to the same state
        for (new, old) in [(&s, &fs), (&q, &fq)] {
            let mut enc = Vec::new();
            old.encode(&mut enc);
            prop_assert_eq!(&decode_state(&enc), new);
        }
    }

    /// The state is a function of the multiset of terms: order, grouping and
    /// removed terms leave no trace, and the encoding is unique.
    #[test]
    fn float_sums_are_canonical(
        terms in prop::collection::vec((arb_f64(), arb_weight()), 0..24),
        removed in prop::collection::vec((arb_f64(), arb_weight()), 0..6),
        split in any::<prop::sample::Index>(),
        seed in any::<u64>(),
    ) {
        let mut forward = FloatSum::default();
        for (x, w) in &terms {
            forward.add(*x, *w);
            forward.add_square(*x, *w);
        }
        // shuffled, with extra terms added and removed again
        let mut shuffled = terms.clone();
        let mut rng = crate::testkit::Rng::new(seed);
        for i in (1..shuffled.len()).rev() {
            shuffled.swap(i, rng.index(i + 1));
        }
        let mut other = FloatSum::default();
        for (x, w) in &removed {
            other.add(*x, *w);
            other.add_square(*x, *w);
        }
        for (x, w) in &shuffled {
            other.add_square(*x, *w);
            other.add(*x, *w);
        }
        for (x, w) in &removed {
            other.add(*x, -*w);
            other.add_square(*x, -*w);
        }
        prop_assert_eq!(&other, &forward);
        // grouped into two partial sums, combined, and one subtracted again
        let k = split.index(terms.len() + 1);
        let (mut a, mut b) = (FloatSum::default(), FloatSum::default());
        for (i, (x, w)) in terms.iter().enumerate() {
            let p = if i < k { &mut a } else { &mut b };
            p.add(*x, *w);
            p.add_square(*x, *w);
        }
        let mut ab = a.clone();
        ab.combine(&b);
        prop_assert_eq!(&ab, &forward);
        ab.subtract(&a);
        prop_assert_eq!(&ab, &b);
        // one encoding, which round-trips and whose length is predicted
        let enc = encode_state(&forward);
        prop_assert_eq!(&enc, &encode_state(&other));
        prop_assert_eq!(&decode_state(&enc), &forward);
        prop_assert_eq!(AggState::SumFloat(Box::new(forward.clone())).encoded_len(), enc.len());
    }

    /// Limb and big-integer arithmetic agree.
    #[test]
    fn dyadic_add_matches_big_integers(terms in prop::collection::vec((any::<i128>(), -1074i32..1200), 0..12)) {
        let mut d = Dyadic::default();
        let mut fixed = BigInt::zero();
        for (m, e) in &terms {
            let m = m >> (m.trailing_zeros().min(127) / 2); // vary the low bits
            d.add_mag(m < 0, limbs_of_product(m.unsigned_abs(), 1), *e);
            fixed += BigInt::from(m) << (*e + 1074) as u32;
            prop_assert_eq!(&d, &Dyadic::from_bigint(&fixed, -1074));
            prop_assert_eq!(d.to_fixed(), fixed.clone());
            prop_assert!(same(d.to_f64(), bigint_scaled_to_f64(&fixed, -1074)));
        }
    }
}

/// Averages of tiny sums, where the quotient is subnormal and the
/// computation on the 2^-1074 grid rounds twice, and of sums around the
/// size where the inline division takes over.
#[test]
fn float_averages_at_the_edges() {
    let mut rng = crate::testkit::Rng::new(7);
    for i in 0..20_000u64 {
        let bits = 1 + rng.below(128) as u32;
        let mut m = (rng.next() as u128) << 64 | rng.next() as u128;
        m >>= 128 - bits;
        let exp = -1074 + rng.below(if i % 2 == 0 { 4 } else { 40 }) as i32;
        let neg = rng.chance(30);
        let fixed = BigInt::from_biguint(if neg { Sign::Minus } else { Sign::Plus }, BigUint::from(m)) << (exp + 1074) as u32;
        let d = Dyadic::from_bigint(&fixed, -1074);
        let s = FloatSum { finite: d, ..Default::default() };
        let r = FixedSum { fixed, ..Default::default() };
        let n = match rng.below(4) {
            0 => 1 + rng.below(16) as i64,
            1 => rng.next() as i64,
            2 => (1i64 << rng.below(63)) + rng.below(3) as i64 - 1,
            _ => 3,
        };
        if n != 0 {
            assert!(same(s.div_value(n), r.div_value(n)), "m={m} exp={exp} neg={neg} n={n}: {:e} vs {:e}", s.div_value(n), r.div_value(n));
        }
    }
}

/// The examples of docs/on-disk-format.md.
#[test]
fn float_sum_encoding_examples() {
    let hex = |s: &FloatSum| encode_state(s).iter().map(|b| format!("{b:02x}")).collect::<Vec<_>>().join(" ");
    let mut s = FloatSum::default();
    for i in 1..=10 {
        s.add(i as f64, 1);
    }
    assert_eq!(hex(&s), "08 00 02 37 00 00 00");
    let mut s = FloatSum::default();
    s.add(0.1, 1);
    s.add(0.2, 1);
    assert_eq!(hex(&s), "08 6d 0e 67 66 66 66 66 66 26 00 00 00");
    s.add(0.1, -1);
    s.add(0.2, -1);
    s.add(f64::NAN, 2);
    assert_eq!(hex(&s), "08 00 00 04 00 00");
}

/// Node size estimates count the summaries as encoded.
#[test]
fn summary_encoded_len_is_exact() {
    use AggKind::*;
    let num = |s: &str| Datum::numeric(Numeric::parse(s).unwrap());
    let interval = |d| Datum::Interval(pgdatetime::Interval { months: 1, days: d, micros: 3 });
    let cases: Vec<(AggKind, Vec<Datum>)> = vec![
        (CountNonNull, vec![Datum::Int8(1), Datum::Int8(2)]),
        (SumInt, vec![Datum::Int8(-5), Datum::Int8(1 << 40)]),
        (SumNumeric, vec![num("123.4500"), num("-1e300"), num("NaN")]),
        (SumFloat, vec![Datum::Float8(0.1), Datum::Float8(-1e300), Datum::Float8(f64::INFINITY)]),
        (Min, vec![Datum::text_owned("x".repeat(100)), Datum::text_owned("w".repeat(200))]),
        (Max, vec![interval(2), interval(-7)]),
        (Min, vec![num("2.50"), num("-0.001")]),
        (Max, vec![Datum::Float8(-0.0), Datum::Float8(f64::NAN)]),
    ];
    for (k, vals) in cases {
        let mut s = Summary::empty(&[k]);
        for (n, v) in vals.iter().enumerate() {
            k.add(&mut s.aggs[0], v, 3 * n as i64 + 1).unwrap();
            s.count += 1 << (20 * n);
            let mut enc = Vec::new();
            s.encode(&mut enc);
            assert_eq!(s.encoded_len(), enc.len(), "{s:?}");
        }
    }
}

/// Typical sums stay inline and small.
#[test]
fn float_sum_states_are_small() {
    let mut s = FloatSum::default();
    let mut sq = FloatSum::default();
    for i in 0..100_000 {
        let x = i as f64 * 1.5 + 0.25;
        s.add(x, 1);
        sq.add_square(x, 1);
    }
    s.add(123.456, 1);
    sq.add_square(123.456, 1);
    assert!(!s.finite.mag.spilled() && !sq.finite.mag.spilled());
    // the fixed-point encoding of either took about 140 bytes; squares of
    // 53-bit mantissas have 106 bits
    assert!(encode_state(&s).len() <= 20, "{} bytes", encode_state(&s).len());
    assert!(encode_state(&sq).len() <= 32, "{} bytes", encode_state(&sq).len());
}

// ---------------------------------------------------------------------------
// MIN/MAX prefixes

fn text(s: String) -> Datum {
    Datum::text_owned(s)
}

/// Strings around the prefix length, many sharing longer prefixes.
fn arb_text() -> impl Strategy<Value = Datum> {
    let stem = prop::sample::select(vec!["", "a", "b", "ab"]);
    (stem, 0usize..3, 100usize..400, prop::sample::select(vec!["", "a", "b", "z", "aa"]))
        .prop_map(|(stem, rep, len, tail)| text(format!("{stem}{}{tail}", ["x", "y", "\u{e9}"][rep].repeat(len / [1, 1, 2][rep]))))
}

/// A wide numeric in several representations (display scales).
fn wide_numeric(digits: usize, last: u8, scale_extra: usize) -> Datum {
    let s = format!("1.{}{}{}", "0".repeat(digits), last, "0".repeat(scale_extra));
    Datum::numeric(Numeric::parse(&s).unwrap())
}

fn bounded(v: &Datum, less: bool) -> AggState {
    let mut s = Summary { count: 1, aggs: vec![extreme_state(less, Some(Ext::Val(v)))] };
    s.bound_extremes();
    s.aggs.pop().unwrap()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(crate::testkit::proptest_cases(512)))]

    /// Prefixes order like their values, except for ties between equal
    /// prefixes; values that fit are kept and never tie with a prefix.
    #[test]
    fn prefixes_order_like_values(a in arb_text(), b in arb_text()) {
        for less in [true, false] {
            let (pa, pb) = (bounded(&a, less), bounded(&b, less));
            let o = ext_cmp(pa.ext().unwrap(), pb.ext().unwrap());
            let sql = a.sql_cmp(&b);
            match (pa.is_prefix(), pb.is_prefix()) {
                (false, false) => prop_assert_eq!(o, sql),
                (true, true) => prop_assert!(o == sql || o == Ordering::Equal),
                _ => prop_assert_eq!(o, sql),
            }
            if pb.is_prefix() {
                // a value compares with a prefix through its own prefix
                prop_assert_eq!(ext_cmp(Ext::Val(&a), pb.ext().unwrap()), o);
            }
            if let AggState::MinPrefix(p) | AggState::MaxPrefix(p) = &pa {
                prop_assert_eq!(p.prefix.len(), EXTREME_PREFIX);
            }
        }
    }
}

proptest! {
    /// The prefix test on strings matches their key encoding.
    #[test]
    fn key_prefix_tests_match_encoding(a in prop::collection::vec(prop::sample::select(vec![0u8, 1, b'a', 0xFF]), 0..12), b in prop::collection::vec(prop::sample::select(vec![0u8, 1, b'a', 0xFF]), 0..12), n in 0usize..20) {
        let (va, vb) = (Datum::bytea(a), Datum::bytea(b));
        let full = |v: &Datum| {
            let mut k = Vec::new();
            encode_key(v, SortSpec::ASC, &mut k);
            k
        };
        let p = key_prefix(&vb, n);
        prop_assert_eq!(&p[..], &full(&vb)[..n.min(full(&vb).len())]);
        prop_assert_eq!(key_starts_with(&va, &p), full(&va).starts_with(&p));
    }
}

/// A node's MIN/MAX maintained through child replacements and entry updates
/// equals one recomputed from its entries, for wide values with long common
/// prefixes and equal values in different representations.
#[test]
fn bounded_extremes_maintained_exactly() {
    let values: Vec<Datum> = (0..6)
        .map(|i| text(format!("{}{}", "p".repeat(200), i % 3)))
        .chain((0..4).map(|i| text(format!("{}{i}", "q".repeat(EXTREME_PREFIX - 4)))))
        .chain([text("a".into()), text("z".into())])
        .chain((0..4).map(|i| wide_numeric(300, 1 + (i % 2) as u8, i / 2)))
        .collect();
    let numeric = |v: &Datum| matches!(v, Datum::Numeric(_));
    assert!(values.iter().filter(|v| numeric(v)).all(|v| bounded(v, true).is_prefix()));
    let mut rng = crate::testkit::Rng::new(3);
    for kind in [AggKind::Min, AggKind::Max] {
        for family in [false, true] {
            // children: a few entries each, (value, weight)
            let pool: Vec<&Datum> = values.iter().filter(|v| numeric(v) == family).collect();
            let mut children: Vec<Vec<(Datum, i64)>> = vec![vec![]; 4];
            let summary_of = |entries: &[(Datum, i64)]| {
                let mut s = Summary::empty(&[kind]);
                for (v, w) in entries {
                    s.count += w;
                    kind.add(&mut s.aggs[0], v, *w).unwrap();
                    s.bound_extremes();
                }
                s
            };
            let node_of = |children: &[Vec<(Datum, i64)>]| summary_of(&children.concat());
            let mut node = node_of(&children);
            let mut stored: Vec<Summary> = children.iter().map(|c| summary_of(c)).collect();
            for _ in 0..3000 {
                let c = rng.index(children.len());
                let old_child = stored[c].clone();
                // update one entry of the child incrementally
                let mut new_child = old_child.clone();
                let (v, before, after) = if !children[c].is_empty() && rng.chance(50) {
                    let i = rng.index(children[c].len());
                    let (v, w) = children[c].remove(i);
                    (v, w, 0)
                } else {
                    let v = pool[rng.index(pool.len())].clone();
                    let pos = rng.index(children[c].len() + 1);
                    children[c].insert(pos, (v.clone(), 1));
                    (v, 0, 1)
                };
                new_child.count += after - before;
                let ok = kind.update(&mut new_child.aggs[0], &v, before, after).unwrap();
                new_child.bound_extremes();
                let recomputed = summary_of(&children[c]);
                if ok {
                    assert!(new_child.exact_eq(&recomputed), "{kind:?} entry update: {new_child:?} vs {recomputed:?}");
                }
                stored[c] = recomputed;
                if !node.replace_child(&old_child, &[&stored[c]]) {
                    node = Summary::empty(&[kind]);
                    for s in &stored {
                        node.combine(s);
                    }
                }
                let want = node_of(&children);
                assert!(node.exact_eq(&want), "{kind:?} node: {node:?} vs {want:?}");
            }
        }
    }
}

/// Values too wide for node summaries, written before prefixes existed,
/// decode as their prefixes.
#[test]
fn wide_extremes_of_the_earlier_encoding_decode_as_prefixes() {
    let wide = text("w".repeat(1000));
    for (tag, less) in [(6u8, true), (7, false)] {
        let mut buf = vec![tag];
        codec::encode_datum(&wide, &mut buf);
        let st = AggState::decode(&mut buf.as_slice()).unwrap();
        // (whether other values share the prefix is not known)
        let (AggState::MinPrefix(w) | AggState::MaxPrefix(w)) = &st else { panic!("{st:?}") };
        assert!(w.same.is_none() && extreme_kind(&st) == Some(less));
        let (AggState::MinPrefix(b) | AggState::MaxPrefix(b)) = bounded(&wide, less) else { unreachable!() };
        assert_eq!(w.prefix, b.prefix);
        let mut enc = Vec::new();
        st.encode(&mut enc);
        assert_eq!(enc.len(), st.encoded_len());
        assert_eq!(AggState::decode(&mut enc.as_slice()).unwrap(), st);
    }
    let narrow = text("n".repeat(EXTREME_PREFIX - 3));
    assert_eq!(bounded(&narrow, true), AggState::Min(Some(narrow.clone())));
    assert!(bounded(&text("n".repeat(EXTREME_PREFIX - 2)), true).is_prefix());
}
