use super::*;
use std::collections::BTreeMap;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn key(i: u64) -> Vec<u8> {
    // variable-length keys sharing prefixes
    let mut k = format!("k{:06}", i).into_bytes();
    if i % 3 == 0 {
        k.extend_from_slice(b"-padding-padding");
    }
    k
}

fn tmpdir(name: &str) -> PathBuf {
    let base = std::env::var("IVM_TEST_TMP").unwrap_or_else(|_| std::env::temp_dir().to_string_lossy().into_owned());
    let d = PathBuf::from(base).join(format!("ivm-storage-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn check_equal(env: &mut Env, tree: TreeId, model: &BTreeMap<Vec<u8>, i64>) {
    let all = env.scan(tree, Bound::Unbounded, Bound::Unbounded).unwrap();
    let got: Vec<(Vec<u8>, i64)> =
        all.iter().map(|(k, h)| (k.to_vec(), h.total())).filter(|(_, w)| *w != 0).collect();
    let want: Vec<(Vec<u8>, i64)> = model.iter().filter(|(_, w)| **w != 0).map(|(k, w)| (k.clone(), *w)).collect();
    assert_eq!(got.len(), want.len(), "entry count mismatch");
    assert_eq!(got, want);
    let total: i64 = want.iter().map(|(_, w)| w).sum();
    assert_eq!(env.tree_count(tree), total);
    // validates the incrementally maintained dirty byte count (debug)
    let _ = env.dirty_bytes();
}

#[test]
fn random_ops_against_model() {
    let dir = tmpdir("random");
    let opts = EnvOptions { cache_bytes: 64 << 10, leaf_bytes: 512, internal_bytes: 512, segment_bytes: 64 << 10 };
    let (mut env, rec) = Env::open(&dir, opts.clone()).unwrap();
    assert!(rec.is_none());
    let tree = env.create_tree(Arc::new(CountSummarizer));
    let mut model: BTreeMap<Vec<u8>, i64> = BTreeMap::new();
    let mut rng = Rng(0x9E3779B97F4A7C15);
    let mut gsn = 0;
    for round in 0..300 {
        gsn += 1;
        env.set_frontier(gsn - 1);
        let n = 1 + rng.below(if round % 50 == 0 { 400 } else { 30 });
        let mut ups = Vec::new();
        for _ in 0..n {
            let k = key(rng.below(2000));
            let cur = *model.get(&k).unwrap_or(&0);
            let w = if cur > 0 && rng.below(2) == 0 { -cur } else { 1 + rng.below(2) as i64 };
            *model.entry(k.clone()).or_insert(0) += w;
            ups.push((k, w));
        }
        env.apply(tree, &mut ups, gsn).unwrap();
        model.retain(|_, w| *w != 0);
        if round % 37 == 0 {
            check_equal(&mut env, tree, &model);
            // point lookups
            for _ in 0..20 {
                let k = key(rng.below(2000));
                let got = env.get(tree, &k).unwrap().map(|h| h.total()).unwrap_or(0);
                assert_eq!(got, *model.get(&k).unwrap_or(&0));
            }
            // range summary and nth vs model
            let a = key(rng.below(2000));
            let b = key(rng.below(2000));
            let (lo, hi) = if a < b { (a, b) } else { (b, a) };
            let s = env.range_summary(tree, Bound::Included(&lo), Bound::Excluded(&hi)).unwrap();
            let want: i64 = model.range(lo.clone()..hi.clone()).map(|(_, w)| *w).sum();
            assert_eq!(s.count, want);
            if want > 0 {
                let target = rng.below(want as u64) as i64;
                let (k, _, off) = env.nth(tree, Bound::Included(&lo), Bound::Excluded(&hi), target).unwrap().unwrap();
                // expected: walk the model
                let mut rem = target;
                let mut expect = None;
                for (mk, w) in model.range(lo.clone()..hi.clone()) {
                    if *w > rem {
                        expect = Some((mk.clone(), rem));
                        break;
                    }
                    rem -= w;
                }
                assert_eq!(Some((k.to_vec(), off)), expect);
            }
        }
        if round % 23 == 0 {
            env.checkpoint(gsn, b"app").unwrap();
            env.gc(50).unwrap();
        }
        if round % 97 == 96 {
            env.checkpoint(gsn, format!("app{gsn}").as_bytes()).unwrap();
            drop(env);
            let (e2, rec) = Env::open(&dir, opts.clone()).unwrap();
            env = e2;
            let rec = rec.unwrap();
            assert_eq!(rec.gsn, gsn);
            assert_eq!(rec.app, format!("app{gsn}").into_bytes());
            env.attach_tree(tree, Arc::new(CountSummarizer)).unwrap();
            check_equal(&mut env, tree, &model);
        }
    }
    check_equal(&mut env, tree, &model);
    let st = env.stats();
    assert!(st.mem_bytes < 1 << 20);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn large_batches() {
    let dir = tmpdir("large");
    let (mut env, _) = Env::open(&dir, EnvOptions::default()).unwrap();
    let t = env.create_tree(Arc::new(CountSummarizer));
    let mut model: BTreeMap<Vec<u8>, i64> = BTreeMap::new();
    for (g, range) in [(1u64, 0u64..10_000), (2, 10_000..20_000), (3, 20_000..25_000)] {
        let mut ups: Vec<(Vec<u8>, i64)> = range
            .map(|i| {
                let mut k = (i as u32).to_be_bytes().to_vec();
                k.extend_from_slice(b"some-payload-bytes-to-make-entries-bigger");
                (k, 1)
            })
            .collect();
        for (k, w) in &ups {
            model.insert(k.clone(), *w);
        }
        env.apply(t, &mut ups, g).unwrap();
        check_equal(&mut env, t, &model);
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn huge_keys() {
    let dir = tmpdir("huge");
    let opts = EnvOptions { cache_bytes: 1 << 20, leaf_bytes: 4096, internal_bytes: 4096, segment_bytes: 1 << 20 };
    let (mut env, _) = Env::open(&dir, opts).unwrap();
    let t = env.create_tree(Arc::new(CountSummarizer));
    let mut model: BTreeMap<Vec<u8>, i64> = BTreeMap::new();
    let mut ups = Vec::new();
    for i in 0..200u32 {
        let mut k = i.to_be_bytes().to_vec();
        // every 7th key is 100KB, some share long prefixes
        if i % 7 == 0 {
            k = vec![b'x'; 100_000];
            k.extend_from_slice(&i.to_be_bytes());
        }
        model.insert(k.clone(), 1);
        ups.push((k, 1));
    }
    env.apply(t, &mut ups, 1).unwrap();
    check_equal(&mut env, t, &model);
    env.checkpoint(1, b"").unwrap();
    let mut ups: Vec<(Vec<u8>, i64)> = model.keys().take(50).map(|k| (k.clone(), -1)).collect();
    for (k, _) in &ups {
        model.remove(k);
    }
    env.set_frontier(1);
    env.apply(t, &mut ups, 2).unwrap();
    check_equal(&mut env, t, &model);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn time_travel_reads() {
    let dir = tmpdir("history");
    let (mut env, _) = Env::open(&dir, EnvOptions::default()).unwrap();
    let t = env.create_tree(Arc::new(CountSummarizer));
    env.apply(t, &mut vec![(b"a".to_vec(), 1)], 1).unwrap();
    env.apply(t, &mut vec![(b"a".to_vec(), -1), (b"b".to_vec(), 1)], 2).unwrap();
    let a = env.get(t, b"a").unwrap().unwrap();
    assert_eq!(a.weight(ReadAt::AsOf(1)), 1);
    assert_eq!(a.weight(ReadAt::Before(2)), 1);
    assert_eq!(a.weight(ReadAt::Latest), 0);
    env.set_frontier(2);
    env.apply(t, &mut vec![(b"c".to_vec(), 1)], 3).unwrap();
    // "a" consolidated away once the frontier passed its retraction
    assert!(env.get(t, b"a").unwrap().is_none());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn drop_tree_reclaims_segments() {
    let dir = tmpdir("drop");
    let opts = EnvOptions { cache_bytes: 1 << 20, leaf_bytes: 256, internal_bytes: 256, segment_bytes: 4096 };
    let (mut env, _) = Env::open(&dir, opts).unwrap();
    let t = env.create_tree(Arc::new(CountSummarizer));
    let mut ups: Vec<(Vec<u8>, i64)> = (0..2000).map(|i| (key(i), 1)).collect();
    env.apply(t, &mut ups, 1).unwrap();
    env.checkpoint(1, b"").unwrap();
    let before: u64 = env.stats().segments.iter().map(|s| s.2).sum();
    assert!(before > 0);
    env.drop_tree(t).unwrap();
    env.checkpoint(2, b"").unwrap();
    let after: u64 = env.stats().segments.iter().map(|s| s.2).sum();
    assert_eq!(after, 0);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Entries `ggg:uuuuu:<numeric text or NULL>`; summaries over the numeric.
struct NumSummarizer {
    spec: Vec<AggKind>,
}

fn num_of(key: &[u8]) -> crate::types::Datum {
    let s = std::str::from_utf8(key).unwrap();
    let text = s.splitn(3, ':').nth(2).unwrap();
    if text == "NULL" {
        crate::types::Datum::Null
    } else {
        crate::types::Datum::numeric(pgnumeric::Numeric::parse(text).unwrap())
    }
}

impl Summarizer for NumSummarizer {
    fn spec(&self) -> &[AggKind] {
        &self.spec
    }
    fn add_entry(&self, acc: &mut Summary, key: &[u8], weight: i64) -> Result<()> {
        acc.count += weight;
        let v = num_of(key);
        for (i, k) in self.spec.iter().enumerate() {
            k.add(&mut acc.aggs[i], &v, weight)?;
        }
        Ok(())
    }
    fn update_entry(&self, acc: &mut Summary, key: &[u8], before: i64, after: i64) -> Result<bool> {
        acc.count += after - before;
        let v = num_of(key);
        for (i, k) in self.spec.iter().enumerate() {
            if !k.update(&mut acc.aggs[i], &v, before, after)? {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

#[test]
fn incremental_summaries_match_recomputation() {
    let opts = EnvOptions { cache_bytes: 1 << 20, leaf_bytes: 300, internal_bytes: 400, segment_bytes: 64 << 10 };
    summaries_case("summaries-small", opts, 0);
    // larger leaves (in-place edits) with checkpoints (clean nodes merge)
    let opts = EnvOptions { cache_bytes: 1 << 20, leaf_bytes: 4096, internal_bytes: 600, segment_bytes: 1 << 20 };
    summaries_case("summaries-large", opts, 25);
}

fn summaries_case(name: &str, opts: EnvOptions, checkpoint_every: u64) {
    let dir = tmpdir(name);
    let (mut env, _) = Env::open(&dir, opts).unwrap();
    let spec = vec![AggKind::SumNumeric, AggKind::Min, AggKind::Max, AggKind::CountNonNull, AggKind::SumSqNumeric];
    let summ = Arc::new(NumSummarizer { spec: spec.clone() });
    let tree = env.create_tree(summ.clone());
    // equal values in several representations, specials and NULLs
    let values = [
        "1", "1.0", "1.00", "2.5", "2.50", "-3.125", "-3.1250", "0", "0.000", "7", "1000000000000.0001", "NaN",
        "Infinity", "-Infinity", "NULL", "-1", "-1.0",
    ];
    let mut model: BTreeMap<Vec<u8>, i64> = BTreeMap::new();
    let mut rng = Rng(0xD1B54A32D192ED03);
    for gsn in 1..=400u64 {
        env.set_frontier(gsn - 1);
        let mut ups = Vec::new();
        let n = 1 + rng.below(if gsn % 40 == 0 { 120 } else { 12 });
        for _ in 0..n {
            let k = if !model.is_empty() && rng.below(3) == 0 {
                // touch an existing entry (often removing it)
                let i = rng.below(model.len() as u64) as usize;
                model.keys().nth(i).unwrap().clone()
            } else {
                // few values: skip the specials most of the time
                let mut v = values[rng.below(values.len() as u64) as usize];
                if matches!(v, "NaN" | "Infinity" | "-Infinity") && rng.below(4) != 0 {
                    v = "1.0";
                }
                format!("{:03}:{:05}:{}", rng.below(4), rng.below(400), v).into_bytes()
            };
            let cur = *model.get(&k).unwrap_or(&0);
            let w = if cur > 0 && rng.below(2) == 0 { -cur } else { 1 + rng.below(3) as i64 };
            *model.entry(k.clone()).or_insert(0) += w;
            ups.push((k, w));
        }
        env.apply(tree, &mut ups, gsn).unwrap();
        model.retain(|_, w| *w != 0);
        env.check_summaries(tree).unwrap();
        let _ = env.dirty_bytes();
        if checkpoint_every > 0 && gsn % checkpoint_every == 0 {
            env.checkpoint(gsn, b"").unwrap();
        }
        // the whole tree and random ranges against a fold over the model
        let fold = |lo: &[u8], hi: &[u8]| {
            let mut s = Summary::empty(&spec);
            for (k, w) in model.range(lo.to_vec()..hi.to_vec()) {
                summ.add_entry(&mut s, k, *w).unwrap();
            }
            s
        };
        assert!(env.tree_summary(tree).exact_eq(&fold(b"", b"\xff")), "tree summary at gsn {gsn}");
        let a = format!("{:03}:{:05}", rng.below(4), rng.below(400)).into_bytes();
        let b = format!("{:03}:{:05}", rng.below(4), rng.below(400)).into_bytes();
        let (lo, hi) = if a < b { (a, b) } else { (b, a) };
        let got = env.range_summary(tree, Bound::Included(&lo), Bound::Excluded(&hi)).unwrap();
        assert!(got.exact_eq(&fold(&lo, &hi)), "range summary at gsn {gsn}: {got:?} vs {:?}", fold(&lo, &hi));
    }
}

/// After a checkpoint (nothing dirty): the live bytes recorded per segment
/// equal the bytes of the nodes reachable from the trees, and every segment
/// holding a reachable node exists.
fn check_live_accounting(env: &mut Env) {
    fn walk(env: &mut Env, ptr: NodePtr, live: &mut BTreeMap<u32, u64>) {
        let (loc, children) = match ptr {
            NodePtr::Disk(loc) => {
                let node = Node::decode(&env.segs.read(loc).unwrap()).unwrap();
                let ch = match node {
                    Node::Internal(ch) => ch.iter().map(|c| c.ptr).collect(),
                    Node::Leaf(_) => vec![],
                };
                (loc, ch)
            }
            NodePtr::Mem(id) => {
                let slot = env.slot(id);
                assert!(!slot.dirty, "dirty node after a checkpoint");
                let ch = match slot.node.as_ref().unwrap() {
                    Node::Internal(ch) => ch.iter().map(|c| c.ptr).collect(),
                    Node::Leaf(_) => vec![],
                };
                (slot.disk.unwrap(), ch)
            }
        };
        *live.entry(loc.seg).or_default() += loc.len as u64;
        for c in children {
            walk(env, c, live);
        }
    }
    let mut live: BTreeMap<u32, u64> = BTreeMap::new();
    let roots: Vec<NodePtr> = env.trees.values().filter_map(|t| t.root).collect();
    for r in roots {
        walk(env, r, &mut live);
    }
    let recorded: BTreeMap<u32, u64> = env.seg_live.iter().filter(|(_, (_, l))| *l > 0).map(|(id, (_, l))| (*id, *l)).collect();
    assert_eq!(live, recorded, "live bytes per segment");
    let files = env.segs.list().unwrap();
    for seg in live.keys() {
        assert!(files.contains(seg), "segment {seg} holds live nodes but is gone");
    }
}

/// Full-history model of one tree.
#[derive(Clone, Default)]
struct HistModel(BTreeMap<Vec<u8>, Vec<(Gsn, i64)>>);

impl HistModel {
    fn weight_at(&self, k: &[u8], t: Gsn) -> i64 {
        self.0.get(k).map(|h| h.iter().filter(|(g, _)| *g <= t).map(|(_, w)| w).sum()).unwrap_or(0)
    }
    fn total(&self, k: &[u8]) -> i64 {
        self.weight_at(k, Gsn::MAX)
    }
    fn truncate_after(&mut self, g: Gsn) {
        for h in self.0.values_mut() {
            h.retain(|(x, _)| *x <= g);
        }
        self.0.retain(|_, h| !h.is_empty());
    }
}

/// Random updates with a moving retention frontier, AS OF reads at every
/// retained time, checkpoints, crashes (reopen without a checkpoint), GC
/// and a cache small enough to evict constantly: reads must equal the
/// model, summaries their recomputation.
#[test]
fn time_travel_crashes_and_gc_against_model() {
    for seed in [0x9E3779B97F4A7C15u64, 0xD1B54A32D192ED03, 0x2545F4914F6CDD1D] {
        let dir = tmpdir(&format!("tt-{seed:x}"));
        let opts = EnvOptions { cache_bytes: 16 << 10, leaf_bytes: 400, internal_bytes: 300, segment_bytes: 16 << 10 };
        let spec = vec![AggKind::SumNumeric, AggKind::Min, AggKind::Max, AggKind::CountNonNull];
        let summ: Arc<dyn Summarizer> = Arc::new(NumSummarizer { spec: spec.clone() });
        let (mut env, _) = Env::open(&dir, opts.clone()).unwrap();
        let tree = env.create_tree(summ.clone());
        let retention = 7;
        let mut rng = Rng(seed);
        let mut model = HistModel::default();
        let mut ckpt: Option<(Gsn, HistModel)> = None;
        let mut gsn: Gsn = 0;
        let values = ["1", "1.0", "2.50", "-3", "0", "7.25", "NULL", "10"];
        for step in 0..1500 {
            gsn += 1;
            env.set_frontier(gsn.saturating_sub(retention));
            let n = 1 + rng.below(if step % 100 == 0 { 300 } else { 10 });
            let mut ups = Vec::new();
            for _ in 0..n {
                let k = format!("{:03}:{:05}:{}", rng.below(5), rng.below(300), values[rng.below(8) as usize]).into_bytes();
                let cur = model.total(&k);
                let w = if cur > 0 && rng.below(2) == 0 { -cur } else { 1 + rng.below(2) as i64 };
                ups.push((k.clone(), w));
                let h = model.0.entry(k).or_default();
                match h.last_mut() {
                    Some((g, lw)) if *g == gsn => *lw += w,
                    _ => h.push((gsn, w)),
                }
            }
            env.apply(tree, &mut ups, gsn).unwrap();
            let frontier = env.frontier();
            // point reads at random retained times
            for _ in 0..5 {
                let t = frontier + rng.below(gsn - frontier + 1);
                let k = format!("{:03}:{:05}:{}", rng.below(5), rng.below(300), values[rng.below(8) as usize]).into_bytes();
                let got = env.get(tree, &k).unwrap().map(|h| h.weight(ReadAt::AsOf(t))).unwrap_or(0);
                assert_eq!(got, model.weight_at(&k, t), "seed {seed:x} step {step}: {k:?} as of {t}");
            }
            if step % 25 == 0 {
                // full scan as of a retained time
                let t = frontier + rng.below(gsn - frontier + 1);
                let got: Vec<(Vec<u8>, i64)> = env
                    .scan(tree, Bound::Unbounded, Bound::Unbounded)
                    .unwrap()
                    .into_iter()
                    .map(|(k, h)| (k.to_vec(), h.weight(ReadAt::AsOf(t))))
                    .filter(|(_, w)| *w != 0)
                    .collect();
                let want: Vec<(Vec<u8>, i64)> =
                    model.0.keys().map(|k| (k.clone(), model.weight_at(k, t))).filter(|(_, w)| *w != 0).collect();
                assert_eq!(got, want, "seed {seed:x} step {step}: scan as of {t}");
                env.check_summaries(tree).unwrap();
                let mut s = Summary::empty(&spec);
                for k in model.0.keys() {
                    summ.add_entry(&mut s, k, model.total(k)).unwrap();
                }
                assert!(env.tree_summary(tree).exact_eq(&s), "seed {seed:x} step {step}: tree summary");
            }
            match rng.below(40) {
                0..=2 => {
                    env.checkpoint(gsn, format!("{gsn}").as_bytes()).unwrap();
                    check_live_accounting(&mut env);
                    ckpt = Some((gsn, model.clone()));
                }
                3 => {
                    env.gc(1 + rng.below(40) as usize).unwrap();
                }
                4 => {
                    // crash: everything after the last checkpoint is lost
                    drop(env);
                    let (e, rec) = Env::open(&dir, opts.clone()).unwrap();
                    env = e;
                    match &ckpt {
                        Some((g, m)) => {
                            let rec = rec.expect("recovered checkpoint");
                            assert_eq!(rec.gsn, *g);
                            assert_eq!(rec.app, format!("{g}").into_bytes());
                            env.attach_tree(tree, summ.clone()).unwrap();
                            gsn = *g;
                            model = m.clone();
                        }
                        None => {
                            assert!(rec.is_none());
                            let t = env.create_tree(summ.clone());
                            assert_eq!(t, tree);
                            gsn = 0;
                            model = HistModel::default();
                        }
                    }
                    env.set_frontier(gsn.saturating_sub(retention));
                    model.truncate_after(gsn);
                }
                _ => {}
            }
        }
        // garbage collection keeps the store bounded
        // garbage collection (one sparse segment per round; relocation
        // rewrites paths to the root, so it never quite settles) keeps the
        // store within a small factor of its live data
        for _ in 0..300 {
            env.gc(10_000).unwrap();
            gsn += 1;
            env.checkpoint(gsn, b"").unwrap();
            check_live_accounting(&mut env);
        }
        let st = env.stats();
        let live: u64 = st.segments.iter().map(|s| s.2).sum();
        let total: u64 = st.segments.iter().map(|s| s.1).sum();
        assert!(total <= 4 * live + 4 * opts.segment_bytes, "{st:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}

