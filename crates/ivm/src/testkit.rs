//! Helpers for randomized tests: a small seedable PRNG, seed selection and
//! scratch directories. Pure std, so the engine tests and ivm-difftest
//! include this file by path as well.
//!
//! Every randomized test runs a fixed list of seeds by default, so plain
//! `cargo test` runs are reproducible. `IVM_TEST_SEED=N` runs only seed N,
//! and a failing run prints its seed. The long variants (`#[ignore]`d tests)
//! run `IVM_TEST_ITERATIONS` (default 20) fresh seeds. Scratch directories go
//! to `IVM_TEST_TMP` (default: the system temp directory) and are kept when
//! a test fails.

#![allow(dead_code)]

use std::path::{Path, PathBuf};

/// SplitMix64: tiny and fast, and every seed (0 included) gives a good stream.
#[derive(Clone, Debug)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Rng {
        Rng(seed)
    }

    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `0..n` (0 when `n` is 0).
    pub fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next() % n }
    }

    /// Uniform index into a collection of `len` elements.
    pub fn index(&mut self, len: usize) -> usize {
        self.below(len as u64) as usize
    }

    /// True with probability `pct` percent.
    pub fn chance(&mut self, pct: u64) -> bool {
        self.below(100) < pct
    }

    /// Uniform in `lo..=hi`.
    pub fn range(&mut self, lo: i64, hi: i64) -> i64 {
        let span = (hi as i128 - lo as i128 + 1) as u128;
        lo.wrapping_add((self.next() as u128 % span) as i64)
    }

    pub fn pick<'a, T>(&mut self, v: &'a [T]) -> &'a T {
        &v[self.index(v.len())]
    }

    /// An independent generator (for a sub-task whose consumption of random
    /// numbers should not shift everything after it).
    pub fn fork(&mut self) -> Rng {
        Rng(self.next())
    }
}

fn env_u64(name: &str) -> Option<u64> {
    let v = std::env::var(name).ok()?;
    Some(v.trim().parse().unwrap_or_else(|_| panic!("{name}={v} is not a number")))
}

/// Seeds for a randomized test: `IVM_TEST_SEED` if set; otherwise
/// `defaults`, or for a long run `IVM_TEST_ITERATIONS` (default 20) fresh
/// seeds.
pub fn seeds(defaults: &[u64], long: bool) -> Vec<u64> {
    if let Some(s) = env_u64("IVM_TEST_SEED") {
        return vec![s];
    }
    if !long {
        return defaults.to_vec();
    }
    let n = env_u64("IVM_TEST_ITERATIONS").unwrap_or(20);
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(1);
    let mut r = Rng::new(now ^ (std::process::id() as u64) << 32);
    // short seeds are easier to pass around
    (0..n).map(|_| r.next() % 1_000_000_000).collect()
}

/// Run `f` for every seed; a panic is reported together with its seed.
pub fn run_seeds(test: &str, seeds: &[u64], mut f: impl FnMut(u64)) {
    for &seed in seeds {
        if seeds.len() > 1 {
            eprintln!("{test}: seed {seed}");
        }
        let _guard = SeedGuard { test, seed };
        f(seed);
    }
}

struct SeedGuard<'a> {
    test: &'a str,
    seed: u64,
}

impl Drop for SeedGuard<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("\n*** {} failed with seed {} (rerun with IVM_TEST_SEED={})\n", self.test, self.seed, self.seed);
        }
    }
}

/// A fresh scratch directory, removed on drop unless the test is failing.
pub struct TempDir(PathBuf);

impl TempDir {
    pub fn new(name: &str) -> TempDir {
        let base = std::env::var_os("IVM_TEST_TMP").map(PathBuf::from).unwrap_or_else(std::env::temp_dir);
        let d = base.join(format!("ivm-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap_or_else(|e| panic!("creating {}: {e}", d.display()));
        TempDir(d)
    }

    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("keeping {} for inspection", self.0.display());
        } else {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}
