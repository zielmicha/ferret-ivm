//! Segment files under a low limit on open files (`RLIMIT_NOFILE`, set for
//! the whole process, which is why this is a test binary of its own, with
//! one test): the store holds a quarter of the limit open for reading
//! however many segments it has and however many forks read it, and a fork
//! reads dead segments that checkpoints have given up since by opening them
//! again (they are kept on disk until it is dropped). The store has many
//! times more segments than the limit allows files; forks read old versions
//! while checkpoints and maintenance passes kill segments; and a child
//! process is killed while forks keep dead segments: the next open deletes
//! them and recovers the last checkpoint.

mod common;
use common::temp_dir as tmpdir;

use ivm::storage::{CountSummarizer, Env, EnvOptions, MaintBudget, TreeId};
use std::collections::BTreeSet;
use std::io::{BufRead, BufReader};
use std::ops::Bound;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;

/// The limit on open files of this process (and of the child it starts).
const LIMIT: u64 = 64;
/// Keys added to the growing tree per GSN.
const GROW: u64 = 400;
/// Keys added to the queue per GSN, and removed again `SPAN` GSNs later.
const QUEUE: u64 = 40;
const SPAN: u64 = 8;
/// Keys of a tree that a fork reads after the environment dropped it.
const DROPPED: u64 = 3000;
/// The child process kills itself (`CHILD` names its store directory).
const CHILD: &str = "IVM_DESCRIPTORS_CHILD";

fn set_limit() {
    let mut lim = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
    // SAFETY: both calls only read or write the struct they are given.
    unsafe {
        assert_eq!(libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim), 0);
        lim.rlim_cur = LIMIT;
        assert_eq!(libc::setrlimit(libc::RLIMIT_NOFILE, &lim), 0);
    }
}

/// Files open in this process (one of them the directory being listed).
fn open_files() -> usize {
    std::fs::read_dir("/proc/self/fd").unwrap().count()
}

/// Segment files on disk.
fn segment_files(dir: &Path) -> BTreeSet<u32> {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| {
            let name = e.unwrap().file_name().to_string_lossy().into_owned();
            name.strip_prefix("seg-")?.strip_suffix(".dat")?.parse().ok()
        })
        .collect()
}

/// Segments with live nodes, and the active one.
fn listed(env: &Env) -> BTreeSet<u32> {
    env.stats().segments.iter().map(|s| s.0).collect()
}

fn opts() -> EnvOptions {
    // small segments, so that there are hundreds of them, and a pool small
    // enough that scans read nodes from the segment files
    EnvOptions { cache_bytes: 64 << 10, leaf_bytes: 1024, internal_bytes: 1024, segment_bytes: 16 << 10, ..Default::default() }
}

fn key(tag: u64, i: u64) -> Vec<u8> {
    (tag.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ i.wrapping_mul(0xD1B5_4A32_D192_ED03)).to_be_bytes().to_vec()
}

/// The keys in `tree` now (each with weight 1).
fn keys(env: &mut Env, tree: TreeId) -> BTreeSet<Vec<u8>> {
    let all = env.scan(tree, Bound::Unbounded, Bound::Unbounded).unwrap_or_else(|e| panic!("scan of tree {tree}: {e}"));
    all.into_iter()
        .map(|(k, w)| {
            assert_eq!(w, 1, "weight of a key of tree {tree}");
            k.into_vec()
        })
        .collect()
}

/// The growing tree (1) and the queue (2) as of `gsn`.
fn expected(gsn: u64) -> (BTreeSet<Vec<u8>>, BTreeSet<Vec<u8>>) {
    let grow = (0..GROW * gsn).map(|i| key(1, i)).collect();
    let queue = (gsn.saturating_sub(SPAN - 1).max(1)..=gsn).flat_map(|g| (0..QUEUE).map(move |j| key(g, j))).collect();
    (grow, queue)
}

/// A store with the growing tree and the queue, the forks taken of it (with
/// the GSN each was taken at, and the dropped tree it reads, if any), and
/// the most files seen open.
struct Store {
    dir: PathBuf,
    env: Env,
    gsn: u64,
    forks: Vec<(Env, u64, Option<TreeId>)>,
    most_open: usize,
}

impl Store {
    fn create(dir: &Path) -> Store {
        let (mut env, rec) = Env::open(dir, opts()).unwrap();
        assert!(rec.is_none());
        assert_eq!(env.create_tree(Arc::new(CountSummarizer)), 1);
        assert_eq!(env.create_tree(Arc::new(CountSummarizer)), 2);
        Store { dir: dir.to_path_buf(), env, gsn: 0, forks: Vec::new(), most_open: 0 }
    }

    fn note_open(&mut self) {
        self.most_open = self.most_open.max(open_files());
    }

    /// The next GSN: updates, sometimes a maintenance pass without limits
    /// (which relocates the live nodes of sparse segments, killing them),
    /// and a checkpoint (which deletes dead segments, unless a fork may
    /// read them). Every 5th takes a fork, every 25th fills a tree, takes a
    /// fork and drops the tree.
    fn step(&mut self) {
        self.gsn += 1;
        let g = self.gsn;
        let mut grow: Vec<_> = (GROW * (g - 1)..GROW * g).map(|i| (key(1, i), 1)).collect();
        self.env.apply(1, &mut grow).unwrap();
        let mut queue: Vec<_> = (0..QUEUE).map(|j| (key(g, j), 1)).collect();
        if g > SPAN {
            queue.extend((0..QUEUE).map(|j| (key(g - SPAN, j), -1)));
        }
        self.env.apply(2, &mut queue).unwrap();
        let dropped = g.is_multiple_of(25).then(|| {
            let t = self.env.create_tree(Arc::new(CountSummarizer));
            let mut ups: Vec<_> = (0..DROPPED).map(|i| (key(t, i), 1)).collect();
            self.env.apply(t, &mut ups).unwrap();
            t
        });
        if g.is_multiple_of(4) {
            self.env.maintain_with(MaintBudget::UNLIMITED).unwrap();
        }
        self.env.checkpoint(g, &g.to_le_bytes()).unwrap();
        if g.is_multiple_of(5) || dropped.is_some() {
            if self.forks.len() >= 6 {
                let (f, at, d) = self.forks.remove(0);
                self.check_fork(f, at, d);
            }
            self.forks.push((self.env.fork().unwrap(), g, dropped));
        }
        if let Some(t) = dropped {
            self.env.drop_tree(t).unwrap();
            self.env.maintain_with(MaintBudget::UNLIMITED).unwrap();
            self.env.checkpoint(g, &g.to_le_bytes()).unwrap();
        }
        self.note_open();
    }

    /// A fork reads every tree as of when it was taken, also a tree
    /// dropped since; then it is dropped.
    fn check_fork(&mut self, mut f: Env, at: u64, dropped: Option<TreeId>) {
        let (grow, queue) = expected(at);
        assert!(keys(&mut f, 1) == grow, "growing tree in a fork taken at gsn {at}");
        assert!(keys(&mut f, 2) == queue, "queue in a fork taken at gsn {at}");
        if let Some(t) = dropped {
            let all: BTreeSet<_> = (0..DROPPED).map(|i| key(t, i)).collect();
            assert!(keys(&mut f, t) == all, "dropped tree {t} in a fork taken at gsn {at}");
        }
        self.note_open();
    }

    fn check(&mut self) {
        let (grow, queue) = expected(self.gsn);
        assert!(keys(&mut self.env, 1) == grow, "growing tree at gsn {}", self.gsn);
        assert!(keys(&mut self.env, 2) == queue, "queue at gsn {}", self.gsn);
        self.note_open();
    }

    /// Dead segments on disk: kept for the forks that may read them.
    fn dead_on_disk(&self) -> BTreeSet<u32> {
        segment_files(&self.dir).difference(&listed(&self.env)).copied().collect()
    }
}

#[test]
fn segment_files_stay_within_the_limit() {
    set_limit();
    if let Ok(dir) = std::env::var(CHILD) {
        return child(Path::new(&dir));
    }
    let base = open_files();
    let tmp = tmpdir("store");
    let dir = tmp.join("store");

    // forks read old versions while checkpoints kill segments
    let mut s = Store::create(&dir);
    let (mut most_segments, mut most_dead) = (0, 0);
    while s.gsn < 300 {
        s.step();
        most_segments = most_segments.max(listed(&s.env).len());
        most_dead = most_dead.max(s.dead_on_disk().len());
        if s.gsn.is_multiple_of(50) {
            s.check();
        }
    }
    while let Some((f, at, d)) = s.forks.pop() {
        s.check_fork(f, at, d);
    }
    // (the last checkpoint may have found more dead segments since)
    s.env.checkpoint(s.gsn, &s.gsn.to_le_bytes()).unwrap();
    assert_eq!(segment_files(&dir), listed(&s.env), "segment files once no fork is left");
    let created = listed(&s.env).last().copied().unwrap();
    eprintln!(
        "limit {LIMIT}: {created} segments created, at most {most_segments} live and {most_dead} dead ones kept for forks at once; \
         at most {} files open ({base} before)",
        s.most_open
    );
    assert!(created as u64 > 8 * LIMIT && most_segments as u64 > 2 * LIMIT, "too few segments to test the limit");
    assert!(most_dead > 0, "no dead segment was kept for a fork");
    // a quarter of the limit for reading, the active segment and the
    // manifest, one opened to be synced, and the listing
    let bound = base + LIMIT as usize / 4 + 4;
    assert!(s.most_open <= bound, "{} files open, more than {bound}", s.most_open);
    drop(s);

    // a process killed while forks keep dead segments
    let child_dir = tmp.join("child");
    let mut c = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "segment_files_stay_within_the_limit", "--nocapture", "--test-threads=1"])
        .env(CHILD, &child_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let mut killed = None;
    for line in BufReader::new(c.stdout.take().unwrap()).lines() {
        let line = line.unwrap();
        // (after the test harness's "test ... " on the same line)
        if let Some(rest) = line.split_once("KILLED ").map(|(_, r)| r) {
            let mut it = rest.split(' ');
            let gsn: u64 = it.next().unwrap().parse().unwrap();
            let dead: BTreeSet<u32> = it.map(|d| d.parse().unwrap()).collect();
            killed = Some((gsn, dead));
        }
    }
    assert_eq!(c.wait().unwrap().signal(), Some(libc::SIGKILL), "the child is killed");
    let (gsn, dead) = killed.expect("the child reports its state before it is killed");
    eprintln!("killed after the checkpoint at gsn {gsn}, with {} dead segments kept for forks", dead.len());
    for d in &dead {
        assert!(segment_files(&child_dir).contains(d), "dead segment {d} kept by the killed process");
    }
    let (mut env, rec) = Env::open(&child_dir, opts()).unwrap();
    let rec = rec.expect("a checkpoint");
    assert_eq!(rec.app, gsn.to_le_bytes(), "the last checkpoint is recovered");
    assert_eq!(segment_files(&child_dir), listed(&env), "segment files after the restart");
    for t in env.unattached_trees() {
        env.attach_tree(t, Arc::new(CountSummarizer)).unwrap();
    }
    let mut s = Store { dir: child_dir.clone(), env, gsn, forks: Vec::new(), most_open: 0 };
    s.check();
    while s.gsn < gsn + 60 {
        s.step();
    }
    s.check();
    while let Some((f, at, d)) = s.forks.pop() {
        s.check_fork(f, at, d);
    }
    s.env.checkpoint(s.gsn, &s.gsn.to_le_bytes()).unwrap();
    assert_eq!(segment_files(&child_dir), listed(&s.env), "segment files once no fork is left");
    assert!(s.most_open <= bound, "{} files open after the restart, more than {bound}", s.most_open);
    drop(s);
    let _ = std::fs::remove_dir_all(&tmp);
}

/// Run until forks keep dead segments on disk, report the last checkpoint
/// and those segments, and die without dropping anything.
fn child(dir: &Path) {
    let mut s = Store::create(dir);
    loop {
        s.step();
        let dead = s.dead_on_disk();
        if s.gsn > 60 && dead.len() >= 3 && s.forks.len() >= 3 {
            let dead: Vec<String> = dead.iter().map(|d| d.to_string()).collect();
            println!("KILLED {} {}", s.gsn, dead.join(" "));
            // SAFETY: kill and getpid have no memory-safety preconditions.
            unsafe {
                libc::kill(libc::getpid(), libc::SIGKILL);
            }
            unreachable!("killed");
        }
        assert!(s.gsn < 1000, "no dead segment kept for forks");
    }
}
