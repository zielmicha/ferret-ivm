//! Per-entry (GSN, weight) history, consolidated behind a retention frontier.

use super::Gsn;
use smallvec::SmallVec;

/// Weight changes of one (key, value) entry, sorted by GSN.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct History(pub SmallVec<[(Gsn, i64); 2]>);

impl History {
    pub fn single(g: Gsn, w: i64) -> History {
        let mut v = SmallVec::new();
        v.push((g, w));
        History(v)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Accumulated weight over all times.
    #[inline]
    pub fn total(&self) -> i64 {
        self.0.iter().map(|(_, w)| *w).sum()
    }

    /// Accumulated weight as of time `t` (inclusive).
    #[inline]
    pub fn weight_at(&self, t: Gsn) -> i64 {
        self.0.iter().take_while(|(g, _)| *g <= t).map(|(_, w)| *w).sum()
    }

    /// Weight according to a read bound.
    #[inline]
    pub fn weight(&self, at: ReadAt) -> i64 {
        match at {
            ReadAt::Latest => self.total(),
            ReadAt::AsOf(t) => self.weight_at(t),
            ReadAt::Before(t) => {
                if t == 0 {
                    0
                } else {
                    self.weight_at(t - 1)
                }
            }
        }
    }

    /// Weight change exactly at time `t`.
    pub fn delta_at(&self, t: Gsn) -> i64 {
        self.0.iter().filter(|(g, _)| *g == t).map(|(_, w)| *w).sum()
    }

    /// Latest time with a change.
    pub fn last_time(&self) -> Option<Gsn> {
        self.0.last().map(|(g, _)| *g)
    }

    /// Add a weight change at `g` and compact times at or before `frontier`.
    pub fn add(&mut self, g: Gsn, w: i64, frontier: Gsn) {
        if w != 0 {
            match self.0.last_mut() {
                Some((lg, lw)) if *lg == g => *lw += w,
                Some((lg, _)) if *lg > g => {
                    let pos = self.0.partition_point(|(x, _)| *x < g);
                    if pos < self.0.len() && self.0[pos].0 == g {
                        self.0[pos].1 += w;
                    } else {
                        self.0.insert(pos, (g, w));
                    }
                }
                _ => self.0.push((g, w)),
            }
        }
        self.compact(frontier);
    }

    /// Merge all changes at or before `frontier` into one, dropping zero weights.
    pub fn compact(&mut self, frontier: Gsn) {
        let n = self.0.partition_point(|(g, _)| *g <= frontier);
        if n >= 2 || (n == 1 && self.0[0].1 == 0) {
            let sum: i64 = self.0[..n].iter().map(|(_, w)| *w).sum();
            let t = self.0[n - 1].0;
            self.0.drain(..n);
            if sum != 0 {
                self.0.insert(0, (t, sum));
            }
        }
        // zero-weight entries after the frontier are also redundant
        self.0.retain(|(_, w)| *w != 0);
    }
}

/// Which version of an arrangement a read observes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadAt {
    /// Everything applied so far.
    Latest,
    /// State as of time t (inclusive).
    AsOf(Gsn),
    /// State strictly before time t.
    Before(Gsn),
}
