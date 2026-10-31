//! The engine's change feeds. Subscriptions and sinks share registration,
//! ordering, queue accounting and replacement/resnapshot state. A consumer
//! chooses only its overflow policy and progress cadence; network retries
//! and target transactions belong to the sink writer.

use super::api::{Chunks, SharedRows};
use crate::dataflow::{Delta, NodeId, Output};
use crate::error::{Result, SqlError, codes};
use crate::memory::{Charge, Meter};
use crate::storage::Gsn;
use crate::types::Row;
use crossbeam_channel::{Receiver, Sender};
use rustc_hash::{FxHashMap, FxHashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// A generation identifies one registration, including after a sink with
/// the same catalog ID has been replaced. Old snapshot requests cannot
/// affect a newer registration.
pub(crate) type FeedId = u64;

#[derive(Debug)]
pub enum ChangeEvent {
    Data {
        gsn: Gsn,
        rows: SharedRows,
    },
    /// Read the entire snapshot before processing subsequent events.
    Snapshot {
        gsn: Gsn,
        rows: Chunks<Delta>,
    },
    /// All changes through this GSN have been delivered.
    Progress {
        gsn: Gsn,
        charge: Charge,
    },
    Error(SqlError),
    /// Changes were dropped. Only a new snapshot can restore continuity;
    /// neither changes nor progress follow this event until that snapshot.
    Behind,
    Stop,
}

impl ChangeEvent {
    pub fn rows(self) -> Result<Vec<(Row, i64)>> {
        match self {
            Self::Data { rows, .. } => Ok(rows.to_vec()),
            Self::Snapshot { rows, .. } => {
                let mut all = Vec::new();
                for c in rows {
                    all.extend(c?.into_parts().0);
                }
                Ok(all)
            }
            _ => Ok(Vec::new()),
        }
    }
}

const EVENT_BYTES: usize = std::mem::size_of::<ChangeEvent>() + 8;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Overflow {
    Terminate,
    Resnapshot,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Progress {
    None,
    Record,
    Batch,
}

#[derive(Clone, Copy)]
enum State {
    Live,
    Behind,
    /// Replacement holds delivery at the previous version. A request made
    /// meanwhile must be answered from the new version after hydration.
    Held {
        behind: bool,
        snapshot_requested: bool,
    },
}

pub(crate) struct Feed {
    pub node: NodeId,
    pub queue: Arc<Meter>,
    pub cancel: Arc<AtomicBool>,
    tx: Sender<ChangeEvent>,
    state: State,
    overflow: Overflow,
    pub progress: Progress,
}

impl Feed {
    pub fn active(&self) -> bool {
        matches!(self.state, State::Live) && !self.cancel.load(Ordering::Acquire)
    }

    pub fn held(&self) -> bool {
        matches!(self.state, State::Held { .. })
    }

    pub fn behind(&self) -> bool {
        matches!(self.state, State::Behind | State::Held { behind: true, .. })
    }

    pub fn hold(&mut self) {
        if !self.held() {
            self.state = State::Held { behind: self.behind(), snapshot_requested: false };
        }
    }

    /// Resume following a hydrated replacement. Returns whether its
    /// deferred snapshot request must be answered before any replay.
    fn resume(&mut self, node: NodeId) -> bool {
        self.node = node;
        let State::Held { behind, snapshot_requested } = self.state else { return false };
        self.state = if behind || snapshot_requested { State::Behind } else { State::Live };
        snapshot_requested
    }

    /// True when a snapshot can be taken now, false while held or stopped.
    fn request_snapshot(&mut self) -> bool {
        if self.cancel.load(Ordering::Acquire) {
            return false;
        }
        if let State::Held { snapshot_requested, .. } = &mut self.state {
            *snapshot_requested = true;
            false
        } else {
            true
        }
    }

    pub fn snapshot(&mut self, gsn: Gsn, rows: Chunks<Delta>) {
        if self.held() || self.cancel.load(Ordering::Acquire) {
            return;
        }
        self.state = State::Live;
        let _ = self.tx.send(ChangeEvent::Snapshot { gsn, rows });
    }

    pub fn fail(&self, error: SqlError) {
        if !self.cancel.load(Ordering::Acquire) {
            let _ = self.tx.send(ChangeEvent::Error(error));
            self.cancel.store(true, Ordering::Release);
        }
    }

    pub fn stop(&self) {
        self.cancel.store(true, Ordering::Release);
        if self.overflow == Overflow::Resnapshot {
            let _ = self.tx.send(ChangeEvent::Stop);
        }
    }

    fn errors(&self, errs: &[((String, String), i64)]) {
        if self.overflow != Overflow::Terminate || !self.active() || errs.is_empty() {
            return;
        }
        let mut errs = errs.to_vec();
        crate::dataflow::consolidate_errs(&mut errs);
        if let Some(((code, msg), _)) = errs.into_iter().find(|(_, w)| *w > 0) {
            self.fail(SqlError::new(&code, msg));
        }
    }

    fn reserve(&mut self, bytes: usize, limit: usize) -> bool {
        if !self.active() {
            return false;
        }
        let queued = self.queue.used().saturating_add(bytes);
        if queued <= limit {
            return true;
        }
        match self.overflow {
            Overflow::Terminate => self.fail(
                SqlError::new(
                    codes::INSUFFICIENT_RESOURCES,
                    format!(
                        "subscription ended: its client does not read changes as fast as they come ({:.1} MiB queued, the limit is {:.1} MiB)",
                        queued as f64 / (1 << 20) as f64,
                        limit as f64 / (1 << 20) as f64
                    ),
                )
                .with_hint(
                    "Read subscriptions promptly, or give the server more memory (--memory-mb; each subscription may queue a sixteenth of it).",
                ),
            ),
            Overflow::Resnapshot => {
                self.state = State::Behind;
                let _ = self.tx.send(ChangeEvent::Behind);
            }
        }
        false
    }

    fn rows(&mut self, gsn: Gsn, rows: &Arc<Delta>, bytes: usize, limit: usize) {
        let bytes = bytes.saturating_add(EVENT_BYTES);
        if self.reserve(bytes, limit) {
            let _ = self.tx.send(ChangeEvent::Data { gsn, rows: SharedRows::shared(rows, self.queue.charge(bytes)) });
        }
    }

    /// Also used for the initial boundary, even without ongoing progress.
    pub fn boundary(&mut self, gsn: Gsn, limit: usize) {
        if self.reserve(EVENT_BYTES, limit) {
            let _ = self.tx.send(ChangeEvent::Progress { gsn, charge: self.queue.charge(EVENT_BYTES) });
        }
    }
}

#[derive(Default, Debug, PartialEq)]
struct Index {
    by_node: FxHashMap<NodeId, Vec<FeedId>>,
    record: Vec<FeedId>,
    batch: Vec<FeedId>,
    nodes: Arc<FxHashSet<NodeId>>,
}

impl Index {
    fn of(feeds: &FxHashMap<FeedId, Feed>) -> Self {
        let mut index = Self::default();
        for (&id, feed) in feeds {
            index.by_node.entry(feed.node).or_default().push(id);
            match feed.progress {
                Progress::None => {}
                Progress::Record => index.record.push(id),
                Progress::Batch => index.batch.push(id),
            }
        }
        for ids in index.by_node.values_mut() {
            ids.sort_unstable();
        }
        index.record.sort_unstable();
        index.batch.sort_unstable();
        index.nodes = Arc::new(index.by_node.keys().copied().collect());
        index
    }
}

#[derive(Default)]
pub(crate) struct Feeds {
    feeds: FxHashMap<FeedId, Feed>,
    index: Option<Index>,
}

impl std::ops::Index<FeedId> for Feeds {
    type Output = Feed;
    fn index(&self, id: FeedId) -> &Feed {
        &self.feeds[&id]
    }
}

impl std::ops::IndexMut<FeedId> for Feeds {
    fn index_mut(&mut self, id: FeedId) -> &mut Feed {
        self.feeds.get_mut(&id).unwrap()
    }
}

impl Feeds {
    pub fn register(&mut self, node: NodeId, parent: &Arc<Meter>, overflow: Overflow, progress: Progress) -> (FeedId, Receiver<ChangeEvent>) {
        static GENERATIONS: AtomicU64 = AtomicU64::new(1);
        let id = GENERATIONS.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = crossbeam_channel::unbounded();
        self.feeds.insert(
            id,
            Feed { node, queue: Meter::child(parent), cancel: Arc::new(AtomicBool::new(false)), tx, state: State::Live, overflow, progress },
        );
        self.index = None;
        (id, rx)
    }

    pub fn remove(&mut self, id: FeedId) {
        if let Some(feed) = self.feeds.remove(&id) {
            feed.stop();
            self.index = None;
        }
    }

    /// Stale generations are ignored, including requests from a writer
    /// whose registration was removed while it was reconnecting.
    pub fn request_snapshot(&mut self, generation: FeedId) -> bool {
        self.feeds.get_mut(&generation).is_some_and(Feed::request_snapshot)
    }

    pub fn resume(&mut self, id: FeedId, node: NodeId) -> bool {
        self.index = None;
        self[id].resume(node)
    }

    fn index(&mut self) -> &Index {
        let index = self.index.get_or_insert_with(|| {
            crate::dataflow::bookkept(self.feeds.len() as u64);
            Index::of(&self.feeds)
        });
        #[cfg(debug_assertions)]
        assert_eq!(*index, Index::of(&self.feeds), "stale feed index");
        index
    }

    pub fn nodes(&mut self) -> Arc<FxHashSet<NodeId>> {
        self.index().nodes.clone()
    }

    /// Visit the smaller of the consumed nodes and changed nodes. Held,
    /// cancelled and behind registrations cannot receive any event here.
    pub fn changed(&mut self, outputs: &FxHashMap<NodeId, Output>) -> Vec<(NodeId, Vec<FeedId>)> {
        let by_node = &self.index().by_node;
        let changed: Vec<_> = if by_node.len() <= outputs.len() {
            by_node.iter().filter(|(n, _)| outputs.contains_key(n)).map(|(n, ids)| (*n, ids.clone())).collect()
        } else {
            outputs.keys().filter_map(|n| by_node.get(n).map(|ids| (*n, ids.clone()))).collect()
        };
        changed
            .into_iter()
            .filter_map(|(node, mut ids)| {
                ids.retain(|id| self[*id].active());
                (!ids.is_empty()).then_some((node, ids))
            })
            .collect()
    }

    pub fn errors(&self, ids: &[FeedId], errs: &[((String, String), i64)]) {
        for &id in ids {
            self[id].errors(errs);
        }
    }

    pub fn rows(&mut self, ids: &[FeedId], gsn: Gsn, rows: Delta, limit: usize) {
        if rows.is_empty() {
            return;
        }
        let rows = Arc::new(rows);
        let bytes = crate::memory::delta_bytes(&rows);
        for &id in ids {
            self[id].rows(gsn, &rows, bytes, limit);
        }
    }

    pub fn progress(&mut self, ids: &[FeedId], cadence: Progress, gsn: Gsn, limit: usize) {
        for &id in ids {
            if self[id].progress == cadence {
                self[id].boundary(gsn, limit);
            }
        }
    }

    pub fn advance(&mut self, cadence: Progress, gsn: Gsn, limit: usize) {
        let index = self.index();
        let ids = match cadence {
            Progress::None => return,
            Progress::Record => index.record.clone(),
            Progress::Batch => index.batch.clone(),
        };
        self.progress(&ids, cadence, gsn, limit);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coord::stream;
    use crate::types::Datum;

    fn row(value: i64) -> Delta {
        vec![(vec![Datum::Int8(value)], 1)]
    }

    #[test]
    fn both_consumers_observe_snapshot_before_complete_changes_and_release_charges() {
        for policy in [Overflow::Terminate, Overflow::Resnapshot] {
            let parent = Meter::new();
            let mut feeds = Feeds::default();
            let (id, rx) = feeds.register(1, &parent, policy, Progress::Record);
            let snapshot = stream::ready(&feeds[id].queue, vec![(row(10), 100)], Ok(()));
            feeds[id].snapshot(5, snapshot);
            feeds[id].boundary(5, usize::MAX);
            feeds.rows(&[id], 6, row(11), usize::MAX);
            feeds.rows(&[id], 6, row(12), usize::MAX);
            feeds.advance(Progress::Record, 6, usize::MAX);
            let snapshot = rx.recv().unwrap();
            assert!(matches!(snapshot, ChangeEvent::Snapshot { gsn: 5, .. }));
            assert_eq!(snapshot.rows().unwrap(), row(10));
            assert!(matches!(rx.recv().unwrap(), ChangeEvent::Progress { gsn: 5, .. }));
            let a = rx.recv().unwrap();
            let b = rx.recv().unwrap();
            assert!(matches!(a, ChangeEvent::Data { gsn: 6, .. }));
            assert!(matches!(b, ChangeEvent::Data { gsn: 6, .. }));
            assert!(matches!(rx.recv().unwrap(), ChangeEvent::Progress { gsn: 6, .. }));
            assert!(parent.used() > 0, "retained events still own their charges");
            drop((a, b));
            assert_eq!(parent.used(), 0);
        }
    }

    #[test]
    fn overflow_after_part_of_a_gsn_never_advances_progress_past_dropped_changes() {
        let parent = Meter::new();
        for policy in [Overflow::Terminate, Overflow::Resnapshot] {
            let mut feeds = Feeds::default();
            let (id, rx) = feeds.register(1, &parent, policy, Progress::Record);
            feeds.rows(&[id], 8, row(1), usize::MAX);
            // The first chunk fills the queue. The rest of this GSN cannot fit.
            let limit = feeds[id].queue.used();
            feeds.rows(&[id], 8, row(2), limit);
            feeds.rows(&[id], 9, row(3), usize::MAX);
            feeds.progress(&[id], Progress::Record, 9, usize::MAX);
            assert!(matches!(rx.recv().unwrap(), ChangeEvent::Data { gsn: 8, .. }));
            match (policy, rx.recv().unwrap()) {
                (Overflow::Terminate, ChangeEvent::Error(e)) => assert_eq!(e.code, codes::INSUFFICIENT_RESOURCES),
                (Overflow::Resnapshot, ChangeEvent::Behind) => {}
                _ => panic!("wrong overflow policy"),
            }
            assert!(rx.try_recv().is_err(), "no later data or progress without continuity");
            assert_eq!(parent.used(), 0);
        }
    }

    #[test]
    fn held_resnapshot_waits_for_new_version_and_stale_generations_are_ignored() {
        let parent = Meter::new();
        let mut feeds = Feeds::default();
        let (id, rx) = feeds.register(1, &parent, Overflow::Resnapshot, Progress::Batch);
        feeds.rows(&[id], 4, row(1), 0);
        assert!(matches!(rx.recv().unwrap(), ChangeEvent::Behind));
        feeds[id].hold();
        assert!(!feeds.request_snapshot(id), "held feed defers its snapshot request");
        feeds.rows(&[id], 5, row(2), usize::MAX);
        feeds.advance(Progress::Batch, 5, usize::MAX);
        assert!(rx.try_recv().is_err());
        assert!(feeds.resume(id, 2), "replacement must answer the deferred request");
        assert_eq!(&*feeds.nodes(), &FxHashSet::from_iter([2]));
        feeds.rows(&[id], 5, row(3), usize::MAX);
        feeds.progress(&[id], Progress::Batch, 5, usize::MAX);
        assert!(rx.try_recv().is_err(), "replay stays suppressed until the snapshot");
        let snapshot = stream::ready(&feeds[id].queue, vec![(row(3), 100)], Ok(()));
        feeds[id].snapshot(5, snapshot);
        feeds.rows(&[id], 6, row(4), usize::MAX);
        feeds.advance(Progress::Batch, 6, usize::MAX);
        assert!(matches!(rx.recv().unwrap(), ChangeEvent::Snapshot { gsn: 5, .. }));
        assert!(matches!(rx.recv().unwrap(), ChangeEvent::Data { gsn: 6, .. }));
        assert!(matches!(rx.recv().unwrap(), ChangeEvent::Progress { gsn: 6, .. }));
        feeds.remove(id);
        assert!(matches!(rx.recv().unwrap(), ChangeEvent::Stop));
        let (new, rx) = feeds.register(2, &parent, Overflow::Resnapshot, Progress::Batch);
        assert_ne!(new, id);
        assert!(!feeds.request_snapshot(id));
        assert!(feeds.request_snapshot(new));
        assert!(rx.try_recv().is_err());
        assert_eq!(parent.used(), 0);
    }

    #[test]
    fn relation_errors_terminate_subscriptions_while_sinks_keep_following() {
        let parent = Meter::new();
        let mut feeds = Feeds::default();
        let (sub, events) = feeds.register(1, &parent, Overflow::Terminate, Progress::Record);
        let (sink, changes) = feeds.register(1, &parent, Overflow::Resnapshot, Progress::Batch);
        let ids = [sub, sink];
        feeds.errors(&ids, &[(("22012".into(), "division by zero".into()), 1)]);
        feeds.rows(&ids, 5, row(1), usize::MAX);
        feeds.advance(Progress::Record, 5, usize::MAX);
        feeds.advance(Progress::Batch, 5, usize::MAX);
        assert!(matches!(events.recv().unwrap(), ChangeEvent::Error(e) if e.code == "22012"));
        assert!(events.try_recv().is_err(), "a terminal error cannot be followed by progress");
        assert!(matches!(changes.recv().unwrap(), ChangeEvent::Data { gsn: 5, .. }));
        assert!(matches!(changes.recv().unwrap(), ChangeEvent::Progress { gsn: 5, .. }));
        assert!(feeds[sink].active());
        assert_eq!(parent.used(), 0);
    }

    #[test]
    fn progress_has_the_same_queue_bound_and_hold_policy_as_data() {
        let parent = Meter::new();
        let mut feeds = Feeds::default();
        let (id, rx) = feeds.register(1, &parent, Overflow::Resnapshot, Progress::Batch);
        feeds.advance(Progress::Batch, 1, EVENT_BYTES);
        feeds.advance(Progress::Batch, 2, EVENT_BYTES);
        assert!(matches!(rx.recv().unwrap(), ChangeEvent::Progress { gsn: 1, .. }));
        assert!(matches!(rx.recv().unwrap(), ChangeEvent::Behind));
        feeds[id].hold();
        assert!(!feeds.resume(id, 2));
        feeds.advance(Progress::Batch, 3, usize::MAX);
        assert!(rx.try_recv().is_err(), "a replacement does not repair an already behind feed");
        assert_eq!(parent.used(), 0);
    }
}
