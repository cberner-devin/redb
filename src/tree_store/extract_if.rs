use crate::tree_store::btree_base::LeafAccessor;
use crate::tree_store::btree_iters::{BtreeRangeIter, EntryGuard, RangeSubtree, RangeVisit};
use crate::tree_store::page_store::{Page, PageImpl};
use crate::tree_store::subtree_rebuild::{
    ClaimSide, InProgressSubtree, LeafRewrite, OriginalSubtreeClaimer, SubtreeBuilder,
    SubtreeRebuildContext, finish_rebuilt_root,
};
use crate::tree_store::{BtreeHeader, PageAllocator, PageHint, PageNumber, PageTrackerPolicy};
use crate::types::{Key, Value};
use crate::{Result, StorageError};
use std::borrow::Borrow;
use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex};

#[derive(Copy, Clone)]
enum ExtractEnd {
    Front,
    Back,
}

impl ExtractEnd {
    fn first_claimed_side(self) -> ClaimSide {
        match self {
            Self::Front => ClaimSide::Low,
            Self::Back => ClaimSide::High,
        }
    }

    fn finish_claimed_side(self) -> ClaimSide {
        match self {
            Self::Front => ClaimSide::High,
            Self::Back => ClaimSide::Low,
        }
    }

    fn path_is_root_to_leaf(self, side: ClaimSide) -> bool {
        matches!(
            (self, side),
            (Self::Front, ClaimSide::Low) | (Self::Back, ClaimSide::High)
        )
    }
}

pub(crate) struct BtreeExtractIf<
    'a,
    K: Key + 'static,
    V: Value + 'static,
    F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
> {
    root: &'a mut Option<BtreeHeader>,
    inner: BtreeRangeIter<K, V>,
    predicate: F,
    predicate_running: bool,
    original_header: Option<BtreeHeader>,
    front: ExtractFrontier,
    back: ExtractFrontier,
    pending_free: Vec<PageNumber>,
    free_on_drop: Vec<PageNumber>,
    removed: u64,
    finalized: bool,
    finalize_failed: bool,
    master_free_list: Arc<Mutex<Vec<PageNumber>>>,
    allocated: Arc<Mutex<PageTrackerPolicy>>,
    page_allocator: PageAllocator,
    _value_type: PhantomData<V>,
}

impl<'a, K: Key, V: Value, F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool>
    BtreeExtractIf<'a, K, V, F>
{
    pub(crate) fn new<'r, KR>(
        root: &'a mut Option<BtreeHeader>,
        range: &impl RangeBounds<KR>,
        predicate: F,
        master_free_list: Arc<Mutex<Vec<PageNumber>>>,
        allocated: Arc<Mutex<PageTrackerPolicy>>,
        page_allocator: PageAllocator,
    ) -> Result<Self>
    where
        K: 'r,
        KR: Borrow<K::SelfType<'r>> + 'r,
    {
        let original_header = *root;
        let manager = page_allocator.resolver();
        let inner = BtreeRangeIter::new_with_subtree_metadata(
            range,
            original_header,
            manager,
            PageHint::None,
        )?;
        Ok(Self {
            root,
            inner,
            predicate,
            predicate_running: false,
            original_header,
            front: ExtractFrontier::left_to_right(),
            back: ExtractFrontier::right_to_left(),
            pending_free: vec![],
            free_on_drop: vec![],
            removed: 0,
            finalized: false,
            finalize_failed: false,
            master_free_list,
            allocated,
            page_allocator,
            _value_type: PhantomData,
        })
    }

    pub(crate) fn predicate_panicked(&self) -> bool {
        self.predicate_running
    }

    fn predicate_matches(&mut self, entry: &EntryGuard<K, V>) -> bool {
        assert!(!self.predicate_running);
        self.predicate_running = true;
        let result = (self.predicate)(entry.key(), entry.value());
        self.predicate_running = false;
        result
    }

    fn fail<T>(&mut self, err: StorageError) -> Result<T> {
        self.finalized = true;
        self.finalize_failed = true;
        self.inner.close();
        self.front.clear();
        self.back.clear();
        self.pending_free.clear();
        Err(err)
    }

    pub(crate) fn finalize_failed(&self) -> bool {
        self.finalize_failed
    }

    fn next_from(&mut self, end: ExtractEnd) -> Option<Result<EntryGuard<K, V>>> {
        if self.finalized {
            return None;
        }

        loop {
            let mut entry_index = None;
            let mut events = Vec::new();
            let item = match end {
                ExtractEnd::Front => self
                    .inner
                    .next_entry_with_visitor(|event| capture_range_event(event, &mut events)),
                ExtractEnd::Back => self
                    .inner
                    .next_back_entry_with_visitor(|event| capture_range_event(event, &mut events)),
            };
            for event in events {
                let result = self.with_frontier_context(end, |context, frontier| {
                    frontier.visit_range_event(context, event)
                });
                match result {
                    Ok(Some(index)) => entry_index = Some(index),
                    Ok(None) => {}
                    Err(err) => return Some(self.fail(err)),
                }
            }

            let entry = match item {
                Some(Ok(entry)) => entry,
                Some(Err(err)) => return Some(Err(err)),
                None => return self.finish_iteration(),
            };
            let entry_index =
                entry_index.expect("range visitor must emit leaf entry before yielding an entry");

            if self.predicate_matches(&entry) {
                if let Err(err) = self.start_side(end) {
                    return Some(self.fail(err));
                }
                if let Err(err) = self.with_frontier_context(end, |context, frontier| {
                    frontier.mark_removed(context, entry_index)
                }) {
                    return Some(self.fail(err));
                }
                self.removed += 1;
                return Some(Ok(entry));
            }
        }
    }

    fn start_side(&mut self, end: ExtractEnd) -> Result {
        if self.frontier(end).has_changes() {
            return Ok(());
        }

        let key = self
            .frontier(end)
            .first_leaf_key()
            .expect("changed extract_if entry must have a first leaf key")
            .to_vec();
        self.push_unchanged_side(end, &key, end.first_claimed_side())?;
        self.frontier_mut(end).mark_changed();
        Ok(())
    }

    fn complete_current_leaf(&mut self, end: ExtractEnd) -> Result {
        self.with_frontier_context(end, |context, frontier| {
            frontier.complete_current_leaf(context)
        })
    }

    fn complete_leaf(&mut self, end: ExtractEnd, leaf: LeafRewrite) -> Result {
        self.with_frontier_context(end, |context, frontier| {
            frontier.complete_leaf(context, leaf)
        })
    }

    fn finish_unidirectional(&mut self, end: ExtractEnd) -> Result {
        self.complete_current_leaf(end)?;
        if !self.frontier(end).has_changes() {
            return Ok(());
        }

        let key = self
            .frontier(end)
            .last_leaf_key()
            .expect("started extract_if frontier must have a boundary key")
            .to_vec();
        self.flush_frontier(end)?;
        self.push_unchanged_side(end, &key, end.finish_claimed_side())?;
        if matches!(end, ExtractEnd::Back) {
            self.append_back_builder()?;
        }
        Ok(())
    }

    fn finish_bidirectional(&mut self) -> Result {
        if self.current_leaves_are_shared() {
            let mut front = self.front.take_current_leaf().unwrap();
            let back = self.back.take_current_leaf().unwrap();
            front.merge_removed_unordered(back);
            self.complete_leaf(ExtractEnd::Front, front)?;
        } else {
            self.complete_current_leaf(ExtractEnd::Front)?;
            self.push_between_frontiers()?;
            self.complete_current_leaf(ExtractEnd::Back)?;
        }

        if self.removed == 0 {
            return Ok(());
        }

        self.flush_frontier(ExtractEnd::Front)?;
        self.flush_frontier(ExtractEnd::Back)?;
        self.append_back_builder()
    }

    pub(crate) fn finalize(&mut self) -> Result {
        if self.finalized {
            return Ok(());
        }

        self.inner.close();
        let finish_result = match (self.front.has_changes(), self.back.has_changes()) {
            (true, true) => self.finish_bidirectional(),
            (true, false) => self.finish_unidirectional(ExtractEnd::Front),
            (false, true) => self.finish_unidirectional(ExtractEnd::Back),
            (false, false) => Ok(()),
        };
        if let Err(err) = finish_result {
            return self.fail(err);
        }
        self.front.clear();
        self.back.clear();

        if self.removed == 0 {
            self.finalized = true;
            return Ok(());
        }

        let header = self
            .original_header
            .expect("changed extract_if must have an original root");
        let finish_result = {
            let mut context: SubtreeRebuildContext<'_, K, V> = SubtreeRebuildContext::new(
                &self.page_allocator,
                &self.allocated,
                &mut self.pending_free,
                true,
            );
            finish_rebuilt_root(
                &mut context,
                std::mem::replace(&mut self.front.builder, SubtreeBuilder::left_to_right()),
                header,
                self.removed,
            )
        };
        match finish_result {
            Ok(root) => *self.root = root,
            Err(err) => return self.fail(err),
        }
        self.pending_free.sort_unstable();
        self.pending_free.dedup();
        self.free_on_drop.append(&mut self.pending_free);
        self.finalized = true;
        Ok(())
    }

    pub(crate) fn cancel_unfinalized(&mut self) {
        if !self.finalized {
            self.inner.close();
            self.pending_free.clear();
            self.front.clear();
            self.back.clear();
            self.finalized = true;
        }
    }

    fn finish_iteration(&mut self) -> Option<Result<EntryGuard<K, V>>> {
        match self.finalize() {
            Ok(()) => None,
            Err(err) => Some(Err(err)),
        }
    }

    fn current_leaves_are_shared(&self) -> bool {
        let (Some(front), Some(back)) = (self.front.current_page(), self.back.current_page())
        else {
            return false;
        };
        front == back
    }

    fn append_back_builder(&mut self) -> Result {
        let back_builder =
            std::mem::replace(&mut self.back.builder, SubtreeBuilder::right_to_left());
        self.with_frontier_context(ExtractEnd::Front, |context, frontier| {
            frontier.builder.append(context, back_builder)
        })
    }

    fn with_frontier_context<T>(
        &mut self,
        end: ExtractEnd,
        f: impl FnOnce(&mut SubtreeRebuildContext<'_, K, V>, &mut ExtractFrontier) -> Result<T>,
    ) -> Result<T> {
        let mut context = SubtreeRebuildContext::new(
            &self.page_allocator,
            &self.allocated,
            &mut self.pending_free,
            true,
        );
        match end {
            ExtractEnd::Front => f(&mut context, &mut self.front),
            ExtractEnd::Back => f(&mut context, &mut self.back),
        }
    }

    fn flush_frontier(&mut self, end: ExtractEnd) -> Result {
        self.with_frontier_context(end, |context, frontier| frontier.flush_in_progress(context))
    }

    fn push_unchanged_side(&mut self, end: ExtractEnd, key: &[u8], side: ClaimSide) -> Result {
        let builder = match end {
            ExtractEnd::Front => &mut self.front.builder,
            ExtractEnd::Back => &mut self.back.builder,
        };
        let mut claimer = OriginalSubtreeClaimer::<K, V>::new(
            builder,
            &self.page_allocator,
            &self.allocated,
            &mut self.pending_free,
        );
        let siblings_before_path = end.path_is_root_to_leaf(side);
        claimer.push_side(self.original_header, key, side, siblings_before_path)
    }

    fn push_between_frontiers(&mut self) -> Result {
        let left_key = self
            .front
            .last_leaf_key()
            .expect("front extract_if frontier must have a boundary key")
            .to_vec();
        let right_key = self
            .back
            .last_leaf_key()
            .expect("back extract_if frontier must have a boundary key")
            .to_vec();
        let mut claimer = OriginalSubtreeClaimer::<K, V>::new(
            &mut self.front.builder,
            &self.page_allocator,
            &self.allocated,
            &mut self.pending_free,
        );
        claimer.push_between(self.original_header, &left_key, &right_key)
    }

    fn frontier(&self, end: ExtractEnd) -> &ExtractFrontier {
        match end {
            ExtractEnd::Front => &self.front,
            ExtractEnd::Back => &self.back,
        }
    }

    fn frontier_mut(&mut self, end: ExtractEnd) -> &mut ExtractFrontier {
        match end {
            ExtractEnd::Front => &mut self.front,
            ExtractEnd::Back => &mut self.back,
        }
    }
}

impl<K: Key, V: Value, F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool> Iterator
    for BtreeExtractIf<'_, K, V, F>
{
    type Item = Result<EntryGuard<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_from(ExtractEnd::Front)
    }
}

impl<K: Key, V: Value, F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool>
    DoubleEndedIterator for BtreeExtractIf<'_, K, V, F>
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.next_from(ExtractEnd::Back)
    }
}

impl<K: Key, V: Value, F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool> Drop
    for BtreeExtractIf<'_, K, V, F>
{
    fn drop(&mut self) {
        let mut master_free_list = self.master_free_list.lock().unwrap();
        let mut allocated = self.allocated.lock().unwrap();
        for page in self.free_on_drop.drain(..) {
            if !self
                .page_allocator
                .free_if_uncommitted(page, &mut allocated)
            {
                master_free_list.push(page);
            }
        }
    }
}

struct ExtractFrontier {
    builder: SubtreeBuilder,
    in_progress: InProgressSubtree,
    current_leaf: Option<LeafRewrite>,
    // A path key from the current leaf. Any key in the leaf identifies the
    // original root-to-leaf path when claiming unchanged side subtrees.
    current_leaf_path_key: Option<Vec<u8>>,
    first_leaf_path_key: Option<Vec<u8>>,
    last_leaf_path_key: Option<Vec<u8>>,
    changed: bool,
}

impl ExtractFrontier {
    fn left_to_right() -> Self {
        Self {
            builder: SubtreeBuilder::left_to_right(),
            in_progress: InProgressSubtree::new(),
            current_leaf: None,
            current_leaf_path_key: None,
            first_leaf_path_key: None,
            last_leaf_path_key: None,
            changed: false,
        }
    }

    fn right_to_left() -> Self {
        Self {
            builder: SubtreeBuilder::right_to_left(),
            in_progress: InProgressSubtree::new(),
            current_leaf: None,
            current_leaf_path_key: None,
            first_leaf_path_key: None,
            last_leaf_path_key: None,
            changed: false,
        }
    }

    fn has_changes(&self) -> bool {
        self.changed
    }

    fn mark_changed(&mut self) {
        self.changed = true;
    }

    fn current_page(&self) -> Option<PageNumber> {
        self.current_leaf.as_ref().map(LeafRewrite::page_number)
    }

    fn has_current_leaf(&self) -> bool {
        self.current_leaf.is_some()
    }

    fn first_leaf_key(&self) -> Option<&[u8]> {
        self.first_leaf_path_key.as_deref()
    }

    fn last_leaf_key(&self) -> Option<&[u8]> {
        self.current_leaf_path_key
            .as_deref()
            .or(self.last_leaf_path_key.as_deref())
    }

    fn set_current_leaf(&mut self, page: PageImpl, subtree: RangeSubtree, key: Vec<u8>) {
        if self.first_leaf_path_key.is_none() {
            self.first_leaf_path_key = Some(key.clone());
        }
        self.current_leaf_path_key = Some(key);
        self.current_leaf = Some(LeafRewrite::from_parts(page, subtree));
    }

    fn clear(&mut self) {
        self.current_leaf = None;
        self.current_leaf_path_key = None;
        self.first_leaf_path_key = None;
        self.last_leaf_path_key = None;
        self.in_progress = InProgressSubtree::new();
        self.changed = false;
    }

    fn take_current_leaf(&mut self) -> Option<LeafRewrite> {
        if self.current_leaf.is_some() {
            self.last_leaf_path_key = self.current_leaf_path_key.take();
        }
        self.current_leaf.take()
    }

    fn visit_range_event<K: Key, V: Value>(
        &mut self,
        context: &mut SubtreeRebuildContext<'_, K, V>,
        event: CapturedRangeEvent,
    ) -> Result<Option<usize>> {
        match event {
            CapturedRangeEvent::BranchEnter(branch) => {
                self.in_progress.enter_branch(branch);
                Ok(None)
            }
            CapturedRangeEvent::LeafEntry {
                page_number,
                page,
                subtree,
                entry_index,
            } => {
                if self.current_page().is_some_and(|page| page != page_number) {
                    self.complete_current_leaf(context)?;
                }
                if !self.has_current_leaf() {
                    let key = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width())
                        .entry(entry_index)
                        .unwrap()
                        .key()
                        .to_vec();
                    self.set_current_leaf(page, subtree, key);
                }
                Ok(Some(entry_index))
            }
            CapturedRangeEvent::LeafExit(page_number) => {
                if self.current_page() == Some(page_number) {
                    self.complete_current_leaf(context)?;
                }
                Ok(None)
            }
            CapturedRangeEvent::BranchExit(branch) => {
                if let Some(replaced_page) =
                    self.in_progress
                        .exit_branch_into(context, &mut self.builder, &branch)?
                {
                    context.defer_free(replaced_page);
                }
                Ok(None)
            }
        }
    }

    fn complete_current_leaf<K: Key, V: Value>(
        &mut self,
        context: &mut SubtreeRebuildContext<'_, K, V>,
    ) -> Result {
        if let Some(leaf) = self.take_current_leaf() {
            self.complete_leaf(context, leaf)?;
        }
        Ok(())
    }

    fn mark_removed<K: Key, V: Value>(
        &mut self,
        context: &mut SubtreeRebuildContext<'_, K, V>,
        entry_index: usize,
    ) -> Result {
        self.changed = true;
        self.in_progress.mark_all_changed();
        self.in_progress.flush_into(context, &mut self.builder)?;
        self.current_leaf
            .as_mut()
            .expect("range visitor must set current leaf before predicate")
            .mark_removed_unordered(entry_index);
        Ok(())
    }

    fn complete_leaf<K: Key, V: Value>(
        &mut self,
        context: &mut SubtreeRebuildContext<'_, K, V>,
        leaf: LeafRewrite,
    ) -> Result {
        leaf.complete_unordered_into(context, &mut self.in_progress, &mut self.builder)
    }

    fn flush_in_progress<K: Key, V: Value>(
        &mut self,
        context: &mut SubtreeRebuildContext<'_, K, V>,
    ) -> Result {
        self.in_progress.flush_into(context, &mut self.builder)
    }
}

enum CapturedRangeEvent {
    // Rebuild work can replace pages that the range iterator still has open
    // while invoking the visitor. Buffer cloned event metadata until after the
    // iterator step returns, then update the extract frontier.
    BranchEnter(RangeSubtree),
    LeafEntry {
        page_number: PageNumber,
        page: PageImpl,
        subtree: RangeSubtree,
        entry_index: usize,
    },
    LeafExit(PageNumber),
    BranchExit(RangeSubtree),
}

fn capture_range_event(event: RangeVisit<'_>, events: &mut Vec<CapturedRangeEvent>) -> Result {
    match event {
        RangeVisit::BranchEnter { branch } => {
            events.push(CapturedRangeEvent::BranchEnter(branch.clone()));
        }
        RangeVisit::SkippedSubtree { subtree: _ } => {}
        RangeVisit::LeafEntry { entry } => {
            events.push(CapturedRangeEvent::LeafEntry {
                page_number: entry.page_number(),
                page: entry.page().clone(),
                subtree: entry.subtree().clone(),
                entry_index: entry.entry_index(),
            });
        }
        RangeVisit::LeafExit { subtree } => {
            events.push(CapturedRangeEvent::LeafExit(subtree.page_number()));
        }
        RangeVisit::BranchExit { branch } => {
            events.push(CapturedRangeEvent::BranchExit(branch.clone()));
        }
    }
    Ok(())
}
