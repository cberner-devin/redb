use crate::tree_store::btree_iters::{
    BtreeRangeIter, EntryGuard, RangeLeafEntry, RangeSubtree, RangeVisit,
};
use crate::tree_store::page_store::PageImpl;
use crate::tree_store::subtree_rebuild::{
    InProgressSubtree, LeafRewrite, SealedSubtree, SubtreeBuilder, SubtreeRebuildContext,
    finish_rebuilt_root,
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
    frontiers: ExtractFrontiers,
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
            frontiers: ExtractFrontiers::new(),
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

    pub(crate) fn close(&mut self) -> Result {
        self.finalize()
    }

    fn fail<T>(&mut self, err: StorageError) -> Result<T> {
        self.finalized = true;
        self.finalize_failed = true;
        self.inner.close();
        self.frontiers.clear();
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
            let item = match end {
                ExtractEnd::Front => {
                    self.with_visitor_deferred_recycling(|inner, frontiers, context| {
                        inner.next_entry_with_visitor(|event| {
                            if let Some(index) = frontiers.process_yielded_event(
                                ExtractEnd::Front,
                                context,
                                event,
                            )? {
                                entry_index = Some(index);
                            }
                            Ok(())
                        })
                    })
                }
                ExtractEnd::Back => {
                    self.with_visitor_deferred_recycling(|inner, frontiers, context| {
                        inner.next_back_entry_with_visitor(|event| {
                            if let Some(index) =
                                frontiers.process_yielded_event(ExtractEnd::Back, context, event)?
                            {
                                entry_index = Some(index);
                            }
                            Ok(())
                        })
                    })
                }
            };

            let entry = match item {
                Some(Ok(entry)) => entry,
                Some(Err(err)) => return Some(self.fail(err)),
                None => return self.finish_iteration(),
            };
            let entry_index =
                entry_index.expect("range visitor must emit leaf entry before yielding an entry");

            if self.predicate_matches(&entry) {
                let remove_result = {
                    let mut context: SubtreeRebuildContext<'_, K, V> = SubtreeRebuildContext::new(
                        &self.page_allocator,
                        &self.allocated,
                        &mut self.pending_free,
                    );
                    self.frontiers.mark_removed(end, &mut context, entry_index)
                };
                if let Err(err) = remove_result {
                    return Some(self.fail(err));
                }
                self.removed += 1;
                return Some(Ok(entry));
            }
        }
    }

    fn finish_frontiers(&mut self) -> Result {
        let mut context: SubtreeRebuildContext<'_, K, V> = SubtreeRebuildContext::new(
            &self.page_allocator,
            &self.allocated,
            &mut self.pending_free,
        );
        self.frontiers.finish(&mut context)
    }

    pub(crate) fn finalize(&mut self) -> Result {
        if self.finalized {
            return Ok(());
        }

        if let Err(err) = self.drain_unvisited() {
            return self.fail(err);
        }
        self.inner.close();
        if let Err(err) = self.finish_frontiers() {
            return self.fail(err);
        }
        self.frontiers.clear();

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
            );
            finish_rebuilt_root(
                &mut context,
                self.frontiers.take_rebuilt_builder(),
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

    fn drain_unvisited(&mut self) -> Result {
        self.close_range_with_exit_visitor(self.frontiers.has_changes())
    }

    pub(crate) fn cancel_unfinalized(&mut self) {
        if !self.finalized {
            self.inner.close();
            self.pending_free.clear();
            self.frontiers.clear();
            self.finalized = true;
        }
    }

    fn finish_iteration(&mut self) -> Option<Result<EntryGuard<K, V>>> {
        match self.finalize() {
            Ok(()) => None,
            Err(err) => Some(Err(err)),
        }
    }

    fn with_visitor_deferred_recycling<T>(
        &mut self,
        f: impl FnOnce(
            &mut BtreeRangeIter<K, V>,
            &mut ExtractFrontiers,
            &mut SubtreeRebuildContext<'_, K, V>,
        ) -> T,
    ) -> T {
        // Range visitor callbacks run before the iterator step has unwound all
        // local Page refs. Record free candidates during the callback, then
        // recycle pages once those refs are gone.
        let free_start = self.pending_free.len();
        let result = {
            let mut context: SubtreeRebuildContext<'_, K, V> =
                SubtreeRebuildContext::defer_recycling(
                    &self.page_allocator,
                    &self.allocated,
                    &mut self.pending_free,
                );
            f(&mut self.inner, &mut self.frontiers, &mut context)
        };
        if self.pending_free.len() != free_start {
            self.recycle_deferred_pages(free_start);
        }
        result
    }

    fn recycle_deferred_pages(&mut self, free_start: usize) {
        let mut deferred = self.pending_free.split_off(free_start);
        deferred.sort_unstable();
        deferred.dedup();

        let mut context: SubtreeRebuildContext<'_, K, V> = SubtreeRebuildContext::new(
            &self.page_allocator,
            &self.allocated,
            &mut self.pending_free,
        );
        for page in deferred {
            context.conditional_free(page);
        }
    }

    fn close_range_with_exit_visitor(&mut self, changed: bool) -> Result {
        self.with_visitor_deferred_recycling(|inner, frontiers, context| {
            inner.close_with_exit_visitor(changed, |event| {
                frontiers.process_structural_event(ExtractEnd::Front, context, event)
            })
        })
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

struct ExtractFrontiers {
    front: ExtractFrontier,
    back: ExtractFrontier,
}

impl ExtractFrontiers {
    fn new() -> Self {
        Self {
            front: ExtractFrontier::left_to_right(),
            back: ExtractFrontier::right_to_left(),
        }
    }

    fn changes(&self) -> (bool, bool) {
        (self.front.has_changes(), self.back.has_changes())
    }

    fn has_changes(&self) -> bool {
        self.front.has_changes() || self.back.has_changes()
    }

    fn clear(&mut self) {
        self.front.clear();
        self.back.clear();
    }

    fn take_rebuilt_builder(&mut self) -> SubtreeBuilder {
        std::mem::replace(&mut self.front.builder, SubtreeBuilder::left_to_right())
    }

    fn finish<K: Key, V: Value>(
        &mut self,
        context: &mut SubtreeRebuildContext<'_, K, V>,
    ) -> Result {
        let (front_changed, back_changed) = self.changes();
        if !front_changed && !back_changed {
            return Ok(());
        }

        if front_changed && back_changed {
            self.merge_shared_current_leaf(ExtractEnd::Front);
        }
        if back_changed {
            self.front.mark_in_progress_changed();
        }
        self.front.finish_in_progress(context)?;
        if back_changed {
            self.back.flush_in_progress(context)?;
            let back_builder =
                std::mem::replace(&mut self.back.builder, SubtreeBuilder::right_to_left());
            self.front.builder.append(context, back_builder)?;
        }
        Ok(())
    }

    fn process_yielded_event<K: Key, V: Value>(
        &mut self,
        end: ExtractEnd,
        context: &mut SubtreeRebuildContext<'_, K, V>,
        event: RangeVisit<'_>,
    ) -> Result<Option<usize>> {
        match event {
            RangeVisit::LeafEntry { entry } => self
                .frontier_mut(end)
                .visit_leaf_entry(context, entry)
                .map(Some),
            event => {
                self.process_structural_event(end, context, event)?;
                Ok(None)
            }
        }
    }

    fn process_structural_event<K: Key, V: Value>(
        &mut self,
        end: ExtractEnd,
        context: &mut SubtreeRebuildContext<'_, K, V>,
        event: RangeVisit<'_>,
    ) -> Result {
        if let RangeVisit::LeafExit { subtree } = &event {
            self.merge_shared_leaf_into(end, subtree.page_number());
        }
        self.frontier_mut(end)
            .visit_structural_event(context, event)
    }

    fn mark_removed<K: Key, V: Value>(
        &mut self,
        end: ExtractEnd,
        context: &mut SubtreeRebuildContext<'_, K, V>,
        entry_index: usize,
    ) -> Result {
        self.frontier_mut(end).mark_removed(context, entry_index)
    }

    fn frontier_mut(&mut self, end: ExtractEnd) -> &mut ExtractFrontier {
        match end {
            ExtractEnd::Front => &mut self.front,
            ExtractEnd::Back => &mut self.back,
        }
    }

    fn current_pages(&self) -> (Option<PageNumber>, Option<PageNumber>) {
        (self.front.current_page(), self.back.current_page())
    }

    fn merge_shared_leaf_into(&mut self, end: ExtractEnd, page_number: PageNumber) {
        let (front, back) = self.current_pages();
        if front != Some(page_number) || back != Some(page_number) {
            return;
        }
        self.merge_shared_current_leaf(end);
    }

    fn merge_shared_current_leaf(&mut self, end: ExtractEnd) {
        let (Some(front), Some(back)) = self.current_pages() else {
            return;
        };
        if front != back {
            return;
        }

        match end {
            ExtractEnd::Front => {
                let back = self
                    .back
                    .take_current_leaf()
                    .expect("shared leaf must have a back leaf");
                self.front
                    .current_leaf_mut()
                    .expect("shared leaf must have a front leaf")
                    .merge_same_leaf(back);
            }
            ExtractEnd::Back => {
                let front = self
                    .front
                    .take_current_leaf()
                    .expect("shared leaf must have a front leaf");
                self.back
                    .current_leaf_mut()
                    .expect("shared leaf must have a back leaf")
                    .merge_same_leaf(front);
            }
        }
    }
}

struct ExtractFrontier {
    builder: SubtreeBuilder,
    in_progress: InProgressSubtree,
    current_leaf: Option<LeafRewrite>,
    changed: bool,
}

impl ExtractFrontier {
    fn left_to_right() -> Self {
        Self {
            builder: SubtreeBuilder::left_to_right(),
            in_progress: InProgressSubtree::new(),
            current_leaf: None,
            changed: false,
        }
    }

    fn right_to_left() -> Self {
        Self {
            builder: SubtreeBuilder::right_to_left(),
            in_progress: InProgressSubtree::new(),
            current_leaf: None,
            changed: false,
        }
    }

    fn has_changes(&self) -> bool {
        self.changed
    }

    fn current_page(&self) -> Option<PageNumber> {
        self.current_leaf.as_ref().map(LeafRewrite::page_number)
    }

    fn has_current_leaf(&self) -> bool {
        self.current_leaf.is_some()
    }

    fn set_current_leaf(&mut self, page: PageImpl, subtree: RangeSubtree) {
        self.current_leaf = Some(LeafRewrite::from_parts(page, subtree));
    }

    fn clear(&mut self) {
        self.current_leaf = None;
        self.in_progress = InProgressSubtree::new();
        self.changed = false;
    }

    fn current_leaf_mut(&mut self) -> Option<&mut LeafRewrite> {
        self.current_leaf.as_mut()
    }

    fn take_current_leaf(&mut self) -> Option<LeafRewrite> {
        self.current_leaf.take()
    }

    fn visit_structural_event<K: Key, V: Value>(
        &mut self,
        context: &mut SubtreeRebuildContext<'_, K, V>,
        event: RangeVisit<'_>,
    ) -> Result {
        match event {
            RangeVisit::BranchEnter { branch } => {
                self.in_progress.enter_branch(branch.clone());
                Ok(())
            }
            RangeVisit::SkippedSubtree { subtree } => {
                self.in_progress
                    .push_subtree(SealedSubtree::from_range(subtree.clone()));
                Ok(())
            }
            RangeVisit::LeafExit { subtree } => {
                let page_number = subtree.page_number();
                if self.current_page() == Some(page_number) {
                    self.complete_current_leaf(context)?;
                }
                Ok(())
            }
            RangeVisit::BranchExit { branch } => {
                self.complete_current_leaf(context)?;
                if let Some(replaced_page) =
                    self.in_progress
                        .exit_branch_into(context, &mut self.builder, branch)?
                {
                    context.conditional_free(replaced_page);
                }
                Ok(())
            }
            RangeVisit::LeafEntry { .. } => {
                unreachable!("structural range visitor emitted a leaf entry")
            }
        }
    }

    fn visit_leaf_entry<K: Key, V: Value>(
        &mut self,
        context: &mut SubtreeRebuildContext<'_, K, V>,
        entry: RangeLeafEntry<'_>,
    ) -> Result<usize> {
        let page_number = entry.page_number();
        if self.current_page().is_some_and(|page| page != page_number) {
            self.complete_current_leaf(context)?;
        }
        if !self.has_current_leaf() {
            self.set_current_leaf(entry.page().clone(), entry.subtree().clone());
        }
        Ok(entry.entry_index())
    }

    fn mark_removed<K: Key, V: Value>(
        &mut self,
        context: &mut SubtreeRebuildContext<'_, K, V>,
        entry_index: usize,
    ) -> Result {
        let first_removed = self
            .current_leaf
            .as_mut()
            .expect("range visitor must set current leaf before predicate")
            .mark_removed_unordered(entry_index);
        if first_removed {
            self.changed = true;
            self.in_progress.mark_changed();
            self.in_progress.flush_into(context, &mut self.builder)?;
        }
        Ok(())
    }

    fn mark_in_progress_changed(&mut self) {
        self.in_progress.mark_changed();
    }

    fn complete_current_leaf<K: Key, V: Value>(
        &mut self,
        context: &mut SubtreeRebuildContext<'_, K, V>,
    ) -> Result {
        if let Some(leaf) = self.current_leaf.take() {
            leaf.complete_unordered_into(context, &mut self.in_progress, &mut self.builder)?;
        }
        Ok(())
    }

    fn finish_in_progress<K: Key, V: Value>(
        &mut self,
        context: &mut SubtreeRebuildContext<'_, K, V>,
    ) -> Result {
        self.complete_current_leaf(context)?;
        let replaced_pages = self.in_progress.finish_into(context, &mut self.builder)?;
        for page in replaced_pages {
            context.conditional_free(page);
        }
        Ok(())
    }

    fn flush_in_progress<K: Key, V: Value>(
        &mut self,
        context: &mut SubtreeRebuildContext<'_, K, V>,
    ) -> Result {
        self.complete_current_leaf(context)?;
        self.in_progress.flush_into(context, &mut self.builder)
    }
}
