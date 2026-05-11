use super::MutateHelper;
use crate::Result;
use crate::tree_store::btree_base::{BRANCH, BranchAccessor, LEAF, LeafAccessor, RawLeafBuilder};
use crate::tree_store::btree_iters::EntryGuard;
use crate::tree_store::btree_range::{child_index_for_bound, leaf_entries, range_is_empty};
use crate::tree_store::page_store::{Page, PageImpl};
use crate::tree_store::{BtreeHeader, PageHint, PageNumber, PageTrackerPolicy};
use crate::types::{Key, Value};
use std::borrow::Borrow;
use std::ops::{Bound, Range, RangeBounds};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub(super) struct CursorBranch {
    pub(super) page: PageImpl,
    pub(super) current_page: PageNumber,
    pub(super) child_index: usize,
}

#[derive(Copy, Clone, Eq, PartialEq)]
enum CursorDirection {
    Forward,
    Backward,
}

struct CursorLeaf {
    page: PageImpl,
    next_entry: Option<usize>,
    start: usize,
    end: usize,
}

pub(crate) struct BtreeCursorEntry {
    page: PageImpl,
    entry_index: usize,
}

pub(super) struct BtreeCursorEntryRef<'cursor> {
    page: &'cursor PageImpl,
    entry_index: usize,
}

impl BtreeCursorEntryRef<'_> {
    pub(super) fn entry<K: Key, V: Value>(
        &self,
    ) -> crate::tree_store::btree_base::EntryAccessor<'_> {
        LeafAccessor::new(self.page.memory(), K::fixed_width(), V::fixed_width())
            .entry(self.entry_index)
            .expect("cursor entry must exist")
    }
}

impl BtreeCursorEntry {
    fn entry<K: Key, V: Value>(&self) -> crate::tree_store::btree_base::EntryAccessor<'_> {
        LeafAccessor::new(self.page.memory(), K::fixed_width(), V::fixed_width())
            .entry(self.entry_index)
            .expect("cursor entry must exist")
    }

    pub(crate) fn key_data<K: Key, V: Value>(&self) -> Vec<u8> {
        self.entry::<K, V>().key().to_vec()
    }

    pub(crate) fn into_entry_guard<K: Key, V: Value>(self) -> EntryGuard<K, V> {
        let (key, value) =
            LeafAccessor::new(self.page.memory(), K::fixed_width(), V::fixed_width())
                .entry_ranges(self.entry_index)
                .expect("cursor entry must exist");
        EntryGuard::new(self.page, key, value)
    }
}

pub(super) struct RetainLeafSpan {
    pub(super) leaf_index: usize,
    pub(super) entries: Range<usize>,
}

#[derive(Default)]
pub(super) struct RetainLeafPagePlan {
    pub(super) spans: Vec<RetainLeafSpan>,
    pub(super) pairs: usize,
    pub(super) key_bytes: usize,
    pub(super) keys_values_bytes: usize,
}

impl RetainLeafPagePlan {
    pub(super) fn push_entry(
        &mut self,
        leaf_index: usize,
        entry_index: usize,
        key_len: usize,
        value_len: usize,
    ) {
        let entry_end = entry_index + 1;
        if let Some(span) = self.spans.last_mut() {
            if span.leaf_index == leaf_index && span.entries.end == entry_index {
                span.entries.end = entry_end;
            } else {
                self.spans.push(RetainLeafSpan {
                    leaf_index,
                    entries: entry_index..entry_end,
                });
            }
        } else {
            self.spans.push(RetainLeafSpan {
                leaf_index,
                entries: entry_index..entry_end,
            });
        }
        self.pairs += 1;
        self.key_bytes += key_len;
        self.keys_values_bytes += key_len + value_len;
    }

    pub(super) fn clear(&mut self) {
        self.spans.clear();
        self.pairs = 0;
        self.key_bytes = 0;
        self.keys_values_bytes = 0;
    }
}

// Changed leaves under the same parent are rebuilt together so sparse survivors
// can be coalesced and the parent is rewritten once.
pub(super) struct BufferedRetainLeafGroup {
    pub(super) path: Vec<CursorBranch>,
    pub(super) start_child_index: usize,
    pub(super) end_child_index: usize,
    pub(super) leaves: Vec<PageImpl>,
    pub(super) retained_spans: Vec<RetainLeafSpan>,
    pub(super) removed: u64,
}

impl BufferedRetainLeafGroup {
    fn new(path: Vec<CursorBranch>, child_index: usize) -> Self {
        Self {
            path,
            start_child_index: child_index,
            end_child_index: child_index,
            leaves: vec![],
            retained_spans: vec![],
            removed: 0,
        }
    }

    fn parent_page(&self) -> PageNumber {
        self.path
            .last()
            .expect("buffered retain leaf groups require a parent branch")
            .current_page
    }

    fn push_leaf<K: Key, V: Value>(&mut self, leaf: CursorLeaf, removed_indexes: &[usize]) {
        let leaf_index = self.leaves.len();
        let accessor = LeafAccessor::new(leaf.page.memory(), K::fixed_width(), V::fixed_width());
        self.retained_spans.reserve(removed_indexes.len() + 1);
        let mut start = 0;
        for &removed in removed_indexes {
            self.push_retained_span(leaf_index, start..removed);
            start = removed + 1;
        }
        self.push_retained_span(leaf_index, start..accessor.num_pairs());
        self.leaves.push(leaf.page);
    }

    fn push_retained_span(&mut self, leaf_index: usize, entries: Range<usize>) {
        if entries.is_empty() {
            return;
        }
        self.retained_spans.push(RetainLeafSpan {
            leaf_index,
            entries,
        });
    }
}

pub(super) struct BtreeCursorMut<'m, 'a, 'b, K: Key, V: Value> {
    helper: &'m mut MutateHelper<'a, 'b, K, V>,
    state: CursorMutState,
}

impl<'m, 'a, 'b, K, V> BtreeCursorMut<'m, 'a, 'b, K, V>
where
    K: Key + 'static,
    V: Value + 'static,
{
    pub(super) fn new<'r, KR>(
        helper: &'m mut MutateHelper<'a, 'b, K, V>,
        range: &'_ impl RangeBounds<KR>,
    ) -> Result<Self>
    where
        KR: Borrow<K::SelfType<'r>> + 'r,
    {
        let state = CursorMutState::new::<K, V, KR, _>(helper, range, CursorDirection::Forward)?;
        Ok(Self { helper, state })
    }

    pub(super) fn next(&mut self) -> Result<Option<BtreeCursorEntryRef<'_>>> {
        self.state.next_ref::<K, V>(self.helper)
    }

    pub(super) fn remove_prev(&mut self) -> Result<bool> {
        Ok(self.state.remove_prev())
    }

    pub(super) fn close(&mut self) -> Result {
        self.state.close::<K, V>(self.helper)
    }
}

pub(crate) struct BtreeRangeCursorMut<'a, K: Key + 'static, V: Value + 'static> {
    root: &'a mut Option<BtreeHeader>,
    page_allocator: crate::tree_store::PageAllocator,
    allocated: Arc<Mutex<PageTrackerPolicy>>,
    master_free_list: Arc<Mutex<Vec<PageNumber>>>,
    free_on_drop: Vec<PageNumber>,
    lower_bound: Bound<Vec<u8>>,
    upper_bound: Bound<Vec<u8>>,
    active: Option<(CursorDirection, CursorMutState)>,
    _key_type: std::marker::PhantomData<K>,
    _value_type: std::marker::PhantomData<V>,
}

impl<'a, K, V> BtreeRangeCursorMut<'a, K, V>
where
    K: Key + 'static,
    V: Value + 'static,
{
    pub(crate) fn new<'r, KR>(
        root: &'a mut Option<BtreeHeader>,
        range: &'_ impl RangeBounds<KR>,
        page_allocator: crate::tree_store::PageAllocator,
        allocated: Arc<Mutex<PageTrackerPolicy>>,
        master_free_list: Arc<Mutex<Vec<PageNumber>>>,
    ) -> Self
    where
        KR: Borrow<K::SelfType<'r>> + 'r,
    {
        Self {
            root,
            page_allocator,
            allocated,
            master_free_list,
            free_on_drop: vec![],
            lower_bound: CursorMutState::owned_bound::<K, KR>(range.start_bound()),
            upper_bound: CursorMutState::owned_bound::<K, KR>(range.end_bound()),
            active: None,
            _key_type: std::marker::PhantomData,
            _value_type: std::marker::PhantomData,
        }
    }

    pub(crate) fn next(&mut self) -> Result<Option<BtreeCursorEntry>> {
        self.next_from(CursorDirection::Forward)
    }

    pub(crate) fn next_back(&mut self) -> Result<Option<BtreeCursorEntry>> {
        self.next_from(CursorDirection::Backward)
    }

    pub(crate) fn remove_prev(&mut self) -> bool {
        let Some((_, state)) = self.active.as_mut() else {
            return false;
        };
        state.remove_prev()
    }

    pub(crate) fn close(&mut self) -> Result {
        if let Some((_, mut state)) = self.active.take() {
            let mut helper = self.helper();
            state.close::<K, V>(&mut helper)?;
        }
        self.free_pending_pages();
        Ok(())
    }

    fn next_from(&mut self, direction: CursorDirection) -> Result<Option<BtreeCursorEntry>> {
        self.activate(direction)?;
        let Some((_, state)) = self.active.as_mut() else {
            return Ok(None);
        };
        let result = {
            let mut helper = MutateHelper::new_do_not_modify(
                self.root,
                self.page_allocator.clone(),
                &mut self.free_on_drop,
                self.allocated.clone(),
            );
            state.next_owned::<K, V>(&mut helper)?
        };
        if let Some(entry) = &result {
            let key = entry.key_data::<K, V>();
            match direction {
                CursorDirection::Forward => self.lower_bound = Bound::Excluded(key),
                CursorDirection::Backward => self.upper_bound = Bound::Excluded(key),
            }
        }
        Ok(result)
    }

    fn activate(&mut self, direction: CursorDirection) -> Result {
        if matches!(self.active, Some((active, _)) if active == direction) {
            return Ok(());
        }
        if let Some((_, mut state)) = self.active.take() {
            let mut helper = self.helper();
            state.close::<K, V>(&mut helper)?;
        }
        if bounds_are_empty::<K>(&self.lower_bound, &self.upper_bound) {
            return Ok(());
        }
        let lower_bound = self.lower_bound.clone();
        let upper_bound = self.upper_bound.clone();
        let mut helper = self.helper();
        let state = CursorMutState::new_from_bounds::<K, V>(
            &mut helper,
            lower_bound,
            upper_bound,
            direction,
        )?;
        self.active = Some((direction, state));
        Ok(())
    }

    fn helper(&mut self) -> MutateHelper<'_, '_, K, V> {
        MutateHelper::new_do_not_modify(
            self.root,
            self.page_allocator.clone(),
            &mut self.free_on_drop,
            self.allocated.clone(),
        )
    }

    fn free_pending_pages(&mut self) {
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

impl<K: Key + 'static, V: Value + 'static> Drop for BtreeRangeCursorMut<'_, K, V> {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

struct CursorMutState {
    direction: CursorDirection,
    lower_bound: Bound<Vec<u8>>,
    upper_bound: Bound<Vec<u8>>,
    path: Vec<CursorBranch>,
    leaf: Option<CursorLeaf>,
    removed_indexes: Vec<usize>,
    buffered_leaf_group: Option<BufferedRetainLeafGroup>,
    last_yielded: Option<(PageNumber, usize)>,
    finished: bool,
}

impl CursorMutState {
    fn new<'r, K, V, KR, R>(
        helper: &mut MutateHelper<'_, '_, K, V>,
        range: &R,
        direction: CursorDirection,
    ) -> Result<Self>
    where
        K: Key + 'static,
        V: Value + 'static,
        KR: Borrow<K::SelfType<'r>> + 'r,
        R: RangeBounds<KR> + ?Sized,
    {
        Self::new_from_bounds::<K, V>(
            helper,
            Self::owned_bound::<K, KR>(range.start_bound()),
            Self::owned_bound::<K, KR>(range.end_bound()),
            direction,
        )
        .map(|mut state| {
            state.finished |= range_is_empty::<K, KR, R>(range);
            state
        })
    }

    fn new_from_bounds<K, V>(
        helper: &mut MutateHelper<'_, '_, K, V>,
        lower_bound: Bound<Vec<u8>>,
        upper_bound: Bound<Vec<u8>>,
        direction: CursorDirection,
    ) -> Result<Self>
    where
        K: Key + 'static,
        V: Value + 'static,
    {
        let finished = bounds_are_empty::<K>(&lower_bound, &upper_bound);
        let mut state = Self {
            direction,
            lower_bound,
            upper_bound,
            path: vec![],
            leaf: None,
            removed_indexes: vec![],
            buffered_leaf_group: None,
            last_yielded: None,
            finished,
        };
        if !state.finished {
            let seek_bound = match direction {
                CursorDirection::Forward => state.lower_bound.clone(),
                CursorDirection::Backward => state.upper_bound.clone(),
            };
            state.seek::<K, V>(helper, seek_bound)?;
        }
        Ok(state)
    }

    fn owned_bound<'r, K, KR>(bound: Bound<&KR>) -> Bound<Vec<u8>>
    where
        K: Key + 'r,
        KR: Borrow<K::SelfType<'r>>,
    {
        match bound {
            Bound::Included(value) => {
                Bound::Included(K::as_bytes(value.borrow()).as_ref().to_vec())
            }
            Bound::Excluded(value) => {
                Bound::Excluded(K::as_bytes(value.borrow()).as_ref().to_vec())
            }
            Bound::Unbounded => Bound::Unbounded,
        }
    }

    fn seek<K, V>(
        &mut self,
        helper: &mut MutateHelper<'_, '_, K, V>,
        bound: Bound<Vec<u8>>,
    ) -> Result
    where
        K: Key + 'static,
        V: Value + 'static,
    {
        assert!(self.buffered_leaf_group.is_none());
        match self.direction {
            CursorDirection::Forward => self.lower_bound = bound.clone(),
            CursorDirection::Backward => self.upper_bound = bound.clone(),
        }
        self.path.clear();
        self.leaf = None;
        let Some(header) = *helper.root else {
            self.finished = true;
            return Ok(());
        };
        let page = helper
            .page_allocator
            .get_page(header.root, PageHint::None)?;
        self.descend_to_bound::<K, V>(helper, page, bound)
    }

    fn descend_to_bound<K, V>(
        &mut self,
        helper: &mut MutateHelper<'_, '_, K, V>,
        mut page: PageImpl,
        bound: Bound<Vec<u8>>,
    ) -> Result
    where
        K: Key + 'static,
        V: Value + 'static,
    {
        loop {
            match page.memory()[0] {
                LEAF => {
                    self.set_leaf::<K, V>(page);
                    return Ok(());
                }
                BRANCH => {
                    let (child_index, child_page) = {
                        let accessor = BranchAccessor::new(&page, K::fixed_width());
                        let unbounded_child = match self.direction {
                            CursorDirection::Forward => 0,
                            CursorDirection::Backward => accessor.count_children() - 1,
                        };
                        let child_index = child_index_for_bound::<K>(
                            &accessor,
                            bound.as_ref().map(Vec::as_slice),
                            unbounded_child,
                        );
                        (child_index, accessor.child_page(child_index).unwrap())
                    };
                    let current_page = page.get_page_number();
                    self.path.push(CursorBranch {
                        page,
                        current_page,
                        child_index,
                    });
                    page = helper.page_allocator.get_page(child_page, PageHint::None)?;
                }
                _ => unreachable!(),
            }
        }
    }

    fn descend_edge<K, V>(
        &mut self,
        helper: &mut MutateHelper<'_, '_, K, V>,
        mut page: PageImpl,
    ) -> Result
    where
        K: Key + 'static,
        V: Value + 'static,
    {
        loop {
            match page.memory()[0] {
                LEAF => {
                    self.set_leaf::<K, V>(page);
                    return Ok(());
                }
                BRANCH => {
                    let (child_index, child_page) = {
                        let accessor = BranchAccessor::new(&page, K::fixed_width());
                        let child_index = match self.direction {
                            CursorDirection::Forward => 0,
                            CursorDirection::Backward => accessor.count_children() - 1,
                        };
                        (child_index, accessor.child_page(child_index).unwrap())
                    };
                    let current_page = page.get_page_number();
                    self.path.push(CursorBranch {
                        page,
                        current_page,
                        child_index,
                    });
                    page = helper.page_allocator.get_page(child_page, PageHint::None)?;
                }
                _ => unreachable!(),
            }
        }
    }

    fn set_leaf<K: Key, V: Value>(&mut self, page: PageImpl) {
        let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
        let entries = leaf_entries::<K>(
            &accessor,
            self.lower_bound.as_ref().map(Vec::as_slice),
            self.upper_bound.as_ref().map(Vec::as_slice),
        );
        let next_entry = if entries.start < entries.end {
            Some(match self.direction {
                CursorDirection::Forward => entries.start,
                CursorDirection::Backward => entries.end - 1,
            })
        } else {
            None
        };
        self.leaf = Some(CursorLeaf {
            page,
            next_entry,
            start: entries.start,
            end: entries.end,
        });
    }

    fn next_ref<K, V>(
        &mut self,
        helper: &mut MutateHelper<'_, '_, K, V>,
    ) -> Result<Option<BtreeCursorEntryRef<'_>>>
    where
        K: Key + 'static,
        V: Value + 'static,
    {
        loop {
            if self.finished {
                return Ok(None);
            }
            if self.next_entry_is_ready::<K, V>(helper)? {
                break;
            }
        }

        let leaf = self.leaf.as_mut().expect("cursor must have a ready leaf");
        let entry_index = leaf.next_entry.expect("cursor entry must be ready");
        let page_number = leaf.page.get_page_number();
        leaf.next_entry = match self.direction {
            CursorDirection::Forward => {
                let next = entry_index + 1;
                (next < leaf.end).then_some(next)
            }
            CursorDirection::Backward => entry_index
                .checked_sub(1)
                .filter(|next| *next >= leaf.start),
        };
        self.last_yielded = Some((page_number, entry_index));
        Ok(Some(BtreeCursorEntryRef {
            page: &leaf.page,
            entry_index,
        }))
    }

    fn next_owned<K, V>(
        &mut self,
        helper: &mut MutateHelper<'_, '_, K, V>,
    ) -> Result<Option<BtreeCursorEntry>>
    where
        K: Key + 'static,
        V: Value + 'static,
    {
        loop {
            if self.finished {
                return Ok(None);
            }
            if self.next_entry_is_ready::<K, V>(helper)? {
                break;
            }
        }

        let leaf = self.leaf.as_mut().expect("cursor must have a ready leaf");
        let entry_index = leaf.next_entry.expect("cursor entry must be ready");
        let page_number = leaf.page.get_page_number();
        leaf.next_entry = match self.direction {
            CursorDirection::Forward => {
                let next = entry_index + 1;
                (next < leaf.end).then_some(next)
            }
            CursorDirection::Backward => entry_index
                .checked_sub(1)
                .filter(|next| *next >= leaf.start),
        };
        self.last_yielded = Some((page_number, entry_index));
        Ok(Some(BtreeCursorEntry {
            page: leaf.page.clone(),
            entry_index,
        }))
    }

    fn next_entry_is_ready<K, V>(&mut self, helper: &mut MutateHelper<'_, '_, K, V>) -> Result<bool>
    where
        K: Key + 'static,
        V: Value + 'static,
    {
        let Some(leaf) = self.leaf.as_ref() else {
            self.finished = true;
            return Ok(false);
        };
        if leaf.next_entry.is_some() {
            return Ok(true);
        }
        self.finish_current_leaf::<K, V>(helper, None, true)?;
        Ok(false)
    }

    fn remove_prev(&mut self) -> bool {
        let Some((page, index)) = self.last_yielded.take() else {
            return false;
        };
        let Some(leaf) = self.leaf.as_ref() else {
            return false;
        };
        assert_eq!(leaf.page.get_page_number(), page);
        if self
            .removed_indexes
            .last()
            .is_none_or(|last| *last != index)
        {
            self.removed_indexes.push(index);
            if self.direction == CursorDirection::Backward {
                self.removed_indexes.sort_unstable();
            }
        }
        true
    }

    fn close<K, V>(&mut self, helper: &mut MutateHelper<'_, '_, K, V>) -> Result
    where
        K: Key + 'static,
        V: Value + 'static,
    {
        self.finish_current_leaf::<K, V>(helper, None, false)?;
        self.flush_buffered_leaf_group::<K, V>(helper, None, false)?;
        self.finished = true;
        Ok(())
    }

    fn next_bound_after_current_leaf<K: Key>(&self) -> Option<Bound<Vec<u8>>> {
        for frame in self.path.iter().rev() {
            let accessor = BranchAccessor::new(&frame.page, K::fixed_width());
            if frame.child_index + 1 < accessor.count_children() {
                return Some(Bound::Excluded(
                    accessor.key(frame.child_index).unwrap().to_vec(),
                ));
            }
        }
        None
    }

    fn previous_bound_before_current_leaf<K: Key>(&self) -> Option<Bound<Vec<u8>>> {
        for frame in self.path.iter().rev() {
            let accessor = BranchAccessor::new(&frame.page, K::fixed_width());
            if frame.child_index > 0 {
                return Some(Bound::Included(
                    accessor.key(frame.child_index - 1).unwrap().to_vec(),
                ));
            }
        }
        None
    }

    fn move_to_adjacent_leaf<K, V>(&mut self, helper: &mut MutateHelper<'_, '_, K, V>) -> Result
    where
        K: Key + 'static,
        V: Value + 'static,
    {
        assert!(self.buffered_leaf_group.is_none());
        loop {
            let Some(mut frame) = self.path.pop() else {
                self.leaf = None;
                self.finished = true;
                return Ok(());
            };
            let next_child = {
                let accessor = BranchAccessor::new(&frame.page, K::fixed_width());
                match self.direction {
                    CursorDirection::Forward => {
                        let child = frame.child_index + 1;
                        (child < accessor.count_children())
                            .then(|| (child, accessor.child_page(child).unwrap()))
                    }
                    CursorDirection::Backward => frame
                        .child_index
                        .checked_sub(1)
                        .map(|child| (child, accessor.child_page(child).unwrap())),
                }
            };
            if let Some((child_index, child_page)) = next_child {
                frame.child_index = child_index;
                self.path.push(frame);
                let page = helper.page_allocator.get_page(child_page, PageHint::None)?;
                return self.descend_edge::<K, V>(helper, page);
            }
        }
    }

    fn finish_current_leaf<K, V>(
        &mut self,
        helper: &mut MutateHelper<'_, '_, K, V>,
        next_bound: Option<Bound<Vec<u8>>>,
        advance: bool,
    ) -> Result
    where
        K: Key + 'static,
        V: Value + 'static,
    {
        self.last_yielded = None;
        let Some(leaf) = self.leaf.take() else {
            return Ok(());
        };
        if self.removed_indexes.is_empty() {
            if self.buffered_leaf_group.is_some() {
                self.buffer_leaf_for_group::<K, V>(leaf, &[]);
                return self.finish_buffered_leaf_group::<K, V>(helper, next_bound, advance);
            }
            self.leaf = Some(leaf);
            if advance {
                return self.move_to_adjacent_leaf::<K, V>(helper);
            }
            return Ok(());
        }

        let removed_bytes = Self::removed_pair_bytes::<K, V>(&leaf, &self.removed_indexes);
        let preserve_path = self.leaf_rewrite_preserves_path::<K, V>(helper, &leaf, removed_bytes);
        let can_buffer = self.direction == CursorDirection::Forward;
        if can_buffer
            && (self.buffered_leaf_group.is_some()
                || (!preserve_path && self.path.last().is_some()))
        {
            let mut removed_indexes = std::mem::take(&mut self.removed_indexes);
            self.buffer_leaf_for_group::<K, V>(leaf, &removed_indexes);
            removed_indexes.clear();
            self.removed_indexes = removed_indexes;
            return self.finish_buffered_leaf_group::<K, V>(helper, next_bound, advance);
        }
        let next_bound = if preserve_path || !advance {
            next_bound
        } else {
            match self.direction {
                CursorDirection::Forward => {
                    next_bound.or_else(|| self.next_bound_after_current_leaf::<K>())
                }
                CursorDirection::Backward => {
                    next_bound.or_else(|| self.previous_bound_before_current_leaf::<K>())
                }
            }
        };
        let path = std::mem::take(&mut self.path);
        let mut removed_indexes = Vec::new();
        std::mem::swap(&mut removed_indexes, &mut self.removed_indexes);
        let result = if preserve_path {
            self.path = helper.rewrite_cursor_leaf_preserving_path(
                leaf.page,
                path,
                &removed_indexes,
                removed_bytes,
            )?;
            if advance {
                self.move_to_adjacent_leaf::<K, V>(helper)?;
            } else {
                self.finished = true;
            }
            Ok(())
        } else if let Some(bound) = next_bound {
            helper.delete_cursor_leaf(leaf.page, path, &removed_indexes, removed_bytes)?;
            self.seek::<K, V>(helper, bound)?;
            Ok(())
        } else {
            helper.delete_cursor_leaf(leaf.page, path, &removed_indexes, removed_bytes)?;
            self.finished = true;
            Ok(())
        };
        removed_indexes.clear();
        self.removed_indexes = removed_indexes;
        result
    }

    fn finish_buffered_leaf_group<K, V>(
        &mut self,
        helper: &mut MutateHelper<'_, '_, K, V>,
        next_bound: Option<Bound<Vec<u8>>>,
        advance: bool,
    ) -> Result
    where
        K: Key + 'static,
        V: Value + 'static,
    {
        let should_flush = !advance || self.current_leaf_is_last_in_parent::<K>();
        let next_bound = if should_flush && advance {
            next_bound.or_else(|| self.next_bound_after_current_leaf::<K>())
        } else {
            next_bound
        };
        if should_flush {
            self.flush_buffered_leaf_group::<K, V>(helper, next_bound, advance)
        } else if advance {
            self.move_to_next_leaf_in_buffered_parent::<K, V>(helper)
        } else {
            Ok(())
        }
    }

    fn current_leaf_is_last_in_parent<K: Key>(&self) -> bool {
        let Some(frame) = self.path.last() else {
            return true;
        };
        let accessor = BranchAccessor::new(&frame.page, K::fixed_width());
        frame.child_index + 1 == accessor.count_children()
    }

    fn move_to_next_leaf_in_buffered_parent<K, V>(
        &mut self,
        helper: &mut MutateHelper<'_, '_, K, V>,
    ) -> Result
    where
        K: Key + 'static,
        V: Value + 'static,
    {
        assert!(self.direction == CursorDirection::Forward);
        assert!(self.buffered_leaf_group.is_some());
        let Some(frame) = self.path.last_mut() else {
            self.leaf = None;
            self.finished = true;
            return Ok(());
        };
        let child_page = {
            let accessor = BranchAccessor::new(&frame.page, K::fixed_width());
            let child = frame.child_index + 1;
            assert!(child < accessor.count_children());
            frame.child_index = child;
            accessor.child_page(child).unwrap()
        };
        let page = helper.page_allocator.get_page(child_page, PageHint::None)?;
        self.descend_edge::<K, V>(helper, page)
    }

    fn buffer_leaf_for_group<K: Key, V: Value>(
        &mut self,
        leaf: CursorLeaf,
        removed_indexes: &[usize],
    ) {
        let child_index = self
            .path
            .last()
            .expect("buffered retain leaf groups require a parent branch")
            .child_index;
        if self.buffered_leaf_group.is_none() {
            self.buffered_leaf_group =
                Some(BufferedRetainLeafGroup::new(self.path.clone(), child_index));
        }

        let group = self.buffered_leaf_group.as_mut().unwrap();
        debug_assert_eq!(
            group.parent_page(),
            self.path
                .last()
                .expect("buffered retain leaf groups require a parent branch")
                .current_page
        );
        debug_assert!(child_index >= group.start_child_index);
        group.end_child_index = child_index;
        group.removed += removed_indexes.len() as u64;

        group.push_leaf::<K, V>(leaf, removed_indexes);
    }

    fn flush_buffered_leaf_group<K, V>(
        &mut self,
        helper: &mut MutateHelper<'_, '_, K, V>,
        next_bound: Option<Bound<Vec<u8>>>,
        advance: bool,
    ) -> Result
    where
        K: Key + 'static,
        V: Value + 'static,
    {
        let Some(group) = self.buffered_leaf_group.take() else {
            return Ok(());
        };
        self.path.clear();
        self.leaf = None;
        helper.replace_buffered_retain_leaf_group(group)?;
        if advance {
            if let Some(bound) = next_bound {
                self.seek::<K, V>(helper, bound)?;
            } else {
                self.finished = true;
            }
        } else {
            self.finished = true;
        }
        Ok(())
    }

    fn removed_pair_bytes<K: Key, V: Value>(leaf: &CursorLeaf, removed_indexes: &[usize]) -> usize {
        let accessor = LeafAccessor::new(leaf.page.memory(), K::fixed_width(), V::fixed_width());
        removed_indexes
            .iter()
            .map(|&index| accessor.length_of_pairs(index, index + 1))
            .sum()
    }

    fn leaf_rewrite_preserves_path<K, V>(
        &self,
        helper: &MutateHelper<'_, '_, K, V>,
        leaf: &CursorLeaf,
        removed_bytes: usize,
    ) -> bool
    where
        K: Key + 'static,
        V: Value + 'static,
    {
        let accessor = LeafAccessor::new(leaf.page.memory(), K::fixed_width(), V::fixed_width());
        let remaining = accessor.num_pairs() - self.removed_indexes.len();
        if remaining == 0 {
            return false;
        }
        let new_kv_bytes = accessor.length_of_pairs(0, accessor.num_pairs()) - removed_bytes;
        let new_required_bytes = RawLeafBuilder::required_bytes(
            remaining,
            new_kv_bytes,
            K::fixed_width(),
            V::fixed_width(),
        );
        new_required_bytes >= helper.page_allocator.get_page_size() / 3
    }
}

fn bounds_are_empty<K: Key>(lower: &Bound<Vec<u8>>, upper: &Bound<Vec<u8>>) -> bool {
    let (lower, lower_included, upper, upper_included) = match (lower, upper) {
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => return false,
        (Bound::Included(lower), Bound::Included(upper)) => {
            (lower.as_slice(), true, upper.as_slice(), true)
        }
        (Bound::Included(lower), Bound::Excluded(upper)) => {
            (lower.as_slice(), true, upper.as_slice(), false)
        }
        (Bound::Excluded(lower), Bound::Included(upper)) => {
            (lower.as_slice(), false, upper.as_slice(), true)
        }
        (Bound::Excluded(lower), Bound::Excluded(upper)) => {
            (lower.as_slice(), false, upper.as_slice(), false)
        }
    };
    let ordering = K::compare(lower, upper);
    ordering.is_gt() || (ordering.is_eq() && !(lower_included && upper_included))
}
