use crate::AccessGuard;
use crate::Result;
use crate::tree_store::btree_base::{BRANCH, BranchAccessor, LEAF, LeafAccessor, RawLeafBuilder};
use crate::tree_store::btree_iters::{
    EntryGuard, child_to_visit, lower_bound_entry, range_is_empty,
};
use crate::tree_store::btree_mutator::{BufferedRetainLeafGroup, CursorPathEntry, MutateHelper};
use crate::tree_store::page_store::{Page, PageHint, PageImpl};
use crate::tree_store::{BtreeHeader, PageAllocator, PageNumber, PageResolver, PageTrackerPolicy};
use crate::types::{Key, Value};
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::Bound;
use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct Branch {
    page: PageImpl,
    current_page: PageNumber,
    child_index: usize,
}

impl Branch {
    fn new(page: PageImpl, child_index: usize) -> Self {
        let current_page = page.get_page_number();
        Self {
            page,
            current_page,
            child_index,
        }
    }

    fn into_parts(self) -> (PageImpl, usize) {
        (self.page, self.child_index)
    }

    fn into_path_entry(self) -> CursorPathEntry {
        CursorPathEntry::new(self.page, self.current_page, self.child_index)
    }

    fn from_path_entry(entry: CursorPathEntry) -> Self {
        let (page, current_page, child_index) = entry.into_parts();
        Self {
            page,
            current_page,
            child_index,
        }
    }
}

fn descend_edge<K: Key + 'static, V: Value + 'static, F>(
    page: PageImpl,
    high_edge: bool,
    path: &mut Vec<Branch>,
    get_page: &mut F,
) -> Result<(PageImpl, usize)>
where
    // TODO: introduce a trait for this that PageResolver can implement too.
    F: FnMut(PageNumber) -> Result<PageImpl>,
{
    match page.memory()[0] {
        LEAF => {
            let len = {
                let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
                accessor.num_pairs()
            };
            Ok((page, len))
        }
        BRANCH => {
            let accessor = BranchAccessor::new(&page, K::fixed_width());
            let child_index = if high_edge {
                accessor.count_children() - 1
            } else {
                0
            };
            let child_page = accessor.child_page(child_index).unwrap();
            path.push(Branch::new(page, child_index));
            let child = get_page(child_page)?;
            descend_edge::<K, V, F>(child, high_edge, path, get_page)
        }
        _ => unreachable!(),
    }
}

#[derive(Clone)]
struct Leaf {
    page: PageImpl,
    position: usize,
    len: usize,
}

pub(super) struct CursorEntry<'cursor> {
    page: &'cursor PageImpl,
    entry_index: usize,
}

impl<'cursor> CursorEntry<'cursor> {
    fn new(page: &'cursor PageImpl, entry_index: usize) -> Self {
        Self { page, entry_index }
    }

    pub(super) fn entry<K: Key, V: Value>(
        &self,
    ) -> crate::tree_store::btree_base::EntryAccessor<'_> {
        LeafAccessor::new(self.page.memory(), K::fixed_width(), V::fixed_width())
            .entry(self.entry_index)
            .expect("cursor entry must exist")
    }
}

#[derive(Clone)]
pub(super) struct Cursor<K: Key + 'static, V: Value + 'static> {
    root: PageNumber,
    path: Vec<Branch>,
    // Gap cursor position: next() returns the entry at position, and prev()
    // returns the entry before position.
    leaf: Option<Leaf>,
    manager: PageResolver,
    hint: PageHint,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<K: Key + 'static, V: Value + 'static> Cursor<K, V> {
    pub(super) fn new(root: PageNumber, manager: PageResolver, hint: PageHint) -> Self {
        Self {
            root,
            path: vec![],
            leaf: None,
            manager,
            hint,
            _key_type: PhantomData,
            _value_type: PhantomData,
        }
    }

    pub(super) fn seek_to(&mut self, bound: Bound<&[u8]>) -> Result {
        self.path.clear();
        let root_page = self.manager.get_page(self.root, self.hint)?;
        self.descend_to_bound(root_page, bound)
    }

    pub(super) fn seek_to_end(&mut self) -> Result {
        self.path.clear();
        let root_page = self.manager.get_page(self.root, self.hint)?;
        self.descend_edge(root_page, true)
    }

    fn descend_to_bound(&mut self, page: PageImpl, bound: Bound<&[u8]>) -> Result {
        match page.memory()[0] {
            LEAF => {
                let (position, len) = {
                    let accessor =
                        LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
                    (
                        lower_bound_entry::<K>(&accessor, bound),
                        accessor.num_pairs(),
                    )
                };
                self.set_leaf(page, position, len);
                Ok(())
            }
            BRANCH => {
                let (child_index, child_page) = {
                    let accessor = BranchAccessor::new(&page, K::fixed_width());
                    let child_index = child_to_visit::<K>(&accessor, bound, false);
                    (child_index, accessor.child_page(child_index).unwrap())
                };
                self.path.push(Branch::new(page, child_index));
                let page = self.manager.get_page(child_page, self.hint)?;
                self.descend_to_bound(page, bound)
            }
            _ => unreachable!(),
        }
    }

    fn descend_edge(&mut self, page: PageImpl, high_edge: bool) -> Result {
        let manager = &self.manager;
        let hint = self.hint;
        let mut get_page = |page| manager.get_page(page, hint);
        let (page, len) = descend_edge::<K, V, _>(page, high_edge, &mut self.path, &mut get_page)?;
        let position = if high_edge { len } else { 0 };
        self.set_leaf(page, position, len);
        Ok(())
    }

    fn set_leaf(&mut self, page: PageImpl, position: usize, len: usize) {
        self.leaf = Some(Leaf {
            page,
            position,
            len,
        });
    }

    fn move_to_adjacent_leaf(&mut self, forward: bool) -> Result<bool> {
        for index in (0..self.path.len()).rev() {
            let next_child = {
                let frame = &self.path[index];
                let accessor = BranchAccessor::new(&frame.page, K::fixed_width());
                if forward {
                    let child_index = frame.child_index + 1;
                    (child_index < accessor.count_children())
                        .then(|| (child_index, accessor.child_page(child_index).unwrap()))
                } else {
                    frame
                        .child_index
                        .checked_sub(1)
                        .map(|child_index| (child_index, accessor.child_page(child_index).unwrap()))
                }
            };

            if let Some((child_index, child_page)) = next_child {
                self.path[index].child_index = child_index;
                self.path.truncate(index + 1);
                let page = self.manager.get_page(child_page, self.hint)?;
                self.descend_edge(page, !forward)?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(super) fn normalize_forward_gap(&mut self) -> Result {
        let Some(leaf) = self.leaf.as_ref() else {
            return Ok(());
        };
        if leaf.position == leaf.len {
            self.move_to_adjacent_leaf(true)?;
        }
        Ok(())
    }

    pub(super) fn next(&mut self) -> Result<Option<EntryGuard<K, V>>> {
        loop {
            let Some(leaf) = self.leaf.as_ref() else {
                return Ok(None);
            };
            if leaf.position < leaf.len {
                let entry = self.get_entry(leaf.position);
                self.leaf
                    .as_mut()
                    .expect("cursor must be positioned")
                    .position += 1;
                return Ok(Some(entry));
            }

            if !self.move_to_adjacent_leaf(true)? {
                return Ok(None);
            }
        }
    }

    pub(super) fn prev(&mut self) -> Result<Option<EntryGuard<K, V>>> {
        loop {
            let Some(leaf) = self.leaf.as_ref() else {
                return Ok(None);
            };
            if leaf.position > 0 {
                let entry = leaf.position - 1;
                self.leaf
                    .as_mut()
                    .expect("cursor must be positioned")
                    .position = entry;
                return Ok(Some(self.get_entry(entry)));
            }

            if !self.move_to_adjacent_leaf(false)? {
                return Ok(None);
            }
        }
    }

    fn page_number(&self) -> PageNumber {
        self.leaf
            .as_ref()
            .expect("cursor must be positioned")
            .page
            .get_page_number()
    }

    fn position(&self) -> usize {
        self.leaf
            .as_ref()
            .expect("cursor must be positioned")
            .position
    }

    pub(super) fn compare_position(&self, other: &Self) -> Ordering {
        let self_page = self.page_number();
        let other_page = other.page_number();
        if self_page == other_page {
            return self.position().cmp(&other.position());
        }

        assert_eq!(self.path.len(), other.path.len());
        for (self_frame, other_frame) in self.path.iter().zip(&other.path) {
            match self_frame.child_index.cmp(&other_frame.child_index) {
                Ordering::Equal => {}
                ordering => return ordering,
            }
        }
        unreachable!("distinct cursor pages must diverge in their branch path")
    }

    fn get_entry(&self, entry: usize) -> EntryGuard<K, V> {
        let leaf = self.leaf.as_ref().expect("cursor must be positioned");
        let (key, value) =
            LeafAccessor::new(leaf.page.memory(), K::fixed_width(), V::fixed_width())
                .entry_ranges(entry)
                .expect("cursor entry must exist");
        EntryGuard::new(leaf.page.clone(), key, value)
    }
}

#[derive(Copy, Clone)]
pub(super) enum CursorMutPosition {
    Start,
    End,
}

pub(super) struct CursorMut<'a, 'b, K: Key + 'static, V: Value + 'static> {
    root: &'b mut Option<BtreeHeader>,
    page_allocator: &'b PageAllocator,
    freed: &'b mut Vec<PageNumber>,
    allocated: &'b Arc<Mutex<PageTrackerPolicy>>,
    right_bound: Bound<Vec<u8>>,
    path: Vec<Branch>,
    leaf: Option<Leaf>,
    removed_indexes: Vec<usize>,
    buffered_leaf_group: Option<BufferedRetainLeafGroup>,
    finished: bool,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
    _lifetime: PhantomData<&'a ()>,
}

impl<'a, 'b, K: Key + 'static, V: Value + 'static> CursorMut<'a, 'b, K, V> {
    pub(super) fn new(
        root: &'b mut Option<BtreeHeader>,
        page_allocator: &'b PageAllocator,
        freed: &'b mut Vec<PageNumber>,
        allocated: &'b Arc<Mutex<PageTrackerPolicy>>,
    ) -> Self {
        Self {
            root,
            page_allocator,
            freed,
            allocated,
            right_bound: Bound::Unbounded,
            path: vec![],
            leaf: None,
            removed_indexes: vec![],
            buffered_leaf_group: None,
            finished: false,
            _key_type: PhantomData,
            _value_type: PhantomData,
            _lifetime: PhantomData,
        }
    }

    pub(super) fn seek_to(&mut self, position: CursorMutPosition) -> Result {
        assert!(self.buffered_leaf_group.is_none());
        self.path.clear();
        self.leaf = None;
        self.right_bound = Bound::Unbounded;
        self.finished = false;
        let Some(header) = *self.root else {
            self.finished = true;
            return Ok(());
        };
        let root = self.page_allocator.get_page(header.root, PageHint::None)?;
        let high_edge = matches!(position, CursorMutPosition::End);
        let page_allocator = self.page_allocator;
        let mut get_page = |page| page_allocator.get_page(page, PageHint::None);
        let (page, len) = descend_edge::<K, V, _>(root, high_edge, &mut self.path, &mut get_page)?;
        let position = if high_edge { len } else { 0 };
        self.leaf = Some(Leaf {
            page,
            position,
            len,
        });
        Ok(())
    }

    pub(super) fn seek_to_range<'r, KR>(&mut self, range: &'_ impl RangeBounds<KR>) -> Result
    where
        KR: Borrow<K::SelfType<'r>> + 'r,
    {
        assert!(self.buffered_leaf_group.is_none());
        let left_bound = Self::owned_bound(range.start_bound());
        self.right_bound = Self::owned_bound(range.end_bound());
        self.finished = range_is_empty::<K, KR, _>(range);
        self.path.clear();
        self.leaf = None;
        if self.finished {
            return Ok(());
        }
        self.seek(left_bound)
    }

    fn owned_bound<'r, KR>(bound: Bound<&KR>) -> Bound<Vec<u8>>
    where
        KR: Borrow<K::SelfType<'r>> + 'r,
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

    fn seek(&mut self, bound: Bound<Vec<u8>>) -> Result {
        assert!(self.buffered_leaf_group.is_none());
        self.path.clear();
        self.leaf = None;
        let Some(header) = *self.root else {
            self.finished = true;
            return Ok(());
        };
        let page = self.page_allocator.get_page(header.root, PageHint::None)?;
        self.descend_to_bound(page, bound)
    }

    fn descend_to_bound(&mut self, mut page: PageImpl, bound: Bound<Vec<u8>>) -> Result {
        loop {
            match page.memory()[0] {
                LEAF => {
                    let accessor =
                        LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
                    let entry_index =
                        lower_bound_entry::<K>(&accessor, bound.as_ref().map(Vec::as_slice));
                    let len = accessor.num_pairs();
                    self.leaf = Some(Leaf {
                        page,
                        position: entry_index,
                        len,
                    });
                    return Ok(());
                }
                BRANCH => {
                    let (child_index, child_page) = {
                        let accessor = BranchAccessor::new(&page, K::fixed_width());
                        let child_index = child_to_visit::<K>(
                            &accessor,
                            bound.as_ref().map(Vec::as_slice),
                            false,
                        );
                        (child_index, accessor.child_page(child_index).unwrap())
                    };
                    self.path.push(Branch::new(page, child_index));
                    page = self.page_allocator.get_page(child_page, PageHint::None)?;
                }
                _ => unreachable!(),
            }
        }
    }

    fn descend_leftmost(&mut self, mut page: PageImpl) -> Result {
        loop {
            match page.memory()[0] {
                LEAF => {
                    let len = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width())
                        .num_pairs();
                    self.leaf = Some(Leaf {
                        page,
                        position: 0,
                        len,
                    });
                    return Ok(());
                }
                BRANCH => {
                    let child_page = {
                        let accessor = BranchAccessor::new(&page, K::fixed_width());
                        accessor.child_page(0).unwrap()
                    };
                    self.path.push(Branch::new(page, 0));
                    page = self.page_allocator.get_page(child_page, PageHint::None)?;
                }
                _ => unreachable!(),
            }
        }
    }

    pub(super) fn peek_next(&mut self) -> Result<Option<CursorEntry<'_>>> {
        loop {
            if self.finished {
                return Ok(None);
            }
            if self.next_entry_is_ready()? {
                break;
            }
        }

        let leaf = self.leaf.as_ref().expect("cursor must have a ready leaf");
        Ok(Some(CursorEntry::new(&leaf.page, leaf.position)))
    }

    fn next_entry_is_ready(&mut self) -> Result<bool> {
        let Some(leaf) = self.leaf.as_ref() else {
            self.finished = true;
            return Ok(false);
        };
        if leaf.position < leaf.len {
            if matches!(self.right_bound, Bound::Unbounded) {
                return Ok(true);
            }
            let accessor =
                LeafAccessor::new(leaf.page.memory(), K::fixed_width(), V::fixed_width());
            let right_bound = self.right_bound.as_ref().map(Vec::as_slice);
            let entry = accessor.entry(leaf.position).unwrap();
            if Self::before_right_bound(right_bound, entry.key()) {
                return Ok(true);
            }
            self.finish_current_leaf(None, false)?;
            self.finished = true;
            return Ok(false);
        }

        self.finish_current_leaf(None, true)?;
        Ok(false)
    }

    pub(super) fn advance_next(&mut self) {
        self.leaf
            .as_mut()
            .expect("cursor must have a ready leaf")
            .position += 1;
    }

    pub(super) fn remove_next_discard(&mut self) {
        let leaf = self.leaf.as_mut().expect("cursor must have a ready leaf");
        assert!(leaf.position < leaf.len);
        if self
            .removed_indexes
            .last()
            .is_none_or(|last| *last != leaf.position)
        {
            self.removed_indexes.push(leaf.position);
        }
        leaf.position += 1;
    }

    pub(super) fn close(&mut self) -> Result {
        self.finish_current_leaf(None, false)?;
        self.flush_buffered_leaf_group(None, false)?;
        self.finished = true;
        Ok(())
    }

    fn before_right_bound(right_bound: Bound<&[u8]>, key: &[u8]) -> bool {
        match right_bound {
            Bound::Included(bound) => K::compare(key, bound).is_le(),
            Bound::Excluded(bound) => K::compare(key, bound).is_lt(),
            Bound::Unbounded => true,
        }
    }

    fn next_bound_after_current_leaf(&self) -> Option<Bound<Vec<u8>>> {
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

    fn move_to_next_leaf(&mut self) -> Result {
        assert!(self.buffered_leaf_group.is_none());
        loop {
            let Some(mut frame) = self.path.pop() else {
                self.leaf = None;
                self.finished = true;
                return Ok(());
            };
            let next_child = {
                let accessor = BranchAccessor::new(&frame.page, K::fixed_width());
                let child = frame.child_index + 1;
                (child < accessor.count_children())
                    .then(|| (child, accessor.child_page(child).unwrap()))
            };
            if let Some((child_index, child_page)) = next_child {
                frame.child_index = child_index;
                self.path.push(frame);
                let page = self.page_allocator.get_page(child_page, PageHint::None)?;
                return self.descend_leftmost(page);
            }
        }
    }

    fn finish_current_leaf(&mut self, next_bound: Option<Bound<Vec<u8>>>, advance: bool) -> Result {
        let Some(leaf) = self.leaf.take() else {
            return Ok(());
        };
        if self.removed_indexes.is_empty() {
            if self.buffered_leaf_group.is_some() {
                self.buffer_leaf_for_group(leaf.page, &[]);
                return self.finish_buffered_leaf_group(next_bound, advance);
            }
            self.leaf = Some(leaf);
            if advance {
                return self.move_to_next_leaf();
            }
            return Ok(());
        }

        let removed_bytes = Self::removed_pair_bytes(&leaf, &self.removed_indexes);
        let preserve_path = self.leaf_rewrite_preserves_path(&leaf, removed_bytes);
        if self.buffered_leaf_group.is_some() || (!preserve_path && self.path.last().is_some()) {
            let mut removed_indexes = std::mem::take(&mut self.removed_indexes);
            self.buffer_leaf_for_group(leaf.page, &removed_indexes);
            removed_indexes.clear();
            self.removed_indexes = removed_indexes;
            return self.finish_buffered_leaf_group(next_bound, advance);
        }

        let next_bound = if preserve_path || !advance {
            next_bound
        } else {
            next_bound.or_else(|| self.next_bound_after_current_leaf())
        };
        let path = std::mem::take(&mut self.path)
            .into_iter()
            .map(Branch::into_path_entry)
            .collect();
        let mut removed_indexes = Vec::new();
        std::mem::swap(&mut removed_indexes, &mut self.removed_indexes);
        let result = if preserve_path {
            let updated_path = {
                let mut helper = self.mutate_helper();
                helper.rewrite_cursor_leaf_preserving_path(
                    leaf.page,
                    path,
                    &removed_indexes,
                    removed_bytes,
                )?
            };
            self.path = updated_path
                .into_iter()
                .map(Branch::from_path_entry)
                .collect();
            if advance {
                self.move_to_next_leaf()?;
            } else {
                self.finished = true;
            }
            Ok(())
        } else if let Some(bound) = next_bound {
            {
                let mut helper = self.mutate_helper();
                helper.delete_cursor_leaf(leaf.page, path, &removed_indexes, removed_bytes)?;
            }
            self.seek(bound)?;
            Ok(())
        } else {
            {
                let mut helper = self.mutate_helper();
                helper.delete_cursor_leaf(leaf.page, path, &removed_indexes, removed_bytes)?;
            }
            self.finished = true;
            Ok(())
        };
        removed_indexes.clear();
        self.removed_indexes = removed_indexes;
        result
    }

    fn finish_buffered_leaf_group(
        &mut self,
        next_bound: Option<Bound<Vec<u8>>>,
        advance: bool,
    ) -> Result {
        let should_flush = !advance || self.current_leaf_is_last_in_parent();
        let next_bound = if should_flush && advance {
            next_bound.or_else(|| self.next_bound_after_current_leaf())
        } else {
            next_bound
        };
        if should_flush {
            self.flush_buffered_leaf_group(next_bound, advance)
        } else if advance {
            self.move_to_next_leaf_in_buffered_parent()
        } else {
            Ok(())
        }
    }

    fn current_leaf_is_last_in_parent(&self) -> bool {
        let Some(frame) = self.path.last() else {
            return true;
        };
        let accessor = BranchAccessor::new(&frame.page, K::fixed_width());
        frame.child_index + 1 == accessor.count_children()
    }

    fn move_to_next_leaf_in_buffered_parent(&mut self) -> Result {
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
        let page = self.page_allocator.get_page(child_page, PageHint::None)?;
        self.descend_leftmost(page)
    }

    fn buffer_leaf_for_group(&mut self, page: PageImpl, removed_indexes: &[usize]) {
        let child_index = self
            .path
            .last()
            .expect("buffered retain leaf groups require a parent branch")
            .child_index;
        if self.buffered_leaf_group.is_none() {
            let path = self
                .path
                .iter()
                .cloned()
                .map(Branch::into_path_entry)
                .collect();
            self.buffered_leaf_group = Some(BufferedRetainLeafGroup::new(path, child_index));
        }

        let group = self.buffered_leaf_group.as_mut().unwrap();
        debug_assert_eq!(
            group.parent_page(),
            self.path
                .last()
                .expect("buffered retain leaf groups require a parent branch")
                .current_page
        );
        group.push_leaf::<K, V>(page, child_index, removed_indexes);
    }

    fn flush_buffered_leaf_group(
        &mut self,
        next_bound: Option<Bound<Vec<u8>>>,
        advance: bool,
    ) -> Result {
        let Some(group) = self.buffered_leaf_group.take() else {
            return Ok(());
        };
        self.path.clear();
        self.leaf = None;
        {
            let mut helper = self.mutate_helper();
            helper.replace_buffered_retain_leaf_group(group)?;
        }
        if advance {
            if let Some(bound) = next_bound {
                self.seek(bound)?;
            } else {
                self.finished = true;
            }
        } else {
            self.finished = true;
        }
        Ok(())
    }

    fn removed_pair_bytes(leaf: &Leaf, removed_indexes: &[usize]) -> usize {
        let accessor = LeafAccessor::new(leaf.page.memory(), K::fixed_width(), V::fixed_width());
        removed_indexes
            .iter()
            .map(|&index| accessor.length_of_pairs(index, index + 1))
            .sum()
    }

    fn leaf_rewrite_preserves_path(&self, leaf: &Leaf, removed_bytes: usize) -> bool {
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
        new_required_bytes >= self.page_allocator.get_page_size() / 3
    }

    pub(super) fn remove_next(
        &mut self,
    ) -> Result<Option<(AccessGuard<'a, K>, AccessGuard<'a, V>)>> {
        let Some(leaf) = self.leaf.take() else {
            self.path.clear();
            return Ok(None);
        };
        if leaf.position == leaf.len {
            self.path.clear();
            return Ok(None);
        }
        self.remove_leaf_entry(leaf.page, leaf.position)
    }

    pub(super) fn remove_prev(
        &mut self,
    ) -> Result<Option<(AccessGuard<'a, K>, AccessGuard<'a, V>)>> {
        let Some(leaf) = self.leaf.take() else {
            self.path.clear();
            return Ok(None);
        };
        let Some(position) = leaf.position.checked_sub(1) else {
            self.path.clear();
            return Ok(None);
        };
        self.remove_leaf_entry(leaf.page, position)
    }

    fn remove_leaf_entry(
        &mut self,
        leaf: PageImpl,
        position: usize,
    ) -> Result<Option<(AccessGuard<'a, K>, AccessGuard<'a, V>)>> {
        assert!(self.buffered_leaf_group.is_none());
        let path = std::mem::take(&mut self.path)
            .into_iter()
            .map(Branch::into_parts)
            .collect();
        let mut helper = MutateHelper::new(
            &mut *self.root,
            (*self.page_allocator).clone(),
            &mut *self.freed,
            Arc::clone(self.allocated),
        );
        Ok(Some(helper.pop_leaf_entry(leaf, path, position)?))
    }

    fn mutate_helper<'c>(&'c mut self) -> MutateHelper<'a, 'c, K, V> {
        MutateHelper::new(
            &mut *self.root,
            (*self.page_allocator).clone(),
            &mut *self.freed,
            Arc::clone(self.allocated),
        )
    }
}
