use crate::AccessGuard;
use crate::Result;
use crate::tree_store::btree_base::{
    BRANCH, BranchAccessor, Checksum, DEFERRED, LEAF, LeafAccessor, OwnedLeafBuilder,
    RawLeafBuilder,
};
use crate::tree_store::btree_iters::EntryGuard;
use crate::tree_store::btree_mutator::MutateHelper;
use crate::tree_store::page_store::{Page, PageHint, PageImpl};
use crate::tree_store::{BtreeHeader, PageAllocator, PageNumber, PageResolver, PageTrackerPolicy};
use crate::types::{Key, Value};
use std::cmp::Ordering;
use std::collections::Bound;
use std::marker::PhantomData;
use std::ops::Range;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct Branch {
    page: PageImpl,
    child_index: usize,
}

impl Branch {
    fn new(page: PageImpl, child_index: usize) -> Self {
        Self { page, child_index }
    }

    fn into_parts(self) -> (PageImpl, usize) {
        (self.page, self.child_index)
    }
}

#[derive(Copy, Clone)]
pub(super) enum Position<'a> {
    // Gap before the first key.
    Start,
    // Gap after the last key.
    End,
    // Gap before `key`, or where `key` would be inserted.
    Before(&'a [u8]),
    // Gap after `key`, or where `key` would be inserted.
    After(&'a [u8]),
}

#[derive(Copy, Clone)]
enum Direction {
    Next,
    Previous,
}

impl Direction {
    fn is_next(self) -> bool {
        matches!(self, Self::Next)
    }
}

fn lower_bound_entry<K: Key>(accessor: &LeafAccessor<'_>, position: Position<'_>) -> usize {
    match position {
        Position::Start => 0,
        Position::End => accessor.num_pairs(),
        Position::Before(query) | Position::After(query) => {
            let (mut position_index, found) = accessor.position::<K>(query);
            if matches!(position, Position::After(_)) && found {
                position_index += 1;
            }
            position_index
        }
    }
}

fn child_to_visit<K: Key>(
    accessor: &BranchAccessor<'_, '_, PageImpl>,
    position: Position<'_>,
) -> usize {
    match position {
        Position::Start => 0,
        Position::End => accessor.count_children() - 1,
        Position::Before(query) | Position::After(query) => accessor.child_for_key::<K>(query).0,
    }
}

fn descend_to_position<K: Key + 'static, V: Value + 'static, F>(
    page: PageImpl,
    position: Position<'_>,
    path: &mut Vec<Branch>,
    get_page: &mut F,
) -> Result<Leaf>
where
    F: FnMut(PageNumber) -> Result<PageImpl>,
{
    match page.memory()[0] {
        LEAF => {
            let (position, len) = {
                let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
                (
                    lower_bound_entry::<K>(&accessor, position),
                    accessor.num_pairs(),
                )
            };
            Ok(Leaf {
                page,
                position,
                len,
            })
        }
        BRANCH => {
            let (child_index, child_page) = {
                let accessor = BranchAccessor::new(&page, K::fixed_width());
                let child_index = child_to_visit::<K>(&accessor, position);
                (child_index, accessor.child_page(child_index).unwrap())
            };
            path.push(Branch::new(page, child_index));
            let child = get_page(child_page)?;
            descend_to_position::<K, V, F>(child, position, path, get_page)
        }
        _ => unreachable!(),
    }
}

fn move_to_adjacent_leaf<K: Key + 'static, V: Value + 'static, F>(
    path: &mut Vec<Branch>,
    direction: Direction,
    get_page: &mut F,
) -> Result<Option<Leaf>>
where
    F: FnMut(PageNumber) -> Result<PageImpl>,
{
    for index in (0..path.len()).rev() {
        let next_child = {
            let frame = &path[index];
            let accessor = BranchAccessor::new(&frame.page, K::fixed_width());
            if direction.is_next() {
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
            path[index].child_index = child_index;
            path.truncate(index + 1);
            let page = get_page(child_page)?;
            let edge = if direction.is_next() {
                Position::Start
            } else {
                Position::End
            };
            return descend_to_position::<K, V, F>(page, edge, path, get_page).map(Some);
        }
    }

    Ok(None)
}

fn prepare_leaf<K: Key + 'static, V: Value + 'static, F>(
    leaf: &mut Option<Leaf>,
    path: &mut Vec<Branch>,
    direction: Direction,
    get_page: &mut F,
) -> Result<bool>
where
    F: FnMut(PageNumber) -> Result<PageImpl>,
{
    while let Some(current) = leaf.as_ref() {
        if (direction.is_next() && current.position < current.len)
            || (!direction.is_next() && current.position > 0)
        {
            return Ok(true);
        }
        let Some(next_leaf) = move_to_adjacent_leaf::<K, V, F>(path, direction, get_page)? else {
            return Ok(false);
        };
        *leaf = Some(next_leaf);
    }

    Ok(false)
}

fn entry<K: Key + 'static, V: Value + 'static>(leaf: &Leaf, position: usize) -> EntryGuard<K, V> {
    let (key, value) = LeafAccessor::new(leaf.page.memory(), K::fixed_width(), V::fixed_width())
        .entry_ranges(position)
        .expect("cursor entry must exist");
    EntryGuard::new(leaf.page.clone(), key, value)
}

fn entry_ref<K: Key + 'static, V: Value + 'static>(
    leaf: &Leaf,
    position: usize,
) -> EntryRef<'_, K, V> {
    let (key_range, value_range) =
        LeafAccessor::new(leaf.page.memory(), K::fixed_width(), V::fixed_width())
            .entry_ranges(position)
            .expect("cursor entry must exist");
    EntryRef {
        page: &leaf.page,
        key_range,
        value_range,
        _key_type: PhantomData,
        _value_type: PhantomData,
    }
}

#[derive(Clone)]
struct Leaf {
    page: PageImpl,
    position: usize,
    len: usize,
}

// A contiguous run of leaf children under one parent branch. CursorMut buffers
// the run when deleting from a leaf would leave it sparse, then replaces it
// with packed leaves in one parent update.
struct LeafRunRewrite<'a> {
    parent_page: PageNumber,
    replaced_children: Range<usize>,
    replacement_leaves: Vec<(PageNumber, Checksum, Vec<u8>)>,
    removed_pairs: u64,
    builder: OwnedLeafBuilder<'a>,
    page_allocator: &'a PageAllocator,
    allocated: &'a Mutex<PageTrackerPolicy>,
    fixed_key_size: Option<usize>,
    fixed_value_size: Option<usize>,
}

impl<'a> LeafRunRewrite<'a> {
    fn new(
        page_allocator: &'a PageAllocator,
        allocated: &'a Mutex<PageTrackerPolicy>,
        parent_page: PageNumber,
        child_index: usize,
        fixed_key_size: Option<usize>,
        fixed_value_size: Option<usize>,
    ) -> Self {
        Self {
            parent_page,
            replaced_children: child_index..child_index,
            replacement_leaves: vec![],
            removed_pairs: 0,
            builder: OwnedLeafBuilder::new(
                page_allocator,
                allocated,
                fixed_key_size,
                fixed_value_size,
            ),
            page_allocator,
            allocated,
            fixed_key_size,
            fixed_value_size,
        }
    }

    fn append_entries_from<K: Key, V: Value>(
        &mut self,
        page: PageImpl,
        child_index: usize,
        removed_indexes: &[usize],
    ) -> Result {
        debug_assert!(removed_indexes.windows(2).all(|pair| pair[0] < pair[1]));
        debug_assert_eq!(child_index, self.replaced_children.end);
        self.replaced_children.end += 1;
        self.removed_pairs += removed_indexes.len() as u64;

        {
            let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
            debug_assert!(
                removed_indexes
                    .last()
                    .is_none_or(|index| *index < accessor.num_pairs())
            );
            let mut next_removed = 0;
            for index in 0..accessor.num_pairs() {
                if next_removed < removed_indexes.len() && removed_indexes[next_removed] == index {
                    next_removed += 1;
                    continue;
                }
                let entry = accessor.entry(index).unwrap();
                if self.builder.would_split_with(entry.key(), entry.value()) {
                    self.flush_current_leaf()?;
                }
                // TODO: This copies retained bytes into the owned builder and
                // then again into the replacement page. If this shows up in
                // profiles, keep source pages plus retained indexes here and
                // copy directly when building each replacement leaf.
                self.builder.push(entry.key(), entry.value());
            }
            debug_assert_eq!(next_removed, removed_indexes.len());
        }

        Ok(())
    }

    fn flush_current_leaf(&mut self) -> Result {
        if self.builder.is_empty() {
            return Ok(());
        }
        let upper_key = self.builder.last_key().to_vec();
        let next_builder = OwnedLeafBuilder::new(
            self.page_allocator,
            self.allocated,
            self.fixed_key_size,
            self.fixed_value_size,
        );
        let builder = std::mem::replace(&mut self.builder, next_builder);
        let page = builder.build()?;
        self.replacement_leaves
            .push((page.get_page_number(), DEFERRED, upper_key));
        Ok(())
    }
}

pub(super) struct EntryRef<'a, K: Key + 'static, V: Value + 'static> {
    page: &'a PageImpl,
    key_range: Range<usize>,
    value_range: Range<usize>,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<K: Key + 'static, V: Value + 'static> EntryRef<'_, K, V> {
    pub(super) fn key_bytes(&self) -> &[u8] {
        &self.page.memory()[self.key_range.clone()]
    }

    pub(super) fn key(&self) -> K::SelfType<'_> {
        K::from_bytes(&self.page.memory()[self.key_range.clone()])
    }

    pub(super) fn value(&self) -> V::SelfType<'_> {
        V::from_bytes(&self.page.memory()[self.value_range.clone()])
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

    pub(super) fn seek_to(&mut self, position: Position<'_>) -> Result {
        self.path.clear();
        let root_page = self.manager.get_page(self.root, self.hint)?;
        let Self {
            manager,
            hint,
            path,
            leaf,
            ..
        } = self;
        let mut get_page = |page| manager.get_page(page, *hint);
        *leaf = Some(descend_to_position::<K, V, _>(
            root_page,
            position,
            path,
            &mut get_page,
        )?);
        Ok(())
    }

    fn ensure_has_entry(&mut self, direction: Direction) -> Result<bool> {
        let Self {
            manager,
            hint,
            path,
            leaf,
            ..
        } = self;
        let mut get_page = |page| manager.get_page(page, *hint);
        prepare_leaf::<K, V, _>(leaf, path, direction, &mut get_page)
    }

    pub(super) fn normalize_forward_gap(&mut self) -> Result {
        if self
            .leaf
            .as_ref()
            .is_none_or(|leaf| leaf.position != leaf.len)
        {
            return Ok(());
        }

        let Self {
            manager,
            hint,
            path,
            leaf,
            ..
        } = self;
        let mut get_page = |page| manager.get_page(page, *hint);
        if let Some(next_leaf) =
            move_to_adjacent_leaf::<K, V, _>(path, Direction::Next, &mut get_page)?
        {
            *leaf = Some(next_leaf);
        }
        Ok(())
    }

    pub(super) fn next(&mut self) -> Result<Option<EntryGuard<K, V>>> {
        if !self.ensure_has_entry(Direction::Next)? {
            return Ok(None);
        }

        let leaf = self.leaf.as_mut().expect("cursor must be positioned");
        let position = leaf.position;
        leaf.position += 1;
        Ok(Some(entry(leaf, position)))
    }

    pub(super) fn prev(&mut self) -> Result<Option<EntryGuard<K, V>>> {
        if !self.ensure_has_entry(Direction::Previous)? {
            return Ok(None);
        }

        let leaf = self.leaf.as_mut().expect("cursor must be positioned");
        leaf.position -= 1;
        Ok(Some(entry(leaf, leaf.position)))
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
}

struct CursorPosition {
    path: Vec<Branch>,
    // Gap cursor position: next operations use `position`, and previous
    // operations use the entry before `position`.
    leaf: Leaf,
}

enum LeafCloseOutcome {
    // The leaf had no pending removals and remains the current cursor leaf.
    Unchanged,
    // The current leaf was rewritten directly; resume from the next range boundary.
    Rewritten {
        resume_bound: Option<Bound<Vec<u8>>>,
    },
    // The current leaf was appended to an open run; continue inside that parent.
    RunContinues {
        path: Vec<Branch>,
    },
    // The current leaf completed an open run; resume from the next range boundary.
    RunFlushed {
        resume_bound: Option<Bound<Vec<u8>>>,
    },
}

pub(super) struct CursorMut<'a, 'b, K: Key + 'static, V: Value + 'static> {
    // Table header state, separate from traversal position.
    root: &'b mut Option<BtreeHeader>,
    page_allocator: &'b PageAllocator,
    freed: &'b mut Vec<PageNumber>,
    allocated: &'b Arc<Mutex<PageTrackerPolicy>>,
    // None means the cursor has not been positioned. Otherwise the ancestor
    // path and current leaf are kept together as one valid gap cursor.
    position: Option<CursorPosition>,
    // Pending removals from the current leaf, recorded in strictly increasing order.
    removed_indexes: Vec<usize>,
    leaf_run_rewrite: Option<LeafRunRewrite<'b>>,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
    _lifetime: PhantomData<&'a ()>,
}

impl CursorPosition {
    fn has_entry(&self, direction: Direction) -> bool {
        match direction {
            Direction::Next => self.leaf.position < self.leaf.len,
            Direction::Previous => self.leaf.position > 0,
        }
    }

    fn entry_index(&self, direction: Direction) -> usize {
        match direction {
            Direction::Next => self.leaf.position,
            Direction::Previous => self.leaf.position - 1,
        }
    }

    fn move_once(&mut self, direction: Direction) {
        match direction {
            Direction::Next => self.leaf.position += 1,
            Direction::Previous => self.leaf.position -= 1,
        }
    }
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
            position: None,
            removed_indexes: vec![],
            leaf_run_rewrite: None,
            _key_type: PhantomData,
            _value_type: PhantomData,
            _lifetime: PhantomData,
        }
    }

    pub(super) fn seek_to(&mut self, target: Position<'_>) -> Result {
        assert!(self.leaf_run_rewrite.is_none());
        assert!(self.removed_indexes.is_empty());
        self.position = None;
        let Some(header) = *self.root else {
            return Ok(());
        };
        let root_page = self.page_allocator.get_page(header.root, PageHint::None)?;
        let page_allocator = self.page_allocator;
        let mut get_page = |page| page_allocator.get_page(page, PageHint::None);
        let mut path = vec![];
        let leaf = descend_to_position::<K, V, _>(root_page, target, &mut path, &mut get_page)?;
        self.position = Some(CursorPosition { path, leaf });
        Ok(())
    }

    // The cursor can be positioned at the edge of a leaf. Before peeking in a
    // direction, move across empty leaf edges until the current leaf has an
    // entry on that side or the cursor reaches the tree edge.
    fn ensure_has_entry(&mut self, direction: Direction) -> Result<bool> {
        loop {
            let Some(position) = self.position.as_ref() else {
                return Ok(false);
            };
            if position.has_entry(direction) {
                return Ok(true);
            }
            if direction.is_next() {
                match self.close_current_leaf()? {
                    LeafCloseOutcome::Unchanged => {
                        if !self.step_to_adjacent_leaf(Direction::Next)? {
                            self.position = None;
                        }
                    }
                    LeafCloseOutcome::Rewritten { resume_bound }
                    | LeafCloseOutcome::RunFlushed { resume_bound } => {
                        self.resume_after_leaf_rewrite(resume_bound)?;
                    }
                    LeafCloseOutcome::RunContinues { path } => self.advance_open_leaf_run(path)?,
                }
                continue;
            }
            assert!(self.leaf_run_rewrite.is_none());
            assert!(self.removed_indexes.is_empty());
            return self.step_to_adjacent_leaf(direction);
        }
    }

    fn step_to_adjacent_leaf(&mut self, direction: Direction) -> Result<bool> {
        let Some(position) = self.position.as_mut() else {
            return Ok(false);
        };
        let page_allocator = self.page_allocator;
        let mut get_page = |page| page_allocator.get_page(page, PageHint::None);
        if let Some(next_leaf) =
            move_to_adjacent_leaf::<K, V, _>(&mut position.path, direction, &mut get_page)?
        {
            position.leaf = next_leaf;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub(super) fn peek_next(&mut self) -> Result<Option<EntryRef<'_, K, V>>> {
        if !self.ensure_has_entry(Direction::Next)? {
            return Ok(None);
        }
        let position = self.position.as_ref().expect("cursor must be positioned");
        Ok(Some(entry_ref(
            &position.leaf,
            position.entry_index(Direction::Next),
        )))
    }

    pub(super) fn peek_prev(&mut self) -> Result<Option<EntryRef<'_, K, V>>> {
        assert!(self.leaf_run_rewrite.is_none());
        assert!(self.removed_indexes.is_empty());
        if !self.ensure_has_entry(Direction::Previous)? {
            return Ok(None);
        }
        let position = self.position.as_ref().expect("cursor must be positioned");
        Ok(Some(entry_ref(
            &position.leaf,
            position.entry_index(Direction::Previous),
        )))
    }

    pub(super) fn next(&mut self) -> Result<bool> {
        if self.peek_next()?.is_none() {
            return Ok(false);
        }
        self.move_cursor(Direction::Next);
        Ok(true)
    }

    pub(super) fn prev(&mut self) -> Result<bool> {
        if self.peek_prev()?.is_none() {
            return Ok(false);
        }
        self.move_cursor(Direction::Previous);
        Ok(true)
    }

    fn move_cursor(&mut self, direction: Direction) {
        self.position
            .as_mut()
            .expect("cursor must be positioned")
            .move_once(direction);
    }

    /// Removes and returns the next entry.
    ///
    /// The returned guards must be dropped before mutating the tree again.
    pub(super) fn remove_next(
        &mut self,
    ) -> Result<Option<(AccessGuard<'a, K>, AccessGuard<'a, V>)>> {
        assert!(self.leaf_run_rewrite.is_none());
        assert!(self.removed_indexes.is_empty());
        if !self.ensure_has_entry(Direction::Next)? {
            return Ok(None);
        }
        let position = self.position.take().expect("cursor must be positioned");
        let index = position.leaf.position;
        self.remove_leaf_entry(position.leaf.page, position.path, index)
    }

    /// Removes and returns the next entry.
    ///
    /// The caller may continue mutating the tree while the returned guards are live.
    pub(super) fn remove_next_detached(
        &mut self,
    ) -> Result<Option<(AccessGuard<'a, K>, AccessGuard<'a, V>)>> {
        assert!(self.leaf_run_rewrite.is_none());
        assert!(self.removed_indexes.is_empty());
        if !self.ensure_has_entry(Direction::Next)? {
            return Ok(None);
        }
        let position = self.position.take().expect("cursor must be positioned");
        let index = position.leaf.position;
        self.remove_leaf_entry_detached(position.leaf.page, position.path, index)
    }

    pub(super) fn remove_next_discard(&mut self) -> Result<bool> {
        if !self.ensure_has_entry(Direction::Next)? {
            return Ok(false);
        }
        let leaf = &mut self
            .position
            .as_mut()
            .expect("cursor must be positioned")
            .leaf;
        assert!(
            self.removed_indexes
                .last()
                .is_none_or(|last| *last < leaf.position),
            "removed indexes must be recorded in strictly increasing order"
        );
        self.removed_indexes.push(leaf.position);
        leaf.position += 1;
        Ok(true)
    }

    /// Removes and returns the previous entry.
    ///
    /// The returned guards must be dropped before mutating the tree again.
    pub(super) fn remove_prev(
        &mut self,
    ) -> Result<Option<(AccessGuard<'a, K>, AccessGuard<'a, V>)>> {
        assert!(self.leaf_run_rewrite.is_none());
        assert!(self.removed_indexes.is_empty());
        if !self.ensure_has_entry(Direction::Previous)? {
            return Ok(None);
        }
        let position = self.position.take().expect("cursor must be positioned");
        let index = position.leaf.position - 1;
        self.remove_leaf_entry(position.leaf.page, position.path, index)
    }

    /// Removes and returns the previous entry.
    ///
    /// The caller may continue mutating the tree while the returned guards are live.
    pub(super) fn remove_prev_detached(
        &mut self,
    ) -> Result<Option<(AccessGuard<'a, K>, AccessGuard<'a, V>)>> {
        assert!(self.leaf_run_rewrite.is_none());
        assert!(self.removed_indexes.is_empty());
        if !self.ensure_has_entry(Direction::Previous)? {
            return Ok(None);
        }
        let position = self.position.take().expect("cursor must be positioned");
        let index = position.leaf.position - 1;
        self.remove_leaf_entry_detached(position.leaf.page, position.path, index)
    }

    pub(super) fn close(&mut self) -> Result {
        if self.position.is_some() {
            match self.close_current_leaf()? {
                LeafCloseOutcome::RunContinues { path } => self.flush_leaf_run_rewrite(path)?,
                LeafCloseOutcome::Unchanged
                | LeafCloseOutcome::Rewritten { .. }
                | LeafCloseOutcome::RunFlushed { .. } => {}
            }
        }
        assert!(self.leaf_run_rewrite.is_none());
        Ok(())
    }

    fn resume_bound_after_path(path: &[Branch]) -> Option<Bound<Vec<u8>>> {
        for frame in path.iter().rev() {
            let accessor = BranchAccessor::new(&frame.page, K::fixed_width());
            if frame.child_index + 1 < accessor.count_children() {
                return Some(Bound::Excluded(
                    accessor
                        .key(frame.child_index)
                        .expect("branch key must exist")
                        .to_vec(),
                ));
            }
        }
        None
    }

    fn close_current_leaf(&mut self) -> Result<LeafCloseOutcome> {
        let position = self.position.take().expect("cursor must be positioned");

        if self.removed_indexes.is_empty() {
            if self.leaf_run_rewrite.is_some() {
                let CursorPosition { path, leaf } = position;
                self.append_entries_from_leaf_to_run(&path, leaf.page, &[])?;
                return self.flush_leaf_run_if_parent_complete(path);
            }

            self.position = Some(position);
            return Ok(LeafCloseOutcome::Unchanged);
        }

        let CursorPosition { path, leaf } = position;
        let removed_bytes = Self::removed_entries_bytes(&leaf, &self.removed_indexes);
        if self.leaf_run_rewrite.is_some()
            || (self.leaf_would_underfill_after_removals(&leaf, removed_bytes)
                && path.last().is_some())
        {
            let removed_indexes = std::mem::take(&mut self.removed_indexes);
            self.append_entries_from_leaf_to_run(&path, leaf.page, &removed_indexes)?;
            self.removed_indexes = removed_indexes;
            self.removed_indexes.clear();
            return self.flush_leaf_run_if_parent_complete(path);
        }

        let resume_bound = Self::resume_bound_after_path(&path);
        let path = path.into_iter().map(Branch::into_parts).collect();
        let mut removed_indexes = std::mem::take(&mut self.removed_indexes);
        {
            let mut helper = self.mutate_helper();
            helper.delete_leaf_entries(leaf.page, path, &removed_indexes)?;
        }
        removed_indexes.clear();
        self.removed_indexes = removed_indexes;

        Ok(LeafCloseOutcome::Rewritten { resume_bound })
    }

    fn flush_leaf_run_if_parent_complete(&mut self, path: Vec<Branch>) -> Result<LeafCloseOutcome> {
        if Self::current_leaf_is_last_in_parent(&path) {
            let resume_bound = Self::resume_bound_after_path(&path);
            self.flush_leaf_run_rewrite(path)?;
            Ok(LeafCloseOutcome::RunFlushed { resume_bound })
        } else {
            Ok(LeafCloseOutcome::RunContinues { path })
        }
    }

    fn current_leaf_is_last_in_parent(path: &[Branch]) -> bool {
        let Some(frame) = path.last() else {
            return true;
        };
        let accessor = BranchAccessor::new(&frame.page, K::fixed_width());
        frame.child_index + 1 == accessor.count_children()
    }

    fn advance_open_leaf_run(&mut self, mut path: Vec<Branch>) -> Result {
        assert!(self.leaf_run_rewrite.is_some());
        let Some(frame) = path.last_mut() else {
            self.position = None;
            return Ok(());
        };
        let child_index = frame.child_index + 1;
        let child_page = {
            let accessor = BranchAccessor::new(&frame.page, K::fixed_width());
            assert!(child_index < accessor.count_children());
            accessor.child_page(child_index).unwrap()
        };
        frame.child_index = child_index;
        let page = self.page_allocator.get_page(child_page, PageHint::None)?;
        let len = {
            let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
            accessor.num_pairs()
        };
        self.position = Some(CursorPosition {
            path,
            leaf: Leaf {
                page,
                position: 0,
                len,
            },
        });
        Ok(())
    }

    fn resume_after_leaf_rewrite(&mut self, resume_bound: Option<Bound<Vec<u8>>>) -> Result {
        if let Some(bound) = resume_bound {
            self.seek_to_resume_bound(bound)
        } else {
            self.position = None;
            Ok(())
        }
    }

    fn append_entries_from_leaf_to_run(
        &mut self,
        path: &[Branch],
        page: PageImpl,
        removed_indexes: &[usize],
    ) -> Result {
        let frame = path
            .last()
            .expect("leaf run rewrites require a parent branch");
        if self.leaf_run_rewrite.is_none() {
            self.leaf_run_rewrite = Some(LeafRunRewrite::new(
                self.page_allocator,
                self.allocated,
                frame.page.get_page_number(),
                frame.child_index,
                K::fixed_width(),
                V::fixed_width(),
            ));
        }

        let rewrite = self.leaf_run_rewrite.as_mut().unwrap();
        debug_assert_eq!(rewrite.parent_page, frame.page.get_page_number());
        rewrite.append_entries_from::<K, V>(page, frame.child_index, removed_indexes)
    }

    fn flush_leaf_run_rewrite(&mut self, path: Vec<Branch>) -> Result {
        let Some(rewrite) = self.leaf_run_rewrite.take() else {
            return Ok(());
        };
        self.position = None;
        {
            let mut rewrite = rewrite;
            rewrite.flush_current_leaf()?;
            let LeafRunRewrite {
                replaced_children,
                replacement_leaves,
                removed_pairs,
                ..
            } = rewrite;
            debug_assert!(!replaced_children.is_empty());
            let mut helper = self.mutate_helper();
            helper.replace_leaf_children(
                path.into_iter().map(Branch::into_parts).collect(),
                replaced_children,
                replacement_leaves,
                removed_pairs,
            )?;
        }
        Ok(())
    }

    fn seek_to_resume_bound(&mut self, bound: Bound<Vec<u8>>) -> Result {
        match bound {
            Bound::Included(key) => self.seek_to(Position::Before(&key)),
            Bound::Excluded(key) => self.seek_to(Position::After(&key)),
            Bound::Unbounded => self.seek_to(Position::Start),
        }
    }

    fn removed_entries_bytes(leaf: &Leaf, removed_indexes: &[usize]) -> usize {
        let accessor = LeafAccessor::new(leaf.page.memory(), K::fixed_width(), V::fixed_width());
        removed_indexes
            .iter()
            .map(|&index| accessor.length_of_pairs(index, index + 1))
            .sum()
    }

    fn leaf_would_underfill_after_removals(&self, leaf: &Leaf, removed_bytes: usize) -> bool {
        let accessor = LeafAccessor::new(leaf.page.memory(), K::fixed_width(), V::fixed_width());
        let remaining = accessor.num_pairs() - self.removed_indexes.len();
        if remaining == 0 {
            return true;
        }
        let new_kv_bytes = accessor.length_of_pairs(0, accessor.num_pairs()) - removed_bytes;
        let new_required_bytes = RawLeafBuilder::required_bytes(
            remaining,
            new_kv_bytes,
            K::fixed_width(),
            V::fixed_width(),
        );
        new_required_bytes < self.page_allocator.get_page_size() / 3
    }

    fn remove_leaf_entry(
        &mut self,
        leaf: PageImpl,
        path: Vec<Branch>,
        index: usize,
    ) -> Result<Option<(AccessGuard<'a, K>, AccessGuard<'a, V>)>> {
        self.remove_leaf_entry_inner(leaf, path, index, true)
    }

    fn remove_leaf_entry_detached(
        &mut self,
        leaf: PageImpl,
        path: Vec<Branch>,
        index: usize,
    ) -> Result<Option<(AccessGuard<'a, K>, AccessGuard<'a, V>)>> {
        self.remove_leaf_entry_inner(leaf, path, index, false)
    }

    fn remove_leaf_entry_inner(
        &mut self,
        leaf: PageImpl,
        path: Vec<Branch>,
        index: usize,
        allow_in_place: bool,
    ) -> Result<Option<(AccessGuard<'a, K>, AccessGuard<'a, V>)>> {
        assert!(self.leaf_run_rewrite.is_none());
        assert!(self.removed_indexes.is_empty());
        let path = path.into_iter().map(Branch::into_parts).collect();
        let mut helper = self.mutate_helper();
        let entry = if allow_in_place {
            helper.pop_leaf_entry(leaf, path, index)?
        } else {
            helper.pop_leaf_entry_detached(leaf, path, index)?
        };
        Ok(Some(entry))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tree_store::btree_base::LeafBuilder;
    use crate::tree_store::{
        AllocationPolicy, InMemoryBackend, PAGE_SIZE, PageTrackerPolicy, TransactionalMemory,
    };

    fn cursor_with_entries(entries: &[u64]) -> Cursor<u64, u64> {
        let mem = TransactionalMemory::new(
            Box::new(InMemoryBackend::new()),
            true,
            PAGE_SIZE,
            None,
            0,
            false,
        )
        .unwrap();
        mem.begin_repair().unwrap();
        let mem = Arc::new(mem);
        let page_allocator = PageAllocator::new(mem, AllocationPolicy::Default);
        let allocated_pages = Mutex::new(PageTrackerPolicy::new_tracking());
        let keys_and_values: Vec<_> = entries
            .iter()
            .map(|entry| {
                (
                    u64::as_bytes(entry).as_ref().to_vec(),
                    u64::as_bytes(entry).as_ref().to_vec(),
                )
            })
            .collect();
        let mut builder = LeafBuilder::new(
            &page_allocator,
            &allocated_pages,
            entries.len(),
            u64::fixed_width(),
            u64::fixed_width(),
        );
        for (key, value) in &keys_and_values {
            builder.push(key, value);
        }
        let page = builder.build().unwrap();
        let root = page.get_page_number();
        drop(page);

        let mut cursor = Cursor::<u64, u64>::new(root, page_allocator.resolver(), PageHint::None);
        cursor.seek_to(Position::Start).unwrap();
        cursor
    }

    #[test]
    fn cursor_preserves_boundary_gap_after_failed_next() {
        let mut cursor = cursor_with_entries(&[1, 2, 3]);

        for expected in [1, 2, 3] {
            assert_eq!(cursor.next().unwrap().unwrap().key(), expected);
        }
        assert!(cursor.next().unwrap().is_none());

        assert_eq!(cursor.prev().unwrap().unwrap().key(), 3);
        assert_eq!(cursor.prev().unwrap().unwrap().key(), 2);
    }

    #[test]
    fn cursor_preserves_boundary_gap_after_failed_prev() {
        let mut cursor = cursor_with_entries(&[1, 2, 3]);

        assert!(cursor.prev().unwrap().is_none());

        assert_eq!(cursor.next().unwrap().unwrap().key(), 1);
        assert_eq!(cursor.next().unwrap().unwrap().key(), 2);
    }
}
