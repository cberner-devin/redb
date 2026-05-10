use crate::tree_store::btree_base::{
    BRANCH, BranchAccessor, BranchBuilder, BranchMutator, Checksum, DEFERRED, LEAF, LeafAccessor,
    LeafBuilder, LeafMutator, RawLeafBuilder,
};
use crate::tree_store::btree_cursor::CursorMut;
use crate::tree_store::btree_mutator::DeletionResult::{
    DeletedBranch, DeletedLeaf, PartialBranch, PartialLeaf, Subtree,
};
use crate::tree_store::page_store::{Page, PageImpl, PageMut};
use crate::tree_store::{
    AccessGuardMutInPlace, BtreeHeader, PageAllocator, PageHint, PageNumber, PageTrackerPolicy,
};
use crate::types::{Key, Value};
use crate::{AccessGuard, Result};
use std::borrow::Borrow;
use std::cmp::{max, min};
use std::marker::PhantomData;
use std::ops::{Range, RangeBounds};
use std::sync::{Arc, Mutex};

// Describes which entry to delete. `Key` navigates via key comparison. `Position`
// is used when a cursor has already found the leaf entry to remove.
#[derive(Copy, Clone)]
enum DeleteTarget<'a> {
    Key(&'a [u8]),
    Position(usize),
}

#[derive(Debug, Copy, Clone)]
enum DeletedPairs<'a> {
    One(usize),
    Many(&'a [usize]),
}

impl DeletedPairs<'_> {
    fn len(self) -> usize {
        match self {
            DeletedPairs::One(_) => 1,
            DeletedPairs::Many(indexes) => indexes.len(),
        }
    }
}

#[derive(Debug)]
enum DeletionResult<'a> {
    // A proper subtree
    Subtree(PageNumber),
    // A leaf with zero children
    DeletedLeaf,
    // A leaf with fewer entries than desired
    PartialLeaf {
        page: Arc<[u8]>,
        deleted_pairs: DeletedPairs<'a>,
    },
    // A branch page subtree with fewer children than desired.
    // Held in unbuilt form: the caller will merge it with a sibling and build a new page,
    // so allocating a page here just to free it again would be wasteful.
    // Checksums are retained because preserved children may be clean pages whose real
    // checksums must be propagated (finalize only recomputes uncommitted pages).
    PartialBranch {
        children: Vec<(PageNumber, Checksum)>,
        keys: Vec<Vec<u8>>,
    },
    // Indicates that the branch node was deleted, and includes the only remaining child.
    // Checksum is retained for the same reason as `PartialBranch`.
    DeletedBranch(PageNumber, Checksum),
}

struct InsertionResult<'a, V: Value + 'static> {
    // the new root page
    new_root: PageNumber,
    // checksum of the root page
    root_checksum: Checksum,
    // Following sibling, if the root had to be split
    additional_sibling: Option<(Vec<u8>, PageNumber, Checksum)>,
    // The inserted value for .insert_reserve() to use
    inserted_value: AccessGuardMutInPlace<'a, V>,
    // The previous value, if any
    old_value: Option<AccessGuard<'a, V>>,
}

#[derive(Clone)]
pub(super) struct CursorPathEntry {
    page: PageImpl,
    current_page: PageNumber,
    child_index: usize,
}

impl CursorPathEntry {
    pub(super) fn new(page: PageImpl, current_page: PageNumber, child_index: usize) -> Self {
        Self {
            page,
            current_page,
            child_index,
        }
    }

    pub(super) fn into_parts(self) -> (PageImpl, PageNumber, usize) {
        (self.page, self.current_page, self.child_index)
    }
}

pub(super) struct BufferedRetainLeaf {
    page: PageImpl,
    retained_spans: Vec<Range<usize>>,
    retained_pairs: usize,
}

impl BufferedRetainLeaf {
    fn new<K: Key, V: Value>(page: PageImpl, removed_indexes: &[usize]) -> Self {
        let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
        let mut retained_spans = vec![];
        let mut start = 0;
        for &removed in removed_indexes {
            if start < removed {
                retained_spans.push(start..removed);
            }
            start = removed + 1;
        }
        if start < accessor.num_pairs() {
            retained_spans.push(start..accessor.num_pairs());
        }

        let retained_pairs = retained_spans.iter().map(Range::len).sum();

        Self {
            page,
            retained_spans,
            retained_pairs,
        }
    }
}

struct RetainLeafPageSpan {
    leaf_index: usize,
    entries: Range<usize>,
}

#[derive(Default)]
struct RetainLeafPagePlan {
    spans: Vec<RetainLeafPageSpan>,
    pairs: usize,
    key_bytes: usize,
    keys_values_bytes: usize,
}

impl RetainLeafPagePlan {
    fn push_entry(
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
                self.spans.push(RetainLeafPageSpan {
                    leaf_index,
                    entries: entry_index..entry_end,
                });
            }
        } else {
            self.spans.push(RetainLeafPageSpan {
                leaf_index,
                entries: entry_index..entry_end,
            });
        }
        self.pairs += 1;
        self.key_bytes += key_len;
        self.keys_values_bytes += key_len + value_len;
    }

    fn clear(&mut self) {
        self.spans.clear();
        self.pairs = 0;
        self.key_bytes = 0;
        self.keys_values_bytes = 0;
    }
}

// Changed leaves under the same parent are rebuilt together so sparse survivors
// can be coalesced and the parent is rewritten once.
pub(super) struct BufferedRetainLeafGroup {
    path: Vec<CursorPathEntry>,
    start_child_index: usize,
    end_child_index: usize,
    leaves: Vec<BufferedRetainLeaf>,
    retained_pairs: usize,
    removed: u64,
}

impl BufferedRetainLeafGroup {
    pub(super) fn new(path: Vec<CursorPathEntry>, child_index: usize) -> Self {
        Self {
            path,
            start_child_index: child_index,
            end_child_index: child_index,
            leaves: vec![],
            retained_pairs: 0,
            removed: 0,
        }
    }

    pub(super) fn parent_page(&self) -> PageNumber {
        self.path
            .last()
            .expect("buffered retain leaf groups require a parent branch")
            .current_page
    }

    pub(super) fn push_leaf<K: Key, V: Value>(
        &mut self,
        page: PageImpl,
        child_index: usize,
        removed_indexes: &[usize],
    ) {
        debug_assert!(child_index >= self.start_child_index);
        self.end_child_index = child_index;
        self.removed += removed_indexes.len() as u64;

        let buffered_leaf = BufferedRetainLeaf::new::<K, V>(page, removed_indexes);
        self.retained_pairs += buffered_leaf.retained_pairs;
        self.leaves.push(buffered_leaf);
    }
}

pub(crate) struct MutateHelper<'a, 'b, K: Key, V: Value> {
    root: &'b mut Option<BtreeHeader>,
    modify_uncommitted: bool,
    page_allocator: PageAllocator,
    freed: &'b mut Vec<PageNumber>,
    allocated: Arc<Mutex<PageTrackerPolicy>>,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
    _lifetime: PhantomData<&'a ()>,
}

impl<'a, 'b, K: Key + 'static, V: Value + 'static> MutateHelper<'a, 'b, K, V> {
    pub(crate) fn new(
        root: &'b mut Option<BtreeHeader>,
        page_allocator: PageAllocator,
        freed: &'b mut Vec<PageNumber>,
        allocated: Arc<Mutex<PageTrackerPolicy>>,
    ) -> Self {
        Self {
            root,
            modify_uncommitted: true,
            page_allocator,
            freed,
            allocated,
            _key_type: PhantomData,
            _value_type: PhantomData,
            _lifetime: PhantomData,
        }
    }

    // Creates a new mutator which will not modify any existing uncommitted pages, or free any existing pages.
    // It will still queue pages for future freeing in the freed vec
    pub(crate) fn new_do_not_modify(
        root: &'b mut Option<BtreeHeader>,
        page_allocator: PageAllocator,
        freed: &'b mut Vec<PageNumber>,
        allocated: Arc<Mutex<PageTrackerPolicy>>,
    ) -> Self {
        Self {
            root,
            modify_uncommitted: false,
            page_allocator,
            freed,
            allocated,
            _key_type: PhantomData,
            _value_type: PhantomData,
            _lifetime: PhantomData,
        }
    }

    fn conditional_free(&mut self, page_number: PageNumber) {
        if self.modify_uncommitted {
            let mut allocated = self.allocated.lock().unwrap();
            if !self
                .page_allocator
                .free_if_uncommitted(page_number, &mut allocated)
            {
                self.freed.push(page_number);
            }
        } else {
            self.freed.push(page_number);
        }
    }

    pub(crate) fn delete(&mut self, key: &K::SelfType<'_>) -> Result<Option<AccessGuard<'a, V>>> {
        let mut found_key = None;
        self.delete_target(DeleteTarget::Key(K::as_bytes(key).as_ref()), &mut found_key)
    }

    pub(super) fn pop_leaf_entry(
        &mut self,
        leaf: PageImpl,
        path: Vec<(PageImpl, usize)>,
        position: usize,
    ) -> Result<(AccessGuard<'a, K>, AccessGuard<'a, V>)> {
        let length = self.root.expect("pop requires a root").length;
        let mut found_key = None;
        let (mut result, found) =
            self.delete_leaf_helper(leaf, DeleteTarget::Position(position), &mut found_key)?;
        for (page, child_index) in path.into_iter().rev() {
            result = self.apply_child_delete_result(page, child_index, result)?;
        }
        *self.root = self.finish_delete_root(result, length - 1)?;
        Ok((
            found_key.expect("deleted key must be returned when requested"),
            found.expect("cursor pop target must exist"),
        ))
    }

    pub(crate) fn retain_in_range<'r, KR, F>(
        &mut self,
        range: &'_ impl RangeBounds<KR>,
        mut predicate: F,
    ) -> Result
    where
        KR: Borrow<K::SelfType<'r>> + 'r,
        F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
    {
        assert!(self.modify_uncommitted);
        let mut cursor: CursorMut<'_, '_, K, V> = CursorMut::new(
            &mut *self.root,
            &self.page_allocator,
            &mut *self.freed,
            &self.allocated,
        );
        cursor.seek_to_range(range)?;
        loop {
            let keep = {
                let Some(entry) = cursor.peek_next()? else {
                    break;
                };
                let entry = entry.entry::<K, V>();
                predicate(K::from_bytes(entry.key()), V::from_bytes(entry.value()))
            };
            if keep {
                cursor.advance_next();
            } else {
                cursor.remove_next_discard();
            }
        }
        cursor.close()?;
        Ok(())
    }

    pub(super) fn delete_cursor_leaf(
        &mut self,
        leaf: PageImpl,
        mut path: Vec<CursorPathEntry>,
        removed_indexes: &[usize],
        removed_bytes: usize,
    ) -> Result {
        let removed = removed_indexes.len() as u64;
        let mut result = self.delete_leaf_indexes(leaf, removed_indexes, removed_bytes)?;
        while let Some(frame) = path.pop() {
            let (page, child_index) = self.current_branch_page(frame)?;
            result = self.delete_branch_child(page, child_index, result)?;
        }

        let Some(header) = *self.root else {
            return Ok(());
        };
        let new_length = header
            .length
            .checked_sub(removed)
            .expect("cursor removed more entries than the tree contains");
        *self.root = self.finish_delete_root(result, new_length)?;
        Ok(())
    }

    pub(super) fn replace_buffered_retain_leaf_group(
        &mut self,
        mut group: BufferedRetainLeafGroup,
    ) -> Result {
        let replacement_leaves = self.build_retain_leaf_pages(&group)?;

        let parent_frame = group
            .path
            .pop()
            .expect("buffered retain leaf groups require a parent branch");
        let (parent_page, _) = self.current_branch_page(parent_frame)?;
        let parent_page_number = parent_page.get_page_number();
        let mut result = {
            let accessor = BranchAccessor::new(&parent_page, K::fixed_width());
            assert!(group.start_child_index <= group.end_child_index);
            assert!(group.end_child_index < accessor.count_children());

            let old_children = accessor.count_children();
            let new_children = old_children - (group.end_child_index - group.start_child_index + 1)
                + replacement_leaves.len();
            if new_children == 0 {
                DeletedLeaf
            } else {
                let mut builder = BranchBuilder::new(
                    &self.page_allocator,
                    &self.allocated,
                    new_children,
                    K::fixed_width(),
                );
                let mut pushed = 0;
                for i in 0..group.start_child_index {
                    builder.push_child(
                        accessor.child_page(i).unwrap(),
                        accessor.child_checksum(i).unwrap(),
                    );
                    pushed += 1;
                    if pushed < new_children {
                        builder.push_key(accessor.key(i).unwrap());
                    }
                }
                for (page, checksum, upper_key) in &replacement_leaves {
                    builder.push_child(*page, *checksum);
                    pushed += 1;
                    if pushed < new_children {
                        builder.push_key(upper_key);
                    }
                }
                for i in (group.end_child_index + 1)..old_children {
                    builder.push_child(
                        accessor.child_page(i).unwrap(),
                        accessor.child_checksum(i).unwrap(),
                    );
                    pushed += 1;
                    if pushed < new_children {
                        builder.push_key(accessor.key(i).unwrap());
                    }
                }
                debug_assert_eq!(pushed, new_children);
                Self::finalize_branch_builder(builder, self.page_allocator.get_page_size())?
            }
        };
        drop(parent_page);
        for leaf in group.leaves {
            let page_number = leaf.page.get_page_number();
            drop(leaf);
            self.conditional_free(page_number);
        }
        self.conditional_free(parent_page_number);

        while let Some(frame) = group.path.pop() {
            let (page, child_index) = self.current_branch_page(frame)?;
            result = self.delete_branch_child(page, child_index, result)?;
        }

        let Some(header) = *self.root else {
            return Ok(());
        };
        let new_length = header
            .length
            .checked_sub(group.removed)
            .expect("cursor removed more entries than the tree contains");
        *self.root = self.finish_delete_root(result, new_length)?;
        Ok(())
    }

    fn build_retain_leaf_pages(
        &mut self,
        group: &BufferedRetainLeafGroup,
    ) -> Result<Vec<(PageNumber, Checksum, Vec<u8>)>> {
        let mut leaves = vec![];
        if group.retained_pairs == 0 {
            return Ok(leaves);
        }

        let mut plan = RetainLeafPagePlan::default();
        for (leaf_index, leaf) in group.leaves.iter().enumerate() {
            let accessor =
                LeafAccessor::new(leaf.page.memory(), K::fixed_width(), V::fixed_width());
            for span in &leaf.retained_spans {
                for entry_index in span.clone() {
                    let entry = accessor.entry(entry_index).unwrap();
                    let key_len = entry.key().len();
                    let value_len = entry.value().len();
                    let next_pairs = plan.pairs + 1;
                    let next_bytes = plan.keys_values_bytes + key_len + value_len;
                    let mut required = RawLeafBuilder::required_bytes(
                        next_pairs,
                        next_bytes,
                        K::fixed_width(),
                        V::fixed_width(),
                    );
                    if plan.pairs > 0 && required > self.page_allocator.get_page_size() {
                        leaves.push(self.build_retain_leaf_page(&plan, &group.leaves)?);
                        plan.clear();
                        required = RawLeafBuilder::required_bytes(
                            1,
                            key_len + value_len,
                            K::fixed_width(),
                            V::fixed_width(),
                        );
                    }

                    plan.push_entry(leaf_index, entry_index, key_len, value_len);
                    if required > self.page_allocator.get_page_size() {
                        leaves.push(self.build_retain_leaf_page(&plan, &group.leaves)?);
                        plan.clear();
                    }
                }
            }
        }
        if plan.pairs > 0 {
            leaves.push(self.build_retain_leaf_page(&plan, &group.leaves)?);
        }
        Ok(leaves)
    }

    fn build_retain_leaf_page(
        &mut self,
        plan: &RetainLeafPagePlan,
        buffered_leaves: &[BufferedRetainLeaf],
    ) -> Result<(PageNumber, Checksum, Vec<u8>)> {
        let last_span = plan
            .spans
            .last()
            .expect("retain leaf page requires at least one span");
        let last_leaf = &buffered_leaves[last_span.leaf_index];
        let last_accessor =
            LeafAccessor::new(last_leaf.page.memory(), K::fixed_width(), V::fixed_width());
        let upper_key = last_accessor
            .entry(last_span.entries.end - 1)
            .expect("retain leaf span entry must exist")
            .key()
            .to_vec();

        let required_size = RawLeafBuilder::required_bytes(
            plan.pairs,
            plan.keys_values_bytes,
            K::fixed_width(),
            V::fixed_width(),
        );
        let mut allocated_pages = self.allocated.lock().unwrap();
        let mut leaf = self
            .page_allocator
            .allocate(required_size, &mut allocated_pages)?;
        let mut builder = RawLeafBuilder::new(
            leaf.memory_mut(),
            plan.pairs,
            K::fixed_width(),
            V::fixed_width(),
            plan.key_bytes,
        );
        for span in &plan.spans {
            let buffered_leaf = &buffered_leaves[span.leaf_index];
            let accessor = LeafAccessor::new(
                buffered_leaf.page.memory(),
                K::fixed_width(),
                V::fixed_width(),
            );
            for entry_index in span.entries.clone() {
                let entry = accessor.entry(entry_index).unwrap();
                builder.append(entry.key(), entry.value());
            }
        }
        drop(builder);
        Ok((leaf.get_page_number(), DEFERRED, upper_key))
    }

    pub(super) fn rewrite_cursor_leaf_preserving_path(
        &mut self,
        leaf: PageImpl,
        mut path: Vec<CursorPathEntry>,
        removed_indexes: &[usize],
        removed_bytes: usize,
    ) -> Result<Vec<CursorPathEntry>> {
        let removed = removed_indexes.len() as u64;
        let mut child_page =
            self.rewrite_leaf_without_rebalance(leaf, removed_indexes, removed_bytes)?;
        let mut updated_path = Vec::with_capacity(path.len());
        while let Some(frame) = path.pop() {
            let CursorPathEntry {
                page: navigation_page,
                current_page,
                child_index,
            } = frame;
            let navigation_page_number = navigation_page.get_page_number();
            let navigation_page_will_stay_valid = current_page != navigation_page_number
                || !self.page_allocator.uncommitted(navigation_page_number);
            let (page, navigation_page) = if current_page == navigation_page_number {
                if navigation_page_will_stay_valid {
                    (navigation_page.clone(), Some(navigation_page))
                } else {
                    (navigation_page, None)
                }
            } else {
                (
                    self.page_allocator.get_page(current_page, PageHint::None)?,
                    Some(navigation_page),
                )
            };
            child_page =
                self.replace_branch_child_preserving_shape(page, child_index, child_page)?;
            let page = navigation_page.map_or_else(
                || self.page_allocator.get_page(child_page, PageHint::None),
                Ok,
            )?;
            updated_path.push(CursorPathEntry::new(page, child_page, child_index));
        }
        updated_path.reverse();

        let Some(header) = *self.root else {
            return Ok(updated_path);
        };
        let new_length = header
            .length
            .checked_sub(removed)
            .expect("cursor removed more entries than the tree contains");
        *self.root = Some(BtreeHeader::new(child_page, DEFERRED, new_length));
        Ok(updated_path)
    }

    fn current_branch_page(&self, frame: CursorPathEntry) -> Result<(PageImpl, usize)> {
        let CursorPathEntry {
            page,
            current_page,
            child_index,
        } = frame;
        if current_page == page.get_page_number() {
            Ok((page, child_index))
        } else {
            Ok((
                self.page_allocator.get_page(current_page, PageHint::None)?,
                child_index,
            ))
        }
    }

    fn rewrite_leaf_without_rebalance(
        &mut self,
        page: PageImpl,
        removed_indexes: &[usize],
        removed_bytes: usize,
    ) -> Result<PageNumber> {
        let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
        assert!(!removed_indexes.is_empty());
        assert!(removed_indexes.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(removed_indexes.last().unwrap() < &accessor.num_pairs());
        assert!(removed_indexes.len() < accessor.num_pairs());

        let remaining = accessor.num_pairs() - removed_indexes.len();
        let new_kv_bytes = accessor.length_of_pairs(0, accessor.num_pairs()) - removed_bytes;
        let new_required_bytes = RawLeafBuilder::required_bytes(
            remaining,
            new_kv_bytes,
            K::fixed_width(),
            V::fixed_width(),
        );
        assert!(new_required_bytes >= self.page_allocator.get_page_size() / 3);

        let old_page = page.get_page_number();
        if self.modify_uncommitted && self.page_allocator.uncommitted(old_page) {
            drop(page);
            let mut page = self.page_allocator.get_page_mut(old_page)?;
            let mut mutator =
                LeafMutator::new(page.memory_mut(), K::fixed_width(), V::fixed_width());
            mutator.remove_indexes(removed_indexes);
            Ok(old_page)
        } else {
            let mut builder = LeafBuilder::new(
                &self.page_allocator,
                &self.allocated,
                remaining,
                K::fixed_width(),
                V::fixed_width(),
            );
            builder.push_all_except_indexes(&accessor, removed_indexes);
            let new_page = builder.build()?;
            drop(page);
            self.conditional_free(old_page);
            Ok(new_page.get_page_number())
        }
    }

    fn replace_branch_child_preserving_shape(
        &mut self,
        page: PageImpl,
        child_index: usize,
        new_child: PageNumber,
    ) -> Result<PageNumber> {
        let accessor = BranchAccessor::new(&page, K::fixed_width());
        let original_page_number = page.get_page_number();
        let child_page_number = accessor.child_page(child_index).unwrap();
        let child_checksum = accessor.child_checksum(child_index).unwrap();
        if new_child == child_page_number && child_checksum == DEFERRED {
            return Ok(original_page_number);
        }

        if self.page_allocator.uncommitted(original_page_number) && self.modify_uncommitted {
            drop(page);
            let mut mutpage = self.page_allocator.get_page_mut(original_page_number)?;
            let mut mutator = BranchMutator::new(mutpage.memory_mut());
            mutator.write_child_page(child_index, new_child, DEFERRED);
            Ok(original_page_number)
        } else {
            let mut builder = BranchBuilder::new(
                &self.page_allocator,
                &self.allocated,
                accessor.count_children(),
                K::fixed_width(),
            );
            builder.push_all(&accessor);
            builder.replace_child(child_index, new_child, DEFERRED);
            let new_page = builder.build()?;
            self.conditional_free(original_page_number);
            Ok(new_page.get_page_number())
        }
    }

    fn finish_delete_root(
        &mut self,
        deletion_result: DeletionResult<'_>,
        new_length: u64,
    ) -> Result<Option<BtreeHeader>> {
        Ok(match deletion_result {
            Subtree(page) => Some(BtreeHeader::new(page, DEFERRED, new_length)),
            DeletedLeaf => None,
            PartialLeaf {
                page,
                deleted_pairs,
            } => {
                let accessor = LeafAccessor::new(&page, K::fixed_width(), V::fixed_width());
                let remaining = accessor.num_pairs() - deleted_pairs.len();
                if remaining == 0 {
                    None
                } else {
                    let mut builder = LeafBuilder::new(
                        &self.page_allocator,
                        &self.allocated,
                        remaining,
                        K::fixed_width(),
                        V::fixed_width(),
                    );
                    Self::push_all_except_deleted(&mut builder, &accessor, deleted_pairs);
                    let page = builder.build()?;
                    assert_eq!(new_length, remaining as u64);
                    Some(BtreeHeader::new(
                        page.get_page_number(),
                        DEFERRED,
                        new_length,
                    ))
                }
            }
            PartialBranch { children, keys } => {
                let mut builder = BranchBuilder::new(
                    &self.page_allocator,
                    &self.allocated,
                    children.len(),
                    K::fixed_width(),
                );
                for (child, child_checksum) in children {
                    builder.push_child(child, child_checksum);
                }
                for key in &keys {
                    builder.push_key(key);
                }
                let page = builder.build()?;
                Some(BtreeHeader::new(
                    page.get_page_number(),
                    DEFERRED,
                    new_length,
                ))
            }
            DeletedBranch(remaining_child, checksum) => {
                Some(BtreeHeader::new(remaining_child, checksum, new_length))
            }
        })
    }

    fn push_all_except_deleted<'leaf>(
        builder: &mut LeafBuilder<'leaf, '_>,
        accessor: &'leaf LeafAccessor<'_>,
        deleted_pairs: DeletedPairs<'_>,
    ) {
        match deleted_pairs {
            DeletedPairs::One(index) => builder.push_all_except(accessor, Some(index)),
            DeletedPairs::Many(indexes) => builder.push_all_except_indexes(accessor, indexes),
        }
    }

    fn delete_leaf_indexes<'p>(
        &mut self,
        page: PageImpl,
        removed_indexes: &'p [usize],
        removed_bytes: usize,
    ) -> Result<DeletionResult<'p>> {
        let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
        assert!(!removed_indexes.is_empty());
        assert!(removed_indexes.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(removed_indexes.last().unwrap() < &accessor.num_pairs());

        let remaining = accessor.num_pairs() - removed_indexes.len();
        let old_page = page.get_page_number();
        let result = if remaining == 0 {
            DeletedLeaf
        } else {
            let new_kv_bytes = accessor.length_of_pairs(0, accessor.num_pairs()) - removed_bytes;
            let new_required_bytes = RawLeafBuilder::required_bytes(
                remaining,
                new_kv_bytes,
                K::fixed_width(),
                V::fixed_width(),
            );
            if new_required_bytes < self.page_allocator.get_page_size() / 3 {
                PartialLeaf {
                    page: page.to_arc(),
                    deleted_pairs: DeletedPairs::Many(removed_indexes),
                }
            } else {
                let mut builder = LeafBuilder::new(
                    &self.page_allocator,
                    &self.allocated,
                    remaining,
                    K::fixed_width(),
                    V::fixed_width(),
                );
                builder.push_all_except_indexes(&accessor, removed_indexes);
                let new_page = builder.build()?;
                Subtree(new_page.get_page_number())
            }
        };
        drop(page);
        self.conditional_free(old_page);
        Ok(result)
    }

    fn delete_branch_child<'p>(
        &mut self,
        page: PageImpl,
        child_index: usize,
        child_result: DeletionResult<'p>,
    ) -> Result<DeletionResult<'p>> {
        self.apply_child_delete_result(page, child_index, child_result)
    }

    fn finalize_branch_builder(
        builder: BranchBuilder<'_, '_>,
        page_size: usize,
    ) -> Result<DeletionResult<'static>> {
        let result = if let Some((only_child, checksum)) = builder.to_single_child() {
            DeletedBranch(only_child, checksum)
        } else if builder.required_bytes() < page_size / 3 {
            // Merge when less than 33% full. Splits occur when a page is full and produce two 50%
            // full pages, so we use 33% instead of 50% to avoid oscillating.
            // Skip the page allocation: the caller will immediately merge this with a sibling.
            let (children, keys) = builder.into_parts();
            PartialBranch { children, keys }
        } else {
            let new_page = builder.build()?;
            Subtree(new_page.get_page_number())
        };
        Ok(result)
    }

    fn apply_child_delete_result<'p>(
        &mut self,
        page: PageImpl,
        child_index: usize,
        child_result: DeletionResult<'p>,
    ) -> Result<DeletionResult<'p>> {
        let accessor = BranchAccessor::new(&page, K::fixed_width());
        let original_page_number = page.get_page_number();
        let child_page_number = accessor.child_page(child_index).unwrap();
        let child_checksum = accessor.child_checksum(child_index).unwrap();

        if let Subtree(new_child) = child_result {
            if new_child == child_page_number && child_checksum == DEFERRED {
                return Ok(Subtree(original_page_number));
            }

            let result_page = if self.page_allocator.uncommitted(original_page_number)
                && self.modify_uncommitted
            {
                drop(page);
                let mut mutpage = self.page_allocator.get_page_mut(original_page_number)?;
                let mut mutator = BranchMutator::new(mutpage.memory_mut());
                mutator.write_child_page(child_index, new_child, DEFERRED);
                original_page_number
            } else {
                let mut builder = BranchBuilder::new(
                    &self.page_allocator,
                    &self.allocated,
                    accessor.count_children(),
                    K::fixed_width(),
                );
                builder.push_all(&accessor);
                builder.replace_child(child_index, new_child, DEFERRED);
                let new_page = builder.build()?;
                self.conditional_free(original_page_number);
                new_page.get_page_number()
            };
            return Ok(Subtree(result_page));
        }

        let mut builder = BranchBuilder::new(
            &self.page_allocator,
            &self.allocated,
            accessor.count_children(),
            K::fixed_width(),
        );

        let final_result = match child_result {
            Subtree(_) => unreachable!(),
            DeletedLeaf => {
                for i in 0..accessor.count_children() {
                    if i == child_index {
                        continue;
                    }
                    builder.push_child(
                        accessor.child_page(i).unwrap(),
                        accessor.child_checksum(i).unwrap(),
                    );
                }
                let end = if child_index == accessor.count_children() - 1 {
                    accessor.count_children() - 2
                } else {
                    accessor.count_children() - 1
                };
                for i in 0..end {
                    if i == child_index {
                        continue;
                    }
                    builder.push_key(accessor.key(i).unwrap());
                }
                Self::finalize_branch_builder(builder, self.page_allocator.get_page_size())?
            }
            PartialLeaf {
                page: partial_child_page,
                deleted_pairs,
            } => {
                let partial_child_accessor =
                    LeafAccessor::new(&partial_child_page, K::fixed_width(), V::fixed_width());
                let partial_child_pairs = partial_child_accessor.num_pairs() - deleted_pairs.len();
                assert!(partial_child_pairs > 0);

                let merge_with = if child_index == 0 { 1 } else { child_index - 1 };
                assert!(merge_with < accessor.count_children());
                let merge_with_page = self
                    .page_allocator
                    .get_page(accessor.child_page(merge_with).unwrap(), PageHint::None)?;

                let single_large_value = {
                    let merge_with_accessor = LeafAccessor::new(
                        merge_with_page.memory(),
                        K::fixed_width(),
                        V::fixed_width(),
                    );
                    merge_with_accessor.num_pairs() == 1
                        && merge_with_accessor.total_length() >= self.page_allocator.get_page_size()
                };
                if single_large_value {
                    let mut child_builder = LeafBuilder::new(
                        &self.page_allocator,
                        &self.allocated,
                        partial_child_pairs,
                        K::fixed_width(),
                        V::fixed_width(),
                    );
                    Self::push_all_except_deleted(
                        &mut child_builder,
                        &partial_child_accessor,
                        deleted_pairs,
                    );
                    let new_page = child_builder.build()?;
                    builder.push_all(&accessor);
                    builder.replace_child(child_index, new_page.get_page_number(), DEFERRED);
                    let result = Self::finalize_branch_builder(
                        builder,
                        self.page_allocator.get_page_size(),
                    )?;

                    drop(page);
                    self.conditional_free(original_page_number);
                    return Ok(result);
                }

                if child_index > 0
                    && self.modify_uncommitted
                    && self.page_allocator.uncommitted(original_page_number)
                    && self
                        .page_allocator
                        .uncommitted(merge_with_page.get_page_number())
                {
                    let merge_with_page_number = merge_with_page.get_page_number();
                    let merge_with_page_copy = merge_with_page.memory().to_vec();
                    let merge_with_copy_accessor = LeafAccessor::new(
                        merge_with_page_copy.as_slice(),
                        K::fixed_width(),
                        V::fixed_width(),
                    );
                    let mut child_builder = LeafBuilder::new(
                        &self.page_allocator,
                        &self.allocated,
                        partial_child_pairs + merge_with_copy_accessor.num_pairs(),
                        K::fixed_width(),
                        V::fixed_width(),
                    );
                    child_builder.push_all_except(&merge_with_copy_accessor, None);
                    Self::push_all_except_deleted(
                        &mut child_builder,
                        &partial_child_accessor,
                        deleted_pairs,
                    );
                    if child_builder.should_split()
                        && child_builder.split_key_len() == accessor.key(merge_with).unwrap().len()
                    {
                        drop(merge_with_page);
                        let merge_page =
                            self.page_allocator.get_page_mut(merge_with_page_number)?;
                        let (split_key, new_page2) =
                            child_builder.build_split_reusing_first(merge_page)?;
                        let new_page2_number = new_page2.get_page_number();
                        drop(new_page2);
                        drop(page);
                        let mut parent_page =
                            self.page_allocator.get_page_mut(original_page_number)?;
                        let mut mutator = BranchMutator::new(parent_page.memory_mut());
                        mutator.write_key(merge_with, split_key, K::fixed_width());
                        mutator.write_child_page(child_index, new_page2_number, DEFERRED);
                        return Ok(Subtree(original_page_number));
                    }
                }

                let merge_with_accessor =
                    LeafAccessor::new(merge_with_page.memory(), K::fixed_width(), V::fixed_width());
                for i in 0..accessor.count_children() {
                    if i == child_index {
                        continue;
                    }
                    let page_number = accessor.child_page(i).unwrap();
                    let page_checksum = accessor.child_checksum(i).unwrap();
                    if i == merge_with {
                        let mut child_builder = LeafBuilder::new(
                            &self.page_allocator,
                            &self.allocated,
                            partial_child_pairs + merge_with_accessor.num_pairs(),
                            K::fixed_width(),
                            V::fixed_width(),
                        );
                        if child_index < merge_with {
                            Self::push_all_except_deleted(
                                &mut child_builder,
                                &partial_child_accessor,
                                deleted_pairs,
                            );
                        }
                        child_builder.push_all_except(&merge_with_accessor, None);
                        if child_index > merge_with {
                            Self::push_all_except_deleted(
                                &mut child_builder,
                                &partial_child_accessor,
                                deleted_pairs,
                            );
                        }
                        if child_builder.should_split() {
                            let (new_page1, split_key, new_page2) = child_builder.build_split()?;
                            builder.push_key(split_key);
                            builder.push_child(new_page1.get_page_number(), DEFERRED);
                            builder.push_child(new_page2.get_page_number(), DEFERRED);
                        } else {
                            let new_page = child_builder.build()?;
                            builder.push_child(new_page.get_page_number(), DEFERRED);
                        }

                        let merged_key_index = max(child_index, merge_with);
                        if merged_key_index < accessor.count_children() - 1 {
                            builder.push_key(accessor.key(merged_key_index).unwrap());
                        }
                    } else {
                        builder.push_child(page_number, page_checksum);
                        if i < accessor.count_children() - 1 {
                            builder.push_key(accessor.key(i).unwrap());
                        }
                    }
                }

                let result =
                    Self::finalize_branch_builder(builder, self.page_allocator.get_page_size())?;
                let page_number = merge_with_page.get_page_number();
                drop(merge_with_page);
                self.conditional_free(page_number);
                result
            }
            DeletedBranch(only_grandchild, grandchild_checksum) => {
                let merge_with = if child_index == 0 { 1 } else { child_index - 1 };
                let merge_with_page = self
                    .page_allocator
                    .get_page(accessor.child_page(merge_with).unwrap(), PageHint::None)?;
                let merge_with_accessor = BranchAccessor::new(&merge_with_page, K::fixed_width());
                assert!(merge_with < accessor.count_children());
                for i in 0..accessor.count_children() {
                    if i == child_index {
                        continue;
                    }
                    let page_number = accessor.child_page(i).unwrap();
                    let page_checksum = accessor.child_checksum(i).unwrap();
                    if i == merge_with {
                        let mut child_builder = BranchBuilder::new(
                            &self.page_allocator,
                            &self.allocated,
                            merge_with_accessor.count_children() + 1,
                            K::fixed_width(),
                        );
                        let separator_key = accessor.key(min(child_index, merge_with)).unwrap();
                        if child_index < merge_with {
                            child_builder.push_child(only_grandchild, grandchild_checksum);
                            child_builder.push_key(separator_key);
                        }
                        child_builder.push_all(&merge_with_accessor);
                        if child_index > merge_with {
                            child_builder.push_key(separator_key);
                            child_builder.push_child(only_grandchild, grandchild_checksum);
                        }
                        if child_builder.should_split() {
                            let (new_page1, separator, new_page2) = child_builder.build_split()?;
                            builder.push_child(new_page1.get_page_number(), DEFERRED);
                            builder.push_key(separator);
                            builder.push_child(new_page2.get_page_number(), DEFERRED);
                        } else {
                            let new_page = child_builder.build()?;
                            builder.push_child(new_page.get_page_number(), DEFERRED);
                        }

                        let merged_key_index = max(child_index, merge_with);
                        if merged_key_index < accessor.count_children() - 1 {
                            builder.push_key(accessor.key(merged_key_index).unwrap());
                        }
                    } else {
                        builder.push_child(page_number, page_checksum);
                        if i < accessor.count_children() - 1 {
                            builder.push_key(accessor.key(i).unwrap());
                        }
                    }
                }
                let result =
                    Self::finalize_branch_builder(builder, self.page_allocator.get_page_size())?;

                let page_number = merge_with_page.get_page_number();
                drop(merge_with_page);
                self.conditional_free(page_number);

                result
            }
            PartialBranch {
                children: partial_children,
                keys: partial_keys,
            } => {
                let merge_with = if child_index == 0 { 1 } else { child_index - 1 };
                let merge_with_page = self
                    .page_allocator
                    .get_page(accessor.child_page(merge_with).unwrap(), PageHint::None)?;
                let merge_with_accessor = BranchAccessor::new(&merge_with_page, K::fixed_width());
                assert!(merge_with < accessor.count_children());
                for i in 0..accessor.count_children() {
                    if i == child_index {
                        continue;
                    }
                    let page_number = accessor.child_page(i).unwrap();
                    let page_checksum = accessor.child_checksum(i).unwrap();
                    if i == merge_with {
                        let mut child_builder = BranchBuilder::new(
                            &self.page_allocator,
                            &self.allocated,
                            merge_with_accessor.count_children() + partial_children.len(),
                            K::fixed_width(),
                        );
                        let separator_key = accessor.key(min(child_index, merge_with)).unwrap();
                        if child_index < merge_with {
                            for &(child, child_checksum) in &partial_children {
                                child_builder.push_child(child, child_checksum);
                            }
                            for key in &partial_keys {
                                child_builder.push_key(key);
                            }
                            child_builder.push_key(separator_key);
                        }
                        child_builder.push_all(&merge_with_accessor);
                        if child_index > merge_with {
                            child_builder.push_key(separator_key);
                            for &(child, child_checksum) in &partial_children {
                                child_builder.push_child(child, child_checksum);
                            }
                            for key in &partial_keys {
                                child_builder.push_key(key);
                            }
                        }
                        if child_builder.should_split() {
                            let (new_page1, separator, new_page2) = child_builder.build_split()?;
                            builder.push_child(new_page1.get_page_number(), DEFERRED);
                            builder.push_key(separator);
                            builder.push_child(new_page2.get_page_number(), DEFERRED);
                        } else {
                            let new_page = child_builder.build()?;
                            builder.push_child(new_page.get_page_number(), DEFERRED);
                        }

                        let merged_key_index = max(child_index, merge_with);
                        if merged_key_index < accessor.count_children() - 1 {
                            builder.push_key(accessor.key(merged_key_index).unwrap());
                        }
                    } else {
                        builder.push_child(page_number, page_checksum);
                        if i < accessor.count_children() - 1 {
                            builder.push_key(accessor.key(i).unwrap());
                        }
                    }
                }
                let result =
                    Self::finalize_branch_builder(builder, self.page_allocator.get_page_size())?;

                let page_number = merge_with_page.get_page_number();
                drop(merge_with_page);
                self.conditional_free(page_number);

                result
            }
        };

        drop(page);
        self.conditional_free(original_page_number);
        Ok(final_result)
    }

    fn delete_target(
        &mut self,
        target: DeleteTarget<'_>,
        found_key: &mut Option<AccessGuard<'a, K>>,
    ) -> Result<Option<AccessGuard<'a, V>>> {
        if let Some(BtreeHeader {
            root: p, length, ..
        }) = *self.root
        {
            let (deletion_result, found) = self.delete_helper(
                self.page_allocator.get_page(p, PageHint::None)?,
                target,
                found_key,
            )?;
            if found.is_none() {
                // The tree was not modified; leave *self.root untouched so that any clean
                // root page keeps its already-valid checksum.
                return Ok(None);
            }
            let new_length = length - 1;
            *self.root = self.finish_delete_root(deletion_result, new_length)?;
            Ok(found)
        } else {
            Ok(None)
        }
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn insert(
        &mut self,
        key: &K::SelfType<'_>,
        value: &V::SelfType<'_>,
    ) -> Result<(Option<AccessGuard<'a, V>>, AccessGuardMutInPlace<'a, V>)> {
        let (new_root, old_value, guard) = if let Some(BtreeHeader {
            root: p,
            checksum,
            length,
        }) = *self.root
        {
            let result = self.insert_helper(
                self.page_allocator.get_page(p, PageHint::None)?,
                checksum,
                K::as_bytes(key).as_ref(),
                V::as_bytes(value).as_ref(),
            )?;

            let new_length = if result.old_value.is_some() {
                length
            } else {
                length + 1
            };

            let new_root = if let Some((key, page2, page2_checksum)) = result.additional_sibling {
                let mut builder =
                    BranchBuilder::new(&self.page_allocator, &self.allocated, 2, K::fixed_width());
                builder.push_child(result.new_root, result.root_checksum);
                builder.push_key(&key);
                builder.push_child(page2, page2_checksum);
                let new_page = builder.build()?;
                BtreeHeader::new(new_page.get_page_number(), DEFERRED, new_length)
            } else {
                BtreeHeader::new(result.new_root, result.root_checksum, new_length)
            };
            (new_root, result.old_value, result.inserted_value)
        } else {
            let key_bytes = K::as_bytes(key);
            let value_bytes = V::as_bytes(value);
            let key_bytes = key_bytes.as_ref();
            let value_bytes = value_bytes.as_ref();
            let mut builder = LeafBuilder::new(
                &self.page_allocator,
                &self.allocated,
                1,
                K::fixed_width(),
                V::fixed_width(),
            );
            builder.push(key_bytes, value_bytes);
            let page = builder.build()?;

            let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
            let offset = accessor.offset_of_first_value();
            let page_num = page.get_page_number();
            let guard = AccessGuardMutInPlace::new(page, offset, value_bytes.len());

            (BtreeHeader::new(page_num, DEFERRED, 1), None, guard)
        };
        *self.root = Some(new_root);
        Ok((old_value, guard))
    }

    fn insert_helper(
        &mut self,
        page: PageImpl,
        page_checksum: Checksum,
        key: &[u8],
        value: &[u8],
    ) -> Result<InsertionResult<'a, V>> {
        let node_mem = page.memory();
        Ok(match node_mem[0] {
            LEAF => {
                let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
                let (position, found) = accessor.position::<K>(key);

                // Fast-path to avoid re-building and splitting pages with a single large value
                let single_large_value = accessor.num_pairs() == 1
                    && accessor.total_length() >= self.page_allocator.get_page_size();
                if !found && single_large_value {
                    let mut builder = LeafBuilder::new(
                        &self.page_allocator,
                        &self.allocated,
                        1,
                        K::fixed_width(),
                        V::fixed_width(),
                    );
                    builder.push(key, value);
                    let new_page = builder.build()?;
                    let new_page_number = new_page.get_page_number();
                    let new_page_accessor =
                        LeafAccessor::new(new_page.memory(), K::fixed_width(), V::fixed_width());
                    let offset = new_page_accessor.offset_of_first_value();
                    let guard = AccessGuardMutInPlace::new(new_page, offset, value.len());
                    return if position == 0 {
                        Ok(InsertionResult {
                            new_root: new_page_number,
                            root_checksum: DEFERRED,
                            additional_sibling: Some((
                                key.to_vec(),
                                page.get_page_number(),
                                page_checksum,
                            )),
                            inserted_value: guard,
                            old_value: None,
                        })
                    } else {
                        let split_key = accessor.last_entry().key().to_vec();
                        Ok(InsertionResult {
                            new_root: page.get_page_number(),
                            root_checksum: page_checksum,
                            additional_sibling: Some((split_key, new_page_number, DEFERRED)),
                            inserted_value: guard,
                            old_value: None,
                        })
                    };
                }

                // Fast-path for uncommitted pages, that can be modified in-place
                let has_inplace_space = || -> bool {
                    if found {
                        LeafMutator::sufficient_replace_inplace_space(
                            &page,
                            position,
                            K::fixed_width(),
                            V::fixed_width(),
                            value,
                        )
                    } else {
                        LeafMutator::sufficient_insert_inplace_space(
                            &page,
                            position,
                            K::fixed_width(),
                            V::fixed_width(),
                            key,
                            value,
                        )
                    }
                };
                if self.page_allocator.uncommitted(page.get_page_number())
                    && self.modify_uncommitted
                    && has_inplace_space()
                {
                    let page_number = page.get_page_number();
                    let existing_value = if found {
                        let copied_value = accessor.entry(position).unwrap().value().to_vec();
                        Some(AccessGuard::with_owned_value(copied_value))
                    } else {
                        None
                    };
                    drop(page);
                    let mut page_mut = self.page_allocator.get_page_mut(page_number)?;
                    let mut mutator =
                        LeafMutator::new(page_mut.memory_mut(), K::fixed_width(), V::fixed_width());
                    if found {
                        mutator.replace(position, value);
                    } else {
                        mutator.insert(position, key, value);
                    }
                    let new_page_accessor =
                        LeafAccessor::new(page_mut.memory(), K::fixed_width(), V::fixed_width());
                    let offset = new_page_accessor.offset_of_value(position).unwrap();
                    let guard = AccessGuardMutInPlace::new(page_mut, offset, value.len());
                    return Ok(InsertionResult {
                        new_root: page_number,
                        root_checksum: DEFERRED,
                        additional_sibling: None,
                        inserted_value: guard,
                        old_value: existing_value,
                    });
                }

                let mut builder = LeafBuilder::new(
                    &self.page_allocator,
                    &self.allocated,
                    accessor.num_pairs() + 1,
                    K::fixed_width(),
                    V::fixed_width(),
                );
                for i in 0..accessor.num_pairs() {
                    if i == position {
                        builder.push(key, value);
                    }
                    if !found || i != position {
                        let entry = accessor.entry(i).unwrap();
                        builder.push(entry.key(), entry.value());
                    }
                }
                if accessor.num_pairs() == position {
                    builder.push(key, value);
                }
                if !builder.should_split() {
                    let new_page = builder.build()?;

                    let page_number = page.get_page_number();
                    let existing_value = if found {
                        let (start, end) = accessor.value_range(position).unwrap();
                        if self.modify_uncommitted && self.page_allocator.uncommitted(page_number) {
                            let arc = page.to_arc();
                            drop(page);
                            let mut allocated = self.allocated.lock().unwrap();
                            self.page_allocator.free(page_number, &mut allocated);
                            Some(AccessGuard::with_arc_page(arc, start..end))
                        } else {
                            self.freed.push(page_number);
                            Some(AccessGuard::with_page(page, start..end))
                        }
                    } else {
                        drop(page);
                        self.conditional_free(page_number);
                        None
                    };

                    let new_page_number = new_page.get_page_number();
                    let accessor =
                        LeafAccessor::new(new_page.memory(), K::fixed_width(), V::fixed_width());
                    let offset = accessor.offset_of_value(position).unwrap();
                    let guard = AccessGuardMutInPlace::new(new_page, offset, value.len());

                    InsertionResult {
                        new_root: new_page_number,
                        root_checksum: DEFERRED,
                        additional_sibling: None,
                        inserted_value: guard,
                        old_value: existing_value,
                    }
                } else {
                    let (new_page1, split_key, new_page2) = builder.build_split()?;
                    let split_key = split_key.to_vec();
                    let page_number = page.get_page_number();
                    let existing_value = if found {
                        let (start, end) = accessor.value_range(position).unwrap();
                        if self.modify_uncommitted && self.page_allocator.uncommitted(page_number) {
                            let arc = page.to_arc();
                            drop(page);
                            let mut allocated = self.allocated.lock().unwrap();
                            self.page_allocator.free(page_number, &mut allocated);
                            Some(AccessGuard::with_arc_page(arc, start..end))
                        } else {
                            self.freed.push(page_number);
                            Some(AccessGuard::with_page(page, start..end))
                        }
                    } else {
                        drop(page);
                        self.conditional_free(page_number);
                        None
                    };

                    let new_page_number = new_page1.get_page_number();
                    let new_page_number2 = new_page2.get_page_number();
                    let accessor =
                        LeafAccessor::new(new_page1.memory(), K::fixed_width(), V::fixed_width());
                    let division = accessor.num_pairs();
                    let guard = if position < division {
                        let accessor = LeafAccessor::new(
                            new_page1.memory(),
                            K::fixed_width(),
                            V::fixed_width(),
                        );
                        let offset = accessor.offset_of_value(position).unwrap();
                        AccessGuardMutInPlace::new(new_page1, offset, value.len())
                    } else {
                        let accessor = LeafAccessor::new(
                            new_page2.memory(),
                            K::fixed_width(),
                            V::fixed_width(),
                        );
                        let offset = accessor.offset_of_value(position - division).unwrap();
                        AccessGuardMutInPlace::new(new_page2, offset, value.len())
                    };

                    InsertionResult {
                        new_root: new_page_number,
                        root_checksum: DEFERRED,
                        additional_sibling: Some((split_key, new_page_number2, DEFERRED)),
                        inserted_value: guard,
                        old_value: existing_value,
                    }
                }
            }
            BRANCH => {
                let accessor = BranchAccessor::new(&page, K::fixed_width());
                let (child_index, child_page) = accessor.child_for_key::<K>(key);
                let child_checksum = accessor.child_checksum(child_index).unwrap();
                let sub_result = self.insert_helper(
                    self.page_allocator.get_page(child_page, PageHint::None)?,
                    child_checksum,
                    key,
                    value,
                )?;

                // Skip-path: if child page number and checksum haven't changed,
                // no branch update is needed. This avoids redundant get_page_mut +
                // write_child_page calls on repeat visits to the same subtree
                // within a transaction.
                if sub_result.additional_sibling.is_none()
                    && sub_result.new_root == child_page
                    && sub_result.root_checksum == child_checksum
                {
                    return Ok(InsertionResult {
                        new_root: page.get_page_number(),
                        root_checksum: page_checksum,
                        additional_sibling: None,
                        inserted_value: sub_result.inserted_value,
                        old_value: sub_result.old_value,
                    });
                }

                if sub_result.additional_sibling.is_none()
                    && self.modify_uncommitted
                    && self.page_allocator.uncommitted(page.get_page_number())
                {
                    let page_number = page.get_page_number();
                    drop(page);
                    let mut mutpage = self.page_allocator.get_page_mut(page_number)?;
                    let mut mutator = BranchMutator::new(mutpage.memory_mut());
                    mutator.write_child_page(
                        child_index,
                        sub_result.new_root,
                        sub_result.root_checksum,
                    );
                    return Ok(InsertionResult {
                        new_root: mutpage.get_page_number(),
                        root_checksum: DEFERRED,
                        additional_sibling: None,
                        inserted_value: sub_result.inserted_value,
                        old_value: sub_result.old_value,
                    });
                }

                // A child was added, or we couldn't use the fast-path above
                let mut builder = BranchBuilder::new(
                    &self.page_allocator,
                    &self.allocated,
                    accessor.count_children() + 1,
                    K::fixed_width(),
                );
                if child_index == 0 {
                    builder.push_child(sub_result.new_root, sub_result.root_checksum);
                    if let Some((ref index_key2, page2, page2_checksum)) =
                        sub_result.additional_sibling
                    {
                        builder.push_key(index_key2);
                        builder.push_child(page2, page2_checksum);
                    }
                } else {
                    builder.push_child(
                        accessor.child_page(0).unwrap(),
                        accessor.child_checksum(0).unwrap(),
                    );
                }
                for i in 1..accessor.count_children() {
                    if let Some(key) = accessor.key(i - 1) {
                        builder.push_key(key);
                        if i == child_index {
                            builder.push_child(sub_result.new_root, sub_result.root_checksum);
                            if let Some((ref index_key2, page2, page2_checksum)) =
                                sub_result.additional_sibling
                            {
                                builder.push_key(index_key2);
                                builder.push_child(page2, page2_checksum);
                            }
                        } else {
                            builder.push_child(
                                accessor.child_page(i).unwrap(),
                                accessor.child_checksum(i).unwrap(),
                            );
                        }
                    } else {
                        unreachable!();
                    }
                }

                let result = if builder.should_split() {
                    let (new_page1, split_key, new_page2) = builder.build_split()?;
                    InsertionResult {
                        new_root: new_page1.get_page_number(),
                        root_checksum: DEFERRED,
                        additional_sibling: Some((
                            split_key.to_vec(),
                            new_page2.get_page_number(),
                            DEFERRED,
                        )),
                        inserted_value: sub_result.inserted_value,
                        old_value: sub_result.old_value,
                    }
                } else {
                    let new_page = builder.build()?;
                    InsertionResult {
                        new_root: new_page.get_page_number(),
                        root_checksum: DEFERRED,
                        additional_sibling: None,
                        inserted_value: sub_result.inserted_value,
                        old_value: sub_result.old_value,
                    }
                };
                // Free the original page, since we've replaced it
                let page_number = page.get_page_number();
                drop(page);
                self.conditional_free(page_number);

                result
            }
            _ => unreachable!(),
        })
    }

    pub(crate) fn insert_inplace(
        &mut self,
        key: &K::SelfType<'_>,
        value: &V::SelfType<'_>,
    ) -> Result<()> {
        assert!(self.modify_uncommitted);
        let header = self.root.expect("Key not found (tree is empty)");
        self.insert_inplace_helper(
            self.page_allocator.get_page_mut(header.root)?,
            K::as_bytes(key).as_ref(),
            V::as_bytes(value).as_ref(),
        )?;
        *self.root = Some(BtreeHeader::new(header.root, DEFERRED, header.length));
        Ok(())
    }

    fn insert_inplace_helper(&mut self, mut page: PageMut, key: &[u8], value: &[u8]) -> Result<()> {
        assert!(self.page_allocator.uncommitted(page.get_page_number()));

        let node_mem = page.memory();
        match node_mem[0] {
            LEAF => {
                let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
                let (position, found) = accessor.position::<K>(key);
                assert!(found);
                let old_len = accessor.entry(position).unwrap().value().len();
                assert!(value.len() <= old_len);
                let mut mutator =
                    LeafMutator::new(page.memory_mut(), K::fixed_width(), V::fixed_width());
                mutator.replace(position, value);
            }
            BRANCH => {
                let accessor = BranchAccessor::new(&page, K::fixed_width());
                let (child_index, child_page) = accessor.child_for_key::<K>(key);
                self.insert_inplace_helper(
                    self.page_allocator.get_page_mut(child_page)?,
                    key,
                    value,
                )?;
                let mut mutator = BranchMutator::new(page.memory_mut());
                mutator.write_child_page(child_index, child_page, DEFERRED);
            }
            _ => unreachable!(),
        }

        Ok(())
    }

    fn delete_leaf_helper(
        &mut self,
        page: PageImpl,
        target: DeleteTarget<'_>,
        found_key: &mut Option<AccessGuard<'a, K>>,
    ) -> Result<(DeletionResult<'static>, Option<AccessGuard<'a, V>>)> {
        let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
        let (position, found) = match target {
            DeleteTarget::Key(key) => accessor.position::<K>(key),
            DeleteTarget::Position(position) => (position, true),
        };
        if !found {
            // Leaf is unchanged; caller short-circuits via `found.is_none()`.
            return Ok((Subtree(page.get_page_number()), None));
        }
        let want_key = matches!(target, DeleteTarget::Position(_));
        let new_kv_bytes = accessor.length_of_pairs(0, accessor.num_pairs())
            - accessor.length_of_pairs(position, position + 1);
        let new_required_bytes = RawLeafBuilder::required_bytes(
            accessor.num_pairs() - 1,
            new_kv_bytes,
            K::fixed_width(),
            V::fixed_width(),
        );
        let uncommitted = self.page_allocator.uncommitted(page.get_page_number());

        // Fast-path for dirty pages: perform in-place removal without allocating a new page.
        // The threshold matches the merge threshold (page_size/3) so that we use in-place
        // removal for all cases where the page won't need merging with a sibling.
        if uncommitted
            && self.modify_uncommitted
            && new_required_bytes >= self.page_allocator.get_page_size() / 3
            && accessor.num_pairs() > 1
        {
            let (start, end) = accessor.value_range(position).unwrap();
            // The returned value guard owns the mutable page and removes the entry on drop,
            // so we can't hand the key back as a borrow into the same page. Copy it instead.
            if want_key {
                *found_key = Some(AccessGuard::with_owned_value(
                    accessor.entry(position).unwrap().key().to_vec(),
                ));
            }
            let page_number = page.get_page_number();
            drop(page);
            let page_mut = self.page_allocator.get_page_mut(page_number)?;

            let guard = AccessGuard::remove_on_drop(
                page_mut,
                start,
                end - start,
                position,
                K::fixed_width(),
            );
            return Ok((Subtree(page_number), Some(guard)));
        }

        let result = if accessor.num_pairs() == 1 {
            DeletedLeaf
        } else if new_required_bytes < self.page_allocator.get_page_size() / 3 {
            // Merge when less than 33% full. Splits occur when a page is full and produce two 50%
            // full pages, so we use 33% instead of 50% to avoid oscillating
            PartialLeaf {
                page: page.to_arc(),
                deleted_pairs: DeletedPairs::One(position),
            }
        } else {
            let mut builder = LeafBuilder::new(
                &self.page_allocator,
                &self.allocated,
                accessor.num_pairs() - 1,
                K::fixed_width(),
                V::fixed_width(),
            );
            for i in 0..accessor.num_pairs() {
                if i == position {
                    continue;
                }
                let entry = accessor.entry(i).unwrap();
                builder.push(entry.key(), entry.value());
            }
            let new_page = builder.build()?;
            Subtree(new_page.get_page_number())
        };
        let (key_range, value_range) = accessor.entry_ranges(position).unwrap();
        let guard = if uncommitted && self.modify_uncommitted {
            let page_number = page.get_page_number();
            let arc = page.to_arc();
            drop(page);
            let mut allocated = self.allocated.lock().unwrap();
            self.page_allocator.free(page_number, &mut allocated);
            if want_key {
                *found_key = Some(AccessGuard::with_arc_page(arc.clone(), key_range));
            }
            Some(AccessGuard::with_arc_page(arc, value_range))
        } else {
            // Won't be freed until the end of the transaction, so returning the page
            // in the AccessGuard below is still safe
            if want_key {
                *found_key = Some(AccessGuard::with_page(page.clone(), key_range));
            }
            self.freed.push(page.get_page_number());
            Some(AccessGuard::with_page(page, value_range))
        };
        Ok((result, guard))
    }

    fn delete_branch_helper(
        &mut self,
        page: PageImpl,
        target: DeleteTarget<'_>,
        found_key: &mut Option<AccessGuard<'a, K>>,
    ) -> Result<(DeletionResult<'static>, Option<AccessGuard<'a, V>>)> {
        let accessor = BranchAccessor::new(&page, K::fixed_width());
        let original_page_number = page.get_page_number();
        let (child_index, child_page_number) = match target {
            DeleteTarget::Key(key) => accessor.child_for_key::<K>(key),
            DeleteTarget::Position(_) => {
                unreachable!("position targets are only valid once the cursor has found a leaf")
            }
        };
        let (result, found) = self.delete_helper(
            self.page_allocator
                .get_page(child_page_number, PageHint::None)?,
            target,
            found_key,
        )?;
        if found.is_none() {
            // Subtree unchanged; caller identifies this via `found.is_none()`.
            return Ok((Subtree(original_page_number), None));
        }
        let result = self.apply_child_delete_result(page, child_index, result)?;
        Ok((result, found))
    }

    // Returns the page number of the sub-tree with this key deleted, or None if the sub-tree is empty.
    // If key is not found, guaranteed not to modify the tree.
    // When `target` is DeleteTarget::First or DeleteTarget::Last, `found_key` is populated
    // with an AccessGuard for the deleted key.
    fn delete_helper(
        &mut self,
        page: PageImpl,
        target: DeleteTarget<'_>,
        found_key: &mut Option<AccessGuard<'a, K>>,
    ) -> Result<(DeletionResult<'static>, Option<AccessGuard<'a, V>>)> {
        let node_mem = page.memory();
        match node_mem[0] {
            LEAF => self.delete_leaf_helper(page, target, found_key),
            BRANCH => self.delete_branch_helper(page, target, found_key),
            _ => unreachable!(),
        }
    }
}
