use crate::db::TransactionGuard;
use crate::tree_store::btree_base::{
    BRANCH, BranchAccessor, BranchBuilder, BranchMutator, BtreeHeader, Checksum, DEFERRED, LEAF,
    LeafAccessor, LeafBuilder, RawLeafBuilder, branch_checksum, leaf_checksum,
};
use crate::tree_store::btree_iters::BtreeExtractIf;
use crate::tree_store::btree_mutator::MutateHelper;
use crate::tree_store::page_store::{Page, PageImpl, PageMut, TransactionalMemory};
use crate::tree_store::{
    AccessGuardMut, AllPageNumbersBtreeIter, BtreeRangeIter, PageHint, PageNumber,
};
use crate::types::{Key, MutInPlaceValue, Value};
use crate::{AccessGuard, Result};
#[cfg(feature = "logging")]
use log::trace;
use std::borrow::Borrow;
use std::cmp::max;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub(crate) enum DeletionResult {
    Subtree(PageNumber, Checksum),
    DeletedLeaf,
    PartialLeaf {
        page: Arc<[u8]>,
        deleted_pair: usize,
    },
    PartialBranch(PageNumber, Checksum),
    DeletedBranch(PageNumber, Checksum),
}

pub(crate) struct BtreeStats {
    pub(crate) tree_height: u32,
    pub(crate) leaf_pages: u64,
    pub(crate) branch_pages: u64,
    pub(crate) stored_leaf_bytes: u64,
    pub(crate) metadata_bytes: u64,
    pub(crate) fragmented_bytes: u64,
}

#[derive(Clone)]
pub(crate) struct PagePath {
    path: Vec<PageNumber>,
}

impl PagePath {
    pub(crate) fn new_root(page_number: PageNumber) -> Self {
        Self {
            path: vec![page_number],
        }
    }

    pub(crate) fn with_child(&self, page_number: PageNumber) -> Self {
        let mut path = self.path.clone();
        path.push(page_number);
        Self { path }
    }

    pub(crate) fn with_subpath(&self, other: &Self) -> Self {
        let mut path = self.path.clone();
        path.extend(&other.path);
        Self { path }
    }

    pub(crate) fn parents(&self) -> &[PageNumber] {
        &self.path[..self.path.len() - 1]
    }

    pub(crate) fn page_number(&self) -> PageNumber {
        self.path[self.path.len() - 1]
    }
}

pub(crate) struct UntypedBtree {
    mem: Arc<TransactionalMemory>,
    root: Option<BtreeHeader>,
    key_width: Option<usize>,
    _value_width: Option<usize>,
}

impl UntypedBtree {
    pub(crate) fn new(
        root: Option<BtreeHeader>,
        mem: Arc<TransactionalMemory>,
        key_width: Option<usize>,
        value_width: Option<usize>,
    ) -> Self {
        Self {
            mem,
            root,
            key_width,
            _value_width: value_width,
        }
    }

    // Applies visitor to pages in the tree
    pub(crate) fn visit_all_pages<F>(&self, mut visitor: F) -> Result
    where
        F: FnMut(&PagePath) -> Result,
    {
        if let Some(page_number) = self.root.map(|x| x.root) {
            self.visit_pages_helper(PagePath::new_root(page_number), &mut visitor)?;
        }

        Ok(())
    }

    fn visit_pages_helper<F>(&self, path: PagePath, visitor: &mut F) -> Result
    where
        F: FnMut(&PagePath) -> Result,
    {
        visitor(&path)?;
        let page = self.mem.get_page(path.page_number())?;

        match page.memory()[0] {
            LEAF => {
                // No-op
            }
            BRANCH => {
                let accessor = BranchAccessor::new(&page, self.key_width);
                for i in 0..accessor.count_children() {
                    let child_page = accessor.child_page(i).unwrap();
                    let child_path = path.with_child(child_page);
                    self.visit_pages_helper(child_path, visitor)?;
                }
            }
            _ => unreachable!(),
        }

        Ok(())
    }
}

pub(crate) struct UntypedBtreeMut {
    mem: Arc<TransactionalMemory>,
    root: Option<BtreeHeader>,
    freed_pages: Arc<Mutex<Vec<PageNumber>>>,
    key_width: Option<usize>,
    value_width: Option<usize>,
}

impl UntypedBtreeMut {
    pub(crate) fn new(
        root: Option<BtreeHeader>,
        mem: Arc<TransactionalMemory>,
        freed_pages: Arc<Mutex<Vec<PageNumber>>>,
        key_width: Option<usize>,
        value_width: Option<usize>,
    ) -> Self {
        Self {
            mem,
            root,
            freed_pages,
            key_width,
            value_width,
        }
    }

    pub(crate) fn get_root(&self) -> Option<BtreeHeader> {
        self.root
    }

    // Recomputes the checksum for all pages that are uncommitted
    pub(crate) fn finalize_dirty_checksums(&mut self) -> Result<Option<BtreeHeader>> {
        let mut root = self.root;
        if let Some(BtreeHeader {
            root: ref p,
            ref mut checksum,
            length: _,
        }) = root
        {
            if !self.mem.uncommitted(*p) {
                // root page is clean
                return Ok(root);
            }

            *checksum = self.finalize_dirty_checksums_helper(*p)?;
            self.root = root;
        }

        Ok(root)
    }

    fn finalize_dirty_checksums_helper(&mut self, page_number: PageNumber) -> Result<Checksum> {
        assert!(self.mem.uncommitted(page_number));
        let mut page = self.mem.get_page_mut(page_number)?;

        match page.memory()[0] {
            LEAF => leaf_checksum(&page, self.key_width, self.value_width),
            BRANCH => {
                let accessor = BranchAccessor::new(&page, self.key_width);
                let mut new_children = vec![];
                for i in 0..accessor.count_children() {
                    let child_page = accessor.child_page(i).unwrap();
                    if self.mem.uncommitted(child_page) {
                        let new_checksum = self.finalize_dirty_checksums_helper(child_page)?;
                        new_children.push(Some((i, child_page, new_checksum)));
                    } else {
                        // Child is clean, skip it
                        new_children.push(None);
                    }
                }

                let mut mutator = BranchMutator::new(&mut page);
                for (child_index, child_page, child_checksum) in new_children.into_iter().flatten()
                {
                    mutator.write_child_page(child_index, child_page, child_checksum);
                }

                branch_checksum(&page, self.key_width)
            }
            _ => unreachable!(),
        }
    }

    // Applies visitor to all dirty leaf pages in the tree
    pub(crate) fn dirty_leaf_visitor<F>(&mut self, visitor: F) -> Result
    where
        F: Fn(PageMut) -> Result,
    {
        if let Some(page_number) = self.root.map(|x| x.root) {
            if !self.mem.uncommitted(page_number) {
                // root page is clean
                return Ok(());
            }

            let page = self.mem.get_page_mut(page_number)?;
            match page.memory()[0] {
                LEAF => {
                    visitor(page)?;
                }
                BRANCH => {
                    drop(page);
                    self.dirty_leaf_visitor_helper(page_number, &visitor)?;
                }
                _ => unreachable!(),
            }
        }

        Ok(())
    }

    fn dirty_leaf_visitor_helper<F>(&mut self, page_number: PageNumber, visitor: &F) -> Result
    where
        F: Fn(PageMut) -> Result,
    {
        assert!(self.mem.uncommitted(page_number));
        let page = self.mem.get_page_mut(page_number)?;

        match page.memory()[0] {
            LEAF => {
                visitor(page)?;
            }
            BRANCH => {
                let accessor = BranchAccessor::new(&page, self.key_width);
                for i in 0..accessor.count_children() {
                    let child_page = accessor.child_page(i).unwrap();
                    if self.mem.uncommitted(child_page) {
                        self.dirty_leaf_visitor_helper(child_page, visitor)?;
                    }
                }
            }
            _ => unreachable!(),
        }

        Ok(())
    }

    pub(crate) fn relocate(
        &mut self,
        relocation_map: &HashMap<PageNumber, PageNumber>,
    ) -> Result<bool> {
        if let Some(root) = self.get_root() {
            if let Some((new_root, new_checksum)) =
                self.relocate_helper(root.root, relocation_map)?
            {
                self.root = Some(BtreeHeader::new(new_root, new_checksum, root.length));
                return Ok(true);
            }
        }
        Ok(false)
    }

    // Relocates the given subtree to the pages specified in relocation_map
    fn relocate_helper(
        &mut self,
        page_number: PageNumber,
        relocation_map: &HashMap<PageNumber, PageNumber>,
    ) -> Result<Option<(PageNumber, Checksum)>> {
        let old_page = self.mem.get_page(page_number)?;
        let mut new_page = if let Some(new_page_number) = relocation_map.get(&page_number) {
            self.mem.get_page_mut(*new_page_number)?
        } else {
            return Ok(None);
        };
        new_page.memory_mut().copy_from_slice(old_page.memory());

        let node_mem = old_page.memory();
        match node_mem[0] {
            LEAF => {
                // No-op
            }
            BRANCH => {
                let accessor = BranchAccessor::new(&old_page, self.key_width);
                let mut mutator = BranchMutator::new(&mut new_page);
                for i in 0..accessor.count_children() {
                    let child = accessor.child_page(i).unwrap();
                    if let Some((new_child, new_checksum)) =
                        self.relocate_helper(child, relocation_map)?
                    {
                        mutator.write_child_page(i, new_child, new_checksum);
                    }
                }
            }
            _ => unreachable!(),
        }

        let mut freed_pages = self.freed_pages.lock().unwrap();
        if !self.mem.free_if_uncommitted(page_number) {
            freed_pages.push(page_number);
        }

        Ok(Some((new_page.get_page_number(), DEFERRED)))
    }
}

pub(crate) struct BtreeMut<'a, K: Key + 'static, V: Value + 'static> {
    mem: Arc<TransactionalMemory>,
    transaction_guard: Arc<TransactionGuard>,
    root: Option<BtreeHeader>,
    freed_pages: Arc<Mutex<Vec<PageNumber>>>,
    modify_uncommitted: bool,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
    _lifetime: PhantomData<&'a ()>,
}

impl<K: Key + 'static, V: Value + 'static> BtreeMut<'_, K, V> {
    pub(crate) fn new(
        root: Option<BtreeHeader>,
        guard: Arc<TransactionGuard>,
        mem: Arc<TransactionalMemory>,
        freed_pages: Arc<Mutex<Vec<PageNumber>>>,
    ) -> Self {
        Self {
            mem,
            transaction_guard: guard,
            root,
            freed_pages,
            modify_uncommitted: true,
            _key_type: Default::default(),
            _value_type: Default::default(),
            _lifetime: Default::default(),
        }
    }

    pub(crate) fn verify_checksum(&self) -> Result<bool> {
        RawBtree::new(
            self.get_root(),
            K::fixed_width(),
            V::fixed_width(),
            self.mem.clone(),
        )
        .verify_checksum()
    }

    pub(crate) fn finalize_dirty_checksums(&mut self) -> Result<Option<BtreeHeader>> {
        let mut tree = UntypedBtreeMut::new(
            self.get_root(),
            self.mem.clone(),
            self.freed_pages.clone(),
            K::fixed_width(),
            V::fixed_width(),
        );
        self.root = tree.finalize_dirty_checksums()?;
        Ok(self.root)
    }

    #[allow(dead_code)]
    pub(crate) fn all_pages_iter(&self) -> Result<Option<AllPageNumbersBtreeIter>> {
        if let Some(root) = self.root.map(|x| x.root) {
            Ok(Some(AllPageNumbersBtreeIter::new(
                root,
                K::fixed_width(),
                V::fixed_width(),
                self.mem.clone(),
            )?))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn visit_all_pages<F>(&self, visitor: F) -> Result
    where
        F: FnMut(&PagePath) -> Result,
    {
        self.read_tree()?.visit_all_pages(visitor)
    }

    pub(crate) fn get_root(&self) -> Option<BtreeHeader> {
        self.root
    }

    pub(crate) fn relocate(
        &mut self,
        relocation_map: &HashMap<PageNumber, PageNumber>,
    ) -> Result<bool> {
        let mut tree = UntypedBtreeMut::new(
            self.get_root(),
            self.mem.clone(),
            self.freed_pages.clone(),
            K::fixed_width(),
            V::fixed_width(),
        );
        if tree.relocate(relocation_map)? {
            self.root = tree.get_root();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub(crate) fn insert(
        &mut self,
        key: &K::SelfType<'_>,
        value: &V::SelfType<'_>,
    ) -> Result<Option<AccessGuard<V>>> {
        #[cfg(feature = "logging")]
        trace!(
            "Btree(root={:?}): Inserting {:?} with value of length {}",
            &self.root,
            key,
            V::as_bytes(value).as_ref().len()
        );
        let mut freed_pages = self.freed_pages.lock().unwrap();
        let mut operation: MutateHelper<'_, '_, K, V> =
            MutateHelper::new(&mut self.root, self.mem.clone(), freed_pages.as_mut());
        let (old_value, _) = operation.insert(key, value)?;
        Ok(old_value)
    }

    // Insert without allocating or freeing any pages. This requires that you've previously
    // inserted the same key, with a value of at least the same serialized length, earlier
    // in the same transaction. If those preconditions aren't satisfied, insert_inplace()
    // will panic; it won't allocate under any circumstances
    pub(crate) fn insert_inplace(
        &mut self,
        key: &K::SelfType<'_>,
        value: &V::SelfType<'_>,
    ) -> Result<()> {
        let mut fake_freed_pages = vec![];
        let mut operation =
            MutateHelper::<K, V>::new(&mut self.root, self.mem.clone(), fake_freed_pages.as_mut());
        operation.insert_inplace(key, value)?;
        assert!(fake_freed_pages.is_empty());
        Ok(())
    }

    pub(crate) fn remove(&mut self, key: &K::SelfType<'_>) -> Result<Option<AccessGuard<V>>> {
        #[cfg(feature = "logging")]
        trace!("Btree(root={:?}): Deleting {:?}", &self.root, key);
        let mut freed_pages = self.freed_pages.lock().unwrap();
        let mut operation: MutateHelper<'_, '_, K, V> =
            MutateHelper::new(&mut self.root, self.mem.clone(), freed_pages.as_mut());
        let result = operation.delete(key)?;
        Ok(result)
    }

    pub(crate) fn pop_last_helper(
        &mut self,
    ) -> Result<Option<(AccessGuard<'static, K>, AccessGuard<'static, V>)>> {
        if let Some(header) = self.root {
            let BtreeHeader {
                root: p,
                checksum,
                length,
            } = header;
            let result = self.pop_last_from_node(self.mem.get_page(p)?, checksum)?;
            match result {
                Some((deletion_result, key_guard, value_guard)) => {
                    let new_length = length - 1;
                    let new_root = match deletion_result {
                        DeletionResult::Subtree(page, checksum) => {
                            Some(BtreeHeader::new(page, checksum, new_length))
                        }
                        DeletionResult::DeletedLeaf => None,
                        DeletionResult::PartialLeaf { page, deleted_pair } => {
                            let accessor =
                                LeafAccessor::new(&page, K::fixed_width(), V::fixed_width());
                            let mut builder = LeafBuilder::new(
                                &self.mem,
                                accessor.num_pairs() - 1,
                                K::fixed_width(),
                                V::fixed_width(),
                            );
                            builder.push_all_except(&accessor, Some(deleted_pair));
                            let page = builder.build()?;
                            assert_eq!(new_length, accessor.num_pairs() as u64 - 1);
                            Some(BtreeHeader::new(
                                page.get_page_number(),
                                DEFERRED,
                                new_length,
                            ))
                        }
                        DeletionResult::PartialBranch(page_number, checksum) => {
                            Some(BtreeHeader::new(page_number, checksum, new_length))
                        }
                        DeletionResult::DeletedBranch(remaining_child, checksum) => {
                            Some(BtreeHeader::new(remaining_child, checksum, new_length))
                        }
                    };
                    self.root = new_root;
                    Ok(Some((key_guard, value_guard)))
                }
                None => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    fn pop_last_from_node(
        &mut self,
        page: PageImpl,
        checksum: Checksum,
    ) -> Result<
        Option<(
            DeletionResult,
            AccessGuard<'static, K>,
            AccessGuard<'static, V>,
        )>,
    > {
        let node_mem = page.memory();
        match node_mem[0] {
            LEAF => {
                let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
                if accessor.num_pairs() == 0 {
                    return Ok(None);
                }

                let position = accessor.num_pairs() - 1;

                let new_kv_bytes = accessor.length_of_pairs(0, accessor.num_pairs())
                    - accessor.length_of_pairs(position, position + 1);
                let new_required_bytes = RawLeafBuilder::required_bytes(
                    accessor.num_pairs() - 1,
                    new_kv_bytes,
                    K::fixed_width(),
                    V::fixed_width(),
                );
                let uncommitted = self.mem.uncommitted(page.get_page_number());

                let entry = accessor.entry(position).unwrap();
                let key_data = entry.key().to_vec();
                let value_data = entry.value().to_vec();
                
                let result = if accessor.num_pairs() == 1 {
                    DeletionResult::DeletedLeaf
                } else if new_required_bytes < self.mem.get_page_size() / 3 {
                    DeletionResult::PartialLeaf {
                        page: page.to_arc(),
                        deleted_pair: position,
                    }
                } else {
                    let mut builder = LeafBuilder::new(
                        &self.mem,
                        accessor.num_pairs() - 1,
                        K::fixed_width(),
                        V::fixed_width(),
                    );
                    for i in 0..accessor.num_pairs() - 1 {
                        let entry = accessor.entry(i).unwrap();
                        builder.push(entry.key(), entry.value());
                    }
                    let new_page = builder.build()?;
                    DeletionResult::Subtree(new_page.get_page_number(), DEFERRED)
                };

                let page_number = page.get_page_number();
                if uncommitted && self.modify_uncommitted {
                    drop(page);
                    self.mem.free(page_number);
                } else {
                    let mut freed_pages = self.freed_pages.lock().unwrap();
                    freed_pages.push(page_number);
                    drop(page);
                }
                
                let key_guard = AccessGuard::with_owned_value(key_data);
                let value_guard = AccessGuard::with_owned_value(value_data);

                Ok(Some((result, key_guard, value_guard)))
            }
            BRANCH => {
                let accessor = BranchAccessor::new(&page, K::fixed_width());
                let child_index = accessor.count_children() - 1;
                let child_page = accessor.child_page(child_index).unwrap();
                let child_checksum = accessor.child_checksum(child_index).unwrap();

                let result =
                    self.pop_last_from_node(self.mem.get_page(child_page)?, child_checksum)?;

                if let Some((deletion_result, key_guard, value_guard)) = result {
                    if let DeletionResult::Subtree(new_child, new_child_checksum) = deletion_result
                    {
                        let result_page = if self.mem.uncommitted(page.get_page_number())
                            && self.modify_uncommitted
                        {
                            let page_number = page.get_page_number();
                            drop(page);
                            let mut mutpage = self.mem.get_page_mut(page_number)?;
                            let mut mutator = BranchMutator::new(&mut mutpage);
                            mutator.write_child_page(child_index, new_child, new_child_checksum);
                            page_number
                        } else {
                            let mut builder = BranchBuilder::new(
                                &self.mem,
                                accessor.count_children(),
                                K::fixed_width(),
                            );
                            builder.push_all(&accessor);
                            builder.replace_child(child_index, new_child, new_child_checksum);
                            let new_page = builder.build()?;
                            let page_number = page.get_page_number();
                            if self.modify_uncommitted {
                                if !self.mem.free_if_uncommitted(page_number) {
                                    let mut freed_pages = self.freed_pages.lock().unwrap();
                                    freed_pages.push(page_number);
                                }
                            } else {
                                let mut freed_pages = self.freed_pages.lock().unwrap();
                                freed_pages.push(page_number);
                            }
                            drop(page);
                            new_page.get_page_number()
                        };
                        Ok(Some((
                            DeletionResult::Subtree(result_page, DEFERRED),
                            key_guard,
                            value_guard,
                        )))
                    } else {
                        let mut builder = BranchBuilder::new(
                            &self.mem,
                            accessor.count_children(),
                            K::fixed_width(),
                        );

                        let final_result = match deletion_result {
                            DeletionResult::DeletedLeaf => {
                                for i in 0..accessor.count_children() - 1 {
                                    builder.push_child(
                                        accessor.child_page(i).unwrap(),
                                        accessor.child_checksum(i).unwrap(),
                                    );
                                    if i < accessor.count_children() - 2 {
                                        builder.push_key(accessor.key(i).unwrap());
                                    }
                                }

                                if accessor.count_children() == 2 {
                                    let child_page = accessor.child_page(0).unwrap();
                                    let child_checksum = accessor.child_checksum(0).unwrap();
                                    let page_number = page.get_page_number();
                                    if self.modify_uncommitted {
                                        if !self.mem.free_if_uncommitted(page_number) {
                                            let mut freed_pages = self.freed_pages.lock().unwrap();
                                            freed_pages.push(page_number);
                                        }
                                    } else {
                                        let mut freed_pages = self.freed_pages.lock().unwrap();
                                        freed_pages.push(page_number);
                                    }
                                    drop(page);
                                    DeletionResult::DeletedBranch(child_page, child_checksum)
                                } else {
                                    let new_page = builder.build()?;
                                    let page_number = page.get_page_number();
                                    if self.modify_uncommitted {
                                        if !self.mem.free_if_uncommitted(page_number) {
                                            let mut freed_pages = self.freed_pages.lock().unwrap();
                                            freed_pages.push(page_number);
                                        }
                                    } else {
                                        let mut freed_pages = self.freed_pages.lock().unwrap();
                                        freed_pages.push(page_number);
                                    }
                                    drop(page);
                                    DeletionResult::Subtree(new_page.get_page_number(), DEFERRED)
                                }
                            }
                            _ => {
                                let page_number = page.get_page_number();
                                drop(page);
                                DeletionResult::PartialBranch(page_number, checksum)
                            }
                        };

                        Ok(Some((final_result, key_guard, value_guard)))
                    }
                } else {
                    Ok(None)
                }
            }
            _ => unreachable!(),
        }
    }

    pub(crate) fn pop_first_helper(
        &mut self,
    ) -> Result<Option<(AccessGuard<'static, K>, AccessGuard<'static, V>)>> {
        if let Some(header) = self.root {
            let BtreeHeader {
                root: p,
                checksum,
                length,
            } = header;
            let result = self.pop_first_from_node(self.mem.get_page(p)?, checksum)?;
            match result {
                Some((deletion_result, key_guard, value_guard)) => {
                    let new_length = length - 1;
                    let new_root = match deletion_result {
                        DeletionResult::Subtree(page, checksum) => {
                            Some(BtreeHeader::new(page, checksum, new_length))
                        }
                        DeletionResult::DeletedLeaf => None,
                        DeletionResult::PartialLeaf { page, deleted_pair } => {
                            let accessor =
                                LeafAccessor::new(&page, K::fixed_width(), V::fixed_width());
                            let mut builder = LeafBuilder::new(
                                &self.mem,
                                accessor.num_pairs() - 1,
                                K::fixed_width(),
                                V::fixed_width(),
                            );
                            builder.push_all_except(&accessor, Some(deleted_pair));
                            let page = builder.build()?;
                            assert_eq!(new_length, accessor.num_pairs() as u64 - 1);
                            Some(BtreeHeader::new(
                                page.get_page_number(),
                                DEFERRED,
                                new_length,
                            ))
                        }
                        DeletionResult::PartialBranch(page_number, checksum) => {
                            Some(BtreeHeader::new(page_number, checksum, new_length))
                        }
                        DeletionResult::DeletedBranch(remaining_child, checksum) => {
                            Some(BtreeHeader::new(remaining_child, checksum, new_length))
                        }
                    };
                    self.root = new_root;
                    Ok(Some((key_guard, value_guard)))
                }
                None => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    fn pop_first_from_node(
        &mut self,
        page: PageImpl,
        checksum: Checksum,
    ) -> Result<
        Option<(
            DeletionResult,
            AccessGuard<'static, K>,
            AccessGuard<'static, V>,
        )>,
    > {
        let node_mem = page.memory();
        match node_mem[0] {
            LEAF => {
                let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
                if accessor.num_pairs() == 0 {
                    return Ok(None);
                }

                let position = 0;

                let new_kv_bytes = accessor.length_of_pairs(0, accessor.num_pairs())
                    - accessor.length_of_pairs(position, position + 1);
                let new_required_bytes = RawLeafBuilder::required_bytes(
                    accessor.num_pairs() - 1,
                    new_kv_bytes,
                    K::fixed_width(),
                    V::fixed_width(),
                );
                let uncommitted = self.mem.uncommitted(page.get_page_number());

                let entry = accessor.entry(position).unwrap();
                let key_data = entry.key().to_vec();
                let value_data = entry.value().to_vec();
                
                let result = if accessor.num_pairs() == 1 {
                    DeletionResult::DeletedLeaf
                } else if new_required_bytes < self.mem.get_page_size() / 3 {
                    DeletionResult::PartialLeaf {
                        page: page.to_arc(),
                        deleted_pair: position,
                    }
                } else {
                    let mut builder = LeafBuilder::new(
                        &self.mem,
                        accessor.num_pairs() - 1,
                        K::fixed_width(),
                        V::fixed_width(),
                    );
                    for i in 1..accessor.num_pairs() {
                        let entry = accessor.entry(i).unwrap();
                        builder.push(entry.key(), entry.value());
                    }
                    let new_page = builder.build()?;
                    DeletionResult::Subtree(new_page.get_page_number(), DEFERRED)
                };

                let page_number = page.get_page_number();
                if uncommitted && self.modify_uncommitted {
                    drop(page);
                    self.mem.free(page_number);
                } else {
                    let mut freed_pages = self.freed_pages.lock().unwrap();
                    freed_pages.push(page_number);
                    drop(page);
                }
                
                let key_guard = AccessGuard::with_owned_value(key_data);
                let value_guard = AccessGuard::with_owned_value(value_data);

                Ok(Some((result, key_guard, value_guard)))
            }
            BRANCH => {
                let accessor = BranchAccessor::new(&page, K::fixed_width());
                let child_index = 0;
                let child_page = accessor.child_page(child_index).unwrap();
                let child_checksum = accessor.child_checksum(child_index).unwrap();

                let result =
                    self.pop_first_from_node(self.mem.get_page(child_page)?, child_checksum)?;

                if let Some((deletion_result, key_guard, value_guard)) = result {
                    if let DeletionResult::Subtree(new_child, new_child_checksum) = deletion_result
                    {
                        let result_page = if self.mem.uncommitted(page.get_page_number())
                            && self.modify_uncommitted
                        {
                            let page_number = page.get_page_number();
                            drop(page);
                            let mut mutpage = self.mem.get_page_mut(page_number)?;
                            let mut mutator = BranchMutator::new(&mut mutpage);
                            mutator.write_child_page(child_index, new_child, new_child_checksum);
                            page_number
                        } else {
                            let mut builder = BranchBuilder::new(
                                &self.mem,
                                accessor.count_children(),
                                K::fixed_width(),
                            );
                            builder.push_all(&accessor);
                            builder.replace_child(child_index, new_child, new_child_checksum);
                            let new_page = builder.build()?;
                            let page_number = page.get_page_number();
                            if self.modify_uncommitted {
                                if !self.mem.free_if_uncommitted(page_number) {
                                    let mut freed_pages = self.freed_pages.lock().unwrap();
                                    freed_pages.push(page_number);
                                }
                            } else {
                                let mut freed_pages = self.freed_pages.lock().unwrap();
                                freed_pages.push(page_number);
                            }
                            drop(page);
                            new_page.get_page_number()
                        };
                        Ok(Some((
                            DeletionResult::Subtree(result_page, DEFERRED),
                            key_guard,
                            value_guard,
                        )))
                    } else {
                        let mut builder = BranchBuilder::new(
                            &self.mem,
                            accessor.count_children(),
                            K::fixed_width(),
                        );

                        let final_result = match deletion_result {
                            DeletionResult::DeletedLeaf => {
                                for i in 1..accessor.count_children() {
                                    if i > 1 {
                                        builder.push_key(accessor.key(i - 2).unwrap());
                                    }
                                    builder.push_child(
                                        accessor.child_page(i).unwrap(),
                                        accessor.child_checksum(i).unwrap(),
                                    );
                                }

                                if accessor.count_children() == 2 {
                                    let child_page = accessor.child_page(1).unwrap();
                                    let child_checksum = accessor.child_checksum(1).unwrap();
                                    let page_number = page.get_page_number();
                                    if self.modify_uncommitted {
                                        if !self.mem.free_if_uncommitted(page_number) {
                                            let mut freed_pages = self.freed_pages.lock().unwrap();
                                            freed_pages.push(page_number);
                                        }
                                    } else {
                                        let mut freed_pages = self.freed_pages.lock().unwrap();
                                        freed_pages.push(page_number);
                                    }
                                    drop(page);
                                    DeletionResult::DeletedBranch(child_page, child_checksum)
                                } else {
                                    let new_page = builder.build()?;
                                    let page_number = page.get_page_number();
                                    if self.modify_uncommitted {
                                        if !self.mem.free_if_uncommitted(page_number) {
                                            let mut freed_pages = self.freed_pages.lock().unwrap();
                                            freed_pages.push(page_number);
                                        }
                                    } else {
                                        let mut freed_pages = self.freed_pages.lock().unwrap();
                                        freed_pages.push(page_number);
                                    }
                                    drop(page);
                                    DeletionResult::Subtree(new_page.get_page_number(), DEFERRED)
                                }
                            }
                            _ => {
                                let page_number = page.get_page_number();
                                drop(page);
                                DeletionResult::PartialBranch(page_number, checksum)
                            }
                        };

                        Ok(Some((final_result, key_guard, value_guard)))
                    }
                } else {
                    Ok(None)
                }
            }
            _ => unreachable!(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn print_debug(&self, include_values: bool) -> Result {
        self.read_tree()?.print_debug(include_values)
    }

    pub(crate) fn stats(&self) -> Result<BtreeStats> {
        btree_stats(
            self.get_root().map(|x| x.root),
            &self.mem,
            K::fixed_width(),
            V::fixed_width(),
        )
    }

    fn read_tree(&self) -> Result<Btree<K, V>> {
        Btree::new(
            self.get_root(),
            PageHint::None,
            self.transaction_guard.clone(),
            self.mem.clone(),
        )
    }

    pub(crate) fn get(&self, key: &K::SelfType<'_>) -> Result<Option<AccessGuard<'_, V>>> {
        self.read_tree()?.get(key)
    }

    pub(crate) fn first(
        &self,
    ) -> Result<Option<(AccessGuard<'static, K>, AccessGuard<'static, V>)>> {
        self.read_tree()?.first()
    }

    pub(crate) fn last(
        &self,
    ) -> Result<Option<(AccessGuard<'static, K>, AccessGuard<'static, V>)>> {
        self.read_tree()?.last()
    }

    pub(crate) fn range<'a0, T: RangeBounds<KR> + 'a0, KR: Borrow<K::SelfType<'a0>> + 'a0>(
        &self,
        range: &'_ T,
    ) -> Result<BtreeRangeIter<K, V>>
    where
        K: 'a0,
    {
        self.read_tree()?.range(range)
    }

    pub(crate) fn extract_from_if<
        'a,
        'a0,
        T: RangeBounds<KR> + 'a0,
        KR: Borrow<K::SelfType<'a0>> + 'a0,
        F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
    >(
        &'a mut self,
        range: &'_ T,
        predicate: F,
    ) -> Result<BtreeExtractIf<'a, K, V, F>>
    where
        K: 'a0,
    {
        let iter = self.range(range)?;

        let result = BtreeExtractIf::new(
            &mut self.root,
            iter,
            predicate,
            self.freed_pages.clone(),
            self.mem.clone(),
        );

        Ok(result)
    }

    pub(crate) fn retain_in<'a, KR, F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool>(
        &mut self,
        mut predicate: F,
        range: impl RangeBounds<KR> + 'a,
    ) -> Result
    where
        KR: Borrow<K::SelfType<'a>> + 'a,
    {
        let iter = self.range(&range)?;
        let mut freed = vec![];
        // Do not modify the existing tree, because we're iterating over it concurrently with the removals
        // TODO: optimize this to iterate and remove at the same time
        let mut operation: MutateHelper<'_, '_, K, V> =
            MutateHelper::new_do_not_modify(&mut self.root, self.mem.clone(), &mut freed);
        for entry in iter {
            let entry = entry?;
            if !predicate(entry.key(), entry.value()) {
                assert!(operation.delete(&entry.key())?.is_some());
            }
        }
        let mut freed_pages = self.freed_pages.lock().unwrap();
        for page in freed {
            if !self.mem.free_if_uncommitted(page) {
                freed_pages.push(page);
            }
        }

        Ok(())
    }

    pub(crate) fn len(&self) -> Result<u64> {
        self.read_tree()?.len()
    }
}

impl<'a, K: Key + 'a, V: MutInPlaceValue + 'a> BtreeMut<'a, K, V> {
    /// Reserve space to insert a key-value pair
    /// The returned reference will have length equal to `value_length`
    // Return type has the same lifetime as &self, because the tree must not be modified until the mutable guard is dropped
    pub(crate) fn insert_reserve(
        &mut self,
        key: &K::SelfType<'_>,
        value_length: u32,
    ) -> Result<AccessGuardMut<V>> {
        #[cfg(feature = "logging")]
        trace!(
            "Btree(root={:?}): Inserting {:?} with {} reserved bytes for the value",
            &self.root, key, value_length
        );
        let mut freed_pages = self.freed_pages.lock().unwrap();
        let mut value = vec![0u8; value_length as usize];
        V::initialize(&mut value);
        let mut operation =
            MutateHelper::<K, V>::new(&mut self.root, self.mem.clone(), freed_pages.as_mut());
        let (_, guard) = operation.insert(key, &V::from_bytes(&value))?;
        Ok(guard)
    }
}

pub(crate) struct RawBtree {
    mem: Arc<TransactionalMemory>,
    root: Option<BtreeHeader>,
    fixed_key_size: Option<usize>,
    fixed_value_size: Option<usize>,
}

impl RawBtree {
    pub(crate) fn new(
        root: Option<BtreeHeader>,
        fixed_key_size: Option<usize>,
        fixed_value_size: Option<usize>,
        mem: Arc<TransactionalMemory>,
    ) -> Self {
        Self {
            mem,
            root,
            fixed_key_size,
            fixed_value_size,
        }
    }

    pub(crate) fn get_root(&self) -> Option<BtreeHeader> {
        self.root
    }

    pub(crate) fn stats(&self) -> Result<BtreeStats> {
        btree_stats(
            self.root.map(|x| x.root),
            &self.mem,
            self.fixed_key_size,
            self.fixed_value_size,
        )
    }

    pub(crate) fn len(&self) -> Result<u64> {
        Ok(self.root.map_or(0, |x| x.length))
    }

    pub(crate) fn verify_checksum(&self) -> Result<bool> {
        if let Some(header) = self.root {
            self.verify_checksum_helper(header.root, header.checksum)
        } else {
            Ok(true)
        }
    }

    fn verify_checksum_helper(
        &self,
        page_number: PageNumber,
        expected_checksum: Checksum,
    ) -> Result<bool> {
        let page = self.mem.get_page(page_number)?;
        let node_mem = page.memory();
        Ok(match node_mem[0] {
            LEAF => {
                if let Ok(computed) =
                    leaf_checksum(&page, self.fixed_key_size, self.fixed_value_size)
                {
                    expected_checksum == computed
                } else {
                    false
                }
            }
            BRANCH => {
                if let Ok(computed) = branch_checksum(&page, self.fixed_key_size) {
                    if expected_checksum != computed {
                        return Ok(false);
                    }
                } else {
                    return Ok(false);
                }
                let accessor = BranchAccessor::new(&page, self.fixed_key_size);
                for i in 0..accessor.count_children() {
                    if !self.verify_checksum_helper(
                        accessor.child_page(i).unwrap(),
                        accessor.child_checksum(i).unwrap(),
                    )? {
                        return Ok(false);
                    }
                }
                true
            }
            _ => false,
        })
    }
}

pub(crate) struct Btree<K: Key + 'static, V: Value + 'static> {
    mem: Arc<TransactionalMemory>,
    transaction_guard: Arc<TransactionGuard>,
    // Cache of the root page to avoid repeated lookups
    cached_root: Option<PageImpl>,
    root: Option<BtreeHeader>,
    hint: PageHint,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<K: Key, V: Value> Btree<K, V> {
    pub(crate) fn new(
        root: Option<BtreeHeader>,
        hint: PageHint,
        guard: Arc<TransactionGuard>,
        mem: Arc<TransactionalMemory>,
    ) -> Result<Self> {
        let cached_root = if let Some(header) = root {
            Some(mem.get_page_extended(header.root, hint)?)
        } else {
            None
        };
        Ok(Self {
            mem,
            transaction_guard: guard,
            cached_root,
            root,
            hint,
            _key_type: Default::default(),
            _value_type: Default::default(),
        })
    }

    pub(crate) fn transaction_guard(&self) -> &Arc<TransactionGuard> {
        &self.transaction_guard
    }

    pub(crate) fn get_root(&self) -> Option<BtreeHeader> {
        self.root
    }

    pub(crate) fn visit_all_pages<F>(&self, visitor: F) -> Result
    where
        F: FnMut(&PagePath) -> Result,
    {
        let tree = UntypedBtree::new(
            self.root,
            self.mem.clone(),
            K::fixed_width(),
            V::fixed_width(),
        );
        tree.visit_all_pages(visitor)
    }

    pub(crate) fn get(&self, key: &K::SelfType<'_>) -> Result<Option<AccessGuard<'static, V>>> {
        if let Some(ref root_page) = self.cached_root {
            self.get_helper(root_page.clone(), K::as_bytes(key).as_ref())
        } else {
            Ok(None)
        }
    }

    // Returns the value for the queried key, if present
    fn get_helper(&self, page: PageImpl, query: &[u8]) -> Result<Option<AccessGuard<'static, V>>> {
        let node_mem = page.memory();
        match node_mem[0] {
            LEAF => {
                let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
                if let Some(entry_index) = accessor.find_key::<K>(query) {
                    let (start, end) = accessor.value_range(entry_index).unwrap();
                    let guard = AccessGuard::with_page(page, start..end);
                    Ok(Some(guard))
                } else {
                    Ok(None)
                }
            }
            BRANCH => {
                let accessor = BranchAccessor::new(&page, K::fixed_width());
                let (_, child_page) = accessor.child_for_key::<K>(query);
                self.get_helper(self.mem.get_page_extended(child_page, self.hint)?, query)
            }
            _ => unreachable!(),
        }
    }

    pub(crate) fn first(
        &self,
    ) -> Result<Option<(AccessGuard<'static, K>, AccessGuard<'static, V>)>> {
        if let Some(ref root) = self.cached_root {
            self.first_helper(root.clone())
        } else {
            Ok(None)
        }
    }

    fn first_helper(
        &self,
        page: PageImpl,
    ) -> Result<Option<(AccessGuard<'static, K>, AccessGuard<'static, V>)>> {
        let node_mem = page.memory();
        match node_mem[0] {
            LEAF => {
                let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
                let (key_range, value_range) = accessor.entry_ranges(0).unwrap();
                let key_guard = AccessGuard::with_page(page.clone(), key_range);
                let value_guard = AccessGuard::with_page(page, value_range);
                Ok(Some((key_guard, value_guard)))
            }
            BRANCH => {
                let accessor = BranchAccessor::new(&page, K::fixed_width());
                let child_page = accessor.child_page(0).unwrap();
                self.first_helper(self.mem.get_page_extended(child_page, self.hint)?)
            }
            _ => unreachable!(),
        }
    }

    pub(crate) fn last(
        &self,
    ) -> Result<Option<(AccessGuard<'static, K>, AccessGuard<'static, V>)>> {
        if let Some(ref root) = self.cached_root {
            self.last_helper(root.clone())
        } else {
            Ok(None)
        }
    }

    fn last_helper(
        &self,
        page: PageImpl,
    ) -> Result<Option<(AccessGuard<'static, K>, AccessGuard<'static, V>)>> {
        let node_mem = page.memory();
        match node_mem[0] {
            LEAF => {
                let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
                let (key_range, value_range) =
                    accessor.entry_ranges(accessor.num_pairs() - 1).unwrap();
                let key_guard = AccessGuard::with_page(page.clone(), key_range);
                let value_guard = AccessGuard::with_page(page, value_range);
                Ok(Some((key_guard, value_guard)))
            }
            BRANCH => {
                let accessor = BranchAccessor::new(&page, K::fixed_width());
                let child_page = accessor.child_page(accessor.count_children() - 1).unwrap();
                self.last_helper(self.mem.get_page_extended(child_page, self.hint)?)
            }
            _ => unreachable!(),
        }
    }

    pub(crate) fn range<'a0, T: RangeBounds<KR>, KR: Borrow<K::SelfType<'a0>>>(
        &self,
        range: &'_ T,
    ) -> Result<BtreeRangeIter<K, V>> {
        BtreeRangeIter::new(range, self.root.map(|x| x.root), self.mem.clone())
    }

    pub(crate) fn len(&self) -> Result<u64> {
        Ok(self.root.map_or(0, |x| x.length))
    }

    pub(crate) fn stats(&self) -> Result<BtreeStats> {
        btree_stats(
            self.root.map(|x| x.root),
            &self.mem,
            K::fixed_width(),
            V::fixed_width(),
        )
    }

    #[allow(dead_code)]
    pub(crate) fn print_debug(&self, include_values: bool) -> Result {
        if let Some(p) = self.root.map(|x| x.root) {
            let mut pages = vec![self.mem.get_page(p)?];
            while !pages.is_empty() {
                let mut next_children = vec![];
                for page in pages.drain(..) {
                    let node_mem = page.memory();
                    match node_mem[0] {
                        LEAF => {
                            eprint!("Leaf[ (page={:?})", page.get_page_number());
                            LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width())
                                .print_node::<K, V>(include_values);
                            eprint!("]");
                        }
                        BRANCH => {
                            let accessor = BranchAccessor::new(&page, K::fixed_width());
                            for i in 0..accessor.count_children() {
                                let child = accessor.child_page(i).unwrap();
                                next_children.push(self.mem.get_page(child)?);
                            }
                            accessor.print_node::<K>();
                        }
                        _ => unreachable!(),
                    }
                    eprint!("  ");
                }
                eprintln!();

                pages = next_children;
            }
        }

        Ok(())
    }
}

pub(crate) fn btree_stats(
    root: Option<PageNumber>,
    mem: &TransactionalMemory,
    fixed_key_size: Option<usize>,
    fixed_value_size: Option<usize>,
) -> Result<BtreeStats> {
    if let Some(root) = root {
        stats_helper(root, mem, fixed_key_size, fixed_value_size)
    } else {
        Ok(BtreeStats {
            tree_height: 0,
            leaf_pages: 0,
            branch_pages: 0,
            stored_leaf_bytes: 0,
            metadata_bytes: 0,
            fragmented_bytes: 0,
        })
    }
}

fn stats_helper(
    page_number: PageNumber,
    mem: &TransactionalMemory,
    fixed_key_size: Option<usize>,
    fixed_value_size: Option<usize>,
) -> Result<BtreeStats> {
    let page = mem.get_page(page_number)?;
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            let accessor = LeafAccessor::new(page.memory(), fixed_key_size, fixed_value_size);
            let leaf_bytes = accessor.length_of_pairs(0, accessor.num_pairs());
            let overhead_bytes = accessor.total_length() - leaf_bytes;
            let fragmented_bytes = (page.memory().len() - accessor.total_length()) as u64;
            Ok(BtreeStats {
                tree_height: 1,
                leaf_pages: 1,
                branch_pages: 0,
                stored_leaf_bytes: leaf_bytes.try_into().unwrap(),
                metadata_bytes: overhead_bytes.try_into().unwrap(),
                fragmented_bytes,
            })
        }
        BRANCH => {
            let accessor = BranchAccessor::new(&page, fixed_key_size);
            let mut max_child_height = 0;
            let mut leaf_pages = 0;
            let mut branch_pages = 1;
            let mut stored_leaf_bytes = 0;
            let mut metadata_bytes = accessor.total_length() as u64;
            let mut fragmented_bytes = (page.memory().len() - accessor.total_length()) as u64;
            for i in 0..accessor.count_children() {
                if let Some(child) = accessor.child_page(i) {
                    let stats = stats_helper(child, mem, fixed_key_size, fixed_value_size)?;
                    max_child_height = max(max_child_height, stats.tree_height);
                    leaf_pages += stats.leaf_pages;
                    branch_pages += stats.branch_pages;
                    stored_leaf_bytes += stats.stored_leaf_bytes;
                    metadata_bytes += stats.metadata_bytes;
                    fragmented_bytes += stats.fragmented_bytes;
                }
            }

            Ok(BtreeStats {
                tree_height: max_child_height + 1,
                leaf_pages,
                branch_pages,
                stored_leaf_bytes,
                metadata_bytes,
                fragmented_bytes,
            })
        }
        _ => unreachable!(),
    }
}
