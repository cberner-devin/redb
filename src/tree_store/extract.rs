use crate::Result;
use crate::tree_store::btree_base::{BRANCH, BranchAccessor, Checksum, LEAF, LeafAccessor};
use crate::tree_store::btree_iters::EntryGuard;
use crate::tree_store::page_store::{Page, PageImpl};
use crate::tree_store::retain::{RetainBuilderContext, RetainSubtree, RetainSubtreeBuilder};
use crate::tree_store::{BtreeHeader, PageAllocator, PageNumber, PageTrackerPolicy};
use crate::types::{Key, Value};
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::{Bound, VecDeque};
use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex};

enum ExtractWalkResult {
    Unchanged(RetainSubtree),
    Changed,
}

struct ExtractWalkContext {
    checksum: Checksum,
    upper_key: Option<Vec<u8>>,
    root_distance: u32,
}

// Stable inputs/outputs that thread through the entire walk: the user's predicate, the range
// it gates on, the running count of extracted entries, the iterator buffer that collects
// EntryGuards backed by the original leaf pages, and the list of those leaf pages whose
// freeing must be deferred until the iterator is dropped (so that the EntryGuard PageImpls
// stay valid bytes).
pub(crate) struct ExtractSink<'p, 'k, K: Key + 'static, V: Value + 'static, F>
where
    F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
{
    predicate: &'p mut F,
    bounds: KeyRange<'k, K>,
    extracted: u64,
    buffer: VecDeque<EntryGuard<K, V>>,
    deferred_free: Vec<PageNumber>,
}

struct KeyRange<'a, K: Key + ?Sized> {
    start: Bound<&'a [u8]>,
    end: Bound<&'a [u8]>,
    _key: PhantomData<K>,
}

impl<K: Key + ?Sized> Copy for KeyRange<'_, K> {}

impl<K: Key + ?Sized> Clone for KeyRange<'_, K> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, K: Key + ?Sized> KeyRange<'a, K> {
    fn new(start: Bound<&'a [u8]>, end: Bound<&'a [u8]>) -> Self {
        Self {
            start,
            end,
            _key: PhantomData,
        }
    }

    fn contains(&self, key: &[u8]) -> bool {
        !self.less_than_start(key) && !self.greater_than_end(key)
    }

    fn less_than_start(&self, key: &[u8]) -> bool {
        match self.start {
            Bound::Included(start) => K::compare(key, start) == Ordering::Less,
            Bound::Excluded(start) => K::compare(key, start) != Ordering::Greater,
            Bound::Unbounded => false,
        }
    }

    fn greater_than_end(&self, key: &[u8]) -> bool {
        match self.end {
            Bound::Included(end) => K::compare(key, end) == Ordering::Greater,
            Bound::Excluded(end) => K::compare(key, end) != Ordering::Less,
            Bound::Unbounded => false,
        }
    }

    fn child_lower_bound_is_past_end(&self, lower_bound: &[u8]) -> bool {
        match self.end {
            Bound::Included(end) | Bound::Excluded(end) => {
                K::compare(lower_bound, end) != Ordering::Less
            }
            Bound::Unbounded => false,
        }
    }
}

pub(crate) struct ExtractResult<K: Key + 'static, V: Value + 'static> {
    pub(crate) buffer: VecDeque<EntryGuard<K, V>>,
    // Leaf pages we extracted entries from. The iterator owns these and frees them in Drop,
    // so the buffered EntryGuard PageImpls stay valid bytes for the iterator's lifetime.
    pub(crate) deferred_free: Vec<PageNumber>,
}

// Eagerly walks the original tree, extracting entries whose key is in `range` and for which
// `predicate` returns true. The remaining entries are stitched into a new tree using the
// streaming subtree builder shared with `retain`. Extracted entries are buffered as
// `EntryGuard`s referencing the original leaf pages; those pages are returned in
// `deferred_free` and freed by the iterator on drop, so the buffered guards stay valid.
pub(crate) fn extract_from_if<'r, K, V, KR, F>(
    root: &mut Option<BtreeHeader>,
    page_allocator: &PageAllocator,
    allocated: &Arc<Mutex<PageTrackerPolicy>>,
    freed: &mut Vec<PageNumber>,
    range: &impl RangeBounds<KR>,
    mut predicate: F,
) -> Result<ExtractResult<K, V>>
where
    K: Key + 'static,
    V: Value + 'static,
    KR: Borrow<K::SelfType<'r>> + 'r,
    F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
{
    let Some(header) = *root else {
        return Ok(ExtractResult {
            buffer: VecDeque::new(),
            deferred_free: Vec::new(),
        });
    };

    let start_tmp = match range.start_bound() {
        Bound::Included(key) | Bound::Excluded(key) => Some(K::as_bytes(key.borrow())),
        Bound::Unbounded => None,
    };
    let end_tmp = match range.end_bound() {
        Bound::Included(key) | Bound::Excluded(key) => Some(K::as_bytes(key.borrow())),
        Bound::Unbounded => None,
    };
    let start_bound = match (range.start_bound(), start_tmp.as_ref()) {
        (Bound::Included(_), Some(bytes)) => Bound::Included(bytes.as_ref()),
        (Bound::Excluded(_), Some(bytes)) => Bound::Excluded(bytes.as_ref()),
        _ => Bound::Unbounded,
    };
    let end_bound = match (range.end_bound(), end_tmp.as_ref()) {
        (Bound::Included(_), Some(bytes)) => Bound::Included(bytes.as_ref()),
        (Bound::Excluded(_), Some(bytes)) => Bound::Excluded(bytes.as_ref()),
        _ => Bound::Unbounded,
    };
    let bounds = KeyRange::<K>::new(start_bound, end_bound);

    let mut sink = ExtractSink {
        predicate: &mut predicate,
        bounds,
        extracted: 0,
        buffer: VecDeque::new(),
        deferred_free: Vec::new(),
    };
    let new_root = {
        // modify_uncommitted=true: branch pages and unmodified leaf pages get freed normally.
        // Leaf pages we extracted from bypass conditional_free and are pushed onto
        // `sink.deferred_free` instead, since the iterator's EntryGuards still reference them.
        let mut context = RetainBuilderContext::<K, V>::new(page_allocator, allocated, freed, true);
        let mut builder = RetainSubtreeBuilder::new();
        let root_page = context.get_page(header.root)?;
        let walk_result = extract_walk(
            &mut context,
            root_page,
            ExtractWalkContext {
                checksum: header.checksum,
                upper_key: None,
                root_distance: 0,
            },
            &mut builder,
            &mut sink,
        )?;

        match walk_result {
            ExtractWalkResult::Unchanged(_) => Some(header),
            ExtractWalkResult::Changed => {
                if let Some((root_page, checksum)) = builder.finish_root(&mut context)? {
                    Some(BtreeHeader::new(
                        root_page,
                        checksum,
                        header.length - sink.extracted,
                    ))
                } else {
                    None
                }
            }
        }
    };
    *root = new_root;

    Ok(ExtractResult {
        buffer: sink.buffer,
        deferred_free: sink.deferred_free,
    })
}

fn extract_walk<K, V, F>(
    context: &mut RetainBuilderContext<'_, K, V>,
    page: PageImpl,
    walk_ctx: ExtractWalkContext,
    builder: &mut RetainSubtreeBuilder,
    sink: &mut ExtractSink<'_, '_, K, V, F>,
) -> Result<ExtractWalkResult>
where
    K: Key + 'static,
    V: Value + 'static,
    F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
{
    match page.memory()[0] {
        LEAF => extract_walk_leaf::<K, V, F>(context, page, walk_ctx, builder, sink),
        BRANCH => extract_walk_branch::<K, V, F>(context, page, walk_ctx, builder, sink),
        _ => unreachable!(),
    }
}

fn extract_walk_leaf<K, V, F>(
    context: &mut RetainBuilderContext<'_, K, V>,
    page: PageImpl,
    walk_ctx: ExtractWalkContext,
    builder: &mut RetainSubtreeBuilder,
    sink: &mut ExtractSink<'_, '_, K, V, F>,
) -> Result<ExtractWalkResult>
where
    K: Key + 'static,
    V: Value + 'static,
    F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
{
    let page_number = page.get_page_number();
    let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
    let num_pairs = accessor.num_pairs();
    let mut extract_mask = Vec::with_capacity(num_pairs);
    let mut extracted_count = 0;
    for i in 0..num_pairs {
        let entry = accessor.entry(i).unwrap();
        let extract_entry = if sink.bounds.contains(entry.key()) {
            (sink.predicate)(K::from_bytes(entry.key()), V::from_bytes(entry.value()))
        } else {
            false
        };
        extract_mask.push(extract_entry);
        if extract_entry {
            extracted_count += 1;
        }
    }

    if extracted_count == 0 {
        return Ok(ExtractWalkResult::Unchanged(RetainSubtree::new(
            page_number,
            walk_ctx.checksum,
            walk_ctx.upper_key,
            walk_ctx.root_distance,
        )));
    }

    // Push extracted entries to the iterator buffer, holding clones of the original leaf page
    // so the bytes remain readable after we queue the page for freeing. Push retained entries
    // (if any) into the streaming builder so they end up in the rebuilt subtree.
    for (i, &extract) in extract_mask.iter().enumerate() {
        if extract {
            let (key_range, value_range) = accessor.entry_ranges(i).unwrap();
            sink.buffer
                .push_back(EntryGuard::new(page.clone(), key_range, value_range));
            sink.extracted += 1;
        } else {
            let entry = accessor.entry(i).unwrap();
            builder.push_leaf_entry(context, entry.key(), entry.value(), walk_ctx.root_distance)?;
        }
    }

    // Defer the leaf-page free until the iterator is dropped so the EntryGuards keep
    // referencing valid bytes. The iterator runs `free_if_uncommitted` (or pushes to the
    // master `freed` list for committed pages) once it goes out of scope.
    drop(page);
    sink.deferred_free.push(page_number);
    Ok(ExtractWalkResult::Changed)
}

fn extract_walk_branch<K, V, F>(
    context: &mut RetainBuilderContext<'_, K, V>,
    page: PageImpl,
    walk_ctx: ExtractWalkContext,
    builder: &mut RetainSubtreeBuilder,
    sink: &mut ExtractSink<'_, '_, K, V, F>,
) -> Result<ExtractWalkResult>
where
    K: Key + 'static,
    V: Value + 'static,
    F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
{
    let page_number = page.get_page_number();
    let accessor = BranchAccessor::new(&page, K::fixed_width());
    let child_count = accessor.count_children();
    let mut pending_unchanged: Vec<RetainSubtree> = Vec::new();
    let child_root_distance = walk_ctx.root_distance + 1;
    let mut changed = false;

    for i in 0..child_count {
        let child_upper_key = if i + 1 < child_count {
            Some(accessor.key(i).unwrap().to_vec())
        } else {
            walk_ctx.upper_key.clone()
        };
        let below_start =
            i + 1 < child_count && sink.bounds.less_than_start(accessor.key(i).unwrap());
        let above_end = i > 0
            && sink
                .bounds
                .child_lower_bound_is_past_end(accessor.key(i - 1).unwrap());
        if below_start || above_end {
            pending_unchanged.push(RetainSubtree::branch_child(
                &accessor,
                walk_ctx.upper_key.as_deref(),
                child_root_distance,
                i,
            ));
            continue;
        }

        let child_page_number = accessor.child_page(i).unwrap();
        let child_checksum = accessor.child_checksum(i).unwrap();
        let child_page = context.get_page(child_page_number)?;
        let mut child_builder = RetainSubtreeBuilder::new();
        let child = extract_walk::<K, V, F>(
            context,
            child_page,
            ExtractWalkContext {
                checksum: child_checksum,
                upper_key: child_upper_key,
                root_distance: child_root_distance,
            },
            &mut child_builder,
            sink,
        )?;
        match child {
            ExtractWalkResult::Unchanged(node) => {
                debug_assert!(child_builder.is_empty());
                debug_assert_eq!(node.root_distance(), child_root_distance);
                pending_unchanged.push(node);
            }
            ExtractWalkResult::Changed => {
                changed = true;
                for subtree in pending_unchanged.drain(..) {
                    builder.push_subtree(context, subtree)?;
                }
                builder.append(context, child_builder)?;
            }
        }
    }

    if !changed {
        return Ok(ExtractWalkResult::Unchanged(RetainSubtree::new(
            page_number,
            walk_ctx.checksum,
            walk_ctx.upper_key,
            walk_ctx.root_distance,
        )));
    }

    for subtree in pending_unchanged {
        builder.push_subtree(context, subtree)?;
    }

    drop(page);
    context.conditional_free(page_number);

    Ok(ExtractWalkResult::Changed)
}
