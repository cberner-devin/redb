use crate::tree_store::btree_base::{BranchAccessor, LeafAccessor};
use crate::tree_store::page_store::PageImpl;
use crate::types::Key;
use std::borrow::Borrow;
use std::ops::{Bound, Range, RangeBounds};

pub(super) fn lower_bound_entry<K: Key>(accessor: &LeafAccessor<'_>, bound: Bound<&[u8]>) -> usize {
    match bound {
        Bound::Included(query) | Bound::Excluded(query) => {
            let (mut position, found) = accessor.position::<K>(query);
            if matches!(bound, Bound::Excluded(_)) && found {
                position += 1;
            }
            position
        }
        Bound::Unbounded => 0,
    }
}

fn upper_bound_entry<K: Key>(accessor: &LeafAccessor<'_>, bound: Bound<&[u8]>) -> usize {
    match bound {
        Bound::Included(query) | Bound::Excluded(query) => {
            let (mut position, found) = accessor.position::<K>(query);
            if matches!(bound, Bound::Included(_)) && found {
                position += 1;
            }
            position
        }
        Bound::Unbounded => accessor.num_pairs(),
    }
}

pub(super) fn leaf_entries<K: Key>(
    accessor: &LeafAccessor<'_>,
    left_bound: Bound<&[u8]>,
    right_bound: Bound<&[u8]>,
) -> Range<usize> {
    let start = lower_bound_entry::<K>(accessor, left_bound);
    let end = upper_bound_entry::<K>(accessor, right_bound);
    start..end
}

pub(super) fn child_index_for_bound<K: Key>(
    accessor: &BranchAccessor<'_, '_, PageImpl>,
    bound: Bound<&[u8]>,
    unbounded_child: usize,
) -> usize {
    match bound {
        Bound::Included(query) | Bound::Excluded(query) => accessor.child_for_key::<K>(query).0,
        Bound::Unbounded => unbounded_child,
    }
}

pub(super) fn range_is_empty<'a, K, KR, T>(range: &T) -> bool
where
    K: Key + 'static,
    KR: Borrow<K::SelfType<'a>>,
    T: RangeBounds<KR> + ?Sized,
{
    match (range.start_bound(), range.end_bound()) {
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => false,
        (Bound::Included(start), Bound::Excluded(end))
        | (Bound::Excluded(start), Bound::Included(end) | Bound::Excluded(end)) => {
            let start = K::as_bytes(start.borrow());
            let end = K::as_bytes(end.borrow());
            K::compare(start.as_ref(), end.as_ref()).is_ge()
        }
        (Bound::Included(start), Bound::Included(end)) => {
            let start = K::as_bytes(start.borrow());
            let end = K::as_bytes(end.borrow());
            K::compare(start.as_ref(), end.as_ref()).is_gt()
        }
    }
}
