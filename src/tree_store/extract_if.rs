use crate::Result;
use crate::tree_store::btree_iters::EntryGuard;
use crate::tree_store::btree_mutator::BtreeRangeCursorMut;
use crate::tree_store::{BtreeHeader, PageAllocator, PageNumber, PageTrackerPolicy};
use crate::types::{Key, Value};
use std::borrow::Borrow;
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex};

pub(crate) struct BtreeExtractIf<
    'a,
    K: Key + 'static,
    V: Value + 'static,
    F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
> {
    inner: BtreeRangeCursorMut<'a, K, V>,
    predicate: F,
    predicate_running: bool,
}

impl<'a, K: Key, V: Value, F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool>
    BtreeExtractIf<'a, K, V, F>
{
    pub(crate) fn new<'r, KR>(
        root: &'a mut Option<BtreeHeader>,
        range: &'_ impl RangeBounds<KR>,
        predicate: F,
        master_free_list: Arc<Mutex<Vec<PageNumber>>>,
        allocated: Arc<Mutex<PageTrackerPolicy>>,
        page_allocator: PageAllocator,
    ) -> Self
    where
        KR: Borrow<K::SelfType<'r>> + 'r,
    {
        Self {
            inner: BtreeRangeCursorMut::new(
                root,
                range,
                page_allocator,
                allocated,
                master_free_list,
            ),
            predicate,
            predicate_running: false,
        }
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
        self.inner.close()
    }
}

impl<K: Key, V: Value, F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool> Iterator
    for BtreeExtractIf<'_, K, V, F>
{
    type Item = Result<EntryGuard<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let entry = match self.inner.next() {
                Ok(Some(entry)) => entry,
                Ok(None) => return None,
                Err(err) => return Some(Err(err)),
            };
            let guard = entry.into_entry_guard::<K, V>();
            if self.predicate_matches(&guard) {
                assert!(self.inner.remove_prev());
                return Some(Ok(guard));
            }
        }
    }
}

impl<K: Key, V: Value, F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool>
    DoubleEndedIterator for BtreeExtractIf<'_, K, V, F>
{
    fn next_back(&mut self) -> Option<Self::Item> {
        loop {
            let entry = match self.inner.next_back() {
                Ok(Some(entry)) => entry,
                Ok(None) => return None,
                Err(err) => return Some(Err(err)),
            };
            let guard = entry.into_entry_guard::<K, V>();
            if self.predicate_matches(&guard) {
                assert!(self.inner.remove_prev());
                return Some(Ok(guard));
            }
        }
    }
}

impl<K: Key, V: Value, F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool> Drop
    for BtreeExtractIf<'_, K, V, F>
{
    fn drop(&mut self) {
        let _ = self.close();
    }
}
