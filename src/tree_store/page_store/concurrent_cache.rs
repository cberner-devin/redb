use std::sync::Arc;
use std::sync::RwLock;
use std::sync::RwLockReadGuard;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

/// A borrowed reference to a cached page.  Holds the cache slot's
/// read-lock, preventing eviction while alive.  No `Arc` refcount
/// operations are performed.
pub(crate) struct CacheGuard<'a> {
    guard: RwLockReadGuard<'a, Option<Arc<[u8]>>>,
}

impl CacheGuard<'_> {
    #[inline]
    pub(crate) fn data(&self) -> &[u8] {
        self.guard.as_ref().unwrap().as_ref()
    }
}

/// Sentinel: slot is unoccupied and terminates probe chains.
const EMPTY: u64 = u64::MAX;
/// Sentinel: slot was deleted; probing continues past it.
const TOMBSTONE: u64 = u64::MAX - 1;
/// Maximum linear-probe distance before giving up or force-evicting.
const MAX_PROBE: usize = 16;

/// A concurrent hash table optimised for the read-cache workload:
///
/// * **Reads are nearly lock-free**: the probe loop loads only atomic keys;
///   a shared per-slot `RwLock` is acquired only on the matching slot for the
///   duration of a single `Arc::clone` or branch-page borrow, allowing many
///   threads to read the same hot page simultaneously.
/// * **Writes (insert / remove / evict)** take an exclusive lock on individual
///   slots, so they never block readers on unrelated pages.
/// * Open-addressing with linear probing, power-of-2 table size.
/// * Clock (second-chance) eviction via a per-slot `recently_used` bit.
pub(super) struct ConcurrentPageCache {
    slots: Box<[CacheSlot]>,
    mask: usize,
    page_shift: u32,
    len: AtomicUsize,
    eviction_clock: AtomicUsize,
}

struct CacheSlot {
    /// The page's file offset, or `EMPTY` / `TOMBSTONE`.
    key: AtomicU64,
    /// Per-slot reader-writer lock.
    value: RwLock<Option<Arc<[u8]>>>,
    /// Second-chance bit for clock eviction.  Accessed with `Relaxed`
    /// ordering; it is only a heuristic for eviction priority.
    recently_used: AtomicBool,
}

impl CacheSlot {
    fn new() -> Self {
        Self {
            key: AtomicU64::new(EMPTY),
            value: RwLock::new(None),
            recently_used: AtomicBool::new(false),
        }
    }
}

impl ConcurrentPageCache {
    /// Create a new cache sized to hold all pages that fit in `max_cache_bytes`
    /// at ≤ 50 % load factor.
    pub(super) fn new(max_cache_bytes: usize, page_size: u64) -> Self {
        let page_size: usize = page_size.try_into().unwrap();
        let max_pages = (max_cache_bytes / page_size).max(1);
        let num_slots = (max_pages * 2).max(16).next_power_of_two();

        let mut slots = Vec::with_capacity(num_slots);
        for _ in 0..num_slots {
            slots.push(CacheSlot::new());
        }

        Self {
            slots: slots.into_boxed_slice(),
            mask: num_slots - 1,
            page_shift: page_size.trailing_zeros(),
            len: AtomicUsize::new(0),
            eviction_clock: AtomicUsize::new(0),
        }
    }

    /// Map a page-aligned file offset to a slot index.
    #[inline]
    fn slot_index(&self, key: u64) -> usize {
        let page_num = key >> self.page_shift;
        // Fibonacci / multiplicative hash – distributes page-aligned offsets
        // uniformly across the power-of-2 table.
        let h = page_num.wrapping_mul(0x517c_c1b7_2722_0a95);
        let mask = u64::try_from(self.mask).unwrap();
        usize::try_from(h & mask).unwrap()
    }

    // ── read path (hot) ──────────────────────────────────────────────────

    /// Look up a cached page and clone the `Arc`.
    #[inline]
    pub(super) fn get(&self, key: u64) -> Option<Arc<[u8]>> {
        debug_assert!(key != EMPTY && key != TOMBSTONE);
        let start = self.slot_index(key);

        for i in 0..MAX_PROBE {
            let idx = (start + i) & self.mask;
            let slot = &self.slots[idx];
            let k = slot.key.load(Ordering::Acquire);

            if k == EMPTY {
                return None;
            }
            if k == key {
                let guard = slot.value.read().unwrap();
                if slot.key.load(Ordering::Acquire) == key {
                    slot.recently_used.store(true, Ordering::Relaxed);
                    let result = guard.clone();
                    return result;
                }
            }
        }
        None
    }

    /// Look up a cached page and return a borrowed guard instead of cloning
    /// the `Arc`.  The guard holds the slot's read-lock, so the data cannot
    /// be evicted while the guard is alive.  Multiple guards on the same
    /// slot can coexist (shared read-lock).
    ///
    /// This is faster than `get()` because it avoids the `Arc::clone` +
    /// `Arc::drop` pair (2 atomic RMW operations on a potentially contended
    /// refcount cache line).
    #[inline]
    pub(super) fn get_borrowed(&self, key: u64) -> Option<CacheGuard<'_>> {
        debug_assert!(key != EMPTY && key != TOMBSTONE);
        let start = self.slot_index(key);

        for i in 0..MAX_PROBE {
            let idx = (start + i) & self.mask;
            let slot = &self.slots[idx];
            let k = slot.key.load(Ordering::Acquire);

            if k == EMPTY {
                return None;
            }
            if k == key {
                let guard = slot.value.read().unwrap();
                if slot.key.load(Ordering::Acquire) == key && guard.is_some() {
                    slot.recently_used.store(true, Ordering::Relaxed);
                    return Some(CacheGuard { guard });
                }
            }
        }
        None
    }

    // ── write path ───────────────────────────────────────────────────────

    /// Insert a page into the cache.
    ///
    /// Returns the previous value if the key already existed, or the evicted
    /// value if a slot had to be reclaimed because the probe chain was full.
    pub(super) fn insert(&self, key: u64, value: Arc<[u8]>) -> Option<Arc<[u8]>> {
        debug_assert!(key != EMPTY && key != TOMBSTONE);
        let start = self.slot_index(key);

        // Probe for the key, or the first usable (empty/tombstone) slot.
        for i in 0..MAX_PROBE {
            let idx = (start + i) & self.mask;
            let slot = &self.slots[idx];

            let mut guard = slot.value.write().unwrap();
            let k = slot.key.load(Ordering::Acquire);

            if k == key || k == EMPTY || k == TOMBSTONE {
                let old = guard.replace(value);
                if k != key {
                    self.len.fetch_add(1, Ordering::Relaxed);
                }
                slot.key.store(key, Ordering::Release);
                slot.recently_used.store(true, Ordering::Relaxed);
                return old;
            }
        }

        // Probe chain exhausted – force-evict the home slot.
        let slot = &self.slots[start & self.mask];
        let mut guard = slot.value.write().unwrap();
        let old_key = slot.key.load(Ordering::Acquire);
        let old = guard.replace(value);
        slot.key.store(key, Ordering::Release);
        slot.recently_used.store(true, Ordering::Relaxed);

        if old_key == EMPTY || old_key == TOMBSTONE {
            self.len.fetch_add(1, Ordering::Relaxed);
        }
        old
    }

    /// Remove a specific key from the cache.
    pub(super) fn remove(&self, key: u64) -> Option<Arc<[u8]>> {
        debug_assert!(key != EMPTY && key != TOMBSTONE);
        let start = self.slot_index(key);

        for i in 0..MAX_PROBE {
            let idx = (start + i) & self.mask;
            let slot = &self.slots[idx];
            let k = slot.key.load(Ordering::Acquire);

            if k == EMPTY {
                return None;
            }
            if k == key {
                let mut guard = slot.value.write().unwrap();
                if slot.key.load(Ordering::Acquire) == key {
                    slot.key.store(TOMBSTONE, Ordering::Release);
                    let old = guard.take();
                    if old.is_some() {
                        self.len.fetch_sub(1, Ordering::Relaxed);
                    }
                    return old;
                }
            }
        }
        None
    }

    /// Evict one entry using a clock-sweep (second-chance) algorithm.
    pub(super) fn pop_one(&self) -> Option<(u64, Arc<[u8]>)> {
        let n = self.slots.len();
        let start = self.eviction_clock.fetch_add(1, Ordering::Relaxed) % n;

        // Sweep up to the full table; second-chance may skip entries once.
        for j in 0..n {
            let idx = (start + j) % n;
            let slot = &self.slots[idx];
            let k = slot.key.load(Ordering::Relaxed);

            if k == EMPTY || k == TOMBSTONE {
                continue;
            }

            let mut guard = slot.value.write().unwrap();
            let k = slot.key.load(Ordering::Acquire);
            if k == EMPTY || k == TOMBSTONE {
                continue;
            }

            // Second-chance: skip if recently used, but clear the bit.
            if slot.recently_used.swap(false, Ordering::Relaxed) {
                continue;
            }

            slot.key.store(TOMBSTONE, Ordering::Release);
            let value = guard.take();

            if let Some(v) = value {
                self.len.fetch_sub(1, Ordering::Relaxed);
                return Some((k, v));
            }
        }
        None
    }

    /// Drop every entry and reset the table.
    pub(super) fn clear(&self) {
        for slot in &*self.slots {
            let mut guard = slot.value.write().unwrap();
            slot.key.store(EMPTY, Ordering::Relaxed);
            *guard = None;
            slot.recently_used.store(false, Ordering::Relaxed);
        }
        self.len.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_insert_get_remove() {
        let cache = ConcurrentPageCache::new(4096 * 16, 4096);

        // Miss
        assert!(cache.get(0).is_none());

        // Insert
        let data: Arc<[u8]> = vec![1u8; 4096].into();
        assert!(cache.insert(0, data.clone()).is_none());

        // Hit
        let got = cache.get(0).unwrap();
        assert_eq!(got.len(), 4096);
        assert_eq!(got[0], 1);

        // Replace
        let data2: Arc<[u8]> = vec![2u8; 4096].into();
        let old = cache.insert(0, data2).unwrap();
        assert_eq!(old[0], 1);

        // New value
        let got = cache.get(0).unwrap();
        assert_eq!(got[0], 2);

        // Remove
        let removed = cache.remove(0).unwrap();
        assert_eq!(removed[0], 2);
        assert!(cache.get(0).is_none());
    }

    #[test]
    fn probe_chain() {
        // Small table to force collisions
        let cache = ConcurrentPageCache::new(4096 * 4, 4096);
        let n = cache.slots.len();

        // Insert many pages and verify they're all retrievable.
        for i in 0..(n / 2) {
            let offset = u64::try_from(i).unwrap() * 4096;
            let data: Arc<[u8]> = vec![u8::try_from(i).unwrap(); 4096].into();
            cache.insert(offset, data);
        }

        for i in 0..(n / 2) {
            let offset = u64::try_from(i).unwrap() * 4096;
            let got = cache.get(offset);
            // Some entries may have been evicted due to probe-chain overflow,
            // but those that remain must have correct data.
            if let Some(v) = got {
                assert_eq!(v[0], u8::try_from(i).unwrap());
            }
        }
    }

    #[test]
    fn eviction_clock() {
        let cache = ConcurrentPageCache::new(4096 * 16, 4096);
        let data: Arc<[u8]> = vec![42u8; 4096].into();

        // Insert a few pages
        for i in 0..4 {
            cache.insert(i * 4096, data.clone());
        }

        // First pop_one sweep will clear the recently_used bits (second-chance).
        // A second call finds an evictable entry.
        let _ = cache.pop_one(); // may or may not evict (clears bits)
        let evicted = cache.pop_one();
        assert!(evicted.is_some());
        let (_, v) = evicted.unwrap();
        assert_eq!(v[0], 42);
    }

    #[test]
    fn clear_resets() {
        let cache = ConcurrentPageCache::new(4096 * 16, 4096);
        let data: Arc<[u8]> = vec![1u8; 4096].into();

        cache.insert(0, data.clone());
        cache.insert(4096, data.clone());
        assert!(cache.get(0).is_some());

        cache.clear();
        assert!(cache.get(0).is_none());
        assert!(cache.get(4096).is_none());
        assert_eq!(cache.len.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn concurrent_reads() {
        let cache = Arc::new(ConcurrentPageCache::new(4096 * 1024, 4096));

        // Pre-populate
        for i in 0u8..100 {
            let offset = u64::from(i) * 4096;
            let data: Arc<[u8]> = vec![i; 4096].into();
            cache.insert(offset, data);
        }

        // Read from 8 threads concurrently
        let mut handles = vec![];
        for _ in 0..8 {
            let c = cache.clone();
            handles.push(std::thread::spawn(move || {
                for _ in 0..1000 {
                    for i in 0..100u8 {
                        let got = c.get(u64::from(i) * 4096);
                        if let Some(v) = got {
                            assert_eq!(v[0], i);
                        }
                    }
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn concurrent_reads_same_key() {
        // Specifically test that many threads can read the same hot key
        // simultaneously without degradation (the root-page scenario).
        let cache = Arc::new(ConcurrentPageCache::new(4096 * 16, 4096));
        let data: Arc<[u8]> = vec![99u8; 4096].into();
        cache.insert(0, data);

        let mut handles = vec![];
        for _ in 0..16 {
            let c = cache.clone();
            handles.push(std::thread::spawn(move || {
                for _ in 0..100_000 {
                    let got = c.get(0).unwrap();
                    assert_eq!(got[0], 99);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }
}
