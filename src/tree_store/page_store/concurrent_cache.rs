use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

/// Sentinel: slot is unoccupied and terminates probe chains.
const EMPTY: u64 = u64::MAX;
/// Sentinel: slot was deleted; probing continues past it.
const TOMBSTONE: u64 = u64::MAX - 1;
/// Maximum linear-probe distance before giving up or force-evicting.
const MAX_PROBE: usize = 16;

/// A concurrent hash table optimised for the read-cache workload:
///
/// * **Reads are nearly lock-free**: the probe loop loads only atomic keys;
///   a tiny per-slot spin-lock is acquired *only* on the matching slot for
///   the duration of a single `Arc::clone`.
/// * **Writes (insert / remove / evict)** lock individual slots, so they
///   never block readers on unrelated pages.
/// * Open-addressing with linear probing, power-of-2 table size.
/// * Clock (second-chance) eviction via a per-slot `recently_used` bit.
pub(super) struct ConcurrentPageCache {
    slots: Box<[CacheSlot]>,
    mask: usize,
    page_shift: u32,
    len: AtomicUsize,
    eviction_clock: AtomicUsize,
}

// SAFETY: All mutable access to `CacheSlot::value` (an `UnsafeCell`) is
// serialised by the per-slot spin-lock.  Concurrent readers acquire the
// same lock before cloning the `Arc`, so no data race is possible.
unsafe impl Sync for ConcurrentPageCache {}
unsafe impl Send for ConcurrentPageCache {}

struct CacheSlot {
    /// The page's file offset, or `EMPTY` / `TOMBSTONE`.
    key: AtomicU64,
    /// Per-slot exclusive spin-lock (test-and-test-and-set).
    lock: AtomicBool,
    /// Second-chance bit for clock eviction.
    recently_used: AtomicBool,
    /// The cached page data.  Access is protected by `lock`.
    value: UnsafeCell<Option<Arc<[u8]>>>,
}

impl CacheSlot {
    fn new() -> Self {
        Self {
            key: AtomicU64::new(EMPTY),
            lock: AtomicBool::new(false),
            recently_used: AtomicBool::new(false),
            value: UnsafeCell::new(None),
        }
    }

    #[inline(always)]
    fn lock(&self) {
        // Fast path: uncontended CAS.
        if self
            .lock
            .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            return;
        }
        // Slow path: spin with back-off.
        self.lock_slow();
    }

    #[cold]
    fn lock_slow(&self) {
        loop {
            // Wait until the lock *looks* free (no cache-line bouncing).
            while self.lock.load(Ordering::Relaxed) {
                std::hint::spin_loop();
            }
            if self
                .lock
                .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
        }
    }

    #[inline(always)]
    fn unlock(&self) {
        self.lock.store(false, Ordering::Release);
    }
}

impl ConcurrentPageCache {
    /// Create a new cache sized to hold all pages that fit in `max_cache_bytes`
    /// at ≤ 50 % load factor.
    pub(super) fn new(max_cache_bytes: usize, page_size: u64) -> Self {
        let max_pages = (max_cache_bytes / page_size as usize).max(1);
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
    #[inline(always)]
    fn slot_index(&self, key: u64) -> usize {
        let page_num = key >> self.page_shift;
        // Fibonacci / multiplicative hash – distributes page-aligned offsets
        // uniformly across the power-of-2 table.
        let h = page_num.wrapping_mul(0x517c_c1b7_2722_0a95);
        (h as usize) & self.mask
    }

    // ── read path (hot) ──────────────────────────────────────────────────

    /// Look up a cached page.  The probe loop is lock-free; a per-slot
    /// spin-lock is acquired only on the matching entry for the `Arc::clone`.
    #[inline]
    pub(super) fn get(&self, key: u64) -> Option<Arc<[u8]>> {
        debug_assert!(key != EMPTY && key != TOMBSTONE);
        let start = self.slot_index(key);

        for i in 0..MAX_PROBE {
            let idx = (start + i) & self.mask;
            let slot = &self.slots[idx];
            let k = slot.key.load(Ordering::Acquire);

            if k == EMPTY {
                return None; // end of probe chain
            }
            if k == key {
                // Candidate hit – lock, re-validate, clone.
                slot.lock();
                if slot.key.load(Ordering::Relaxed) == key {
                    slot.recently_used.store(true, Ordering::Relaxed);
                    // SAFETY: lock is held, so no concurrent mutation.
                    let result = unsafe { (*slot.value.get()).clone() };
                    slot.unlock();
                    return result;
                }
                slot.unlock();
                // Key was changed (evicted) between the optimistic load and the
                // lock.  The entry may have moved further down the chain, so
                // keep probing.
            }
            // TOMBSTONE or different key → continue probing
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

            slot.lock();
            let k = slot.key.load(Ordering::Relaxed);

            if k == key || k == EMPTY || k == TOMBSTONE {
                // Usable slot: replace / fresh insert.
                // SAFETY: lock is held.
                let old = unsafe { (*slot.value.get()).replace(value) };
                if k != key {
                    self.len.fetch_add(1, Ordering::Relaxed);
                }
                slot.key.store(key, Ordering::Release);
                slot.recently_used.store(true, Ordering::Relaxed);
                slot.unlock();
                return old;
            }

            slot.unlock();
        }

        // Probe chain exhausted – force-evict the home slot.
        let slot = &self.slots[start & self.mask];
        slot.lock();
        let old_key = slot.key.load(Ordering::Relaxed);
        // SAFETY: lock is held.
        let old = unsafe { (*slot.value.get()).replace(value) };
        slot.key.store(key, Ordering::Release);
        slot.recently_used.store(true, Ordering::Relaxed);
        slot.unlock();

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
                slot.lock();
                if slot.key.load(Ordering::Relaxed) == key {
                    slot.key.store(TOMBSTONE, Ordering::Release);
                    // SAFETY: lock is held.
                    let old = unsafe { (*slot.value.get()).take() };
                    slot.unlock();
                    if old.is_some() {
                        self.len.fetch_sub(1, Ordering::Relaxed);
                    }
                    return old;
                }
                slot.unlock();
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

            slot.lock();
            let k = slot.key.load(Ordering::Relaxed);
            if k == EMPTY || k == TOMBSTONE {
                slot.unlock();
                continue;
            }

            // Second-chance: skip if recently used, but clear the bit.
            if slot.recently_used.load(Ordering::Relaxed) {
                slot.recently_used.store(false, Ordering::Relaxed);
                slot.unlock();
                continue;
            }

            slot.key.store(TOMBSTONE, Ordering::Release);
            // SAFETY: lock is held.
            let value = unsafe { (*slot.value.get()).take() };
            slot.unlock();

            if let Some(v) = value {
                self.len.fetch_sub(1, Ordering::Relaxed);
                return Some((k, v));
            }
        }
        None
    }

    /// Drop every entry and reset the table.
    pub(super) fn clear(&self) {
        for slot in self.slots.iter() {
            slot.lock();
            slot.key.store(EMPTY, Ordering::Relaxed);
            // SAFETY: lock is held.
            unsafe {
                *slot.value.get() = None;
            }
            slot.recently_used.store(false, Ordering::Relaxed);
            slot.unlock();
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

        // Insert pages that all hash to the same home slot
        // We can't easily control the hash, so insert many pages and verify
        // they're all retrievable.
        for i in 0..(n / 2) {
            let offset = (i as u64) * 4096;
            let data: Arc<[u8]> = vec![i as u8; 4096].into();
            cache.insert(offset, data);
        }

        for i in 0..(n / 2) {
            let offset = (i as u64) * 4096;
            let got = cache.get(offset);
            // Some entries may have been evicted due to probe-chain overflow,
            // but those that remain must have correct data.
            if let Some(v) = got {
                assert_eq!(v[0], i as u8);
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
        for i in 0..100 {
            let offset = (i as u64) * 4096;
            let data: Arc<[u8]> = vec![i as u8; 4096].into();
            cache.insert(offset, data);
        }

        // Read from 8 threads
        let mut handles = vec![];
        for _ in 0..8 {
            let c = cache.clone();
            handles.push(std::thread::spawn(move || {
                for _ in 0..1000 {
                    for i in 0..100u8 {
                        let got = c.get((i as u64) * 4096);
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
}
