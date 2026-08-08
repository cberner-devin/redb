use crate::tree_store::page_store::cached_file::WritablePage;
use crate::tree_store::page_store::fast_hash::PageNumberHashSet;
use crate::tree_store::page_store::page_manager::MAX_MAX_PAGE_ORDER;
use std::alloc::{GlobalAlloc, Layout, System, handle_alloc_error};
use std::cell::UnsafeCell;
use std::cmp::Ordering;
#[cfg(debug_assertions)]
use std::collections::HashMap;
#[cfg(debug_assertions)]
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::mem;
use std::ops::Range;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering, fence};

pub(crate) const MAX_VALUE_LENGTH: usize = 3 * 1024 * 1024 * 1024;
pub(crate) const MAX_PAIR_LENGTH: usize = 3 * 1024 * 1024 * 1024 + 768 * 1024 * 1024;
pub(crate) const MAX_PAGE_INDEX: u32 = 0x000F_FFFF;
pub(crate) const MAX_REGIONS: u32 = 0x0010_0000;

// On-disk format is:
// TODO: consider implementing an optimization in which we store the number of order-0 pages that
// are actually used, in these reserved bits, so that the reads to the PagedCachedFile layer can avoid
// reading all the zeros at the end of the page.
// lowest 20bits: page index within the region. Only the lowest `20 - order_exponent` bits may be read.
// The remaining bits are reserved for future use and must be ignored
// second 20bits: region number
// 19bits: reserved
// highest 5bits: page order exponent
//
// Assuming a reasonable page size, like 4kiB, this allows for 4kiB * 2^20 * 2^20 = 4PiB of usable space
#[derive(Copy, Clone, Eq, PartialEq)]
pub(crate) struct PageNumber {
    pub(crate) region: u32,
    pub(crate) page_index: u32,
    pub(crate) page_order: u8,
}

impl Hash for PageNumber {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let mut temp = 0x000F_FFFF & u64::from(self.page_index);
        temp |= (0x000F_FFFF & u64::from(self.region)) << 20;
        temp |= (0b0001_1111 & u64::from(self.page_order)) << 59;
        state.write_u64(temp);
    }
}

// PageNumbers are ordered as determined by their starting address in the database file
impl Ord for PageNumber {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.region.cmp(&other.region) {
            Ordering::Less => Ordering::Less,
            Ordering::Equal => {
                let self_order0 = self.page_index * 2u32.pow(self.page_order.into());
                let other_order0 = other.page_index * 2u32.pow(other.page_order.into());
                assert!(
                    self_order0 != other_order0 || self.page_order == other.page_order,
                    "{self:?} overlaps {other:?}, but is not equal"
                );
                self_order0.cmp(&other_order0)
            }
            Ordering::Greater => Ordering::Greater,
        }
    }
}

impl PartialOrd for PageNumber {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PageNumber {
    pub(crate) const fn serialized_size() -> usize {
        8
    }

    pub(crate) fn new(region: u32, page_index: u32, page_order: u8) -> Self {
        debug_assert!(region <= 0x000F_FFFF);
        debug_assert!(page_index <= MAX_PAGE_INDEX);
        debug_assert!(page_order <= MAX_MAX_PAGE_ORDER);
        Self {
            region,
            page_index,
            page_order,
        }
    }

    pub(crate) fn to_le_bytes(self) -> [u8; 8] {
        let mut temp = 0x000F_FFFF & u64::from(self.page_index);
        temp |= (0x000F_FFFF & u64::from(self.region)) << 20;
        temp |= (0b0001_1111 & u64::from(self.page_order)) << 59;
        temp.to_le_bytes()
    }

    pub(crate) fn from_le_bytes(bytes: [u8; 8]) -> Self {
        let temp = u64::from_le_bytes(bytes);
        let order = (temp >> 59) as u8;
        let index = u32::try_from(temp & (0x000F_FFFF >> order)).unwrap();
        let region = ((temp >> 20) & 0x000F_FFFF) as u32;

        Self {
            region,
            page_index: index,
            page_order: order,
        }
    }

    #[cfg(test)]
    pub(crate) fn to_order0(self) -> Vec<PageNumber> {
        let mut pages = vec![self];
        loop {
            let mut progress = false;
            let mut new_pages = vec![];
            for page in pages {
                if page.page_order == 0 {
                    new_pages.push(page);
                } else {
                    progress = true;
                    new_pages.push(PageNumber::new(
                        page.region,
                        page.page_index * 2,
                        page.page_order - 1,
                    ));
                    new_pages.push(PageNumber::new(
                        page.region,
                        page.page_index * 2 + 1,
                        page.page_order - 1,
                    ));
                }
            }
            pages = new_pages;
            if !progress {
                break;
            }
        }

        pages
    }

    pub(crate) fn address_range(
        &self,
        data_section_offset: u64,
        region_size: u64,
        region_pages_start: u64,
        page_size: u32,
    ) -> Range<u64> {
        let regional_start =
            region_pages_start + u64::from(self.page_index) * self.page_size_bytes(page_size);
        debug_assert!(regional_start < region_size);
        let region_base = u64::from(self.region) * region_size;
        let start = data_section_offset + region_base + regional_start;
        let end = start + self.page_size_bytes(page_size);
        start..end
    }

    pub(crate) fn page_size_bytes(&self, page_size: u32) -> u64 {
        let pages = 1u64 << self.page_order;
        pages * u64::from(page_size)
    }
}

impl Debug for PageNumber {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "r{}.{}/{}",
            self.region, self.page_index, self.page_order
        )
    }
}

pub(crate) trait Page {
    fn memory(&self) -> &[u8];

    fn get_page_number(&self) -> PageNumber;
}

const PAGE_DATA_SLAB_TARGET: usize = 1024 * 1024;
const PAGE_DATA_SLAB_MAX_SLOTS: usize = 256;

// Keeping this header next to its payload preserves the locality of `Arc<[u8]>`,
// without making the allocator round a 4096-byte payload plus the Arc header up
// to its next size class.
#[repr(C)]
struct PageDataRef {
    strong: AtomicUsize,
    len: usize,
    pool: UnsafeCell<Option<Arc<PageDataPool>>>,
}

impl PageDataRef {
    fn new(len: usize) -> Self {
        Self {
            strong: AtomicUsize::new(0),
            len,
            pool: UnsafeCell::new(None),
        }
    }
}

// `pool` is initialized before publishing the first reference and taken only
// by the thread that decrements `strong` to zero.
unsafe impl Send for PageDataRef {}
unsafe impl Sync for PageDataRef {}

struct FreePageData(NonNull<PageDataRef>);

unsafe impl Send for FreePageData {}

struct PageDataSlab {
    allocation: NonNull<u8>,
    layout: Layout,
    slot_size: usize,
    slots: usize,
}

unsafe impl Send for PageDataSlab {}

impl Drop for PageDataSlab {
    fn drop(&mut self) {
        for index in 0..self.slots {
            let reference = unsafe {
                &mut *self
                    .allocation
                    .cast::<PageDataRef>()
                    .as_ptr()
                    .byte_add(index * self.slot_size)
            };
            debug_assert_eq!(reference.strong.load(AtomicOrdering::Relaxed), 0);
            debug_assert!(unsafe { (&*reference.pool.get()).is_none() });
            unsafe { std::ptr::drop_in_place(reference) };
        }
        unsafe { System.dealloc(self.allocation.as_ptr(), self.layout) };
    }
}

#[derive(Default)]
struct PageDataPoolClass {
    free: Vec<FreePageData>,
    slabs: Vec<PageDataSlab>,
}

struct PageDataPoolState {
    classes: [PageDataPoolClass; usize::BITS as usize],
}

impl Default for PageDataPoolState {
    fn default() -> Self {
        Self {
            classes: std::array::from_fn(|_| PageDataPoolClass::default()),
        }
    }
}

#[derive(Default)]
pub(crate) struct PageDataPool {
    state: Mutex<PageDataPoolState>,
}

impl PageDataPool {
    pub(crate) fn allocate_zeroed(self: &Arc<Self>, len: usize) -> PageData {
        if !len.is_power_of_two() {
            return PageData::new(vec![0; len].into_boxed_slice());
        }

        let class_index = len.trailing_zeros() as usize;
        let free_page = {
            let mut state = self.state.lock().unwrap();
            let class = &mut state.classes[class_index];
            if class.free.is_empty() {
                Self::allocate_slab(class, len);
            }
            class.free.pop().unwrap()
        };
        let reference = free_page.0;
        let data = unsafe {
            NonNull::new_unchecked(
                reference
                    .as_ptr()
                    .cast::<u8>()
                    .add(std::mem::size_of::<PageDataRef>()),
            )
        };

        unsafe {
            std::slice::from_raw_parts_mut(data.as_ptr(), len).fill(0);
            debug_assert_eq!(reference.as_ref().strong.load(AtomicOrdering::Relaxed), 0);
            debug_assert!((*reference.as_ref().pool.get()).is_none());
            *reference.as_ref().pool.get() = Some(self.clone());
            reference.as_ref().strong.store(1, AtomicOrdering::Relaxed);
        }
        PageData { data, reference }
    }

    fn allocate_slab(class: &mut PageDataPoolClass, len: usize) {
        let slot_size = std::mem::size_of::<PageDataRef>()
            .checked_add(len)
            .unwrap()
            .next_multiple_of(std::mem::align_of::<PageDataRef>());
        let slots = (PAGE_DATA_SLAB_TARGET / slot_size).clamp(1, PAGE_DATA_SLAB_MAX_SLOTS);
        let allocation_size = slot_size.checked_mul(slots).unwrap();
        let layout = Layout::from_size_align(allocation_size, 64).unwrap();
        // Large System allocations avoid size-class rounding in an application's
        // global allocator. Slots are recycled, so the pool grows only to its
        // high-water mark for simultaneously live page buffers.
        let allocation = unsafe { System.alloc(layout) };
        if allocation.is_null() {
            handle_alloc_error(layout);
        }
        let allocation = unsafe { NonNull::new_unchecked(allocation) };
        for index in 0..slots {
            let reference = unsafe {
                allocation
                    .cast::<PageDataRef>()
                    .as_ptr()
                    .byte_add(index * slot_size)
            };
            unsafe { reference.write(PageDataRef::new(len)) };
            class
                .free
                .push(FreePageData(unsafe { NonNull::new_unchecked(reference) }));
        }
        class.slabs.push(PageDataSlab {
            allocation,
            layout,
            slot_size,
            slots,
        });
    }

    fn release(&self, reference: NonNull<PageDataRef>) {
        let len = unsafe { reference.as_ref() }.len;
        debug_assert_eq!(
            unsafe { reference.as_ref() }
                .strong
                .load(AtomicOrdering::Relaxed),
            0
        );
        self.state.lock().unwrap().classes[len.trailing_zeros() as usize]
            .free
            .push(FreePageData(reference));
    }
}

pub(crate) struct PageData {
    data: NonNull<u8>,
    reference: NonNull<PageDataRef>,
}

impl PageData {
    pub(crate) fn new(mut data: Box<[u8]>) -> Self {
        let reference = Box::new(PageDataRef::new(data.len()));
        reference.strong.store(1, AtomicOrdering::Relaxed);
        let reference = NonNull::from(Box::leak(reference));
        let pointer = NonNull::new(data.as_mut_ptr()).unwrap();
        let _ = Box::into_raw(data);
        Self {
            data: pointer,
            reference,
        }
    }

    #[cfg(test)]
    pub(crate) fn zeroed(len: usize) -> Self {
        Self::new(vec![0; len].into_boxed_slice())
    }

    #[inline]
    pub(crate) fn get_mut(&mut self) -> Option<&mut [u8]> {
        let reference = unsafe { self.reference.as_ref() };
        if reference.strong.load(AtomicOrdering::Acquire) == 1 {
            Some(unsafe { std::slice::from_raw_parts_mut(self.data.as_ptr(), self.len()) })
        } else {
            None
        }
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        unsafe { self.reference.as_ref().len }
    }

    #[inline]
    pub(crate) fn slice(&self, offset: usize, len: usize) -> &[u8] {
        debug_assert!(offset + len <= self.len());
        unsafe { std::slice::from_raw_parts(self.data.as_ptr().add(offset), len) }
    }
}

impl AsRef<[u8]> for PageData {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.slice(0, self.len())
    }
}

impl std::ops::Deref for PageData {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl Clone for PageData {
    #[inline]
    fn clone(&self) -> Self {
        let old_count = unsafe { self.reference.as_ref() }
            .strong
            .fetch_add(1, AtomicOrdering::Relaxed);
        if old_count >= isize::MAX as usize {
            std::process::abort();
        }
        Self {
            data: self.data,
            reference: self.reference,
        }
    }
}

impl Drop for PageData {
    #[inline]
    fn drop(&mut self) {
        let reference = unsafe { self.reference.as_ref() };
        if reference.strong.fetch_sub(1, AtomicOrdering::Release) != 1 {
            return;
        }
        fence(AtomicOrdering::Acquire);

        unsafe {
            let pool = (&mut *reference.pool.get()).take();
            if let Some(pool) = pool {
                pool.release(self.reference);
            } else {
                drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(
                    self.data.as_ptr(),
                    reference.len,
                )));
                drop(Box::from_raw(self.reference.as_ptr()));
            }
        }
    }
}

impl std::fmt::Debug for PageData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageData")
            .field("len", &self.len())
            .finish()
    }
}

// The payload and reference-count pointers are stable until the last handle is dropped.
unsafe impl Send for PageData {}
unsafe impl Sync for PageData {}

pub struct PageImpl {
    pub(super) mem: PageData,
    pub(super) mem_len: usize,
    pub(super) page_number: PageNumber,
    #[cfg(debug_assertions)]
    pub(super) open_pages: Arc<Mutex<HashMap<PageNumber, u64>>>,
}

impl PageImpl {
    pub(crate) fn page_data(&self) -> PageData {
        self.mem.clone()
    }
}

impl Debug for PageImpl {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("PageImpl: page_number={:?}", self.page_number))
    }
}

#[cfg(debug_assertions)]
impl Drop for PageImpl {
    fn drop(&mut self) {
        let mut open_pages = self.open_pages.lock().unwrap();
        let value = open_pages.get_mut(&self.page_number).unwrap();
        assert!(*value > 0);
        *value -= 1;
        if *value == 0 {
            open_pages.remove(&self.page_number);
        }
    }
}

impl Page for PageImpl {
    fn memory(&self) -> &[u8] {
        self.mem.slice(0, self.mem_len)
    }

    fn get_page_number(&self) -> PageNumber {
        self.page_number
    }
}

impl Clone for PageImpl {
    fn clone(&self) -> Self {
        #[cfg(debug_assertions)]
        {
            *self
                .open_pages
                .lock()
                .unwrap()
                .get_mut(&self.page_number)
                .unwrap() += 1;
        }
        Self {
            mem: self.mem.clone(),
            mem_len: self.mem_len,
            page_number: self.page_number,
            #[cfg(debug_assertions)]
            open_pages: self.open_pages.clone(),
        }
    }
}

// The lifetime should be bound to the lifetime of the transaction in which this page was opened.
// It is used in the Drop impl to ensure that the page is dropped before the transaction is committed.
pub(crate) struct PageMut<'txn> {
    pub(super) mem: WritablePage,
    pub(super) page_number: PageNumber,
    pub(super) _lifetime: PhantomData<&'txn ()>,
    #[cfg(debug_assertions)]
    pub(super) open_pages: Arc<Mutex<HashSet<PageNumber>>>,
}

impl PageMut<'_> {
    pub(crate) fn memory_mut(&mut self) -> &mut [u8] {
        self.mem.mem_mut()
    }
}

impl Page for PageMut<'_> {
    fn memory(&self) -> &[u8] {
        self.mem.mem()
    }

    fn get_page_number(&self) -> PageNumber {
        self.page_number
    }
}

impl Drop for PageMut<'_> {
    fn drop(&mut self) {
        #[cfg(debug_assertions)]
        assert!(self.open_pages.lock().unwrap().remove(&self.page_number));
    }
}

#[derive(Copy, Clone)]
pub(crate) enum PageHint {
    None,
    // The page is guaranteed not to be dirtied by the in-progress write transaction. It may
    // still be in the write buffer, if a non-durable commit left committed pages there.
    Clean,
}

pub(crate) enum PageTrackerPolicy {
    Ignore,
    Track(PageNumberHashSet),
    Closed,
}

impl PageTrackerPolicy {
    pub(crate) fn new_tracking() -> Self {
        PageTrackerPolicy::Track(PageNumberHashSet::default())
    }

    pub(crate) fn is_empty(&self) -> bool {
        match self {
            PageTrackerPolicy::Ignore | PageTrackerPolicy::Closed => true,
            PageTrackerPolicy::Track(x) => x.is_empty(),
        }
    }

    pub(super) fn remove(&mut self, page: PageNumber) {
        match self {
            PageTrackerPolicy::Ignore => {}
            PageTrackerPolicy::Track(x) => {
                assert!(x.remove(&page));
            }
            PageTrackerPolicy::Closed => {
                panic!("Page tracker is closed");
            }
        }
    }

    /// Removes `page` if present. Returns whether it was in the set.
    pub(crate) fn remove_if_present(&mut self, page: PageNumber) -> bool {
        match self {
            PageTrackerPolicy::Ignore => false,
            PageTrackerPolicy::Track(x) => x.remove(&page),
            PageTrackerPolicy::Closed => panic!("Page tracker is closed"),
        }
    }

    pub(crate) fn contains(&self, page: PageNumber) -> bool {
        match self {
            PageTrackerPolicy::Ignore => false,
            PageTrackerPolicy::Track(x) => x.contains(&page),
            PageTrackerPolicy::Closed => panic!("Page tracker is closed"),
        }
    }

    pub(super) fn insert(&mut self, page: PageNumber) {
        match self {
            PageTrackerPolicy::Ignore => {}
            PageTrackerPolicy::Track(x) => {
                assert!(x.insert(page));
            }
            PageTrackerPolicy::Closed => {
                panic!("Page tracker is closed");
            }
        }
    }

    pub(crate) fn close(&mut self) -> PageNumberHashSet {
        let old = mem::replace(self, PageTrackerPolicy::Closed);
        match old {
            PageTrackerPolicy::Ignore => PageNumberHashSet::default(),
            PageTrackerPolicy::Track(x) => x,
            PageTrackerPolicy::Closed => {
                panic!("Page tracker is closed");
            }
        }
    }

    pub(crate) fn reset(&mut self) -> PageNumberHashSet {
        if matches!(self, PageTrackerPolicy::Ignore) {
            return PageNumberHashSet::default();
        }
        let old = mem::replace(self, PageTrackerPolicy::Track(PageNumberHashSet::default()));
        match old {
            PageTrackerPolicy::Ignore => PageNumberHashSet::default(),
            PageTrackerPolicy::Track(x) => x,
            PageTrackerPolicy::Closed => {
                panic!("Page tracker is closed");
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::{PageData, PageDataPool};
    use crate::tree_store::PageNumber;
    use std::mem::size_of;
    use std::sync::Arc;

    #[test]
    fn page_data() {
        assert_eq!(size_of::<PageData>(), size_of::<Arc<[u8]>>());

        let mut data = PageData::zeroed(4096);
        data.get_mut().unwrap()[7] = 42;
        let clone = data.clone();
        assert!(data.get_mut().is_none());
        assert_eq!(clone[7], 42);
        drop(clone);
        assert_eq!(data.get_mut().unwrap()[7], 42);

        let arbitrary = PageData::new(vec![3; 777].into_boxed_slice());
        assert_eq!(arbitrary.len(), 777);
        assert!(arbitrary.iter().all(|value| *value == 3));
    }

    #[test]
    fn pooled_page_data() {
        let pool = Arc::new(PageDataPool::default());
        let mut data = pool.allocate_zeroed(4096);
        data.get_mut().unwrap()[7] = 42;
        let pointer = data.data;

        let clone = data.clone();
        let clone = std::thread::spawn(move || {
            assert_eq!(clone[7], 42);
            clone
        })
        .join()
        .unwrap();
        drop(clone);
        drop(data);

        let reused = pool.allocate_zeroed(4096);
        assert_eq!(reused.data, pointer);
        assert!(reused.iter().all(|value| *value == 0));

        let weak = Arc::downgrade(&pool);
        drop(pool);
        assert!(weak.upgrade().is_some());
        drop(reused);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn arbitrary_page_data_does_not_retain_pool() {
        let pool = Arc::new(PageDataPool::default());
        let weak = Arc::downgrade(&pool);
        let data = pool.allocate_zeroed(777);
        drop(pool);
        assert!(weak.upgrade().is_none());
        assert_eq!(data.len(), 777);
    }

    #[test]
    fn last_page() {
        let region_data_size = 2u64.pow(32);
        let page_size = 4096;
        let pages_per_region = region_data_size / page_size;
        let region_header_size = 2u64.pow(16);
        let last_page_index = pages_per_region - 1;
        let page_number = PageNumber::new(1, last_page_index.try_into().unwrap(), 0);
        page_number.address_range(
            4096,
            region_data_size + region_header_size,
            region_header_size,
            page_size.try_into().unwrap(),
        );
    }

    #[test]
    fn reserved_bits() {
        let page_number = PageNumber::new(0, 0, 12);
        let mut bytes = page_number.to_le_bytes();
        bytes[1] = 0xFF;
        let page_number2 = PageNumber::from_le_bytes(bytes);
        assert_eq!(page_number, page_number2);
    }
}
