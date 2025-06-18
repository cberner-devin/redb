use std::collections::{HashMap, VecDeque};

#[derive(Default)]
pub struct LRUCache<T> {
    cache: HashMap<u64, T>,
    lru_queue: VecDeque<u64>,
}

impl<T> LRUCache<T> {
    pub(crate) fn new() -> Self {
        Self {
            cache: Default::default(),
            lru_queue: Default::default(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.cache.len()
    }

    pub(crate) fn insert(&mut self, key: u64, value: T) -> Option<T> {
        let result = self.cache.insert(key, value);
        if result.is_none() {
            self.lru_queue.push_back(key);
        }
        result
    }

    pub(crate) fn remove(&mut self, key: u64) -> Option<T> {
        if let Some(value) = self.cache.remove(&key) {
            if let Some(pos) = self.lru_queue.iter().position(|&x| x == key) {
                self.lru_queue.remove(pos);
            }
            Some(value)
        } else {
            None
        }
    }

    pub(crate) fn get(&self, key: u64) -> Option<&T> {
        self.cache.get(&key)
    }

    pub(crate) fn get_mut(&mut self, key: u64) -> Option<&mut T> {
        self.cache.get_mut(&key)
    }

    pub(crate) fn iter(&self) -> impl ExactSizeIterator<Item = (&u64, &T)> {
        self.cache.iter()
    }

    pub(crate) fn iter_mut(&mut self) -> impl ExactSizeIterator<Item = (&u64, &mut T)> {
        self.cache.iter_mut()
    }

    pub(crate) fn pop_lowest_priority(&mut self) -> Option<(u64, T)> {
        if let Some(key) = self.lru_queue.pop_front() {
            if let Some(value) = self.cache.remove(&key) {
                Some((key, value))
            } else {
                self.pop_lowest_priority()
            }
        } else {
            None
        }
    }

    pub(crate) fn clear(&mut self) {
        self.cache.clear();
        self.lru_queue.clear();
    }
}
