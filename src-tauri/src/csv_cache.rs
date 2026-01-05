use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::Mutex;

pub struct CsvCache {
    cache: Mutex<LruCache<(usize, usize), Vec<Vec<String>>>>,
}

impl CsvCache {
    pub fn new(capacity: usize) -> Self {
        let size = NonZeroUsize::new(capacity).unwrap_or_else(|| NonZeroUsize::new(1).unwrap());
        Self {
            cache: Mutex::new(LruCache::new(size)),
        }
    }

    pub fn get(&self, start: usize, count: usize) -> Option<Vec<Vec<String>>> {
        self.cache.lock().unwrap().get(&(start, count)).cloned()
    }

    pub fn put(&self, start: usize, count: usize, data: Vec<Vec<String>>) {
        self.cache.lock().unwrap().put((start, count), data);
    }
}
