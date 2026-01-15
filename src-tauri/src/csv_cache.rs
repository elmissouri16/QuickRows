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

    pub fn clear(&self) {
        self.cache.lock().unwrap().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::CsvCache;

    #[test]
    fn put_get_clear_and_eviction() {
        let cache = CsvCache::new(1);
        cache.put(0, 1, vec![vec!["a".to_string()]]);
        assert_eq!(cache.get(0, 1), Some(vec![vec!["a".to_string()]]));

        cache.put(1, 1, vec![vec!["b".to_string()]]);
        assert!(cache.get(0, 1).is_none());
        assert_eq!(cache.get(1, 1), Some(vec![vec!["b".to_string()]]));

        cache.clear();
        assert!(cache.get(1, 1).is_none());
    }
}
