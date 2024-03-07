// a basic memtable, based on crossbeam-skiplist.
#![allow(unused_imports)]
#![allow(dead_code)]
use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;
use std::iter::Skip;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
pub struct MemTable {
    map: Arc<SkipMap<Bytes, Bytes>>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
}
impl MemTable {
    pub fn create(id: usize) -> Self {
        Self {
            id,
            map: Arc::new(SkipMap::new()),
            approximate_size: Arc::new(AtomicUsize::new(0)),
        }
    }
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.map.get(key).map(|e| e.value().clone())
    }
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let estimated_size = key.len() + value.len();
        self.map
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        self.approximate_size
            .fetch_add(estimated_size, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
}
