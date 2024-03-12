// a basic memtable, based on crossbeam-skiplist.
#![allow(unused_imports)]
#![allow(dead_code)]
use anyhow::Result;
use bytes::Bytes;
use core::borrow;
use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use std::iter::Skip;
use std::ops::Bound;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use crate::iterators::StorageIterator;
use crate::key::{Key, KeySlice};
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

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

// define a self-referential struct, to hold refs to its own fields.
#[self_referencing]
pub struct MemTableIterator {
    map: Arc<SkipMap<Bytes, Bytes>>,
    #[borrows(map)] //the iterator `iter` borrows the `map` field
    #[not_covariant] //the iterator is not Covariant along with the struct.
    iter: SkipMapRangeIter<'this>,
    item: (Bytes, Bytes), // stores the current key-value pair pointed to by the Iter.
}

impl MemTableIterator {
    fn entry_to_item(entry: Option<Entry<'_, Bytes, Bytes>>) -> (Bytes, Bytes) {
        entry
            .map(|x| (x.key().clone(), x.value().clone()))
            .unwrap_or_else(|| (Bytes::from_static(&[]), Bytes::from_static(&[])))
    }
}

impl StorageIterator for MemTableIterator {
    type KeyType<'a> = KeySlice<'a>;
    fn value(&self) -> &[u8] {
        &self.borrow_item().1[..]
    }
    fn key(&self) -> KeySlice {
        KeySlice::from_slice(&self.borrow_item().0[..])
    }
    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }
    /// moves the iterator to the next position.
    fn next(&mut self) -> anyhow::Result<()> {
        let entry = self.with_iter_mut(|iter| MemTableIterator::entry_to_item(iter.next()));
        self.with_mut(|x| *x.item = entry);
        Ok(())
    }
}
