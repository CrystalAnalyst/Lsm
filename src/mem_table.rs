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
use crate::key::{Key, KeyBytes, KeySlice};

/// Create a bound of `Bytes` from a bound of `&[u8]`.
pub(crate) fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
        Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

/// Create a bound of `Bytes` from a bound of `KeySlice`.
pub(crate) fn map_key_bound(bound: Bound<KeySlice>) -> Bound<KeyBytes> {
    match bound {
        Bound::Included(x) => Bound::Included(KeyBytes::from_bytes_with_ts(
            Bytes::copy_from_slice(x.key_ref()),
            x.ts(),
        )),
        Bound::Excluded(x) => Bound::Excluded(KeyBytes::from_bytes_with_ts(
            Bytes::copy_from_slice(x.key_ref()),
            x.ts(),
        )),
        Bound::Unbounded => Bound::Unbounded,
    }
}

/// Data Structure 1: MemTable in the Memory.
pub struct MemTable {
    // store the key-value pairs, inside it'a SkipMap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    // index of the current memtable.
    id: usize,
    // the approximate_size of the current table.
    approximate_size: Arc<AtomicUsize>,
}

impl MemTable {
    /// create a new memtable with a `specified index`.
    pub fn create(id: usize) -> Self {
        Self {
            id,
            map: Arc::new(SkipMap::new()),
            approximate_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// core functionality 1: get() the pair with a key.
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.map.get(key).map(|e| e.value().clone())
    }

    /// core functionality 2: put() the entry with a key-value pair.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        // pre-calculate the size
        let estimated_size = key.len() + value.len();
        // insert the key-value pair.
        self.map
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        // update the estimated_size.
        self.approximate_size
            .fetch_add(estimated_size, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Define a SkipMap Range-Iterator for `Range Query`, like scan() function.
type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

// define a self-referential struct, to hold refs to its own fields.
#[self_referencing]
pub struct MemTableIterator {
    // store the map, which contain all the key-value pairs.
    map: Arc<SkipMap<Bytes, Bytes>>,

    #[borrows(map)] //the iterator `iter` borrows the `map` field
    #[not_covariant] //the iterator is not Covariant along with the struct.
    // iter is the actual Iterator when Range-Query is executed.
    iter: SkipMapRangeIter<'this>,
    // item stores the current key-value pair pointed to by the iter.
    item: (Bytes, Bytes),
}

impl MemTableIterator {
    /// Convert an entry to an item.
    fn entry_to_item(entry: Option<Entry<'_, Bytes, Bytes>>) -> (Bytes, Bytes) {
        entry
            .map(|x| (x.key().clone(), x.value().clone()))
            .unwrap_or_else(|| (Bytes::from_static(&[]), Bytes::from_static(&[])))
    }
}

// We need to impl the `StorageIterator`  for MemTableIterator for general purpose.
impl StorageIterator for MemTableIterator {
    // appoint the KeyType as KeySlice.
    type KeyType<'a> = KeySlice<'a>;

    // get the current entry's key.
    fn key(&self) -> KeySlice {
        todo!()
    }

    // get the current entry's value.
    fn value(&self) -> &[u8] {
        // borrow_item() provides an `immutable_reference` to item, just like &item.
        &self.borrow_item().1[..]
    }

    // check the validitity of the current entry.
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
