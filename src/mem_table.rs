// a basic memtable, based on crossbeam-skiplist.
#![allow(unused)]
#![allow(dead_code)]
use anyhow::Result;
use bytes::Bytes;
use core::borrow;
use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use std::iter::Skip;
use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use crate::iterators::StorageIterator;
use crate::key::{KeyBytes, KeySlice};
use crate::table::SsTableBuilder;
use crate::wal::Wal;

/// Create a bound of `Bytes` from a bound of `&[u8]`(Native).
pub(crate) fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
        Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

/// Create a bound of `Bytes` from a bound of `KeySlice`(KeyType:Slice).
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

/// Create a bound of `KeySlice` from a bound of `&[u8]`(Native) add ts.
pub(crate) fn map_key_bound_plus_ts(bound: Bound<&[u8]>, ts: u64) -> Bound<KeySlice> {
    match bound {
        Bound::Included(x) => Bound::Included(KeySlice::from_slice(x, ts)),
        Bound::Excluded(x) => Bound::Excluded(KeySlice::from_slice(x, ts)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

/// Data Structure 1: MemTable in the Memory.
pub struct MemTable {
    pub(crate) map: Arc<SkipMap<KeyBytes, Bytes>>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
    wal: Option<Wal>,
}

impl MemTable {
    /*----------------MemTable creation and Initialization------------*/
    pub fn create(id: usize) -> Self {
        Self {
            id,
            map: Arc::new(SkipMap::new()),
            approximate_size: Arc::new(AtomicUsize::new(0)),
            wal: None,
        }
    }

    pub fn create_with_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            id,
            wal: Some(Wal::create(path)?),
            map: Arc::new(SkipMap::new()),
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub fn recover_from_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let map = Arc::new(SkipMap::new());
        Ok(Self {
            id,
            wal: Some(Wal::recover(path, &map)?),
            map,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    /*----------------CRUD API and Data Manipulation------------------*/
    pub fn get(&self, key: KeySlice) -> Option<Bytes> {
        let key_bytes = KeyBytes::from_bytes_with_ts(
            Bytes::from_static(unsafe { std::mem::transmute(key.key_ref()) }),
            key.ts(),
        );
        self.map.get(&key_bytes).map(|e| e.value().clone())
    }

    pub fn scan(&self, lower: Bound<KeySlice>, upper: Bound<KeySlice>) -> MemTableIterator {
        let (lower, upper) = (map_key_bound(lower), map_key_bound(upper));
        let mut iter = MemTableIteratorBuilder {
            map: self.map.clone(),
            iter_builder: |map| map.range((lower, upper)),
            item: (KeyBytes::new(), Bytes::new()),
        }
        .build();
        iter.next().unwrap();
        iter
    }

    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        // 先写WAL, 再写内存.
        if let Some(ref wal) = self.wal {
            wal.put(key, value)?;
        }
        // 写内存.
        let estimated_size = key.raw_len() + value.len();
        self.map.insert(
            key.to_key_vec().into_key_bytes(),
            Bytes::copy_from_slice(value),
        );
        self.approximate_size
            .fetch_add(estimated_size, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    /*----------------WAL Management: Flush and Sync------------------*/
    /// Flush the mem-table to SSTable. Implement in week 1 day 6.
    pub fn flush(&self, builder: &mut SsTableBuilder) -> Result<()> {
        for entry in self.map.iter() {
            builder.add(entry.key().as_key_slice(), &entry.value()[..]);
        }
        Ok(())
    }

    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    /*-----------------Util function for common use-------------------*/
    pub fn id(&self) -> usize {
        self.id
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Define a SkipMap Range-Iterator for `Range Query`, like scan() function.
type SkipMapRangeIter<'a> = crossbeam_skiplist::map::Range<
    'a,
    KeyBytes,
    (Bound<KeyBytes>, Bound<KeyBytes>),
    KeyBytes,
    Bytes,
>;

// define a self-referential struct, to hold refs to its own fields.
#[self_referencing]
pub struct MemTableIterator {
    // store the map, which contain all the key-value pairs.
    map: Arc<SkipMap<KeyBytes, Bytes>>,

    #[borrows(map)] //the iterator `iter` borrows the `map` field
    #[not_covariant] //the iterator is not Covariant along with the struct.
    // iter is the actual Iterator when Range-Query is executed.
    iter: SkipMapRangeIter<'this>,
    // item stores the current key-value pair pointed to by the iter.
    item: (KeyBytes, Bytes),
}

impl MemTableIterator {
    /// Convert an entry to an item.
    fn entry_to_item(entry: Option<Entry<'_, KeyBytes, Bytes>>) -> (KeyBytes, Bytes) {
        entry
            .map(|x| (x.key().clone(), x.value().clone()))
            .unwrap_or_else(|| (KeyBytes::new(), Bytes::new()))
    }
}

// We need to impl the `StorageIterator`  for MemTableIterator for general purpose.
impl StorageIterator for MemTableIterator {
    // appoint the KeyType as KeySlice.
    type KeyType<'a> = KeySlice<'a>;

    // get the current entry's key.
    fn key(&self) -> KeySlice {
        self.borrow_item().0.as_key_slice()
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
