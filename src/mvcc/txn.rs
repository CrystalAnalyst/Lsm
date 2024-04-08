use core::borrow;
use std::ops::Bound;
use std::{
    collections::HashSet,
    sync::{atomic::AtomicBool, Arc},
};

use crate::mem_table::map_bound;
use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use parking_lot::Mutex;

use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::lsm_storage::LsmStorageInner;

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl Transaction {
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        // Status check
        let committed = self.committed.load(std::sync::atomic::Ordering::SeqCst);
        assert!(
            !committed,
            "Cannot operate on Transaction that's committed!"
        );
        // check the Read/Write Set of This Txn.
        if let Some(key_hashes) = &self.key_hashes {
            let mut key_hash = key_hashes.lock();
            let (_, read_set) = &mut *key_hash;
            read_set.insert(farmhash::hash32(key));
        }
        // get the actual key-value pair
        if let Some(entry) = self.local_storage.get(key) {
            if entry.value().is_empty() {
                return Ok(None);
            } else {
                return Ok(Some(entry.value().clone()));
            }
        }
        // call the underlying `get_with_ts()` method.
        self.inner.get_with_ts(key, self.read_ts)
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        let committed = self.committed.load(std::sync::atomic::Ordering::SeqCst);
        assert!(
            !committed,
            "Cannot operate on Transaction that's committed!"
        );
        let mut local_iter = TxnLocalIteratorBuilder {
            map: self.local_storage.clone(),
            iter_builder: |map| map.range((map_bound(lower), map_bound(upper))),
            item: (Bytes::new(), Bytes::new()),
        }
        .build();
        let entry = local_iter.with_iter_mut(|iter| TxnLocalIterator::entry_to_item(iter.next()));
        local_iter.with_mut(|x| *x.item = entry);

        TxnIterator::create(
            self.clone(),
            TwoMergeIterator::create(
                local_iter,
                self.inner.scan_with_ts(lower, upper, self.read_ts)?,
            )?,
        )
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        // check Txn Status
        let committed = self.committed.load(std::sync::atomic::Ordering::SeqCst);
        assert!(
            !committed,
            "Cannot operate on Transaction that's committed!"
        );
        // Insert or Update key-value pair.
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        // Update Write Set
        if let Some(key_hashes) = &self.key_hashes {
            let mut key_hashes = key_hashes.lock();
            let (write_hash, _) = &mut *key_hashes;
            write_hash.insert(farmhash::hash32(key));
        }
    }

    pub fn delete(&self, key: &[u8]) {
        let committed = self.committed.load(std::sync::atomic::Ordering::SeqCst);
        assert!(
            !committed,
            "Cannot operate on Transaction that's committed!"
        );
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::new());
        if let Some(key_hashes) = self.key_hashes {
            let mut key_hashes = key_hashes.lock();
            let (write_hash, _) = &mut *key_hashes;
            write_hash.insert(farmhash::hash32(key));
        }
    }

    pub fn commit() {
        todo!()
    }
}

impl Drop for Transaction {
    /// remove the read_ts from the Watermark when the Txn drops.
    fn drop(&mut self) {
        self.inner.mvcc().ts.lock().1.remove_reader(self.read_ts);
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct TxnLocalIterator {
    map: Arc<SkipMap<Bytes, Bytes>>,
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    item: (Bytes, Bytes),
}

impl TxnLocalIterator {
    pub fn entry_to_item(entry: Option<Entry<'_, Bytes, Bytes>>) -> (Bytes, Bytes) {
        entry
            .map(|x| (x.key().clone(), x.value().clone()))
            .unwrap_or_else(|| (Bytes::new(), Bytes::new()))
    }
}

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn next(&mut self) -> anyhow::Result<()> {
        let entry = self.with_iter_mut(|iter| Self::entry_to_item(iter.next()));
        self.with_mut(|x| *x.item = entry);
        Ok(())
    }

    fn key(&self) -> Self::KeyType<'_> {
        &self.borrow_item().0[..]
    }

    fn value(&self) -> &[u8] {
        &self.borrow_item().1[..]
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }
}

pub struct TxnIterator {
    txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<TxnIterator> {
        let iter = Self { txn, iter };
        iter.skip_delete()?;
        if iter.is_valid() {
            iter.add_to_read_set(iter.key());
        }
        Ok(iter)
    }

    pub fn skip_delete(&self) -> Result<()> {
        while self.iter.is_valid() && self.iter.value().is_empty() {
            self.iter.next()?;
        }
        Ok(())
    }

    /// add the key(hashed) to the read_set when Iter come to this element.
    pub fn add_to_read_set(&self, key: &[u8]) {
        if let Some(guard) = &self.txn.key_hashes {
            let mut guard = guard.lock();
            let (_, read_set) = &mut *guard;
            read_set.insert(farmhash::hash32(key));
        }
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a> = &'a [u8] where Self: 'a;

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> anyhow::Result<()> {
        self.iter.next()?;
        self.skip_delete()?;
        if self.is_valid() {
            self.add_to_read_set(self.key());
        }
        Ok(())
    }

    fn number_of_iterators(&self) -> usize {
        self.iter.number_of_iterators()
    }
}
