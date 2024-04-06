use std::ops::Bound;
use std::{
    collections::HashSet,
    sync::{atomic::AtomicBool, Arc},
};

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use parking_lot::Mutex;

use crate::iterators::StorageIterator;
use crate::lsm_storage::LsmStorageInner;

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl Transaction {
    pub fn get() {
        todo!()
    }

    pub fn scan() {
        todo!()
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

impl TxnLocalIterator {}

impl StorageIterator for TxnLocalIterator {}

pub struct TxnIterator {}

impl TxnIterator {}

impl StorageIterator for TxnIterator {}
