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

    pub fn put() {
        todo!()
    }

    pub fn delete() {
        todo!()
    }

    pub fn commit() {
        todo!()
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        todo!()
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
