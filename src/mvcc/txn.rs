use std::{
    collections::HashSet,
    sync::{atomic::AtomicBool, Arc},
};

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

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
