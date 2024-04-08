#![allow(unused)]
#![allow(dead_code)]

pub mod txn;
pub mod watermark;

use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};
use txn::Transaction;

use parking_lot::Mutex;

use crate::lsm_storage::LsmStorageInner;

use self::watermark::Watermark;

pub(crate) struct CommittedTxnData {
    pub(crate) key_hashes: HashSet<u32>,
    pub(crate) read_ts: u64,
    pub(crate) commit_ts: u64,
}

/// the Mvcc entity to manage all that related to Concurrency Control.
pub(crate) struct LsmMvccInner {
    pub(crate) write_lock: Mutex<()>,
    pub(crate) commit_lock: Mutex<()>,
    pub(crate) ts: Arc<Mutex<(u64, Watermark)>>,
    pub(crate) committed_txns: Arc<Mutex<BTreeMap<u64, CommittedTxnData>>>,
}

impl LsmMvccInner {
    pub fn new(init_ts: u64) -> Self {
        Self {
            write_lock: Mutex::new(()),
            commit_lock: Mutex::new(()),
            ts: Arc::new(Mutex::new((init_ts, Watermark::new()))),
            committed_txns: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn new_txn(&self, inner: Arc<LsmStorageInner>, ser: bool) -> Arc<Transaction> {
        todo!()
    }

    pub fn update_commit_ts(&self, ts: u64) {
        self.ts.lock().0 = ts;
    }

    pub fn latest_commit_ts(&self) -> u64 {
        self.ts.lock().0
    }

    pub fn watermark(&self) -> u64 {
        let ts = self.ts.lock();
        ts.1.watermark().unwrap_or(ts.0)
    }
}
