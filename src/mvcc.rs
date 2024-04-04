#![allow(unused)]
#![allow(dead_code)]

use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use parking_lot::Mutex;

pub(crate) struct CommittedTxnData {
    pub(crate) key_hashes: HashSet<u32>,
    pub(crate) read_ts: u64,
    pub(crate) commit_ts: u64,
}

pub(crate) struct LsmMvccInner {
    pub(crate) write_lock: Mutex<()>,
    pub(crate) commit_lock: Mutex<()>,
    pub(crate) ts: Arc<Mutex<u64>>,
    pub(crate) committed_txns: Arc<Mutex<BTreeMap<u64, CommittedTxnData>>>,
}

impl LsmMvccInner {
    pub fn new() {
        todo!()
    }

    pub fn new_txn() {
        todo!()
    }

    pub fn update_commit_ts() {
        todo!()
    }

    pub fn latest_commit_ts() {
        todo!()
    }

    pub fn watermark() {
        todo!()
    }
}
