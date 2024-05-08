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

/// 为了管理事务的生命周期，需要为每个事务和全局层面记录两部分元信息
/// 每个事务层面，需要记录自己读写的key列表，以及事务的开始时间戳和提交时间戳
/// 这部分数据存储在事务试图提交的数据中, 这里用CommittedTxnData进行描述.
pub(crate) struct CommittedTxnData {
    pub(crate) key_hashes: HashSet<u32>,
    pub(crate) read_ts: u64,
    pub(crate) commit_ts: u64,
}

/// 全局层面，需要管理全局时间戳，以及最近提交的事务列表，
/// 用于在新的事务提交中对事务开始与提交时间戳中间提交过的事务范围进行冲突检查，乃至当前活跃的事务的最小时间戳，
/// 用于清理旧事务信息，这部分信息维护在 oracle 结构体中
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
