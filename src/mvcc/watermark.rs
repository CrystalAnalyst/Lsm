use std::collections::BTreeMap;

use anyhow::Result;

use super::txn::Transaction;

pub struct Watermark {
    // for this read_ts(u64), how many snapshots(usize) are using.
    readers: BTreeMap<u64, usize>,
}

impl Default for Watermark {
    fn default() -> Self {
        Watermark {
            readers: BTreeMap::new(),
        }
    }
}

impl Watermark {
    pub fn new(&self) -> Self {
        Self::default()
    }

    /// when a Txn is created(the `ts` is attached to him),
    /// call `add_reader()` to add its read timestamp for tracking.
    pub fn add_reader(&mut self, ts: u64) {
        *self.readers.entry(ts).or_default() += 1;
    }

    /// when a Txn aborts or commits, remove itself from the watermark.
    pub fn remove_reader(&mut self, ts: u64) {
        let cnt = self.readers.get_mut(&ts).unwrap();
        *cnt -= 1;
        // 删除后, 当前ts没有readers(无事务使用在这个时间点的快照), 删除这个kv对
        if *cnt == 0 {
            self.readers.remove(&ts);
        }
    }

    /// returns the lowest `read_ts` in the system.
    /// returns None if there're no ongoing Transcations.
    pub fn watermark(&self) -> Option<u64> {
        self.readers.first_key_value().map(|(ts, _)| *ts)
    }

    /// returns the Number of Snapshots using in the system now.
    pub fn num_of_snapshots(&self) -> usize {
        self.readers.len()
    }
}
