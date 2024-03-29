#![allow(unused)]
#![allow(dead_code)]

use anyhow::Result;
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::{
    block::Block,
    compact::{CompactionController, CompactionOptions},
    iterators::concat_iterator::SstConcatIterator,
    iterators::merge_iterator::MergeIterator,
    iterators::two_merge_iterator::TwoMergeIterator,
    iterators::StorageIterator,
    key::KeySlice,
    manifest::Manifest,
    mem_table::MemTable,
    table::iterator::SsTableIterator,
    table::SsTable,
};
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{atomic::AtomicUsize, Arc},
};

/// BlockCache for `read block from disk`, this is used when SSTable is built.
pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// stores the state of the storage Engine.
/// This is the core structure for Concurrenty Control and MetaData Manangement.
#[derive(Clone)]
pub struct LsmStroageState {
    // mutable memtable (only one at any time, allow multi-thread to access)
    pub memtable: Arc<MemTable>,
    // immutable_memtable for flush to the disk (A vector of)
    pub imm_memtables: Vec<Arc<MemTable>>,
    // the L0_SsTables stored in the disk.
    pub l0_sstables: Vec<usize>,
    // SSTables sorted by key-range : L1(index:0) ~ Lmax for compaction
    pub levels: Vec<(usize, Vec<usize>)>,
    // SST objects : map index(usize) to SST Object(Arc<SsTable>)
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

impl LsmStroageState {
    fn create(options: &LsmStorageOptions) -> Self {
        Self {
            // when first create, the index of the memtable is 0.
            memtable: Arc::new(MemTable::create(0)),
            // Init the immu_memtable vector and L0_Sstable vector.
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels: Vec::new(),
            sstables: HashMap::new(),
        }
    }
}

/// Provide Configurable options when Initializing the StorageState.
#[derive(Clone, Debug)]
pub struct LsmStorageOptions {
    // configure block size.
    pub block_size: usize,
    // configure the one SSTable size.
    pub target_sst_size: usize,
    // configure the max number of memtables.
    pub max_memtable_limit: usize,
    // Compaction option
    pub compaction_option: CompactionOptions,
    // serilization or not
    // open WAL or not
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

fn key_within(user_key: &[u8], table_begin: KeySlice, table_end: KeySlice) -> bool {
    table_begin.raw_ref() <= user_key && user_key <= table_end.raw_ref()
}

/// the core data-structure of LsmStorage Engine.
/// only visible inside the crate.
pub(crate) struct LsmStorageInner {
    // lock the state for concurrent R/w.
    pub(crate) state: Arc<RwLock<Arc<LsmStroageState>>>,
    // lock for sync.
    pub(crate) state_lock: Mutex<()>,
    // the path to the storage location on the file system.
    path: PathBuf,
    // cache data blocks read from the storage(disk)
    pub(crate) block_cache: Arc<BlockCache>,
    // generate unique ids for SSTables.
    next_sst_id: AtomicUsize,
    // configuration settings control the behavior of LSM Tree
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
    // pub(crate) mvcc: Option<LsmMvccInner>,
}

impl LsmStorageInner {
    // CRUD API

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        // 1. get the snapshot to ensure consistency.
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        }; // drop global lock here

        // Search on the current memtable.
        if let Some(value) = snapshot.memtable.get(key) {
            if value.is_empty() {
                // found tomestone, return key not exists
                return Ok(None);
            }
            return Ok(Some(value));
        }

        // Search on immutable memtables.
        for memtable in snapshot.imm_memtables.iter() {
            if let Some(value) = memtable.get(key) {
                if value.is_empty() {
                    // found tomestone, return key not exists
                    return Ok(None);
                }
                return Ok(Some(value));
            }
        }

        // Search in SSTables.
        // a. L0 SSTables
        let mut l0_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        let keep_table = |key: &[u8], table: &SsTable| {
            if key_within(
                key,
                table.first_key().as_key_slice(),
                table.last_key().as_key_slice(),
            ) {
                if let Some(bloom) = &table.bloom {
                    if bloom.may_contain(farmhash::fingerprint32(key)) {
                        return true;
                    }
                } else {
                    return true;
                }
            }
            false
        };
        for table in &snapshot.l0_sstables {
            let table = snapshot.sstables[table].clone();
            if keep_table(key, &table) {
                l0_iters.push(Box::new(SsTableIterator::create_and_seek_to_key(
                    table,
                    KeySlice::from_slice(key),
                )?));
            }
        }
        let l0_iter = MergeIterator::create(l0_iters);
        // Higher-Level SSTables.
        let mut level_iters = Vec::with_capacity(snapshot.levels.len());
        for (_, level_sst_ids) in &snapshot.levels {
            let mut level_ssts = Vec::with_capacity(snapshot.levels[0].1.len());
            for table in level_sst_ids {
                let table = snapshot.sstables[table].clone();
                if keep_table(key, &table) {
                    level_ssts.push(table);
                }
            }
            let level_iter =
                SstConcatIterator::create_and_seek_to_key(level_ssts, KeySlice::from_slice(key))?;
            level_iters.push(Box::new(level_iter));
        }
        // Merge Iteration( merges into a single Iterator )
        let iter = TwoMergeIterator::create(l0_iter, MergeIterator::create(level_iters))?;
        // Key Lookup
        if iter.is_valid() && iter.key().raw_ref() == key && !iter.value().is_empty() {
            return Ok(Some(Bytes::copy_from_slice(iter.value())));
        }
        Ok(None)
    }

    pub fn scan() {
        todo!()
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Put(key, value)])
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Del(key)])
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        for record in batch {
            match record {
                WriteBatchRecord::Put(key, value) => {
                    let key = key.as_ref();
                    let value = value.as_ref();
                    assert!(!key.is_empty(), "key should not be emtpy!");
                    assert!(!value.is_empty(), "value should not be empty!");
                    let size;
                    {
                        let guard = self.state.read();
                        guard.memtable.put(key, value)?;
                        size = guard.memtable.approximate_size();
                    }
                    todo!()
                }
                WriteBatchRecord::Del(key) => {
                    let key = key.as_ref();
                    let size;
                    {
                        let guard = self.state.read();
                        // put a TombStone on the specified key.
                        guard.memtable.put(key, b"")?;
                        size = guard.memtable.approximate_size();
                    }
                    todo!()
                }
            }
        }
        Ok(())
    }

    // Freeze API
    pub fn force_freeze_memtable() {
        todo!()
    }

    fn try_freeze() {
        todo!()
    }

    fn freeze_memtable_with_memtable() {
        todo!()
    }

    // Flush & Compact API
    pub fn force_flush_next_imm_memtable() {
        todo!()
    }

    pub fn sync() {
        todo!()
    }

    pub fn add_compaction_filter() {
        todo!()
    }

    // Txn API
    pub fn new_txn(&self) -> Result<()> {
        todo!()
    }

    // Inner util methods or functions
    pub(crate) fn open() {
        todo!()
    }

    pub(crate) fn next_sst_id() {
        todo!()
    }

    pub(crate) fn path_of_sst_static() {
        todo!()
    }

    pub(crate) fn path_of_sst() {
        todo!()
    }

    pub(crate) fn path_of_wal_static() {
        todo!()
    }

    pub(crate) fn path_of_wal() {
        todo!()
    }

    pub(super) fn sync_dir() {
        todo!()
    }
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

/// MiniLsm is a wrapper outside the LsmStorageInner, publicly accessible.
pub struct MiniLsm {
    // maintains a StorageInner inside of it.
    pub(crate) inner: Arc<LsmStorageInner>,
}
