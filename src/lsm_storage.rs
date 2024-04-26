#![allow(unused)]
#![allow(dead_code)]

use anyhow::Result;
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::{
    block::Block,
    compact::{CompactionController, CompactionOptions},
    iterators::{
        concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator, StorageIterator,
    },
    key::{KeyBytes, KeySlice},
    lsm_iterator::{FusedIterator, LsmIterator},
    manifest::Manifest,
    mem_table::MemTable,
    mvcc::{txn::TxnIterator, LsmMvccInner},
    table::{iterator::SsTableIterator, SsTable},
};
use std::{
    collections::HashMap,
    fs::File,
    ops::Bound,
    path::{Path, PathBuf},
    sync::{atomic::AtomicUsize, Arc},
    thread, usize,
};

/// BlockCache for `read block from disk`, this is used when SSTable is built.
pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// stores the state of the storage Engine.
/// This is the core structure for Concurrenty Control and MetaData Manangement.
#[derive(Clone)]
pub struct LsmStorageState {
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

impl LsmStorageState {
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
    pub enable_wal: bool,
    pub serializable: bool,
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

fn key_within(user_key: &[u8], table_begin: KeySlice, table_end: KeySlice) -> bool {
    table_begin.key_ref() <= user_key && user_key <= table_end.key_ref()
}

/// the core data-structure of LsmStorage Engine.
/// only visible inside the crate.
pub(crate) struct LsmStorageInner {
    // lock the state for concurrent R/w.
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
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
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

impl LsmStorageInner {
    /*---------------------------Boost and Init---------------------------------*/
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        todo!()
    }

    /*---------helper functions: Id-generator, MVCC entity and manifest---------*/
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn mvcc(&self) -> &LsmMvccInner {
        self.mvcc.as_ref().unwrap()
    }

    pub(crate) fn manifest(&self) -> &Manifest {
        self.manifest.as_ref().unwrap()
    }

    /*----------------------------Util functions---------------------------------*/

    /// 根据SST的id, 返回它的实际路径
    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    ///
    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    /*-----------------------------Txn and CRUD API---------------------------------*/
    pub fn new_txn(&self) -> Result<()> {
        todo!()
    }

    pub fn get(self: &Arc<Self>, key: &[u8]) -> Result<Option<Bytes>> {
        todo!()
    }

    pub fn get_with_ts(&self, key: &[u8], ts: u64) -> Result<Option<Bytes>> {
        todo!()
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        todo!()
    }

    pub fn scan_with_ts(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        read_ts: u64,
    ) -> Result<FusedIterator<LsmIterator>> {
        todo!()
    }

    pub fn put(self: &Arc<Self>, key: &[u8], value: &[u8]) -> Result<()> {
        todo!()
    }

    pub fn delete(self: &Arc<Self>, key: &[u8]) -> Result<()> {
        todo!()
    }

    pub fn write_batch<T: AsRef<[u8]>>(
        self: &Arc<Self>,
        batch: &[WriteBatchRecord<T>],
    ) -> Result<()> {
        if !self.options.serializable {
            todo!()
        }
        todo!()
    }

    /// A helper function `write_batch_inner()` that processes a write batch.
    /// return a u64 commit timestamp so that Transaction::Commit can correctly
    /// store the committed transaction data into the MVCC structure.
    pub fn write_batch_inner<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<u64> {
        todo!()
    }

    pub fn sync() {
        todo!()
    }

    /*----------------------------MemTable Management------------------------------*/
    fn try_freeze() {
        todo!()
    }

    pub fn force_freeze_memtable() {
        todo!()
    }

    fn freeze_memtable_with_memtable() {
        todo!()
    }

    pub fn force_flush_next_imm_memtable() {
        todo!()
    }

    /*------------------------------Compaction Opt----------------------------------*/
    pub fn add_compaction_filter() {
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
    // todo : add flush thread and compaction thread.
}

impl MiniLsm {
    /*----------------Open and Close ------------------*/
    pub fn open() -> Result<()> {
        todo!()
    }

    pub fn close() -> Result<()> {
        todo!()
    }

    /*----------------Data Manipulation------------------*/
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        self.inner.scan(lower, upper)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn del(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    /*----------------Sync and Compaction------------------*/
    pub fn flush(&self) -> Result<()> {
        todo!()
    }

    pub fn compact(&self) -> Result<()> {
        todo!()
    }

    pub fn sync() -> Result<()> {
        todo!()
    }
}
