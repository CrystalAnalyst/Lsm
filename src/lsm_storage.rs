#![allow(unused_imports)]
#![allow(dead_code)]
#![allow(unused_variables)]

use crate::{block::Block, mem_table::MemTable};
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex, RwLock},
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
    // pub levels: Vec<(usize, Vec<usize>)>,
}

impl LsmStroageState {
    fn create(options: &LsmStorageOptions) -> Self {
        Self {
            // when first create, the index of the memtable is 0.
            memtable: Arc::new(MemTable::create(0)),
            // Init the immu_memtable vector and L0_Sstable vector.
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
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
}

/// the core data-structure of LsmStorage Engine.
/// only visible inside the crate.
pub(crate) struct LsmStorageInner {
    // lock the state for concurrent R/w.
    pub(crate) state: Arc<RwLock<Arc<LsmStroageState>>>,
    // lock for sync.
    pub(crate) state_lock: Mutex<()>,
    // the path.
    path: PathBuf,
}

impl LsmStorageInner {}

/// MiniLsm is a wrapper outside the LsmStorageInner, publicly accessible.
pub struct MiniLsm {
    // maintains a StorageInner inside of it.
    pub(crate) inner: Arc<LsmStorageInner>,
}
