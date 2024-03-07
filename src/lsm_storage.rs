#![allow(unused_imports)]
#![allow(dead_code)]
#![allow(unused_variables)]

use crate::mem_table::MemTable;
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex, RwLock},
};
// stores the state of the storage Engine.
#[derive(Clone)]
pub struct LsmStroageState {
    pub memtable: Arc<MemTable>,
    pub imm_memtables: Vec<Arc<MemTable>>,
    pub l0_sstables: Vec<usize>,
    // pub levels: Vec<(usize, Vec<usize>)>,
}

impl LsmStroageState {
    fn create(options: &LsmStorageOptions) -> Self {
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
        }
    }
}
// stores the options when creating the StorageState
#[derive(Clone, Debug)]
pub struct LsmStorageOptions {
    pub block_size: usize,
    pub target_sst_size: usize,
    pub max_memtable_limit: usize,
}

pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStroageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
}

impl LsmStorageInner {}

pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
}
