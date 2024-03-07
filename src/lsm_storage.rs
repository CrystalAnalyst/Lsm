#![allow(unused_imports)]
#![allow(dead_code)]
#![allow(unused_variables)]

use crate::mem_table::MemTable;
use std::{
    path::PathBuf,
    sync::{Arc, Mutex, RwLock},
};
// stores the state of the storage Engine.
#[derive(Clone)]
pub struct LsmStroageState {
    pub memtable: Arc<MemTable>,
}

impl LsmStroageState {
    fn create(options: &LsmStorageOptions) -> Self {
        Self {
            memtable: Arc::new(MemTable::create(0)),
        }
    }
}
// stores the options when creating the StorageState
#[derive(Clone, Debug)]
pub struct LsmStorageOptions {
    pub block_size: usize,
}

pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStroageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
}

impl LsmStorageInner {}
