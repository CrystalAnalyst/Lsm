use std::{
    fs::File,
    path::Path,
    sync::{Arc, Mutex, MutexGuard},
};

use crate::compact::CompactionTask;
use anyhow::Result;
use serde::{Deserialize, Serialize};

/// Manifest stores the metadata of SSTs in the disk
pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemTable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        todo!()
    }

    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        todo!()
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        todo!()
    }

    pub fn add_record_when_init(&self, record: ManifestRecord) -> Result<()> {
        todo!()
    }
}
