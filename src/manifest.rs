use bytes::BufMut;
use parking_lot::{Mutex, MutexGuard};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::Path,
    sync::Arc,
};

use crate::compact::CompactionTask;
use anyhow::{Context, Result};
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
        Ok(Self {
            file: Arc::new(Mutex::new(
                OpenOptions::new()
                    .read(true)
                    .create_new(true)
                    .write(true)
                    .open(path)
                    .context("fail to create manifest")?,
            )),
        })
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
        let mut file = self.file.lock();
        let mut buf = serde_json::to_vec(&record)?;
        let hash = crc32fast::hash(&buf);
        // writing record length and hash to file
        file.write_all(&(buf.len() as u64).to_be_bytes())?;
        buf.put_u32(hash);
        file.write_all(&buf)?;
        file.sync_all()?;
        Ok(())
    }
}
