use bytes::{Buf, BufMut};
use parking_lot::{Mutex, MutexGuard};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::Path,
    sync::Arc,
};

use crate::compact::CompactionTask;
use anyhow::{bail, Context, Ok, Result};
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

    /// reads the manifest file, parses it into Individual records,
    /// verifies their integrity using checksums before returning the Record List.
    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        // open the file
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .context("cannot open the manifest!")?;
        // reads the content of the file into a buffer
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut buf_ptr = &buf[..];
        let mut records = Vec::new();
        // iterates over the buffer and parsing each record one by one
        while buf_ptr.has_remaining() {
            let len = buf_ptr.get_u64();
            let slice = &buf_ptr[..len as usize];
            let json = serde_json::from_slice::<ManifestRecord>(slice)?;
            buf_ptr.advance(len as usize);
            let checksum = buf_ptr.get_u32();
            if checksum != crc32fast::hash(slice) {
                bail!("checksum mismatched");
            }
            records.push(json);
        }
        // return the Recovered Manifest with all of its parsed record.
        Ok((
            Self {
                file: Arc::new(Mutex::new(file)),
            },
            records,
        ))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    /// add the serialized record length, the record(including the hash) to the file
    /// and sync to the persistent storage.
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
