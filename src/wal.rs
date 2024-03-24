#![allow(unused)]

use std::{
    fs::{File, OpenOptions},
    hash::Hasher,
    io::{BufWriter, Write},
    path::Path,
    sync::Arc,
};

use parking_lot:: {Mutex}

use anyhow::{bail, Context, Ok, Result};

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(
                OpenOptions::new()
                    .read(true)
                    .create_new(true)
                    .write(true)
                    .open(path)
                    .context("fail to create WAL")?,
            ))),
        })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        todo!()
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut file = self.file.lock();
        let mut buf: Vec<u8> =
            Vec::with_capacity(key.len() + value.len() + std::mem::size_of::<u16>());
        let mut hasher = crc32fast::Hasher::new();
        hasher.write_u16(key.len() as u16);
        todo!()
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        // write buffered data(in the file) to the OS.
        file.flush()?;
        // sync_all() further ensures that the changes are
        // physically written to the storage device.
        // Necessary especially when OS may cache writes.
        file.get_mut().sync_all()?;
        Ok(())
    }
}
