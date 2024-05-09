#![allow(unused)]

use std::{
    fs::{File, OpenOptions},
    hash::Hasher,
    io::{BufWriter, Read, Write},
    path::Path,
    sync::Arc,
};

use parking_lot::Mutex;

use anyhow::{bail, Context, Ok, Result};

use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;

use crate::key::{KeyBytes, KeySlice};

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

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .context("failed to open the wal")?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf);
        let mut buf_ptr = &buf[..];
        while buf_ptr.has_remaining() {
            let mut hasher = crc32fast::Hasher::new();
            // get the key
            let key_len = buf_ptr.get_u16() as usize;
            hasher.write_u16(key_len as u16);
            let key = Bytes::copy_from_slice(&buf_ptr[..key_len]);
            hasher.write(&key);
            buf_ptr.advance(key_len);
            // get the ts
            let ts = buf_ptr.get_u64();
            hasher.write_u64(ts);
            // get the value
            let value_len = buf_ptr.get_u16() as usize;
            hasher.write_u16(value_len as u16);
            let value = Bytes::copy_from_slice(&buf_ptr[..value_len]);
            hasher.write(&value);
            buf_ptr.advance(value_len);
            // get the checksum and validate
            if hasher.finalize() != buf_ptr.get_u32() {
                bail!("checksum mismatched!");
            }
            skiplist.insert(KeyBytes::from_bytes_with_ts(key, ts), value);
        }
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        let mut file = self.file.lock();
        let mut buf: Vec<u8> =
            Vec::with_capacity(key.raw_len() + value.len() + std::mem::size_of::<u16>());
        let mut hasher = crc32fast::Hasher::new();
        hasher.write_u16(key.key_len() as u16);
        buf.put_u16(key.key_len() as u16);
        hasher.write(key.key_ref());
        buf.put_slice(key.key_ref());
        hasher.write_u64(key.ts());
        buf.put_u64(key.ts());
        hasher.write_u16(value.len() as u16);
        buf.put_u16(value.len() as u16);
        buf.put_slice(value);
        hasher.write(value);
        buf.put_u32(hasher.finalize());
        file.write_all(&buf)?;
        Ok(())
    }

    /// ensure that any data written to the Write-Ahead Log (WAL)
    /// is flushed to disk and synchronized across storage devices.
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
