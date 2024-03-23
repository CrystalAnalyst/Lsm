#![allow(unused)]

use std::{
    fs::File,
    io::BufWriter,
    path::Path,
    sync::{Arc, Mutex},
};

use anyhow::{bail, Context, Result};

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        todo!()
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        todo!()
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        todo!()
    }

    pub fn sync(&self) -> Result<()> {
        todo!()
    }
}
