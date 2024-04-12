#![allow(unused)]

use crate::{
    block::builder::BlockBuilder,
    key::{Key, KeySlice, KeyVec}, lsm_storage::BlockCache,
};
use anyhow::Result;

use std::path::Path;
use super::{BlockMeta, SsTable};
use farmhash::FarmHasher;

/// Builds an SsTable from key-value pairs.
pub struct SsTableBuilder {
    // Builder fields
    builder: BlockBuilder,
    block_size: usize,
    // Key and Data fields
    first_key: KeyVec,
    last_key: KeyVec,
    data: Vec<u8>,
    // Metadata fields
    pub(crate) meta: Vec<BlockMeta>,
    key_hashes: Vec<u32>,
}

impl SsTableBuilder {
    // constructor
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            block_size,
            first_key: KeyVec::new(),
            last_key: KeyVec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            key_hashes: Vec::new(),
        }
    }

    /*-----------Executors(core functional API)--------------*/

    /// adds a Key-value pair to the SsTable
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key.set_from_slice(key);
        }

        self.key_hashes.push(farmhash::fingerprint32(key.key_ref()));

        if self.builder.add(key, value) {
            self.last_key.set_from_slice(key);
            return;
        }

        self.finish_block();

        assert!(self.builder.add(key, value));
        self.first_key.set_from_slice(key);
        self.last_key.set_from_slice(key);
    }

    /// builds the SSTable and writes it to the given path
    fn build(self, id: usize, block_cache: Option<Arc<BlockCache>, path: impl AsRef<Path>) -> Result<SsTable> {
        todo!()
    }

    /*-----------------Accessor------------------*/
    
    /// get the estimated size of the SSTable for Compaction need
    /// since the data itself is much more larger than metadata
    /// so we directly return the data size 
    pub fn estimate_size(&self) -> usize {
        self.data.len()
    }

    /*----------------Modificator------------------*/
    
    /// Fanalize the current block being built
    fn finish_block(&mut self) {
        todo!()
    }
}
