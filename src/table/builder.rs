use crate::{
    block::builder::BlockBuilder,
    key::{Key, KeySlice, KeyVec},
};

use super::BlockMeta;

/// Builds an SsTable from key-value pairs.
pub struct SsTableBuilder {
    //
    builder: BlockBuilder,
    //
    first_key: KeyVec,
    last_key: KeyVec,
    data: Vec<u8>,
    //
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    key_hashes: Vec<u32>,
}

impl SsTableBuilder {
    /// constructor
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: KeyVec::new(),
            last_key: KeyVec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
            key_hashes: Vec::new(),
        }
    }
    /// adds a key-value pair to SSTables
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key.set_from_slice(key);
        }
    }
}
