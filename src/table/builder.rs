#![allow(unused)]

use crate::{
    block::builder::BlockBuilder,
    key::{Key, KeySlice, KeyVec},
    lsm_storage::BlockCache,
};
use anyhow::Result;
use bytes::BufMut;

use super::{bloom::Bloom, BlockMeta, FileObject, SsTable};
use farmhash::FarmHasher;
use std::{path::Path, sync::Arc};

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
    max_ts: u64,
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
            max_ts: 0,
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
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.finish_block();
        let mut buf = self.data;
        let meta_offset = buf.len();
        BlockMeta::encode_block_meta(&self.meta, self.max_ts, &mut buf);
        buf.put_u32(meta_offset as u32);
        let bloom = Bloom::build_from_key_hashes(
            &self.key_hashes,
            Bloom::bloom_bits_per_key(self.key_hashes.len(), 0.01),
        );
        let bloom_offset = buf.len();
        bloom.encode(&mut buf);
        buf.put_u32(bloom_offset as u32);
        let file = FileObject::create(path.as_ref(), buf)?;
        Ok(SsTable {
            id,
            file,
            first_key: self.meta.first().unwrap().first_key.clone(),
            last_key: self.meta.last().unwrap().last_key.clone(),
            block_meta: self.meta,
            block_meta_offset: meta_offset,
            block_cache,
            bloom: Some(bloom),
            max_ts: self.max_ts,
        })
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
        let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let encoded_block = builder.build().encode();
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: std::mem::take(&mut self.first_key).into_key_bytes(),
            last_key: std::mem::take(&mut self.last_key).into_key_bytes(),
        });
        let checksum = crc32fast::hash(&encoded_block);
        self.data.extend(encoded_block);
        self.data.put_u32(checksum);
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
