use crate::{block::iterator::BlockIterator, iterators::StorageIterator, key::KeySlice};
use anyhow::{Ok, Result};
use std::sync::Arc;

use super::SsTable;

// An iterator over the contents of an SSTable
pub struct SsTableIterator {
    table: Arc<SsTable>,
    block_iter: BlockIterator,
    block_idx: usize,
}

impl SsTableIterator {
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let (block_idx, block_iter) = Self::seek_to_first_inner(&table)?;
        let iter = Self {
            block_iter,
            block_idx,
            table,
        };
        Ok(iter)
    }

    pub fn seek_to_first(&mut self) -> Result<()> {
        let (blk_idx, blk_iter) = Self::seek_to_first_inner(&self.table)?;
        self.block_idx = blk_idx;
        self.block_iter = blk_iter;
        Ok(())
    }

    fn seek_to_first_inner(table: &Arc<SsTable>) -> Result<(usize, BlockIterator)> {
        Ok((
            0,
            BlockIterator::create_and_seek_to_first(table.read_block_cached(0)?),
        ))
    }

    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let (block_idx, block_iter) = Self::seek_to_key_inner(&table, key)?;
        let iter = Self {
            block_idx,
            block_iter,
            table,
        };
        Ok(iter)
    }

    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let (block_idx, block_iter) = Self::seek_to_key_inner(&self.table, key)?;
        self.block_iter = block_iter;
        self.block_idx = block_idx;
        Ok(())
    }

    fn seek_to_key_inner(table: &Arc<SsTable>, key: KeySlice) -> Result<(usize, BlockIterator)> {
        let mut block_index = table.find_block_idx(key);
        let mut block_iter =
            BlockIterator::create_and_seek_to_key(table.read_block_cached(block_index)?, key);
        if !block_iter.is_valid() {
            block_index += 1;
            if block_index < table.num_of_blocks() {
                block_iter =
                    BlockIterator::create_and_seek_to_first(table.read_block_cached(block_index)?);
            }
        }
        Ok((block_index, block_iter))
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        self.block_iter.value()
    }

    fn key(&self) -> KeySlice {
        self.block_iter.key()
    }

    fn is_valid(&self) -> bool {
        self.block_iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.block_iter.next();
        if !self.block_iter.is_valid() {
            self.block_idx += 1;
            if self.block_idx < self.table.num_of_blocks() {
                self.block_iter = BlockIterator::create_and_seek_to_first(
                    self.table.read_block_cached(self.block_idx)?,
                );
            }
        }
        Ok(())
    }
}
