use std::sync::Arc;

use crate::{block::iterator::BlockIterator, iterators::StorageIterator, key::KeySlice};
use anyhow::{Ok, Result};

use super::SsTable;

// An iterator over the contents of an SSTable
pub struct SsTableIterator {
    table: Arc<SsTable>,
    block_iter: BlockIterator,
    block_idx: usize,
}

impl SsTableIterator {
    fn create_and_seek_to_first() {
        todo!()
    }

    fn seek_to_first() {
        todo!()
    }

    fn seek_to_first_inner() {
        todo!()
    }

    fn create_and_seek_to_key() {
        todo!()
    }

    fn seek_to_key() {
        todo!()
    }

    fn seek_to_key_inner() {
        todo!()
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
