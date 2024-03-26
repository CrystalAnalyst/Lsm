use anyhow::Ok;

use crate::key::KeySlice;
use crate::table::iterator::SsTableIterator;
use crate::table::SsTable;

use std::sync::Arc;

use super::StorageIterator;

/// Concatenate multiple iters ordered in key-order and their key ranges do no overlap.
/// iterators when
pub struct SstConcatIterator {
    // Ensentially it's a SsTableIterator
    current: Option<SsTableIterator>,
    // the index of next SST
    next_sst_id: usize,
    // the SSTables holding
    sstables: Vec<Arc<SsTable>>,
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn is_valid(&self) -> bool {
        if let Some(iter) = &self.current {
            assert!(iter.is_valid());
            true
        } else {
            false
        }
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn next(&mut self) -> anyhow::Result<()> {
        self.current.as_mut().unwrap().next()?;
        Ok(())
    }

    fn number_of_iterators(&self) -> usize {
        1
    }
}
