use anyhow::{Ok, Result};

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

impl SstConcatIterator {
    /// create a new ConcatIterator Instance and move to the first key-value pairs.
    pub fn create_and_seek_to_first() {
        todo!()
    }

    /// create a new ConcatIterator Instance and move to the specified key-value pairs.
    pub fn create_and_seek_to_key() {
        todo!()
    }

    /// check the SSTables satisfy the ordering rule or not.
    /// The vector of SSTs that pass the check is manothonically key-increasing.
    fn check_sst_valid(sstables: &[Arc<SsTable>]) {
        // Inside:
        for sst in sstables {
            assert!(sst.first_key() <= sst.last_key());
        }
        // Interactive:
        if !sstables.is_empty() {
            for i in 0..(sstables.len() - 1) {
                assert!(sstables[i].last_key() < sstables[i + 1].first_key())
            }
        }
    }

    /// move to the next sst until that one is valid.
    fn move_until_valid(&mut self) -> Result<()> {
        while let Some(iter) = self.current.as_mut() {
            if iter.is_valid() {
                break;
            }
            // If the current sst Iter is not valid, then:
            if self.next_sst_id >= self.sstables.len() {
                self.current = None;
            } else {
                self.current = Some(SsTableIterator::create_and_seek_to_first(
                    self.sstables[self.next_sst_id].clone(),
                )?);
                self.next_sst_id += 1;
            }
        }
        Ok(())
    }
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
