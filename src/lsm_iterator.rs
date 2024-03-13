#![allow(unused)]

use std::{ops::Bound, thread::current};

use anyhow::Ok;
use anyhow::Result;
use bytes::Bytes;

use crate::key;
use crate::{
    iterators::{merge_iterator::MergeIterator, StorageIterator},
    mem_table::MemTableIterator,
};

// users should not call next(), key() and value()
// when the iterator is invalid.
type LsmIteratorInner = MergeIterator<MemTableIterator>;

pub struct LsmIterator {
    // inner iterator, a comb of merge ieterators on various data types.
    inner: LsmIteratorInner,
    // tracks the end bound of the iteration range.
    end_bound: Bound<Bytes>,
    // maintains a flag.
    is_valid: bool,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, end_bound: Bound<Bytes>) -> Result<Self> {
        let mut iter = Self {
            is_valid: iter.is_valid(),
            inner: iter,
            end_bound,
        };
        // move to non-delete.
        iter.move_to_non_delete()?;
        Ok(iter)
    }

    fn next_inner(&mut self) -> Result<()> {
        self.inner.next()?;
        if !self.inner.is_valid() {
            self.is_valid = false;
            return Ok(());
        }
        match self.end_bound.as_ref() {
            Bound::Unbounded => {}
            Bound::Included(key) => self.is_valid = self.inner.key().raw_ref() <= key.as_ref(),
            Bound::Excluded(key) => self.is_valid = self.inner.key().raw_ref() < key.as_ref(),
        }
        Ok(())
    }

    fn move_to_non_delete(&mut self) -> Result<()> {
        while self.is_valid() && self.inner.value().is_empty() {
            self.next_inner()?;
        }
        Ok(())
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn key(&self) -> &[u8] {
        self.inner.key().raw_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> anyhow::Result<()> {
        self.next_inner()?;
        self.move_to_non_delete()?;
        Ok(())
    }

    fn number_of_iterators(&self) -> usize {
        self.inner.number_of_iterators()
    }
}

// using FusedIterator to wraps the Iter, preventing user bad call.
pub struct FusedIterator<I: StorageIterator> {
    //trait I as the inner Type.
    iter: I,
    // track whether an error occured during Iteration.
    has_error: bool,
}
