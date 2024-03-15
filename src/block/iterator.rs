#![allow(unused)]
use bytes::Buf;

use crate::key::{Key, KeySlice, KeyVec};
use std::sync::Arc;

use super::Block;

pub struct BlockIterator {
    // reference to the block
    block: Arc<Block>,
    // the `current key` at the iterator position
    key: KeyVec,
    // the ` first key ` in the block.
    first_key: KeyVec,
    // the value range from the block
    value_range: (usize, usize),
    // the current index at the iterator pos.
    idx: usize,
}

impl Block {
    fn get_first_key(&self) -> KeyVec {
        // get data stored in buffer.
        let mut buf = &self.data[..];
        // get the common_prefix.
        buf.get_u16();
        // get the key_len.
        let key_len = buf.get_u16();
        // get the key.
        let key = &buf[..key_len as usize];
        // type convert: from Key to KeyVec.
        KeyVec::from_vec(key.to_vec())
    }
}

impl BlockIterator {
    /// constructor: create a new BlockIteraotr.
    fn new(block: Arc<Block>) -> Self {
        Self {
            first_key: block.get_first_key(),
            key: KeyVec::new(),
            block,
            value_range: (0, 0),
            idx: 0,
        }
    }

    /// constructorOption1: move the cursor to 0.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        // seek to first.
        iter.seek_to_first();
        iter
    }

    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        todo!()
    }

    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    ///find the first key.
    pub fn seek_to_first(&mut self) {
        self.seek_to(0);
    }

    /// seek to a specific index.
    fn seek_to(&mut self, idx: usize) {
        // check boundary.
        if idx >= self.block.offsets.len() {
            todo!()
        }
        // normal process.
        let offset = self.block.offsets[idx] as usize;
        self.seek_to_offset(offset);
        self.idx = idx;
    }

    /// move to specified offset
    fn seek_to_offset(&mut self, offset: usize) {
        todo!()
    }

    /// find the key (or first greater than the key)
    pub fn seek_to_key(&mut self, key: KeySlice) {
        todo!()
    }

    /// move to next entry.
    pub fn next(&mut self) {
        todo!()
    }
}
