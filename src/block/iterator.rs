#![allow(unused)]
use bytes::Buf;

use crate::key::{Key, KeySlice, KeyVec};
use std::sync::Arc;

use super::{Block, SIZEOF_U16};

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
        iter.seek_to_first();
        iter
    }

    /// constructorOption2: move to the appointed key.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
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

    /// seek to a specific index (the idx is `the ith of the entries `).
    fn seek_to(&mut self, idx: usize) {
        // check boundary.
        if idx >= self.block.offsets.len() {
            self.key.clear();
            self.value_range = (0, 0);
            return;
        }
        // normal process.
        let offset = self.block.offsets[idx] as usize;
        self.seek_to_offset(offset);
        self.idx = idx;
    }

    /// move to specified offset and update the current key-value pair.
    /// index update will be handled by calller
    fn seek_to_offset(&mut self, offset: usize) {
        // get the entry.
        let mut entry = &self.block.data[offset..];
        // get the key (including the prefix)
        let prefix = entry.get_u16() as usize;
        let key_len = entry.get_u16() as usize;
        let key = &entry[..key_len];
        // update the key
        self.key.clear();
        self.key.append(&self.first_key.raw_ref()[..prefix]);
        self.key.append(key);
        entry.advance(key_len);
        // update the value and value range.
        let value_len = entry.get_u16() as usize;
        let value_offset_begin = offset + SIZEOF_U16 + SIZEOF_U16 + key_len + SIZEOF_U16;
        let value_offset_end = value_offset_begin + value_len;
        self.value_range = (value_offset_begin, value_offset_end);
        entry.advance(value_len);
    }

    /// find the key (or first greater than the key)
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let mut low = 0;
        let mut high = self.block.offsets.len();
        while low < high {
            let mid = low + (high - low) / 2;
            self.seek_to(mid);
            assert!(self.is_valid());
            match self.key().cmp(&key) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Greater => high = mid,
                std::cmp::Ordering::Equal => return,
            }
        }
        self.seek_to(low)
    }

    /// move to next entry.
    pub fn next(&mut self) {
        self.idx += 1;
        self.seek_to(self.idx);
    }
}
