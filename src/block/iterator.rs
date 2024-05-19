use bytes::Buf;

use crate::key::{KeySlice, KeyVec};
use std::sync::Arc;

use super::{Block, SIZEOF_U16};

/// The Iterator Over Blocks
/// So you can see all the key here is `KeyVec` means that
/// Block has the true ownership of the data.
pub struct BlockIterator {
    block: Arc<Block>,
    // Block Metadata
    idx: usize,
    first_key: KeyVec,
    value_range: (usize, usize),
    // Current Entry's key
    key: KeyVec,
}

impl Block {
    /// get the first_key(the key and ts) from One Block
    fn get_first_key(&self) -> KeyVec {
        let mut buf = &self.data[..];
        // skip the overlap(CommonPrefix)
        buf.get_u16();
        // get the key_len.
        let key_len = buf.get_u16() as usize;
        // get the key.
        let key = &buf[..key_len as usize];
        buf.advance(key_len);
        // type convert: Merge-up the elements(the key, and the timestamp) to `KeyVec`.
        KeyVec::from_vec_with_ts(key.to_vec(), buf.get_u64())
    }
}

impl BlockIterator {
    // Constructor(Associate Function)
    fn new(block: Arc<Block>) -> Self {
        Self {
            first_key: block.get_first_key(),
            block,
            idx: 0,
            value_range: (0, 0),
            key: KeyVec::new(),
        }
    }

    /*----------------- Accessors------------------*/

    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /*-----------------Seek Methods---------------------*/

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

    /*------------------Util Methods-------------------- */

    /// find the first entry.
    pub fn seek_to_first(&mut self) {
        self.seek_to(0);
    }

    /// move to next entry.
    pub fn next(&mut self) {
        self.idx += 1;
        self.seek_to(self.idx);
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

    /// move to specified offset("per Bytes") and update the current key-value pair.
    /// index update will be handled by caller
    fn seek_to_offset(&mut self, offset: usize) {
        let mut entry = &self.block.data[offset..];
        let prefix = entry.get_u16() as usize;
        let key_len = entry.get_u16() as usize;
        let key = &entry[..key_len];
        self.key.clear();
        self.key.append(&self.first_key.key_ref()[..prefix]);
        self.key.append(key);
        entry.advance(key_len);
        let value_len = entry.get_u16() as usize;
        let value_offset_begin = offset + SIZEOF_U16 + SIZEOF_U16 + key_len + SIZEOF_U16;
        let value_offset_end = value_offset_begin + value_len;
        self.value_range = (value_offset_begin, value_offset_end);
        entry.advance(value_len);
    }
}
