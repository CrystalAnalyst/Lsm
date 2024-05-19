use bytes::BufMut;

use super::Block;
use super::SIZEOF_U16;
use crate::key::{KeySlice, KeyVec};

/// Builds a block
pub struct BlockBuilder {
    // block data
    data: Vec<u8>,
    offsets: Vec<u16>,
    // metadata
    first_key: KeyVec,
    block_size: usize,
}

/// to compare how many common places between the first_key and the selected key
/// and return the place they differs First time from each other
fn common_prefix(first_key: KeySlice, key: KeySlice) -> usize {
    let mut i = 0;
    loop {
        // boundary check.
        if i >= first_key.key_len() || i >= key.key_len() {
            break;
        }
        // compare to find the common.
        if first_key.key_ref()[i] != key.key_ref()[i] {
            break;
        }
        i += 1;
    }
    i
}

impl BlockBuilder {
    /// creates a new block builder
    pub fn new(block_size: usize) -> Self {
        Self {
            data: Vec::new(),
            offsets: Vec::new(),
            first_key: KeyVec::new(),
            block_size,
        }
    }

    /// return the estimated_size of the `current`` Block
    /// Entries + offsets + #Entry
    fn estimated_size(&self) -> usize {
        self.data.len() + self.offsets.len() * SIZEOF_U16 + SIZEOF_U16
    }

    /// Adds a new k-v pair(entry) to the block, return false when block is full
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        assert!(!key.is_empty(), "key must not be empty");
        let add_on = key.raw_len() + value.len() + SIZEOF_U16 * 3;
        let size_expect = self.estimated_size() + add_on;
        if size_expect > self.block_size && !self.is_empty() {
            return false;
        }
        self.offsets.push(self.data.len() as u16);
        let prefix = common_prefix(self.first_key.as_key_slice(), key);
        self.data.put_u16(prefix as u16);
        self.data.put_u16((key.key_len() - prefix) as u16);
        self.data.put(&key.key_ref()[prefix..]);
        self.data.put_u64(key.ts());
        self.data.put_u16(value.len() as u16);
        self.data.put(value);
        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
        }
        true
    }

    /// check the blockbuilder whether it's empty or not.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// using BlockBuidler to build a block.
    pub fn build(self) -> Block {
        if self.is_empty() {
            panic!("block should not be empty!")
        }
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
