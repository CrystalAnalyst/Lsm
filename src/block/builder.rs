use bytes::BufMut;

use super::Block;
use super::SIZEOF_U16;
use crate::key::{Key, KeySlice, KeyVec};

/// Builds a block
pub struct BlockBuilder {
    /// all key-value pairs(serilized) in the block.
    data: Vec<u8>,
    /// offsets of each k-v entries
    offsets: Vec<u16>,
    /// the whole block size
    block_size: usize,
    /// the first key in the block
    first_key: KeyVec,
}

fn common_prefix(first_key: KeySlice, key: KeySlice) -> usize {
    let mut i = 0;
    loop {
        // boundary check.
        if i >= first_key.len() || i >= key.len() {
            break;
        }
        // compare to find the common.
        if first_key.raw_ref()[i] != key.raw_ref()[i] {
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
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// return the estimated_size of the `current`` Block
    fn estimated_size(&self) -> usize {
        self.data.len() + self.offsets.len() * SIZEOF_U16 + SIZEOF_U16
    }
    
    /// Adds a new k-v pair(entry) to the block, return false when block is full
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        // 0. Convince the key is not empty.
        assert!(!key.is_empty(), "key must not be empty");
        // 1. calculate the Size after adding the new kv pair.
        let add_on = key.len() + value.len() + SIZEOF_U16 * 3;
        let size_expect = self.estimated_size() + add_on;
        if size_expect > self.block_size && !self.is_empty() {
            return false;
        }

        // 2. add the offset of the data into the offset array.
        self.offsets.push(self.data.len() as u16);

        // 3.  add the new k-v pairs( using common_prefix to save space)
        // 3.1 add the common_prefix
        let prefix = common_prefix(self.first_key.as_key_slice(), key);
        self.data.put_u16(prefix as u16);
        // 3.2 add the kv pair: key_len + key + value_len + value.
        self.data.put_u16((key.len() - prefix) as u16);
        self.data.put(&key.raw_ref()[prefix..]);
        self.data.put_u16(value.len() as u16);
        self.data.put(value);

        // 4. check the first_key, if its empty then replace.
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
