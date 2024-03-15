#![allow(unused)]
use bytes::Buf;

use crate::key::{Key, KeyVec};
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
    fn new(block: Arc<Block>) -> Self {
        todo!()
    }
}
