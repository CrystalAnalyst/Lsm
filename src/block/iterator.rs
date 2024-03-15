#![allow(unused)]
use bytes::Buf;

use crate::key::{Key, KeyVec};
use std::sync::Arc;

use super::Block;

pub struct BlockIterator {
    // reference to the block
    block: Arc<Block>,
    // the current key at the iterator position
    key: KeyVec,
    // the value range from the block
    value_range: (usize, usize),
    // the current index at the iterator pos.
    idx: usize,
    // the first key in the block.
    first_key: KeyVec,
}

impl Block {
    fn get_first_key(&self) -> KeyVec {
        let mut buf = &self.data[..];
        buf.get_u16();
        let key_len = buf.get_u16();
        let key = &buf[..key_len as usize];
        KeyVec::from_vec(key.to_vec())
    }
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        todo!()
    }
}
