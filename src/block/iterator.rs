#![allow(unused)]
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
