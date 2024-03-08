#![allow(unused_variables)]
#![allow(dead_code)]

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    offset: usize,
    frist_key: usize,
    last_key: usize,
}
