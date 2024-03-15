use crate::key::KeyVec;

/// Builds a block
pub struct BlockBuilder {
    data: Vec<u8>,
    offsets: Vec<u16>,
    block_size: usize,
    first_key: KeyVec,
}
