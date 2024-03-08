#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(unused_mut)]

use crate::key::KeyBytes;
use anyhow::Result;
use bytes::BufMut;
use std::{fs::File, path::Path};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    offset: usize,
    first_key: KeyBytes,
    last_key: KeyBytes,
}

impl BlockMeta {
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        // calculate the estimated_size of the encoded data.
        let mut estimated_size = std::mem::size_of::<u32>();
        for meta in block_meta {
            // calculate the size of each block's metadata.
            estimated_size += std::mem::size_of::<u32>();
            estimated_size += std::mem::size_of::<u16>() + meta.first_key.len();
            estimated_size += std::mem::size_of::<u16>() + meta.last_key.len();
        }
        // size of the checksum
        estimated_size += std::mem::size_of::<u32>();

        // reserve space in the buffer to improve perf.
        buf.reserve(estimated_size);
        let original_len = buf.len();
        buf.put_u32(block_meta.len() as u32);

        for meta in block_meta {
            buf.put_u32(meta.offset as u32);
            buf.put_u16(meta.first_key.len() as u16);
            buf.put_slice(meta.first_key.raw_ref());
            buf.put_u16(meta.last_key.len() as u16);
            buf.put_slice(meta.last_key.raw_ref());
        }
        buf.put_u32(crc32fast::hash(&buf[original_len + 4..]));
        assert_eq!(estimated_size, buf.len() - original_len)
    }
    pub fn decode_block_meta(mut buf: &[u8]) -> Result<Vec<BlockMeta>> {
        todo!()
    }
}

/// A file object
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        todo!()
    }
    pub fn open(path: &Path) -> Result<Self> {
        todo!()
    }
}
