#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(unused_mut)]
#![allow(unused_imports)]
pub(crate) mod bloom;
pub(crate) mod builder;

use self::bloom::Bloom;
use crate::{
    key::{Key, KeyBytes},
    lsm_storage::BlockCache,
};
use anyhow::Result;
use bytes::{Buf, BufMut};
use std::{fs::File, io::Read, path::Path, sync::Arc};

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
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }

    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }
}

pub struct SsTable {
    pub(crate) file: FileObject,
    pub(crate) block_meta: Vec<BlockMeta>,
    pub(crate) block_meta_offset: usize,
    id: usize,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
}

impl SsTable {
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let len = file.size();
        let raw_bloom_offset = file.read(len - 4, 4)?;
        let bloom_offset = (&raw_bloom_offset[..]).get_u32() as u64;
        let raw_bloom = file.read(bloom_offset, len - 4 - bloom_offset)?;
        todo!()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }
    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }
    pub fn table_size(&self) -> u64 {
        self.file.1
    }
    pub fn sst_id(&self) -> usize {
        self.id
    }
}
