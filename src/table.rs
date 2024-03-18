#![allow(unused)]
pub(crate) mod bloom;
pub(crate) mod builder;

use self::bloom::Bloom;
use crate::key::{Key, KeyBytes};
use crate::lsm_storage::BlockCache;
use anyhow::bail;
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
        let mut block_meta = Vec::new();
        let num = buf.get_u32() as usize;
        let checksum = crc32fast::hash(&buf[..buf.remaining() - 4]);
        for _ in 0..num {
            let offset = buf.get_u32() as usize;
            let first_key_len = buf.get_u16() as usize;
            let first_key = KeyBytes::from_bytes(buf.copy_to_bytes(first_key_len));
            let last_key_len: usize = buf.get_u16() as usize;
            let last_key = KeyBytes::from_bytes(buf.copy_to_bytes(last_key_len));
            block_meta.push(BlockMeta {
                offset,
                first_key,
                last_key,
            });
        }
        if buf.get_u32() != checksum {
            bail!("meta checksum mismatched");
        }
        Ok(block_meta)
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

/// An SSTable is a file format used for storing key-value pairs sorted by keys.
pub struct SsTable {
    // the actual storage unit of SsTable.
    pub(crate) file: FileObject,
    // the meda blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    // the offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
}

impl SsTable {
    /// `open()` is responsible for opening an SSTable from a file.
    /// this function reads the necessary metadata from the file,
    /// including the Bloom filter and constructs an `SSTable` object.
    /// id : an identifier for the SSTable
    /// block_cache: Optional, used to store blocks of data read from the SSTable file.
    /// file : the file object representing the SSTable file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        // Read metadata.
        let len = file.size();
        let raw_bloom_offset = file.read(len - 4, 4)?;
        let bloom_offset = (&raw_bloom_offset[..]).get_u32() as u64;
        let raw_bloom = file.read(bloom_offset, len - 4 - bloom_offset)?;
        let bloom_filter = Bloom::decode(&raw_bloom)?;
        // read block metadata.
        let raw_meta_offset = file.read(bloom_offset - 4, 4)?;
        let block_meta_offset = (&raw_meta_offset[..]).get_u32() as u64;
        let raw_meta = file.read(block_meta_offset, bloom_offset - 4 - block_meta_offset)?;
        let block_meta = BlockMeta::decode_block_meta(&raw_meta[..])?;
        // construct SSTable Object.
        Ok(Self {
            file,
            first_key: block_meta.first().unwrap().first_key.clone(),
            last_key: block_meta.last().unwrap().last_key.clone(),
            block_meta,
            block_meta_offset: block_meta_offset as usize,
            id,
            block_cache,
            bloom: Some(bloom_filter),
            // todo : add timestamp.
        })
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
