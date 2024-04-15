#![allow(unused)]
pub(crate) mod bloom;
pub(crate) mod builder;
pub mod iterator;

use self::bloom::Bloom;
pub use self::builder::SsTableBuilder;
pub use self::iterator::SsTableIterator;
use crate::block::{self, Block};
use crate::key::{Key, KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use anyhow::anyhow;
use anyhow::Result;
use anyhow::{bail, Ok};
use bytes::{Buf, BufMut};
use std::{fs::File, io::Read, path::Path, sync::Arc};

/// Here you can see the Actual BlockMeta(the metadata for managing the Block)
/// that store Every block's offset in the File and the (FristKey, LastKey) contained.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    offset: usize,
    first_key: KeyBytes,
    last_key: KeyBytes,
}

impl BlockMeta {
    pub fn encode_block_meta(block_meta: &[BlockMeta], max_ts: u64, buf: &mut Vec<u8>) {
        // Init with u32, which represents the overall Number of Blocks existing.
        let mut estimated_size = std::mem::size_of::<u32>();
        for meta in block_meta {
            /*----------calculate the size of each block's metadata----------*/
            // offset.
            estimated_size += std::mem::size_of::<u32>();
            // double key_len and the actual length of key and timestamp.
            estimated_size += std::mem::size_of::<u16>() + meta.first_key.raw_len();
            estimated_size += std::mem::size_of::<u16>() + meta.last_key.raw_len();
        }
        // size of the TimeStamp
        estimated_size += std::mem::size_of::<u64>();
        // size of the checksum
        estimated_size += std::mem::size_of::<u32>();
        // reserve space in the buffer to improve perf.
        buf.reserve(estimated_size);
        // stored the original len to add checksum at the bottom
        let original_len = buf.len();
        /*------------Put all these staff into buffer--------*/
        buf.put_u32(block_meta.len() as u32);
        for meta in block_meta {
            buf.put_u32(meta.offset as u32);
            //first key
            buf.put_u16(meta.first_key.key_len() as u16);
            buf.put_slice(meta.first_key.key_ref());
            buf.put_u64(meta.first_key.ts());
            //last key
            buf.put_u16(meta.last_key.key_len() as u16);
            buf.put_slice(meta.last_key.key_ref());
            buf.put_u64(meta.last_key.ts());
        }
        buf.put_u64(max_ts);
        buf.put_u32(crc32fast::hash(&buf[original_len + 4..]));
        assert_eq!(estimated_size, buf.len() - original_len)
    }

    pub fn decode_block_meta(mut buf: &[u8]) -> Result<(Vec<BlockMeta>, u64)> {
        let mut block_meta = Vec::new();
        let num = buf.get_u32() as usize;
        let checksum = crc32fast::hash(&buf[..buf.remaining() - 4]);
        for _ in 0..num {
            // offset
            let offset = buf.get_u32() as usize;
            // first key
            let first_key_len = buf.get_u16() as usize;
            let first_key =
                KeyBytes::from_bytes_with_ts(buf.copy_to_bytes(first_key_len), buf.get_u64());
            // last key
            let last_key_len: usize = buf.get_u16() as usize;
            let last_key =
                KeyBytes::from_bytes_with_ts(buf.copy_to_bytes(last_key_len), buf.get_u64());
            // The One Indepedent Entity
            block_meta.push(BlockMeta {
                offset,
                first_key,
                last_key,
            });
        }
        let max_ts = buf.get_u64();
        if buf.get_u32() != checksum {
            bail!("meta checksum mismatched");
        }
        Ok((block_meta, max_ts))
    }
}

/// A file object
pub struct FileObject(Option<File>, u64);

impl FileObject {
    /// open the file lies in the Given Path and return the File object
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }

    /// Write given data to the path
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    // Executor
    /// read the file from: `offset`,  read `len` bytes.
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    // Accessor
    pub fn size(&self) -> u64 {
        self.1
    }
}

/// An SSTable is a file format used for storing key-value pairs sorted by keys.
pub struct SsTable {
    // File handle
    pub(crate) file: FileObject,
    // BlockMeta
    pub(crate) block_meta: Vec<BlockMeta>,
    pub(crate) block_meta_offset: usize,
    // Meta data member of SsTable
    id: usize,
    first_key: KeyBytes,
    last_key: KeyBytes,
    max_ts: u64,
    // Optimization: Cache and Bloom Filter
    block_cache: Option<Arc<BlockCache>>,
    pub(crate) bloom: Option<Bloom>,
}

impl SsTable {
    /*-----------------------Constructor--------------------------- */

    /// `open()` is responsible for opening an SSTable from a file.
    /// this function reads the necessary metadata from the file,
    /// including the Bloom filter and constructs an `SSTable` object.
    /// params:
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
        let (block_meta, max_ts) = BlockMeta::decode_block_meta(&raw_meta[..])?;
        // construct SSTable Object.
        Ok(Self {
            file,
            first_key: block_meta.first().unwrap().first_key.clone(),
            last_key: block_meta.last().unwrap().last_key.clone(),
            block_meta,
            block_meta_offset: block_meta_offset as usize,
            id,
            max_ts,
            block_cache,
            bloom: Some(bloom_filter),
        })
    }

    /// create a `mock SST`(means that It has not File object underlying)
    /// with only [first key + last key] metadata.
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            first_key,
            last_key,
            max_ts: 0,
            block_cache: None,
            bloom: None,
        }
    }

    /*-----------------------Executor--------------------------- */

    /// reads a block from the disk based on the given block index.
    /// block_idx: index of the block to be read.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        // determines the offset(Start) and length of the block data in the file.
        let offset = self.block_meta[block_idx].offset;
        let offset_end = self
            .block_meta
            .get(block_idx + 1)
            .map_or(self.block_meta_offset, |x| x.offset);
        let block_len = offset_end - offset - 4;
        // reads the block data along with the checksum from  the file
        let block_data_with_checksum: Vec<u8> = self
            .file
            .read(offset as u64, (offset_end - offset) as u64)?;
        let block_data = &block_data_with_checksum[..block_len];
        let checksum = (&block_data_with_checksum[block_len..]).get_u32();
        // verifies the checksum against the pre-calculated checksum
        if checksum != crc32fast::hash(block_data) {
            bail!("block checksum mismatched!");
        }
        // decodes the block data and return it as an Arc reference
        Ok(Arc::new(Block::decode(block_data)))
    }

    /// Read a block from the disk, with block cache.
    /// block_idx: index of the block to be read.
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        // Checks if a block cache is available
        if let Some(ref block_cache) = self.block_cache {
            // if available, attempts to retrieve the block from the cache.
            let block = block_cache
                .try_get_with((self.id, block_idx), || self.read_block(block_idx))
                .map_err(|e| anyhow!("{}", e))?;
            Ok(block)
        } else {
            // if not, simply reads the block from disk.
            self.read_block(block_idx)
        }
    }

    /// Find the index of the block that many contain `Key`
    /// key: the Key to search for, usize: the index of the block.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        self.block_meta
            .partition_point(|meta| meta.first_key.as_key_slice() <= key)
            .saturating_sub(1)
    }

    /*-----------------------Accessor--------------------------- */
    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
