#![allow(unused)]
use anyhow::{Context, Result};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};
use rustyline::validate;

use crate::{
    block::Block,
    compact::{CompactionController, CompactionOptions, LeveledCompactionController},
    iterators::{
        concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator, StorageIterator,
    },
    key::{self, KeyBytes, KeySlice},
    lsm_iterator::{FusedIterator, LsmIterator},
    manifest::{Manifest, ManifestRecord},
    mem_table::{map_bound, map_key_bound_plus_ts, MemTable},
    mvcc::{
        txn::{Transaction, TxnIterator},
        LsmMvccInner,
    },
    table::{FileObject, SsTable, SsTableBuilder, SsTableIterator},
};
use std::{
    collections::{BTreeSet, HashMap},
    fs::File,
    ops::Bound,
    path::{Path, PathBuf},
    sync::{atomic::AtomicUsize, Arc},
    thread, usize,
};

/// BlockCache for `read block from disk`, this is used when SSTable is built.
pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// stores the state of the storage Engine.
/// This is the core structure for Concurrenty Control and MetaData Manangement.
#[derive(Clone)]
pub struct LsmStorageState {
    // mutable memtable (only one at any time, allow multi-thread to access)
    pub memtable: Arc<MemTable>,
    // immutable_memtable for flush to the disk (A vector of)
    pub imm_memtables: Vec<Arc<MemTable>>,
    // the L0_SsTables stored in the disk, using `usize` to represents SSTable ID.
    pub l0_sstables: Vec<usize>,
    // SSTables sorted by key-range : L1(index:0) ~ Lmax for compaction
    pub levels: Vec<(usize, Vec<usize>)>,
    // SST objects : map index(usize) to SST Object(Arc<SsTable>)
    // I made every SSTable a ID, then use a vector of IDs to represents SSTables in one Level.
    // The smaller ID it is, then earlier it creates.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels: Vec::new(),
            sstables: HashMap::new(),
        }
    }
}

/// Provide Configurable options when Initializing the StorageState.
#[derive(Clone, Debug)]
pub struct LsmStorageOptions {
    // configure block size.
    pub block_size: usize,
    // configure the one SSTable/MemTable size.
    pub target_sst_size: usize,
    // configure the max number of memtables(Imms + 1).
    pub num_memtable_limit: usize,
    // Compaction option
    pub compaction_options: CompactionOptions,
    // serilization or not
    // open WAL or not
    pub enable_wal: bool,
    pub serializable: bool,
}

impl Default for LsmStorageOptions {
    fn default() -> Self {
        Self {
            block_size: 4 * 1024,
            target_sst_size: 1 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 3,
            serializable: false,
        }
    }
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

fn key_within(user_key: &[u8], table_begin: KeySlice, table_end: KeySlice) -> bool {
    table_begin.key_ref() <= user_key && user_key <= table_end.key_ref()
}

/// this function is used to efficiently determine if there is any overlap
/// between two ranges defined by the user and a table, based on their respective bounds.
fn range_overlap(
    user_begin: Bound<&[u8]>,
    user_end: Bound<&[u8]>,
    table_begin: KeySlice,
    table_end: KeySlice,
) -> bool {
    match user_end {
        Bound::Excluded(key) if key <= table_begin.key_ref() => {
            return false;
        }
        Bound::Included(key) if key < table_begin.key_ref() => {
            return false;
        }
        _ => {}
    }
    match user_begin {
        Bound::Excluded(key) if key >= table_end.key_ref() => {
            return false;
        }
        Bound::Included(key) if key > table_end.key_ref() => {
            return false;
        }
        _ => {}
    }
    true
}

/// the core data-structure of LsmStorage Engine.
/// only visible inside the crate.
pub(crate) struct LsmStorageInner {
    // lock the state for concurrent R/w.
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    // lock for sync.
    pub(crate) state_lock: Mutex<()>,
    // the path to the storage location on the file system.
    path: PathBuf,
    // cache data blocks read from the storage(disk)
    pub(crate) block_cache: Arc<BlockCache>,
    // generate unique ids for SSTables.
    next_sst_id: AtomicUsize,
    // configuration settings control the behavior of LSM Tree
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

impl LsmStorageInner {
    /*---------------------------Boost and Init---------------------------------*/
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let mut state = LsmStorageState::create(&options);
        let path = path.as_ref();
        let mut next_sst_id = 1;
        let block_cache = Arc::new(BlockCache::new(1 << 20)); // 4GB block cache,
        let manifest;
        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::NoCompaction => CompactionController::None,
        };
        if !path.exists() {
            std::fs::create_dir_all(path).context("failed to create DB dir")?;
        }
        let manifest_path = path.join("MANIFEST");
        let mut last_commit_ts = 0;
        if !manifest_path.exists() {
            if options.enable_wal {
                state.memtable = Arc::new(MemTable::create_with_wal(
                    state.memtable.id(),
                    Self::path_of_wal_static(path, state.memtable.id()),
                )?);
            }
            manifest = Manifest::create(&manifest_path).context("failed to create manifest")?;
            manifest.add_record_when_init(ManifestRecord::NewMemTable(state.memtable.id()))?;
        } else {
            let (m, records) = Manifest::recover(&manifest_path)?;
            let mut memtables = BTreeSet::new();
            for record in records {
                match record {
                    ManifestRecord::Flush(sst_id) => {
                        let res = memtables.remove(&sst_id);
                        assert!(res, "memtable not exist?");
                        if compaction_controller.flush_to_l0() {
                            state.l0_sstables.insert(0, sst_id);
                        } else {
                            state.levels.insert(0, (sst_id, vec![sst_id]));
                        }
                        next_sst_id = next_sst_id.max(sst_id);
                    }
                    ManifestRecord::NewMemTable(x) => {
                        next_sst_id = next_sst_id.max(x);
                        memtables.insert(x);
                    }
                    ManifestRecord::Compaction(task, output) => {
                        let (new_state, _) =
                            compaction_controller.apply_compaction_result(&state, &task, &output);
                        state = new_state;
                        next_sst_id =
                            next_sst_id.max(output.iter().max().copied().unwrap_or_default());
                    }
                }
            }
            let mut sst_cnt = 0;
            // recover SSTs
            for table_id in state
                .l0_sstables
                .iter()
                .chain(state.levels.iter().flat_map(|(_, files)| files))
            {
                let table_id = *table_id;
                let sst = SsTable::open(
                    table_id,
                    Some(block_cache.clone()),
                    FileObject::open(&Self::path_of_sst_static(path, table_id))
                        .context("failed to open SST")?,
                )?;
                last_commit_ts = last_commit_ts.max(sst.max_ts());
                state.sstables.insert(table_id, Arc::new(sst));
                sst_cnt += 1;
            }
            println!("{} SSTs opened", sst_cnt);
            next_sst_id += 1;
            // recover memtables
            if options.enable_wal {
                let mut wal_cnt = 0;
                for id in memtables.iter() {
                    let memtable =
                        MemTable::recover_from_wal(*id, Self::path_of_wal_static(path, *id))?;
                    let max_ts = memtable
                        .map
                        .iter()
                        .map(|x| x.key().ts())
                        .max()
                        .unwrap_or_default();
                    last_commit_ts = last_commit_ts.max(max_ts);
                    if !memtable.is_empty() {
                        state.imm_memtables.insert(0, Arc::new(memtable));
                        wal_cnt += 1;
                    }
                }
                println!("{} WALs recovered", wal_cnt);
                state.memtable = Arc::new(MemTable::create_with_wal(
                    next_sst_id,
                    Self::path_of_wal_static(path, next_sst_id),
                )?);
            } else {
                state.memtable = Arc::new(MemTable::create(next_sst_id));
            }
            m.add_record_when_init(ManifestRecord::NewMemTable(state.memtable.id()))?;
            next_sst_id += 1;
            manifest = m;
        };
        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id: AtomicUsize::new(next_sst_id),
            compaction_controller,
            manifest: Some(manifest),
            options: options.into(),
            mvcc: Some(LsmMvccInner::new(last_commit_ts)),
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };
        storage.sync_dir()?;
        Ok(storage)
    }

    /*---------helper functions: Id-generator, MVCC entity and manifest---------*/
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn mvcc(&self) -> &LsmMvccInner {
        self.mvcc.as_ref().unwrap()
    }

    pub(crate) fn manifest(&self) -> &Manifest {
        self.manifest.as_ref().unwrap()
    }

    /*----------------------------Util functions---------------------------------*/

    /// 根据SST的id, 返回它的实际路径
    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    /// 根据Wal的id, 返回它的实际路径
    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    /*-----------------------------Txn and CRUD API---------------------------------*/
    pub fn new_txn(self: &Arc<Self>) -> Result<Arc<Transaction>> {
        Ok(self.mvcc().new_txn(self.clone(), self.options.serializable))
    }

    pub fn get(self: &Arc<Self>, key: &[u8]) -> Result<Option<Bytes>> {
        let txn = self.mvcc().new_txn(self.clone(), self.options.serializable);
        txn.get(key)
    }

    pub fn get_with_ts(&self, key: &[u8], ts: u64) -> Result<Option<Bytes>> {
        // 1.snapshot generation
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        // 2.Iterators creation
        // MemTable iters
        let mut memtable_iters = Vec::with_capacity(snapshot.imm_memtables.len() + 1);
        memtable_iters.push(Box::new(snapshot.memtable.scan(
            Bound::Included(KeySlice::from_slice(key, key::TS_RANGE_BEGIN)),
            Bound::Included(KeySlice::from_slice(key, key::TS_RANGE_END)),
        )));
        for memtable in snapshot.imm_memtables.iter() {
            memtable_iters.push(Box::new(memtable.scan(
                Bound::Included(KeySlice::from_slice(key, key::TS_RANGE_BEGIN)),
                Bound::Included(KeySlice::from_slice(key, key::TS_RANGE_END)),
            )));
        }
        let memtable_iter = MergeIterator::create(memtable_iters);
        // L0 SSTable iters
        let mut l0_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        let keep_table = |key: &[u8], table: &SsTable| {
            if key_within(
                key,
                table.first_key().as_key_slice(),
                table.last_key().as_key_slice(),
            ) {
                if let Some(bloom) = &table.bloom {
                    if bloom.may_contain(farmhash::fingerprint32(key)) {
                        return true;
                    }
                } else {
                    return true;
                }
            }
            false
        };
        for table in snapshot.l0_sstables.iter() {
            let table = snapshot.sstables[table].clone();
            if keep_table(key, &table) {
                l0_iters.push(Box::new(SsTableIterator::create_and_seek_to_key(
                    table,
                    KeySlice::from_slice(key, key::TS_RANGE_BEGIN),
                )?));
            }
        }
        let l0_iter = MergeIterator::create(l0_iters);
        // Level SSTable iters
        let mut level_iters = Vec::with_capacity(snapshot.levels.len());
        for (_, level_sst_ids) in &snapshot.levels {
            let mut level_ssts = Vec::with_capacity(snapshot.levels[0].1.len());
            for table in level_sst_ids {
                let table = snapshot.sstables[table].clone();
                if keep_table(key, &table) {
                    level_ssts.push(table);
                }
            }
            let level_iter = SstConcatIterator::create_and_seek_to_key(
                level_ssts,
                KeySlice::from_slice(key, key::TS_RANGE_BEGIN),
            )?;
            level_iters.push(Box::new(level_iter));
        }
        // 3. Merging Iterators (merge these 3 iters to A single Iterator).
        let iter = LsmIterator::new(
            TwoMergeIterator::create(
                TwoMergeIterator::create(memtable_iter, l0_iter)?,
                MergeIterator::create(level_iters),
            )?,
            Bound::Unbounded,
            ts,
        )?;
        // 4. Key Filtering
        if iter.is_valid() && iter.key() == key && !iter.value().is_empty() {
            return Ok(Some(Bytes::copy_from_slice(iter.value())));
        }
        Ok(None)
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        let txn = self.mvcc().new_txn(self.clone(), self.options.serializable);
        txn.scan(lower, upper)
    }

    pub fn scan_with_ts(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        read_ts: u64,
    ) -> Result<FusedIterator<LsmIterator>> {
        // 1. snapshot generation
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        // 2. Iterators generation
        // MemTable iter
        let mut memtable_iters = Vec::with_capacity(snapshot.imm_memtables.len() + 1);
        memtable_iters.push(Box::new(snapshot.memtable.scan(
            map_key_bound_plus_ts(lower, key::TS_RANGE_BEGIN),
            map_key_bound_plus_ts(upper, key::TS_RANGE_END),
        )));
        for memtable in snapshot.imm_memtables.iter() {
            memtable_iters.push(Box::new(memtable.scan(
                map_key_bound_plus_ts(lower, key::TS_RANGE_BEGIN),
                map_key_bound_plus_ts(upper, key::TS_RANGE_END),
            )));
        }
        let memtable_iter = MergeIterator::create(memtable_iters);
        // SSTable L0 iter
        let mut table_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for table_id in snapshot.l0_sstables.iter() {
            let table = snapshot.sstables[table_id].clone();
            if range_overlap(
                lower,
                upper,
                table.first_key().as_key_slice(),
                table.last_key().as_key_slice(),
            ) {
                let iter = match lower {
                    Bound::Included(key) => SsTableIterator::create_and_seek_to_key(
                        table,
                        KeySlice::from_slice(key, key::TS_RANGE_BEGIN),
                    )?,
                    Bound::Excluded(key) => {
                        let mut iter = SsTableIterator::create_and_seek_to_key(
                            table,
                            KeySlice::from_slice(key, key::TS_RANGE_BEGIN),
                        )?;
                        while iter.is_valid() && iter.key().key_ref() == key {
                            iter.next()?;
                        }
                        iter
                    }
                    Bound::Unbounded => SsTableIterator::create_and_seek_to_first(table)?,
                };

                table_iters.push(Box::new(iter));
            }
        }
        let l0_iter = MergeIterator::create(table_iters);
        // SSTable Levels iter
        let mut level_iters = Vec::with_capacity(snapshot.levels.len());
        for (_, level_sst_ids) in &snapshot.levels {
            let mut level_ssts = Vec::with_capacity(level_sst_ids.len());
            for table in level_sst_ids {
                let table = snapshot.sstables[table].clone();
                if range_overlap(
                    lower,
                    upper,
                    table.first_key().as_key_slice(),
                    table.last_key().as_key_slice(),
                ) {
                    level_ssts.push(table);
                }
            }

            let level_iter = match lower {
                Bound::Included(key) => SstConcatIterator::create_and_seek_to_key(
                    level_ssts,
                    KeySlice::from_slice(key, key::TS_RANGE_BEGIN),
                )?,
                Bound::Excluded(key) => {
                    let mut iter = SstConcatIterator::create_and_seek_to_key(
                        level_ssts,
                        KeySlice::from_slice(key, key::TS_RANGE_BEGIN),
                    )?;
                    while iter.is_valid() && iter.key().key_ref() == key {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(level_ssts)?,
            };
            level_iters.push(Box::new(level_iter));
        }

        // 3. Merge Iterators
        let iter = TwoMergeIterator::create(memtable_iter, l0_iter)?;
        let iter = TwoMergeIterator::create(iter, MergeIterator::create(level_iters))?;

        // 4. Return values
        Ok(FusedIterator::new(LsmIterator::new(
            iter,
            map_bound(upper),
            read_ts,
        )?))
    }

    pub fn put(self: &Arc<Self>, key: &[u8], value: &[u8]) -> Result<()> {
        if !self.options.serializable {
            self.write_batch_inner(&[WriteBatchRecord::Put(key, value)])?;
        } else {
            let txn = self.mvcc().new_txn(self.clone(), self.options.serializable);
            txn.put(key, value);
            txn.commit()?;
        }
        Ok(())
    }

    pub fn delete(self: &Arc<Self>, key: &[u8]) -> Result<()> {
        if !self.options.serializable {
            self.write_batch_inner(&[WriteBatchRecord::Del(key)])?;
        } else {
            let txn = self.mvcc().new_txn(self.clone(), self.options.serializable);
            txn.delete(key);
            txn.commit()?;
        }
        Ok(())
    }

    pub fn write_batch<T: AsRef<[u8]>>(
        self: &Arc<Self>,
        batch: &[WriteBatchRecord<T>],
    ) -> Result<()> {
        if !self.options.serializable {
            self.write_batch_inner(batch)?;
        } else {
            let txn = self.mvcc().new_txn(self.clone(), self.options.serializable);
            for record in batch {
                match record {
                    WriteBatchRecord::Put(key, value) => txn.put(key.as_ref(), value.as_ref()),
                    WriteBatchRecord::Del(key) => txn.delete(key.as_ref()),
                }
                txn.commit()?;
            }
        }
        Ok(())
    }

    /// A helper function `write_batch_inner()` that processes a write batch.
    /// return a u64 commit timestamp so that Transaction::Commit can correctly
    /// store the committed transaction data into the MVCC structure.
    pub fn write_batch_inner<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<u64> {
        let _lck = self.mvcc().write_lock.lock();
        let commit_ts = self.mvcc().latest_commit_ts() + 1;
        for record in batch {
            match record {
                WriteBatchRecord::Put(key, value) => {
                    let key = key.as_ref();
                    let value = value.as_ref();
                    assert!(!key.is_empty(), "key cannot be empty!");
                    assert!(!value.is_empty(), "value cannot be empty!");
                    let size;
                    {
                        let guard = self.state.read();
                        guard
                            .memtable
                            .put(KeySlice::from_slice(key, commit_ts), value)?;
                        size = guard.memtable.approximate_size();
                    }
                    self.try_freeze(size)?;
                }
                WriteBatchRecord::Del(key) => {
                    let key = key.as_ref();
                    assert!(!key.is_empty(), "key cannot be empty!");
                    let size;
                    {
                        let guard = self.state.read();
                        guard
                            .memtable
                            .put(KeySlice::from_slice(key, commit_ts), b"")?;
                        size = guard.memtable.approximate_size();
                    }
                    self.try_freeze(size)?;
                }
            }
        }
        self.mvcc().update_commit_ts(commit_ts);
        Ok(commit_ts)
    }

    pub fn sync(&self) -> Result<()> {
        self.state.read().memtable.sync_wal()
    }

    /*----------------------------MemTable Management------------------------------*/
    fn try_freeze(&self, estimated_size: usize) -> Result<()> {
        if estimated_size > self.options.target_sst_size {
            let lock = self.state_lock.lock();
            let guard = self.state.read();
            if estimated_size > self.options.target_sst_size {
                drop(guard);
                self.force_freeze_memtable(&lock)?;
            }
        }
        Ok(())
    }

    pub fn force_freeze_memtable(&self, guard: &MutexGuard<'_, ()>) -> Result<()> {
        // step1. generate a new MemTable.
        let memtable_id = self.next_sst_id();
        let memtable = if self.options.enable_wal {
            Arc::new(MemTable::create_with_wal(
                memtable_id,
                self.path_of_wal(memtable_id),
            )?)
        } else {
            Arc::new(MemTable::create(memtable_id))
        };

        // step2. the actual freeze logic.
        self.freeze_memtable_with_memtable(memtable)?;

        // step3. using manifest to record the ops and sync.
        self.manifest()
            .add_record(guard, ManifestRecord::NewMemTable(memtable_id))?;
        self.sync_dir()?;

        Ok(())
    }

    fn freeze_memtable_with_memtable(&self, memtable: Arc<MemTable>) -> Result<()> {
        // step1. get snapshot
        let mut guard = self.state.write();
        let mut snapshot = guard.as_ref().clone();

        // step2. remove old memtable and insert it to the imm_list
        let old_memtable = std::mem::replace(&mut snapshot.memtable, memtable);
        snapshot.imm_memtables.insert(0, old_memtable.clone());

        // step3. update the state and sync
        *guard = Arc::new(snapshot);
        drop(guard);
        old_memtable.sync_wal()?;

        Ok(())
    }

    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        // step1. get the resource ready
        let state_lock = self.state_lock.lock();
        let flush_memtable;
        {
            let guard = self.state.read();
            flush_memtable = guard
                .imm_memtables
                .last()
                .expect("No memtable to be flushed!")
                .clone();
        }

        // step2. doing on purpose
        let mut builder = SsTableBuilder::new(self.options.block_size);
        flush_memtable.flush(&mut builder)?;
        let sst_id = flush_memtable.id();
        let sst = Arc::new(builder.build(
            sst_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(sst_id),
        )?);
        {
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();

            let mem = snapshot
                .imm_memtables
                .pop()
                .expect("No memtables to flush!");

            if self.compaction_controller.flush_to_l0() {
                // In leveled compaction or no compaction, simply flush to L0
                snapshot.l0_sstables.insert(0, sst_id);
            } else {
                // In tiered compaction, create a new tier
                snapshot.levels.insert(0, (sst_id, vec![sst_id]));
            }

            snapshot.sstables.insert(sst_id, sst);
            *guard = Arc::new(snapshot);
        }

        // update manifest and sync : wal, manifest and flush to Disk
        if self.options.enable_wal {
            std::fs::remove_file(self.path_of_wal(sst_id))?;
        }
        self.manifest()
            .add_record(&state_lock, ManifestRecord::Flush(sst_id))?;
        self.sync_dir()?;

        Ok(())
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

/// MiniLsm is a wrapper outside the LsmStorageInner, publicly accessible.
pub struct MiniLsm {
    // maintains a StorageInner inside of it.
    pub(crate) inner: Arc<LsmStorageInner>,
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    comapction_notifier: crossbeam::channel::Sender<()>,
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    flush_notifier: crossbeam::channel::Sender<()>,
}

impl MiniLsm {
    /*----------------Open and Close ------------------*/
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam::channel::unbounded();
        let compaction_thread = Mutex::new(inner.spawn_compaction_thread(rx)?);
        let (tx2, rx) = crossbeam::channel::unbounded();
        let flush_thread = Mutex::new(inner.spawn_flush_thread(rx)?);
        Ok(Arc::new(Self {
            inner,
            comapction_notifier: tx1,
            compaction_thread,
            flush_notifier: tx2,
            flush_thread,
        }))
    }

    /// Ensuring a graceful shutdown is crucial for data integrity and system stability,
    /// especially in storage systems where data persistence and consistency are paramount.
    /// By properly synchronizing and joining threads, flushing pending changes,
    /// and handling remaining in-memory data, the close method helps maintain
    /// the reliability and consistency of the LSM storage system during shutdown.
    pub fn close(&self) -> Result<()> {
        // sync and shutdown background threads
        self.inner.sync_dir()?;

        self.flush_notifier.send(()).ok();
        self.comapction_notifier.send(()).ok();
        let mut compaction_thread = self.compaction_thread.lock();
        if let Some(compaction_thread) = compaction_thread.take() {
            compaction_thread
                .join()
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }
        let mut flush_thread = self.flush_thread.lock();
        if let Some(flush_thread) = flush_thread.take() {
            flush_thread
                .join()
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }

        // When WAL is enabled, any changes made to the data are first recorded
        // in the WAL before they are applied to the main data store.
        // Therefore, even if there are remaining memtables in memory
        // that have not been flushed to disk,
        // their changes are already captured in the WAL.
        // So we can directly return Ok(()) here.
        if self.inner.options.enable_wal {
            // Sync wal.
            self.inner.sync()?;
            // Sync all the LsmStorageInner.
            self.inner.sync_dir()?;
            return Ok(());
        }

        // If No Wal, then check MemTable ( freeze & flush )
        // Chain of Thoughts: Freeze current MemTable and force all flush to the disk.
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .freeze_memtable_with_memtable(Arc::new(MemTable::create(
                    self.inner.next_sst_id(),
                )))?;
        }
        while {
            let snapshot = self.inner.state.read();
            !snapshot.imm_memtables.is_empty()
        } {
            // flush the pending MemTable to disk to persist.
            self.inner.force_flush_next_imm_memtable()?;
        }
        self.inner.sync_dir()?;

        Ok(())
    }

    /*----------------Data Manipulation------------------*/
    pub fn new_txn(&self) -> Result<Arc<Transaction>> {
        self.inner.new_txn()
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        self.inner.scan(lower, upper)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    /*----------------Sync and Compaction------------------*/
    pub fn flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn compact(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    /*-----------------Tesing usage-----------------------*/
    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}
