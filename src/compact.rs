#![allow(dead_code)]
#![allow(unused)]
mod leveled;

use crate::iterators::*;
use crate::key::KeySlice;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};
use crate::{iterators::StorageIterator, manifest::ManifestRecord};
use anyhow::Result;
use crossbeam::channel::{self, Receiver};
pub use leveled::{LeveledCompactionController, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use self::concat_iterator::SstConcatIterator;
pub use self::leveled::LeveledCompactionOptions;
use self::merge_iterator::MergeIterator;
use self::two_merge_iterator::TwoMergeIterator;
use crate::lsm_storage::{CompactionFilter, LsmStorageInner, LsmStorageState};
use crossbeam::select;

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
        }
    }
}

/// Controller for different Compaction strategy
pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    None,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(handle) => handle
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::None => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(self, Self::None | Self::Leveled(_))
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    Leveled(LeveledCompactionOptions),
    NoCompaction,
}

impl LsmStorageInner {
    /*------------------------------compact logic--------------------------*/

    /// initiates a full compaction process, which involves merging
    /// all SSTables from the L0 and L1 levels into new SSTables.
    pub fn force_full_compaction(&self) -> Result<()> {
        // step1. pre-flight check and get resource ready
        let CompactionOptions::NoCompaction = self.options.compaction_options else {
            panic!("full compaction can only be called with compaction is not enabled")
        };
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };

        // step2. genereate taks and execute it.
        let l0_sstables = snapshot.l0_sstables.clone();
        let l1_sstables = snapshot.levels[0].1.clone();
        let compaction_task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        };
        println!("force full compaction: {:?}", compaction_task);
        let sstables = self.compact(&compaction_task)?;

        // step3. finish touches (update state, make records, persistence etc)
        let mut ids = Vec::with_capacity(sstables.len());
        {
            let state_lock = self.state_lock.lock();
            let mut state = self.state.read().as_ref().clone();
            for sst in l0_sstables.iter().chain(l1_sstables.iter()) {
                let result = state.sstables.remove(sst);
                assert!(result.is_some());
            }
            for new_sst in sstables {
                ids.push(new_sst.sst_id());
                let result = state.sstables.insert(new_sst.sst_id(), new_sst);
                assert!(result.is_none());
            }
            assert_eq!(l1_sstables, state.levels[0].1);
            state.levels[0].1 = ids.clone();
            let mut l0_sstables_map = l0_sstables.iter().copied().collect::<HashSet<_>>();
            state.l0_sstables = state
                .l0_sstables
                .iter()
                .filter(|x| !l0_sstables_map.remove(x))
                .copied()
                .collect::<Vec<_>>();
            assert!(l0_sstables_map.is_empty());
            *self.state.write() = Arc::new(state);
            self.sync_dir()?;
            self.manifest.as_ref().unwrap().add_record(
                &state_lock,
                ManifestRecord::Compaction(compaction_task, ids.clone()),
            )?;
        }
        for sst in l0_sstables.iter().chain(l1_sstables.iter()) {
            std::fs::remove_file(self.path_of_sst(*sst))?;
        }
        println!("force full compaction done, new SSTs: {:?}", ids);

        Ok(())
    }

    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };
        match task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let mut l0_iters = Vec::with_capacity(l0_sstables.len());
                for id in l0_sstables.iter() {
                    l0_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                        snapshot.sstables.get(id).unwrap().clone(),
                    )?));
                }
                let mut l1_iters = Vec::with_capacity(l1_sstables.len());
                for id in l1_sstables.iter() {
                    l1_iters.push(snapshot.sstables.get(id).unwrap().clone());
                }
                let iter = TwoMergeIterator::create(
                    MergeIterator::create(l0_iters),
                    SstConcatIterator::create_and_seek_to_first(l1_iters)?,
                )?;
                self.compact_generate_sst(iter, task.compact_to_bottom_level())
            }
            CompactionTask::Leveled(LeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level: _,
                lower_level_sst_ids,
                ..
            }) => match upper_level {
                Some(_) => {
                    let mut upper_ssts = Vec::with_capacity(upper_level_sst_ids.len());
                    for id in upper_level_sst_ids.iter() {
                        upper_ssts.push(snapshot.sstables.get(id).unwrap().clone());
                    }
                    let upper_iter = SstConcatIterator::create_and_seek_to_first(upper_ssts)?;
                    let mut lower_ssts = Vec::with_capacity(upper_level_sst_ids.len());
                    for id in lower_level_sst_ids.iter() {
                        lower_ssts.push(snapshot.sstables.get(id).unwrap().clone());
                    }
                    let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_ssts)?;
                    self.compact_generate_sst(
                        TwoMergeIterator::create(upper_iter, lower_iter)?,
                        task.compact_to_bottom_level(),
                    )
                }
                None => {
                    let mut upper_iters = Vec::with_capacity(upper_level_sst_ids.len());
                    for id in upper_level_sst_ids.iter() {
                        upper_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                            snapshot.sstables.get(id).unwrap().clone(),
                        )?));
                    }
                    let upper_iter = MergeIterator::create(upper_iters);
                    let mut lower_ssts = Vec::with_capacity(upper_level_sst_ids.len());
                    for id in lower_level_sst_ids.iter() {
                        lower_ssts.push(snapshot.sstables.get(id).unwrap().clone());
                    }
                    let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_ssts)?;
                    self.compact_generate_sst(
                        TwoMergeIterator::create(upper_iter, lower_iter)?,
                        task.compact_to_bottom_level(),
                    )
                }
            },
        }
    }

    /// compact and organize data stored in the LSM storage engine into SSTables.
    /// responsible for generating new SSTables during compaction.
    fn compact_generate_sst(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
        compact_to_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut builder = None;
        let mut new_sst = Vec::new();
        let watermark = self.mvcc().watermark();
        let mut last_key = Vec::<u8>::new();
        let mut first_key_below_watermark = false;
        let compaction_filters = self.compaction_filters.lock().clone();
        'outer: while iter.is_valid() {
            if builder.is_none() {
                builder = Some(SsTableBuilder::new(self.options.block_size));
            }

            let same_as_last_key = iter.key().key_ref() == last_key;
            if !same_as_last_key {
                first_key_below_watermark = true;
            }

            if compact_to_bottom_level
                && !same_as_last_key
                && iter.key().ts() <= watermark
                && iter.value().is_empty()
            {
                last_key.clear();
                last_key.extend(iter.key().key_ref());
                iter.next()?;
                first_key_below_watermark = false;
                continue;
            }

            if iter.key().ts() <= watermark {
                if same_as_last_key && !first_key_below_watermark {
                    iter.next()?;
                    continue;
                }

                first_key_below_watermark = false;

                if !compaction_filters.is_empty() {
                    for filter in &compaction_filters {
                        match filter {
                            CompactionFilter::Prefix(x) => {
                                if iter.key().key_ref().starts_with(x) {
                                    iter.next()?;
                                    continue 'outer;
                                }
                            }
                        }
                    }
                }
            }

            let builder_inner = builder.as_mut().unwrap();
            if builder_inner.estimate_size() >= self.options.target_sst_size && !same_as_last_key {
                let sst_id = self.next_sst_id();
                let old_builder = builder.take().unwrap();
                let sst = Arc::new(old_builder.build(
                    sst_id,
                    Some(self.block_cache.clone()),
                    self.path_of_sst(sst_id),
                )?);
                new_sst.push(sst);
                builder = Some(SsTableBuilder::new(self.options.block_size));
            }

            let builder_inner = builder.as_mut().unwrap();
            builder_inner.add(iter.key(), iter.value());

            if !same_as_last_key {
                last_key.clear();
                last_key.extend(iter.key().key_ref());
            }

            iter.next()?;
        }
        if let Some(builder) = builder {
            let sst_id = self.next_sst_id(); // lock dropped here
            let sst = Arc::new(builder.build(
                sst_id,
                Some(self.block_cache.clone()),
                self.path_of_sst(sst_id),
            )?);
            new_sst.push(sst);
        }
        Ok(new_sst)
    }

    /* --------background thread---------- */
    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_) = self.options.compaction_options {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = channel::tick(Duration::from_millis(50));
                loop {
                    channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    /// Initiates the compaction process within the storage system.
    fn trigger_compaction(&self) -> Result<()> {
        // Retrieves a snapshot of the current storage system state.
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };
        // Generates a compaction task based on the snapshot
        // using the compaction controller.
        // If no compaction task is generated (indicating no compaction is needed),
        // returns early with Ok(()).
        let task = self
            .compaction_controller
            .generate_compaction_task(&snapshot);
        let Some(task) = task else {
            return Ok(());
        };
        self.dump_structure();
        println!("running compaction task: {:?}", task);
        // Executes the compaction task by calling the compact function,
        // which compacts the data according to the task.
        let sstables = self.compact(&task)?;
        // Updates the state by applying the compaction result and synchronizing the directory.
        let output = sstables.iter().map(|x| x.sst_id()).collect::<Vec<_>>();
        // Removes old SSTables that were replaced during compaction and synchronizes the directory again for cleanup.
        let ssts_to_remove = {
            // Preparation and Setup:
            let state_lock = self.state_lock.lock();
            let mut snapshot = self.state.read().as_ref().clone();
            let mut new_sst_ids = Vec::new();
            // Compaction Operations: file_to_add, ssts_to_remove
            for file_to_add in sstables {
                new_sst_ids.push(file_to_add.sst_id());
                let result = snapshot.sstables.insert(file_to_add.sst_id(), file_to_add);
                assert!(result.is_none());
            }
            // Apply the compaction result to the snapshot
            // which may involve removing old SSTables.
            let (mut snapshot, files_to_remove) = self
                .compaction_controller
                .apply_compaction_result(&snapshot, &task, &output);
            let mut ssts_to_remove = Vec::with_capacity(files_to_remove.len());
            for file_to_remove in &files_to_remove {
                let result = snapshot.sstables.remove(file_to_remove);
                assert!(result.is_some(), "cannot remove {}.sst", file_to_remove);
                ssts_to_remove.push(result.unwrap());
            }
            let mut state = self.state.write();
            *state = Arc::new(snapshot);
            drop(state);
            // finish touch: Sync and Updates
            self.sync_dir()?;
            self.manifest()
                .add_record(&state_lock, ManifestRecord::Compaction(task, new_sst_ids))?;
            ssts_to_remove
        };
        println!(
            "compaction finished: {} files removed, {} files added, output={:?}",
            ssts_to_remove.len(),
            output.len(),
            output
        );
        for sst in ssts_to_remove {
            std::fs::remove_file(self.path_of_sst(sst.sst_id()))?;
        }
        self.sync_dir()?;

        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = thread::spawn(move || {
            let ticker = channel::tick(Duration::from_millis(50));
            channel::select! {
                recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                    eprintln!("error occured: {}!",e);
                },
                recv(rx) -> _ => return
            }
        });
        Ok(Some(handle))
    }

    fn trigger_flush(&self) -> Result<()> {
        let cond = {
            let state = self.state.read();
            state.imm_memtables.len() >= self.options.num_memtable_limit
        };
        if cond {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }
}
