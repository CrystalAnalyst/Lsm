#![allow(dead_code)]
#![allow(unused)]
mod leveled;
mod simple_leveled;
mod tiered;

use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::table::SsTable;
use anyhow::Result;
use crossbeam::channel::{self, Receiver};
pub use leveled::{LeveledCompactionController, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{SimpleLeveledCompactionController, SimpleLeveledCompactionTask};
use std::sync::Arc;
pub use tiered::{TieredCompactionController, TieredCompactionTask};

use crate::lsm_storage::{LsmStorageInner, LsmStorageState};

pub use self::{
    leveled::LeveledCompactionOptions, simple_leveled::SimpleLeveledCompactionOptions,
    tiered::TieredCompactionOptions,
};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

/// Controller for different Compaction strategy
pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    None,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Result<CompactionTask> {
        todo!()
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: CompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        todo!()
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(self, Self::None | Self::Simple(_) | Self::Leveled(_))
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    Leveled(LeveledCompactionOptions),
    Tiered(TieredCompactionOptions),
    Simple(SimpleLeveledCompactionOptions),
    NoCompaction,
}

impl LsmStorageInner {
    /* ----------compact logic----------- */
    pub fn force_compact(&self) -> Result<()> {
        todo!()
    }

    fn compact_inner(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        todo!()
    }

    fn compact_generate_sst(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
        compact_to_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        todo!()
    }

    /* --------background thread---------- */
    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        todo!()
    }

    fn trigger_compaction(&self) -> Result<()> {
        todo!()
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        todo!()
    }

    fn trigger_flush(&self) -> Result<()> {
        todo!()
    }
}
