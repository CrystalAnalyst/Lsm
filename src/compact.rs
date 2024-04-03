#![allow(dead_code)]
mod leveled;
mod simple_leveled;
mod tiered;

use leveled::{LeveledCompactionController, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
use simple_leveled::{SimpleLeveledCompactionController, SimpleLeveledCompactionTask};
use tiered::{TieredCompactionController, TieredCompactionTask};

use self::{
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

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    Leveled(LeveledCompactionOptions),
    Tiered(TieredCompactionOptions),
    Simple(SimpleLeveledCompactionOptions),
    NoCompaction,
}
