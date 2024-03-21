mod leveled;
mod simple_leveled;
mod tiered;

use leveled::LeveledCompactionController;
use simple_leveled::SimpleLeveledCompactionController;
use tiered::TieredCompactionController;

use self::{
    leveled::LeveledCompactionOptions, simple_leveled::SimpleLeveledCompactionOptions,
    tiered::TieredCompactionOptions,
};

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
