mod leveled;
mod simple_leveled;
mod tiered;

use leveled::LeveledCompactionController;
use simple_leveled::SimpleLeveledCompactionController;
use tiered::TieredCompactionController;

/// Controller for different Compaction strategy
pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    None,
}
