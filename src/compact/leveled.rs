#![allow(unused)]

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is None, means L0-compaction.
    pub upper_level: Option<usize>,
    pub upper_level_ssts_id: usize,
    pub lower_level: usize,
    pub lower_level_ssts_id: usize,
    pub is_lower_level_the_bottom: bool,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_files_num_compaction_threshold: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}
