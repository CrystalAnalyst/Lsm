#![allow(unused)]

use std::process::Output;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStroageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is None, means L0-compaction.
    pub upper_level: Option<usize>,
    pub upper_level_ssts_id: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_ssts_id: Vec<usize>,
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

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlaping_ssts(
        &self,
        snapshot: &LsmStroageState,
        sst_ids: &[usize],
        in_level: usize,
    ) -> Vec<usize> {
        todo!()
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStroageState,
    ) -> Option<LeveledCompactionTask> {
        todo!()
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStroageState,
        task: &LeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStroageState, Vec<usize>) {
        todo!()
    }
}
