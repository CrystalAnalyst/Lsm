#![allow(unused)]
use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStroageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    upper_level: Option<usize>,
    upper_level_sst_ids: Vec<usize>,
    lower_level: usize,
    lower_level_sst_ids: Vec<usize>,
    is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_precent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        todo!()
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStroageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        todo!()
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStroageState,
        task: &SimpleLeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStroageState, Vec<usize>) {
        todo!()
    }
}
