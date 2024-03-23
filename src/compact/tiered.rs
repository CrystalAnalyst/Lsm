#![allow(unused)]

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStroageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub level_size_multiplier: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStroageState,
    ) -> Option<TieredCompactionTask> {
        todo!()
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStroageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStroageState, Vec<usize>) {
        todo!()
    }
}
