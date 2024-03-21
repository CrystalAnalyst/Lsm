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
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStroageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        // 1. calculate level sizes.
        let mut level_sizes = Vec::new();
        level_sizes.push(snapshot.l0_sstables.len());
        for (_, files) in &snapshot.levels {
            level_sizes.push(files.len());
        }
        // 2. Iterate over levels
        // during iteration, check the compaction trigger conditions.
        for i in 0..self.options.max_levels {
            if i == 0
                && snapshot.l0_sstables.len() < self.options.level0_file_num_compaction_trigger
            {
                continue;
            }
            // check the compaction trigger conditions.
            let lower = i + 1;
            let ratio = level_sizes[lower] as f64 / level_sizes[i] as f64;
            // if actual ratio less than expected ratio, then trigger compaction.
            if ratio < self.options.size_ratio_precent as f64 / 100.0 {
                return Some(SimpleLeveledCompactionTask {
                    upper_level: if i == 0 { None } else { Some(i) },
                    upper_level_sst_ids: if i == 0 {
                        snapshot.l0_sstables.clone()
                    } else {
                        snapshot.levels[i - 1].1.clone()
                    },

                    lower_level: lower,
                    lower_level_sst_ids: snapshot.levels[lower - 1].1.clone(),

                    is_lower_level_bottom_level: lower == self.options.max_levels,
                });
            }
        }
        None
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
