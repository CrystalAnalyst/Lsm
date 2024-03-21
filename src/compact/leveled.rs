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
        // 1. Initialization: params -> snapshot, a list of SSTable IDs and level
        // where the search is conducted.
        // 2. Find Key Range
        let begin_key = sst_ids
            .iter()
            .map(|id| snapshot.sstables[id].first_key())
            .min()
            .cloned()
            .unwrap();
        let end_key = sst_ids
            .iter()
            .map(|id| snapshot.sstables[id].last_key())
            .max()
            .cloned()
            .unwrap();
        // 3. Search for Overlapping SSTables
        let mut overlap_ssts = Vec::new();
        for sst_id in &snapshot.levels[in_level - 1].1 {
            let sst = &snapshot.sstables[sst_id];
            let first_key = sst.first_key();
            let last_key = sst.last_key();
            if !(last_key < &begin_key || first_key > &end_key) {
                overlap_ssts.push(*sst_id);
            }
        }
        // 4. Return
        overlap_ssts
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
