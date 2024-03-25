#![allow(unused)]

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStroageState;

/// represents a compaction task, which includes the tiers
/// to comapct and whether the bottom tier is included.
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
    pub num_of_tiers: usize,
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
        // 0. Precondition check
        assert!(
            snapshot.l0_sstables.is_empty(),
            "L0 should not present in Tiered compaction"
        );
        if snapshot.levels.len() < self.options.num_of_tiers {
            return None;
        }
        // 1.compaction triggered by space Amplification ratio
        let mut size = 0;
        for id in 0..(snapshot.levels.len() - 1) {
            size += snapshot.levels[id].1.len();
        }
        let space_amp_ratio =
            (size as f64) / (snapshot.levels.last().unwrap().1.len() as f64) * 100.0;
        if space_amp_ratio >= self.options.max_size_amplification_percent as f64 {
            return Some(TieredCompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }
        // 2. size ratio check
        let size_ratio_trigger = (100.0 + self.options.size_ratio as f64) / 100.0;
        let mut size = 0;
        for id in 0..(snapshot.levels.len() - 1) {
            size += snapshot.levels[id].1.len();
            let next_level_size = snapshot.levels[id + 1].1.len();
            let cur_level_size = size as f64 / next_level_size as f64;
            if cur_level_size >= size_ratio_trigger && id + 2 >= self.options.min_merge_width {
                return Some(TieredCompactionTask {
                    tiers: snapshot
                        .levels
                        .iter()
                        .take(id + 2)
                        .cloned()
                        .collect::<Vec<_>>(),
                    bottom_tier_included: id + 2 >= snapshot.levels.len(),
                });
            }
        }
        // 3. reduce sorted run compaction
        let num_iters_to_take = snapshot.levels.len() - self.options.num_of_tiers + 2;
        println!("compaction triggered  by reducing sorted runs");
        return Some(TieredCompactionTask {
            tiers: snapshot.levels.iter().take(2).cloned().collect::<Vec<_>>(),
            bottom_tier_included: snapshot.levels.len() >= num_iters_to_take,
        });
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStroageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStroageState, Vec<usize>) {
        // part1: clone the Sp and Init vars.
        let mut snapshot = snapshot.clone();
        let mut tier_to_remove = task
            .tiers
            .iter()
            .map(|(x, y)| (*x, y))
            .collect::<HashMap<_, _>>();
        let mut levels = Vec::new();
        let mut new_tier_added = false;
        let mut files_to_remove = Vec::new();
        // part 2
        // Iterating Through Tiers and Processing
        for (tier_id, files) in &snapshot.levels {
            if let Some(ffiles) = tier_to_remove.remove(tier_id) {
                // the tier should be removed
                assert_eq!(ffiles, files, "file changed after issuing compaction task");
                files_to_remove.extend(ffiles.iter().copied());
            } else {
                // retain the tier
                levels.push((*tier_id, files.clone()));
            }
            if tier_to_remove.is_empty() && !new_tier_added {
                // add the compacted tier to the LSM tree
                new_tier_added = true;
                levels.push((output[0], output.to_vec()));
            }
        }
        // part3
        if !tier_to_remove.is_empty() {
            unreachable!("some tiers not found?");
        }
        snapshot.levels = levels;
        (snapshot, files_to_remove)
    }
}
