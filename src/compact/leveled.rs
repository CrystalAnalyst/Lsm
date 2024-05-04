#![allow(unused)]

use std::{collections::HashSet, process::Output};

use serde::{Deserialize, Serialize};

use crate::{compact::leveled, lsm_storage::LsmStorageState};

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is None, means L0-compaction.
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlaping_ssts(
        &self,
        snapshot: &LsmStorageState,
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
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        // calculate the target size
        let mut target_level_sizes = (0..self.options.max_levels).map(|_| 0).collect::<Vec<_>>();
        let mut real_level_sizes = Vec::with_capacity(self.options.max_levels);
        let mut base_level = self.options.max_levels;
        for i in 0..self.options.max_levels {
            real_level_sizes.push(
                snapshot.levels[i]
                    .1
                    .iter()
                    .map(|id| snapshot.sstables.get(id).unwrap().table_size())
                    .sum::<u64>() as usize,
            );
        }
        let base_level_size_bytes = self.options.base_level_size_mb * 1024 * 1024;
        target_level_sizes[self.options.max_levels - 1] =
            real_level_sizes[self.options.max_levels - 1].max(base_level_size_bytes);
        for level in (0..self.options.max_levels - 1).rev() {
            let next_level_size = target_level_sizes[level + 1];
            let this_level_size = next_level_size / self.options.level_size_multiplier;
            if next_level_size > base_level_size_bytes {
                target_level_sizes[level] = this_level_size;
            }
            if target_level_sizes[level] > 0 {
                base_level = level + 1;
            }
        }

        // generate compaction task for Both L0 and other levels.
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: base_level,
                lower_level_sst_ids: self.find_overlaping_ssts(
                    snapshot,
                    &snapshot.l0_sstables,
                    base_level,
                ),
                is_lower_level_bottom_level: base_level == self.options.max_levels,
            });
        }
        let mut priority = Vec::with_capacity(self.options.max_levels);
        for i in (0..self.options.max_levels) {
            let prio = real_level_sizes[i] as f64 / target_level_sizes[i] as f64;
            priority.push((prio, i + 1));
        }
        priority.sort_by(|a, b| a.partial_cmp(b).unwrap().reverse());
        let priority = priority.first();
        if let Some((_, level)) = priority {
            let level = *level;
            let select_sst = snapshot.levels[level - 1].1.iter().min().copied().unwrap();
            return Some(LeveledCompactionTask {
                upper_level: Some(level),
                upper_level_sst_ids: vec![select_sst],
                lower_level: level + 1,
                lower_level_sst_ids: self.find_overlaping_ssts(snapshot, &[select_sst], level + 1),
                is_lower_level_bottom_level: level + 1 == self.options.max_levels,
            });
        }
        None
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();
        let mut files_to_remove = Vec::new();
        let mut upper_level_sst_ids_set = task
            .upper_level_sst_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        let mut lower_level_sst_ids_set = task
            .lower_level_sst_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();

        if let Some(upper_level) = task.upper_level {
            let new_upper_level_ssts = snapshot.levels[upper_level - 1]
                .1
                .iter()
                .filter_map(|x| {
                    if upper_level_sst_ids_set.remove(x) {
                        return None;
                    }
                    Some(*x)
                })
                .collect::<Vec<_>>();
        } else {
            let new_l0_ssts = snapshot
                .l0_sstables
                .iter()
                .filter_map(|x| {
                    if upper_level_sst_ids_set.remove(x) {
                        return None;
                    }
                    Some(*x)
                })
                .collect::<Vec<_>>();
            snapshot.l0_sstables = new_l0_ssts;
        }

        files_to_remove.extend(&task.upper_level_sst_ids);
        files_to_remove.extend(&task.lower_level_sst_ids);

        let mut new_lower_level_ssts = snapshot.levels[task.lower_level - 1]
            .1
            .iter()
            .filter_map(|x| {
                if lower_level_sst_ids_set.remove(x) {
                    return None;
                }
                Some(*x)
            })
            .collect::<Vec<_>>();

        new_lower_level_ssts.extend(output);
        new_lower_level_ssts.sort_by(|x, y| {
            snapshot
                .sstables
                .get(x)
                .unwrap()
                .first_key()
                .cmp(snapshot.sstables.get(y).unwrap().first_key())
        });
        snapshot.levels[task.lower_level - 1].1 = new_lower_level_ssts;
        (snapshot, files_to_remove)
    }
}
