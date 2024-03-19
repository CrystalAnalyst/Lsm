pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_precent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}
