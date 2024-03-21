#![allow(unused)]

use serde::{Deserialize, Serialize};

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
}
