pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub level_size_multiplier: usize,
}
