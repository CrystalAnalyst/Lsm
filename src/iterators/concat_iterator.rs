use crate::table::iterator::SsTableIterator;
use crate::table::SsTable;

use std::sync::Arc;

/// Concatenate multiple iters ordered in key-order and their key ranges do no overlap.
/// iterators when
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_id: usize,
    sstables: Vec<Arc<SsTable>>,
}
