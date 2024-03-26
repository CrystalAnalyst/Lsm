use std::sync::Arc;

use crate::block::iterator::BlockIterator;

use super::SsTable;

// An iterator over the contents of an SSTable
pub struct SsTableIterator {
    table: Arc<SsTable>,
    block_iter: BlockIterator,
    block_idx: usize,
}
