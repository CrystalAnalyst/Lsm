use bytes::Bytes;
use tempfile::tempdir;

use crate::{
    compact::CompactionOptions,
    lsm_storage::{CompactionFilter, LsmStorageOptions, MiniLsm, WriteBatchRecord},
};

use super::harness::{check_iter_result_by_key, construct_merge_iterator_over_storage};

/*
Testing Intentions:Validate MVCC (Multi-Version Concurrency Control) with compaction and snapshot management:
    1.Write initial data into the storage and flush to ensure it persists.
    2.Create a snapshot to test the effect of further modifications on previously read data.
    3.Data Modification: Write a new batch of data, including updates and deletions, and force a flush to persist these changes.
    4.Compaction with Filter: Add a compaction filter to selectively compact data prefixed with "table2_". Force a full compaction to apply this filter.
    5.Verification Post-Compaction: Construct an iterator to verify that the expected state of the data includes both old and new versions for "table1_" and appropriate filtered results for "table2_".
    6.Snapshot Cleanup and Final Compaction: Drop the snapshot and force another full compaction to clean up obsolete versions, then verify the final state of the data to ensure only the latest versions for "table1_" remain and "table2_" entries are appropriately handled by the filter.
*/

#[test]
fn test_task3_mvcc_compaction() {
    let dir = tempdir().unwrap();
    let options = LsmStorageOptions::default_for_week2_test(CompactionOptions::NoCompaction);
    let storage = MiniLsm::open(&dir, options.clone()).unwrap();
    storage
        .write_batch(&[
            WriteBatchRecord::Put("table1_a", "1"),
            WriteBatchRecord::Put("table1_b", "1"),
            WriteBatchRecord::Put("table1_c", "1"),
            WriteBatchRecord::Put("table2_a", "1"),
            WriteBatchRecord::Put("table2_b", "1"),
            WriteBatchRecord::Put("table2_c", "1"),
        ])
        .unwrap();
    storage.force_flush().unwrap();
    let snapshot0 = storage.new_txn().unwrap();
    storage
        .write_batch(&[
            WriteBatchRecord::Put("table1_a", "2"),
            WriteBatchRecord::Del("table1_b"),
            WriteBatchRecord::Put("table1_c", "2"),
            WriteBatchRecord::Put("table2_a", "2"),
            WriteBatchRecord::Del("table2_b"),
            WriteBatchRecord::Put("table2_c", "2"),
        ])
        .unwrap();
    storage.force_flush().unwrap();
    storage.add_compaction_filter(CompactionFilter::Prefix(Bytes::from("table2_")));
    storage.force_full_compaction().unwrap();

    let mut iter = construct_merge_iterator_over_storage(&storage.inner.state.read());
    check_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from("table1_a"), Bytes::from("2")),
            (Bytes::from("table1_a"), Bytes::from("1")),
            (Bytes::from("table1_b"), Bytes::new()),
            (Bytes::from("table1_b"), Bytes::from("1")),
            (Bytes::from("table1_c"), Bytes::from("2")),
            (Bytes::from("table1_c"), Bytes::from("1")),
            (Bytes::from("table2_a"), Bytes::from("2")),
            (Bytes::from("table2_b"), Bytes::new()),
            (Bytes::from("table2_c"), Bytes::from("2")),
        ],
    );

    drop(snapshot0);

    storage.force_full_compaction().unwrap();

    let mut iter = construct_merge_iterator_over_storage(&storage.inner.state.read());
    check_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from("table1_a"), Bytes::from("2")),
            (Bytes::from("table1_c"), Bytes::from("2")),
        ],
    );
}
