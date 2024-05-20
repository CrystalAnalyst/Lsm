use bytes::Bytes;
use tempfile::tempdir;

use crate::{
    compact::CompactionOptions,
    lsm_storage::{LsmStorageOptions, MiniLsm, WriteBatchRecord},
    mvcc::watermark::Watermark,
};

use super::harness::{check_iter_result_by_key, construct_merge_iterator_over_storage};

/// Testing: validates the watermark mechanism in the MVCC system.
/// step1.Adds readers and checks if the watermark remains
///       at the first reader and the number of retained snapshots increases.
/// step2.Removes readers incrementally and checks if the
///       watermark correctly advances to the next reader and the number of retained snapshots decreases.
/// *:Handles the case of duplicate readers and ensures the watermark behaves correctly when duplicates are removed.
#[test]
fn test_task1_watermark() {
    let mut watermark = Watermark::new();
    watermark.add_reader(0);
    for i in 1..=1000 {
        watermark.add_reader(i);
        assert_eq!(watermark.watermark(), Some(0));
        assert_eq!(watermark.num_of_snapshots(), i as usize + 1);
    }
    let mut cnt = 1001;
    for i in 0..500 {
        watermark.remove_reader(i);
        assert_eq!(watermark.watermark(), Some(i + 1));
        cnt -= 1;
        assert_eq!(watermark.num_of_snapshots(), cnt);
    }
    for i in (501..=1000).rev() {
        watermark.remove_reader(i);
        assert_eq!(watermark.watermark(), Some(500));
        cnt -= 1;
        assert_eq!(watermark.num_of_snapshots(), cnt);
    }
    watermark.remove_reader(500);
    assert_eq!(watermark.watermark(), None);
    assert_eq!(watermark.num_of_snapshots(), 0);
    watermark.add_reader(2000);
    watermark.add_reader(2000);
    watermark.add_reader(2001);
    assert_eq!(watermark.num_of_snapshots(), 2);
    assert_eq!(watermark.watermark(), Some(2000));
    watermark.remove_reader(2000);
    assert_eq!(watermark.num_of_snapshots(), 2);
    assert_eq!(watermark.watermark(), Some(2000));
    watermark.remove_reader(2000);
    assert_eq!(watermark.num_of_snapshots(), 1);
    assert_eq!(watermark.watermark(), Some(2001));
}

/// Testing: MVCC watermark updates correctly with transaction snapshots.
/// Creates three transactions and inserts a key-value pair.
/// Checks if the watermark is correctly set to the earliest active transaction timestamp
/// after each snapshot is dropped, eventually aligning with the latest commit timestamp.
#[test]
fn test_task2_snapshot_watermark() {
    let dir = tempdir().unwrap();
    let options = LsmStorageOptions::default_for_week2_test(CompactionOptions::NoCompaction);
    let storage = MiniLsm::open(&dir, options.clone()).unwrap();
    let txn1 = storage.new_txn().unwrap();
    let txn2 = storage.new_txn().unwrap();
    storage.put(b"233", b"23333").unwrap();
    let txn3 = storage.new_txn().unwrap();
    assert_eq!(storage.inner.mvcc().watermark(), txn1.read_ts);
    drop(txn1);
    assert_eq!(storage.inner.mvcc().watermark(), txn2.read_ts);
    drop(txn2);
    assert_eq!(storage.inner.mvcc().watermark(), txn3.read_ts);
    drop(txn3);
    assert_eq!(
        storage.inner.mvcc().watermark(),
        storage.inner.mvcc().latest_commit_ts()
    );
}

/// Testing: examines the MVCC behavior during compaction processes.
/// Performs multiple write operations and creates snapshots to track different states.
/// Forces the LSM storage to flush and perform a full compaction.
/// Uses a merge iterator to validate the state of the storage at different stages,
/// ensuring that deleted entries are handled correctly and earlier versions of keys are properly maintained or removed.
#[test]
fn test_task3_mvcc_compaction() {
    let dir = tempdir().unwrap();
    let options = LsmStorageOptions::default_for_week2_test(CompactionOptions::NoCompaction);
    let storage = MiniLsm::open(&dir, options.clone()).unwrap();
    let snapshot0 = storage.new_txn().unwrap();
    storage
        .write_batch(&[
            WriteBatchRecord::Put(b"a", b"1"),
            WriteBatchRecord::Put(b"b", b"1"),
        ])
        .unwrap();
    let snapshot1 = storage.new_txn().unwrap();
    storage
        .write_batch(&[
            WriteBatchRecord::Put(b"a", b"2"),
            WriteBatchRecord::Put(b"d", b"2"),
        ])
        .unwrap();
    let snapshot2 = storage.new_txn().unwrap();
    storage
        .write_batch(&[
            WriteBatchRecord::Put(b"a", b"3"),
            WriteBatchRecord::Del(b"d"),
        ])
        .unwrap();
    let snapshot3 = storage.new_txn().unwrap();
    storage
        .write_batch(&[
            WriteBatchRecord::Put(b"c", b"4"),
            WriteBatchRecord::Del(b"a"),
        ])
        .unwrap();

    storage.force_flush().unwrap();
    storage.force_full_compaction().unwrap();

    let mut iter = construct_merge_iterator_over_storage(&storage.inner.state.read());
    check_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from("a"), Bytes::new()),
            (Bytes::from("a"), Bytes::from("3")),
            (Bytes::from("a"), Bytes::from("2")),
            (Bytes::from("a"), Bytes::from("1")),
            (Bytes::from("b"), Bytes::from("1")),
            (Bytes::from("c"), Bytes::from("4")),
            (Bytes::from("d"), Bytes::new()),
            (Bytes::from("d"), Bytes::from("2")),
        ],
    );

    drop(snapshot0);
    storage.force_full_compaction().unwrap();

    let mut iter = construct_merge_iterator_over_storage(&storage.inner.state.read());
    check_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from("a"), Bytes::new()),
            (Bytes::from("a"), Bytes::from("3")),
            (Bytes::from("a"), Bytes::from("2")),
            (Bytes::from("a"), Bytes::from("1")),
            (Bytes::from("b"), Bytes::from("1")),
            (Bytes::from("c"), Bytes::from("4")),
            (Bytes::from("d"), Bytes::new()),
            (Bytes::from("d"), Bytes::from("2")),
        ],
    );

    drop(snapshot1);
    storage.force_full_compaction().unwrap();

    let mut iter = construct_merge_iterator_over_storage(&storage.inner.state.read());
    check_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from("a"), Bytes::new()),
            (Bytes::from("a"), Bytes::from("3")),
            (Bytes::from("a"), Bytes::from("2")),
            (Bytes::from("b"), Bytes::from("1")),
            (Bytes::from("c"), Bytes::from("4")),
            (Bytes::from("d"), Bytes::new()),
            (Bytes::from("d"), Bytes::from("2")),
        ],
    );

    drop(snapshot2);
    storage.force_full_compaction().unwrap();

    let mut iter = construct_merge_iterator_over_storage(&storage.inner.state.read());
    check_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from("a"), Bytes::new()),
            (Bytes::from("a"), Bytes::from("3")),
            (Bytes::from("b"), Bytes::from("1")),
            (Bytes::from("c"), Bytes::from("4")),
        ],
    );

    drop(snapshot3);
    storage.force_full_compaction().unwrap();

    let mut iter = construct_merge_iterator_over_storage(&storage.inner.state.read());
    check_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from("b"), Bytes::from("1")),
            (Bytes::from("c"), Bytes::from("4")),
        ],
    );
}
