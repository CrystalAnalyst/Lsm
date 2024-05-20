use std::ops::Bound;

use bytes::Bytes;
use tempfile::tempdir;

use crate::{
    compact::CompactionOptions,
    key::KeySlice,
    lsm_storage::{LsmStorageOptions, MiniLsm},
    table::SsTableBuilder,
    tests::harness::check_lsm_iter_result_by_key,
};

/// Testing MVCC of MemTables.
/// Initial Data Insertion: Inserts key-value pairs and takes snapshots to simulate different transaction states.
/// Snapshot Validation: Asserts the state of data at different points in time,
/// ensuring that each snapshot reflects the correct values and deletions.
/// Memtable Freezing and Additional Data Insertion:
/// Freezes the memtable and repeats the insertion and snapshot process to validate MVCC consistency across frozen and new memtables.
#[test]
fn test_task2_memtable_mvcc() {
    let dir = tempdir().unwrap();
    let mut options = LsmStorageOptions::default_for_week2_test(CompactionOptions::NoCompaction);
    options.enable_wal = true;
    let storage = MiniLsm::open(&dir, options.clone()).unwrap();
    storage.put(b"a", b"1").unwrap();
    storage.put(b"b", b"1").unwrap();
    let snapshot1 = storage.new_txn().unwrap();
    storage.put(b"a", b"2").unwrap();
    let snapshot2 = storage.new_txn().unwrap();
    storage.delete(b"b").unwrap();
    storage.put(b"c", b"1").unwrap();
    let snapshot3 = storage.new_txn().unwrap();
    assert_eq!(snapshot1.get(b"a").unwrap(), Some(Bytes::from_static(b"1")));
    assert_eq!(snapshot1.get(b"b").unwrap(), Some(Bytes::from_static(b"1")));
    assert_eq!(snapshot1.get(b"c").unwrap(), None);
    check_lsm_iter_result_by_key(
        &mut snapshot1.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
        vec![
            (Bytes::from("a"), Bytes::from("1")),
            (Bytes::from("b"), Bytes::from("1")),
        ],
    );
    assert_eq!(snapshot2.get(b"a").unwrap(), Some(Bytes::from_static(b"2")));
    assert_eq!(snapshot2.get(b"b").unwrap(), Some(Bytes::from_static(b"1")));
    assert_eq!(snapshot2.get(b"c").unwrap(), None);
    check_lsm_iter_result_by_key(
        &mut snapshot2.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
        vec![
            (Bytes::from("a"), Bytes::from("2")),
            (Bytes::from("b"), Bytes::from("1")),
        ],
    );
    assert_eq!(snapshot3.get(b"a").unwrap(), Some(Bytes::from_static(b"2")));
    assert_eq!(snapshot3.get(b"b").unwrap(), None);
    assert_eq!(snapshot3.get(b"c").unwrap(), Some(Bytes::from_static(b"1")));
    check_lsm_iter_result_by_key(
        &mut snapshot3.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
        vec![
            (Bytes::from("a"), Bytes::from("2")),
            (Bytes::from("c"), Bytes::from("1")),
        ],
    );
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.put(b"a", b"3").unwrap();
    storage.put(b"b", b"3").unwrap();
    let snapshot4 = storage.new_txn().unwrap();
    storage.put(b"a", b"4").unwrap();
    let snapshot5 = storage.new_txn().unwrap();
    storage.delete(b"b").unwrap();
    storage.put(b"c", b"5").unwrap();
    let snapshot6 = storage.new_txn().unwrap();
    assert_eq!(snapshot1.get(b"a").unwrap(), Some(Bytes::from_static(b"1")));
    assert_eq!(snapshot1.get(b"b").unwrap(), Some(Bytes::from_static(b"1")));
    assert_eq!(snapshot1.get(b"c").unwrap(), None);
    check_lsm_iter_result_by_key(
        &mut snapshot1.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
        vec![
            (Bytes::from("a"), Bytes::from("1")),
            (Bytes::from("b"), Bytes::from("1")),
        ],
    );
    assert_eq!(snapshot2.get(b"a").unwrap(), Some(Bytes::from_static(b"2")));
    assert_eq!(snapshot2.get(b"b").unwrap(), Some(Bytes::from_static(b"1")));
    assert_eq!(snapshot2.get(b"c").unwrap(), None);
    check_lsm_iter_result_by_key(
        &mut snapshot2.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
        vec![
            (Bytes::from("a"), Bytes::from("2")),
            (Bytes::from("b"), Bytes::from("1")),
        ],
    );
    assert_eq!(snapshot3.get(b"a").unwrap(), Some(Bytes::from_static(b"2")));
    assert_eq!(snapshot3.get(b"b").unwrap(), None);
    assert_eq!(snapshot3.get(b"c").unwrap(), Some(Bytes::from_static(b"1")));
    check_lsm_iter_result_by_key(
        &mut snapshot3.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
        vec![
            (Bytes::from("a"), Bytes::from("2")),
            (Bytes::from("c"), Bytes::from("1")),
        ],
    );
    assert_eq!(snapshot4.get(b"a").unwrap(), Some(Bytes::from_static(b"3")));
    assert_eq!(snapshot4.get(b"b").unwrap(), Some(Bytes::from_static(b"3")));
    assert_eq!(snapshot4.get(b"c").unwrap(), Some(Bytes::from_static(b"1")));
    check_lsm_iter_result_by_key(
        &mut snapshot4.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
        vec![
            (Bytes::from("a"), Bytes::from("3")),
            (Bytes::from("b"), Bytes::from("3")),
            (Bytes::from("c"), Bytes::from("1")),
        ],
    );
    assert_eq!(snapshot5.get(b"a").unwrap(), Some(Bytes::from_static(b"4")));
    assert_eq!(snapshot5.get(b"b").unwrap(), Some(Bytes::from_static(b"3")));
    assert_eq!(snapshot5.get(b"c").unwrap(), Some(Bytes::from_static(b"1")));
    check_lsm_iter_result_by_key(
        &mut snapshot5.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
        vec![
            (Bytes::from("a"), Bytes::from("4")),
            (Bytes::from("b"), Bytes::from("3")),
            (Bytes::from("c"), Bytes::from("1")),
        ],
    );
    assert_eq!(snapshot6.get(b"a").unwrap(), Some(Bytes::from_static(b"4")));
    assert_eq!(snapshot6.get(b"b").unwrap(), None);
    assert_eq!(snapshot6.get(b"c").unwrap(), Some(Bytes::from_static(b"5")));
    check_lsm_iter_result_by_key(
        &mut snapshot6.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
        vec![
            (Bytes::from("a"), Bytes::from("4")),
            (Bytes::from("c"), Bytes::from("5")),
        ],
    );
}

/// Testing: MVCC functionality with an iterator over LSM storage:
/// 1.Inserts key-value pairs and takes snapshots to simulate different transaction states.
/// 2.Forces a flush to move data from memtable to SSTables.
/// 3.Asserts data correctness for each snapshot and uses iterators
/// to validate the range of data returned, ensuring MVCC integrity across flushed SSTables.
#[test]
fn test_task2_lsm_iterator_mvcc() {
    let dir = tempdir().unwrap();
    let mut options = LsmStorageOptions::default_for_week2_test(CompactionOptions::NoCompaction);
    options.enable_wal = true;
    let storage = MiniLsm::open(&dir, options.clone()).unwrap();
    storage.put(b"a", b"1").unwrap();
    storage.put(b"b", b"1").unwrap();
    let snapshot1 = storage.new_txn().unwrap();
    storage.put(b"a", b"2").unwrap();
    let snapshot2 = storage.new_txn().unwrap();
    storage.delete(b"b").unwrap();
    storage.put(b"c", b"1").unwrap();
    let snapshot3 = storage.new_txn().unwrap();
    storage.force_flush().unwrap();
    assert_eq!(snapshot1.get(b"a").unwrap(), Some(Bytes::from_static(b"1")));
    assert_eq!(snapshot1.get(b"b").unwrap(), Some(Bytes::from_static(b"1")));
    assert_eq!(snapshot1.get(b"c").unwrap(), None);
    check_lsm_iter_result_by_key(
        &mut snapshot1.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
        vec![
            (Bytes::from("a"), Bytes::from("1")),
            (Bytes::from("b"), Bytes::from("1")),
        ],
    );
    assert_eq!(snapshot2.get(b"a").unwrap(), Some(Bytes::from_static(b"2")));
    assert_eq!(snapshot2.get(b"b").unwrap(), Some(Bytes::from_static(b"1")));
    assert_eq!(snapshot2.get(b"c").unwrap(), None);
    check_lsm_iter_result_by_key(
        &mut snapshot2.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
        vec![
            (Bytes::from("a"), Bytes::from("2")),
            (Bytes::from("b"), Bytes::from("1")),
        ],
    );
    assert_eq!(snapshot3.get(b"a").unwrap(), Some(Bytes::from_static(b"2")));
    assert_eq!(snapshot3.get(b"b").unwrap(), None);
    assert_eq!(snapshot3.get(b"c").unwrap(), Some(Bytes::from_static(b"1")));
    check_lsm_iter_result_by_key(
        &mut snapshot3.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
        vec![
            (Bytes::from("a"), Bytes::from("2")),
            (Bytes::from("c"), Bytes::from("1")),
        ],
    );
    storage.put(b"a", b"3").unwrap();
    storage.put(b"b", b"3").unwrap();
    let snapshot4 = storage.new_txn().unwrap();
    storage.put(b"a", b"4").unwrap();
    let snapshot5 = storage.new_txn().unwrap();
    storage.delete(b"b").unwrap();
    storage.put(b"c", b"5").unwrap();
    let snapshot6 = storage.new_txn().unwrap();
    storage.force_flush().unwrap();
    assert_eq!(snapshot1.get(b"a").unwrap(), Some(Bytes::from_static(b"1")));
    assert_eq!(snapshot1.get(b"b").unwrap(), Some(Bytes::from_static(b"1")));
    assert_eq!(snapshot1.get(b"c").unwrap(), None);
    check_lsm_iter_result_by_key(
        &mut snapshot1.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
        vec![
            (Bytes::from("a"), Bytes::from("1")),
            (Bytes::from("b"), Bytes::from("1")),
        ],
    );
    assert_eq!(snapshot2.get(b"a").unwrap(), Some(Bytes::from_static(b"2")));
    assert_eq!(snapshot2.get(b"b").unwrap(), Some(Bytes::from_static(b"1")));
    assert_eq!(snapshot2.get(b"c").unwrap(), None);
    check_lsm_iter_result_by_key(
        &mut snapshot2.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
        vec![
            (Bytes::from("a"), Bytes::from("2")),
            (Bytes::from("b"), Bytes::from("1")),
        ],
    );
    assert_eq!(snapshot3.get(b"a").unwrap(), Some(Bytes::from_static(b"2")));
    assert_eq!(snapshot3.get(b"b").unwrap(), None);
    assert_eq!(snapshot3.get(b"c").unwrap(), Some(Bytes::from_static(b"1")));
    check_lsm_iter_result_by_key(
        &mut snapshot3.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
        vec![
            (Bytes::from("a"), Bytes::from("2")),
            (Bytes::from("c"), Bytes::from("1")),
        ],
    );
    assert_eq!(snapshot4.get(b"a").unwrap(), Some(Bytes::from_static(b"3")));
    assert_eq!(snapshot4.get(b"b").unwrap(), Some(Bytes::from_static(b"3")));
    assert_eq!(snapshot4.get(b"c").unwrap(), Some(Bytes::from_static(b"1")));
    check_lsm_iter_result_by_key(
        &mut snapshot4.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
        vec![
            (Bytes::from("a"), Bytes::from("3")),
            (Bytes::from("b"), Bytes::from("3")),
            (Bytes::from("c"), Bytes::from("1")),
        ],
    );
    assert_eq!(snapshot5.get(b"a").unwrap(), Some(Bytes::from_static(b"4")));
    assert_eq!(snapshot5.get(b"b").unwrap(), Some(Bytes::from_static(b"3")));
    assert_eq!(snapshot5.get(b"c").unwrap(), Some(Bytes::from_static(b"1")));
    check_lsm_iter_result_by_key(
        &mut snapshot5.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
        vec![
            (Bytes::from("a"), Bytes::from("4")),
            (Bytes::from("b"), Bytes::from("3")),
            (Bytes::from("c"), Bytes::from("1")),
        ],
    );
    assert_eq!(snapshot6.get(b"a").unwrap(), Some(Bytes::from_static(b"4")));
    assert_eq!(snapshot6.get(b"b").unwrap(), None);
    assert_eq!(snapshot6.get(b"c").unwrap(), Some(Bytes::from_static(b"5")));
    check_lsm_iter_result_by_key(
        &mut snapshot6.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
        vec![
            (Bytes::from("a"), Bytes::from("4")),
            (Bytes::from("c"), Bytes::from("5")),
        ],
    );
    check_lsm_iter_result_by_key(
        &mut snapshot6
            .scan(Bound::Included(b"a"), Bound::Included(b"a"))
            .unwrap(),
        vec![(Bytes::from("a"), Bytes::from("4"))],
    );
    check_lsm_iter_result_by_key(
        &mut snapshot6
            .scan(Bound::Excluded(b"a"), Bound::Excluded(b"c"))
            .unwrap(),
        vec![],
    );
}

/// Testing: timestamped keys in SSTables.
/// step1.Constructs an SSTable with keys having associated timestamps.
/// step2.Verifies that the SSTable correctly identifies the highest timestamp among the entries.
#[test]
fn test_task3_sst_ts() {
    let mut builder = SsTableBuilder::new(16);
    builder.add(KeySlice::for_testing_from_slice_with_ts(b"11", 1), b"11");
    builder.add(KeySlice::for_testing_from_slice_with_ts(b"22", 2), b"22");
    builder.add(KeySlice::for_testing_from_slice_with_ts(b"33", 3), b"11");
    builder.add(KeySlice::for_testing_from_slice_with_ts(b"44", 4), b"22");
    builder.add(KeySlice::for_testing_from_slice_with_ts(b"55", 5), b"11");
    builder.add(KeySlice::for_testing_from_slice_with_ts(b"66", 6), b"22");
    let dir = tempdir().unwrap();
    let sst = builder.build_for_test(dir.path().join("1.sst")).unwrap();
    assert_eq!(sst.max_ts(), 6);
}
