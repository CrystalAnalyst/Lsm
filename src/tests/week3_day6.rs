use std::ops::Bound;

use bytes::Bytes;
use tempfile::tempdir;

use crate::{
    compact::CompactionOptions,
    iterators::StorageIterator,
    lsm_storage::{LsmStorageOptions, MiniLsm},
};

/// Intention: Ensure serializable isolation prevents conflicting writes:
/// Verify one transaction commits while another with conflicting data fails, ensuring data consistency.
#[test]
fn test_serializable_1() {
    let dir = tempdir().unwrap();
    let mut options = LsmStorageOptions::default_for_week2_test(CompactionOptions::NoCompaction);
    options.serializable = true;
    let storage = MiniLsm::open(&dir, options.clone()).unwrap();
    storage.put(b"key1", b"1").unwrap();
    storage.put(b"key2", b"2").unwrap();
    let txn1 = storage.new_txn().unwrap();
    let txn2 = storage.new_txn().unwrap();
    txn1.put(b"key1", &txn1.get(b"key2").unwrap().unwrap());
    txn2.put(b"key2", &txn2.get(b"key1").unwrap().unwrap());
    txn1.commit().unwrap();
    assert!(txn2.commit().is_err());
    drop(txn2);
    assert_eq!(storage.get(b"key1").unwrap(), Some(Bytes::from("2")));
    assert_eq!(storage.get(b"key2").unwrap(), Some(Bytes::from("2")));
}

/// Intention: Confirm independent transactions with serializable isolation:
/// Ensure the final value in storage reflects the last committed transaction.
#[test]
fn test_serializable_2() {
    let dir = tempdir().unwrap();
    let mut options = LsmStorageOptions::default_for_week2_test(CompactionOptions::NoCompaction);
    options.serializable = true;
    let storage = MiniLsm::open(&dir, options.clone()).unwrap();
    let txn1 = storage.new_txn().unwrap();
    let txn2 = storage.new_txn().unwrap();
    txn1.put(b"key1", b"1");
    txn2.put(b"key1", b"2");
    txn1.commit().unwrap();
    txn2.commit().unwrap();
    assert_eq!(storage.get(b"key1").unwrap(), Some(Bytes::from("2")));
}

/// test_serializable_3_ts_range
/// Intention: Validate serializable isolation with sequential transactions:
/// Ensure that updates reflect the latest committed transaction in a sequential manner.
#[test]
fn test_serializable_3_ts_range() {
    let dir = tempdir().unwrap();
    let mut options = LsmStorageOptions::default_for_week2_test(CompactionOptions::NoCompaction);
    options.serializable = true;
    let storage = MiniLsm::open(&dir, options.clone()).unwrap();
    storage.put(b"key1", b"1").unwrap();
    storage.put(b"key2", b"2").unwrap();
    let txn1 = storage.new_txn().unwrap();
    txn1.put(b"key1", &txn1.get(b"key2").unwrap().unwrap());
    txn1.commit().unwrap();
    let txn2 = storage.new_txn().unwrap();
    txn2.put(b"key2", &txn2.get(b"key1").unwrap().unwrap());
    txn2.commit().unwrap();
    drop(txn2);
    assert_eq!(storage.get(b"key1").unwrap(), Some(Bytes::from("2")));
    assert_eq!(storage.get(b"key2").unwrap(), Some(Bytes::from("2")));
}

/// Intention: Test scan operations under serializable isolation:
/// Ensure that committing a transaction after scanning fails if data has been modified by another committed transaction.
#[test]
fn test_serializable_4_scan() {
    let dir = tempdir().unwrap();
    let mut options = LsmStorageOptions::default_for_week2_test(CompactionOptions::NoCompaction);
    options.serializable = true;
    let storage = MiniLsm::open(&dir, options.clone()).unwrap();
    storage.put(b"key1", b"1").unwrap();
    storage.put(b"key2", b"2").unwrap();
    let txn1 = storage.new_txn().unwrap();
    let txn2 = storage.new_txn().unwrap();
    txn1.put(b"key1", &txn1.get(b"key2").unwrap().unwrap());
    txn1.commit().unwrap();
    let mut iter = txn2.scan(Bound::Unbounded, Bound::Unbounded).unwrap();
    while iter.is_valid() {
        iter.next().unwrap();
    }
    txn2.put(b"key2", b"1");
    assert!(txn2.commit().is_err());
    drop(txn2);
    assert_eq!(storage.get(b"key1").unwrap(), Some(Bytes::from("2")));
    assert_eq!(storage.get(b"key2").unwrap(), Some(Bytes::from("2")));
}

/// Intention: Verify read-only transaction handling under serializable isolation:
/// Ensure read-only transactions commit successfully without altering the data, preserving the state reflecting prior updates.
#[test]
fn test_serializable_5_read_only() {
    let dir = tempdir().unwrap();
    let mut options = LsmStorageOptions::default_for_week2_test(CompactionOptions::NoCompaction);
    options.serializable = true;
    let storage = MiniLsm::open(&dir, options.clone()).unwrap();
    storage.put(b"key1", b"1").unwrap();
    storage.put(b"key2", b"2").unwrap();
    let txn1 = storage.new_txn().unwrap();
    txn1.put(b"key1", &txn1.get(b"key2").unwrap().unwrap());
    txn1.commit().unwrap();
    let txn2 = storage.new_txn().unwrap();
    txn2.get(b"key1").unwrap().unwrap();
    let mut iter = txn2.scan(Bound::Unbounded, Bound::Unbounded).unwrap();
    while iter.is_valid() {
        iter.next().unwrap();
    }
    txn2.commit().unwrap();
    assert_eq!(storage.get(b"key1").unwrap(), Some(Bytes::from("2")));
    assert_eq!(storage.get(b"key2").unwrap(), Some(Bytes::from("2")));
}
