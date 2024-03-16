#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(unused_imports)]

use crate::key::{Key, KeySlice};
use anyhow::Result;

use super::StorageIterator;
use std::{
    cmp,
    collections::{binary_heap::PeekMut, BinaryHeap},
    fmt::Binary,
};

/// HeapWrapper wraps `an item from a storage iterator` along with its index.
/// usize : represents the index of the Item.
/// Box<I>: represents the `boxed storage iterator`.
struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

/// PartialOrd: allows comparing Instances of `HeapWrapper` for partial ordering.
impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    #[allow(clippy::non_canonical_partial_ord_impl)]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(&other.1.key()) {
            // smaller keys are of higher priority (min-heap).
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            // if the key is the same, compare the index (the insertion order).
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
        .map(|x| x.reverse())
    }
}

/// Ord: provides a total ordering for instances of `HeapWrapper`
/// Used when you need strict ordering of elements.
/// Ord is Necessary for types that implment `PartialOrd`.
impl<I: StorageIterator> Ord for HeapWrapper<I> {
    // here simply delegates to the `partial_cmp()` method.
    // just Unwrap the `Option` to get the Ordering.
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Eq: states that instances of `HeapWrapper` are equatable.
/// automatically impl when `PartialEq` is impl.
impl<I: StorageIterator> Eq for HeapWrapper<I> {}

/// PartialEq: allows comparing instances of `HeapWrapper` for equality.
impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    // delegates to `partial_cmp()` and check the result is `Ordering::Equal`.
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}
/// MergeIterator Merges multiple storage Iterators.
pub struct MergeIterator<I: StorageIterator> {
    // A binaryHeap of `HeapWrapper<I>` instances.
    // this heap maintains the iterators to be merged.
    iters: BinaryHeap<HeapWrapper<I>>,
    // an optional HeapWrapper<I> representing the current iterator.
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    /// a constructor method for creating a MergeIterator.
    /// takes a vector of boxed Storage iterators `iters`
    /// and return a new Instance of MergeIterator.
    pub fn create(iters: Vec<Box<I>>) -> Self {
        // if iter is empty, returns an empty `MergeIterator`.
        if iters.is_empty() {
            return Self {
                iters: BinaryHeap::new(),
                current: None,
            };
        }

        let mut heap = BinaryHeap::new();

        // If all iterators in iters are invalid.
        // select the last iterator as the current one.
        if iters.iter().all(|x| !x.is_valid()) {
            let mut iters = iters;
            return Self {
                iters: heap,
                current: Some(HeapWrapper(0, iters.pop().unwrap())),
            };
        }

        // iterators are valid, pushing them into the binary heap.
        for (idx, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                heap.push(HeapWrapper(idx, iter));
            }
        }

        // pop the top iterator from the heap and sets it as the current iterator.
        let current = heap.pop().unwrap();
        Self {
            iters: heap,
            current: Some(current),
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .map(|x| x.1.is_valid())
            .unwrap_or(false)
    }

    fn next(&mut self) -> Result<()> {
        // retrieves the current element.
        let current = self.current.as_mut().unwrap();
        // compares the `keys of current element` with `the keys at heap top`.
        while let Some(mut inner_iter) = self.iters.peek_mut() {
            debug_assert!(
                inner_iter.1.key() >= current.1.key(),
                "heap invariant violated"
            );
            if inner_iter.1.key() == current.1.key() {
                //case 1 : an error occurred when calling `next`.
                if let e @ Err(_) = inner_iter.1.next() {
                    PeekMut::pop(inner_iter);
                    return e;
                }
                //case 2: the iterator at the top is no longer valid.
                if !inner_iter.1.is_valid() {
                    PeekMut::pop(inner_iter);
                }
            } else {
                break;
            }
        }

        current.1.next()?;

        if !current.1.is_valid() {
            if let Some(iter) = self.iters.pop() {
                *current = iter;
            }
            return Ok(());
        }

        if let Some(mut inner_iter) = self.iters.peek_mut() {
            if *current < *inner_iter {
                std::mem::swap(&mut *inner_iter, current);
            }
        }
        Ok(())
    }

    fn number_of_iterators(&self) -> usize {
        // provides a count of all active iterators.
        // including those stored in the `BinaryHeap` and current Iterator.
        self.iters
            .iter()
            .map(|x| x.1.number_of_iterators())
            .sum::<usize>()
            + self
                .current
                .as_ref()
                .map(|x| x.1.number_of_iterators())
                .unwrap_or(0)
    }
}
