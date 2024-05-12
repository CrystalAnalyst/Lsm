use super::StorageIterator;
use anyhow::{Ok, Result};

/// merges two iterators of different types into one.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    // Determines whether to choose the key from iterator a or iterator b.
    // It compares the keys of both iterators and returns true if a should be chosen.
    choose_a: bool,
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    // constructor
    pub fn create(a: A, b: B) -> Result<Self> {
        let mut iter = Self {
            a,
            b,
            choose_a: false,
        };
        iter.skip_b()?;
        iter.choose_a = Self::choose_a(&iter.a, &iter.b);
        Ok(iter)
    }

    /// Determines whether to choose the key from iterator a or iterator b.
    /// It compares the keys of both iterators and returns true if a should be chosen.
    fn choose_a(a: &A, b: &B) -> bool {
        if !a.is_valid() {
            return false;
        }
        if !b.is_valid() {
            return false;
        }
        // If a.key() is smaller then return true
        // otherwise return false, means that a.key() is the larger one.
        a.key() < b.key()
    }

    /// Skips elements in iterator b if the current keys of both iterators are equal.
    fn skip_b(&mut self) -> Result<()> {
        if self.a.is_valid() && self.b.is_valid() && self.b.key() == self.a.key() {
            self.b.next()?;
        }
        Ok(())
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn is_valid(&self) -> bool {
        if self.choose_a {
            self.a.is_valid()
        } else {
            self.b.is_valid()
        }
    }

    fn key(&self) -> Self::KeyType<'_> {
        if self.choose_a {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.choose_a {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn next(&mut self) -> anyhow::Result<()> {
        if self.choose_a {
            self.a.next()?;
        } else {
            self.b.next()?;
        }
        self.skip_b()?;
        self.choose_a = Self::choose_a(&self.a, &self.b);
        Ok(())
    }

    fn number_of_iterators(&self) -> usize {
        self.a.number_of_iterators() + self.b.number_of_iterators()
    }
}
