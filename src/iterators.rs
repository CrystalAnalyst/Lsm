pub mod concat_iterator;
pub mod merge_iterator;
pub mod two_merge_iterator;

pub trait StorageIterator {
    // 'a means that the keys may have a Lifetime ited to the iterator itself.
    type KeyType<'a>: PartialEq + Eq + PartialOrd + Ord
    where
        Self: 'a;

    /// get the current value.
    fn value(&self) -> &[u8];
    /// get the current key
    fn key(&self) -> Self::KeyType<'_>;
    /// check if the current iterator is valid.
    fn is_valid(&self) -> bool;
    /// move to the next Position
    fn next(&mut self) -> anyhow::Result<()>;
    /// Number of underlying Active sub-Iterators for this Iterator
    fn number_of_iterators(&self) -> usize {
        1
    }
}
