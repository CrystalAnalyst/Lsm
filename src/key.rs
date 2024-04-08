use bytes::Bytes;
use std::fmt::Debug;

// Some Constants used for TimeStamp Management.
pub const TS_MAX: u64 = std::u64::MAX;
pub const TS_MIN: u64 = std::u64::MIN;
pub const TS_RANGE_BEGIN: u64 = std::u64::MAX;
pub const TS_RANGE_END: u64 = std::u64::MIN;

// define the Key.
pub struct Key<T: AsRef<[u8]>>(T, u64);

/// Impl necessary methods for Key<T>
impl<T: AsRef<[u8]>> Key<T> {
    pub fn into_inner(self) -> T {
        self.0
    }

    pub fn key_len(&self) -> usize {
        self.0.as_ref().len()
    }

    pub fn raw_len(&self) -> usize {
        self.key_len() + std::mem::size_of::<u64>()
    }

    pub fn is_empty(&self) -> bool {
        self.0.as_ref().is_empty()
    }
}

/*----------Impl Trait for Key<T>--------------*/
impl<T: AsRef<[u8]> + Clone> Clone for Key<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone(), self.1)
    }
}

impl<T: AsRef<[u8]> + Copy> Copy for Key<T> {}

impl<T: AsRef<[u8]> + Debug> Debug for Key<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<T: AsRef<[u8]> + Default> Default for Key<T> {
    fn default() -> Self {
        Self(T::default(), 0)
    }
}

impl<T: AsRef<[u8]> + PartialEq> PartialEq for Key<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl<T: AsRef<[u8]> + Eq> Eq for Key<T> {}

impl<T: AsRef<[u8]> + PartialOrd> PartialOrd for Key<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl<T: AsRef<[u8]> + Ord> Ord for Key<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

// set alias for KeySlice(borrowing)
pub type KeySlice<'a> = Key<&'a [u8]>;
impl<'a> Key<&'a [u8]> {
    // Constructor
    pub fn from_slice(slice: &'a [u8], ts: u64) -> Self {
        Self(slice, ts)
    }

    // Accessors
    pub fn key_ref(self) -> &'a [u8] {
        self.0
    }

    pub fn ts(&self) -> u64 {
        self.1
    }

    // Converters : converts from KeySlice to KeyVec
    pub fn to_key_vec(self) -> KeyVec {
        Key(self.0.to_vec(), self.1)
    }
}

/// set alias for KeyVec(Ownership)
pub type KeyVec = Key<Vec<u8>>;
impl Key<Vec<u8>> {
    // Constructors
    pub fn new() -> Self {
        Self(Vec::new(), 0)
    }

    pub fn from_vec_with_ts(key: Vec<u8>, ts: u64) -> Self {
        Self(key, ts)
    }

    // Accessors
    pub fn key_ref(&self) -> &[u8] {
        self.0.as_ref()
    }

    pub fn ts(&self) -> u64 {
        self.1
    }

    // Converters
    /// converts from KeyVec to KeySlice
    pub fn as_key_slice(&self) -> KeySlice {
        Key(self.0.as_slice(), self.1)
    }
    /// converts from KeyVec to KeyBytes
    pub fn into_key_bytes(self) -> KeyBytes {
        Key(self.0.into(), self.1)
    }

    // Modificators (mutate the inner data-structure)
    pub fn append(&mut self, data: &[u8]) {
        self.0.extend(data)
    }

    pub fn clear(&mut self) {
        self.0.clear()
    }

    pub fn set_from_slice(&mut self, key_slice: KeySlice) {
        self.0.clear();
        self.0.extend(key_slice.0);
    }

    pub fn set_ts(&mut self, ts: u64) {
        self.1 = ts
    }
}

// set alias for KeyBytes (Byte buffer).
pub type KeyBytes = Key<Bytes>;
impl Key<Bytes> {
    // Constructor
    pub fn new() -> Self {
        Self(Bytes::new(), 0)
    }

    pub fn from_bytes_with_ts(bytes: Bytes, ts: u64) -> KeyBytes {
        Key(bytes, ts)
    }

    // Accessor
    pub fn key_ref(&self) -> &[u8] {
        self.0.as_ref()
    }

    pub fn ts(&self) -> u64 {
        self.1
    }

    // Convertor
    pub fn as_key_slice(&self) -> KeySlice {
        Key(&self.0, self.1)
    }
}
