// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use anyhow::{bail, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Implements a bloom filter
pub struct Bloom {
    /// data of filter in bits
    pub(crate) filter: Bytes,
    /// number of hash functions
    pub(crate) k: u8,
}

pub trait BitSlice {
    fn get_bit(&self, idx: usize) -> bool;
    fn bit_len(&self) -> usize;
}

pub trait BitSliceMut {
    fn set_bit(&mut self, idx: usize, val: bool);
}

impl<T: AsRef<[u8]>> BitSlice for T {
    fn get_bit(&self, idx: usize) -> bool {
        let pos = idx / 8;
        let offset = idx % 8;
        (self.as_ref()[pos] & (1 << offset)) != 0
    }

    fn bit_len(&self) -> usize {
        self.as_ref().len() * 8
    }
}

impl<T: AsMut<[u8]>> BitSliceMut for T {
    fn set_bit(&mut self, idx: usize, val: bool) {
        let pos = idx / 8;
        let offset = idx % 8;
        if val {
            self.as_mut()[pos] |= 1 << offset;
        } else {
            self.as_mut()[pos] &= !(1 << offset);
        }
    }
}

impl Bloom {
    /// Decode a bloom filter
    pub fn decode(buf: &[u8]) -> Result<Self> {
        let checksum = (&buf[buf.len() - 4..buf.len()]).get_u32();
        if checksum != crc32fast::hash(&buf[..buf.len() - 4]) {
            bail!("checksum mismatched for bloom filters");
        }
        let filter = &buf[..buf.len() - 5];
        let k = buf[buf.len() - 5];
        Ok(Self {
            filter: filter.to_vec().into(),
            k,
        })
    }

    /// Encode a bloom filter
    pub fn encode(&self, buf: &mut Vec<u8>) {
        let offset = buf.len();
        buf.extend(&self.filter);
        buf.put_u8(self.k);
        let checksum = crc32fast::hash(&buf[offset..]);
        buf.put_u32(checksum);
    }

    /// Get bloom filter bits per key from entries count and FPR
    pub fn bloom_bits_per_key(entries: usize, false_positive_rate: f64) -> usize {
        let size =
            -1.0 * (entries as f64) * false_positive_rate.ln() / std::f64::consts::LN_2.powi(2);
        let locs = (size / (entries as f64)).ceil();
        locs as usize
    }

    /// Build bloom filter from key hashes
    pub fn build_from_key_hashes(keys: &[u32], bits_per_key: usize) -> Self {
        let k = (bits_per_key as f64 * 0.69) as u32;
        let k = k.min(30).max(1);
        let nbits = (keys.len() * bits_per_key).max(64);
        let nbytes = (nbits + 7) / 8;
        let nbits = nbytes * 8;
        let mut filter = BytesMut::with_capacity(nbytes);
        filter.resize(nbytes, 0);
        for h in keys {
            let mut h = *h;
            let delta = (h >> 17) | (h << 15);
            for _ in 0..k {
                let bit_pos = (h as usize) % nbits;
                filter.set_bit(bit_pos, true);
                h = h.wrapping_add(delta);
            }
        }
        Self {
            filter: filter.freeze(),
            k: k as u8,
        }
    }

    /// Check if a bloom filter may contain some data
    pub fn may_contain(&self, mut h: u32) -> bool {
        if self.k > 30 {
            // potential new encoding for short bloom filters
            true
        } else {
            let nbits = self.filter.bit_len();
            let delta = (h >> 17) | (h << 15);
            for _ in 0..self.k {
                let bit_pos = h % (nbits as u32);
                if !self.filter.get_bit(bit_pos as usize) {
                    return false;
                }
                h = h.wrapping_add(delta);
            }
            true
        }
    }
}

#[cfg(test)]
mod tests {
    // Import the Bloom struct and other necessary items
    use super::super::*;

    // Define your unit tests within the tests module
    #[test]
    fn test_bloom_filter() {
        // Define some example data and parameters
        let keys = vec![123, 456, 789];
        let bits_per_key = 10;
        let false_positive_rate = 0.01;

        // Build a bloom filter from key hashes
        let bloom_filter = Bloom::build_from_key_hashes(&keys, bits_per_key);

        // Encode the bloom filter into a vector
        let mut encoded_bloom = Vec::new();
        bloom_filter.encode(&mut encoded_bloom);

        // Decode the encoded bloom filter
        let decoded_bloom = Bloom::decode(&encoded_bloom).unwrap();

        // Check if a hash may be contained in the bloom filter
        let hash_to_check = 123;
        let may_contain = decoded_bloom.may_contain(hash_to_check as u32);
        assert!(may_contain, "Bloom filter should contain the hash");

        // Get bloom filter bits per key
        let bits_per_key_calculated = Bloom::bloom_bits_per_key(keys.len(), false_positive_rate);
        assert_eq!(
            bits_per_key, bits_per_key_calculated,
            "Calculated bits per key should match expected value"
        );
    }

    // Define more unit tests as needed
}
