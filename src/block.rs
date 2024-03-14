#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(unused_mut)]
#![allow(unused_imports)]

use bytes::{Buf, BufMut, Bytes};
pub mod iterator;

pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();

pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    pub fn encode(&self) -> Bytes {
        // add data itself.
        let mut buf = self.data.clone();
        let offsets_len = self.offsets.len();
        // add offset one by one.
        for offset in &self.offsets {
            buf.put_u16(*offset);
        }
        // add offset_len
        buf.put_u16(offsets_len as u16);
        // Now buf eqauls: data + offset + #offset(#Elements)
        buf.into()
    }

    pub fn decode(data: &[u8]) -> Self {
        // the alter-process of encode.
        // 1. get the number of elements(pairs) in the block
        let entry_offsets_len = (&data[data.len() - SIZEOF_U16..]).get_u16() as usize;
        let data_end = data.len() - SIZEOF_U16 - entry_offsets_len * SIZEOF_U16;
        let offsets_raw = &data[data_end..data.len() - SIZEOF_U16];
        // 2. get the offsets.
        let offsets = offsets_raw
            .chunks(SIZEOF_U16)
            .map(|mut x| x.get_u16())
            .collect();
        // 3. get the data (Kv pairs).
        let data = data[0..data_end].to_vec();
        Self { data, offsets }
    }
}
