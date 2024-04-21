#![allow(unused)]
#![allow(dead_code)]

use lsm::key::KeySlice;
mod wrapper;

/*
    暂定支持put, delete, get, scan
*/

#[derive(Debug)]
enum Command {
    Put {
        key: String,
        value: String,
    },
    Del {
        key: String,
    },
    Get {
        key: String,
    },
    Scan {
        lower: Option<String>,
        upper: Option<String>,
    },
}

fn main() {
    todo!()
}
