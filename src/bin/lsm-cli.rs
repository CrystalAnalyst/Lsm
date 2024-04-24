#![allow(unused)]
#![allow(dead_code)]

use anyhow::Result;
use bytes::Bytes;
use lsm::iterators::StorageIterator;
use lsm::key::KeySlice;
use lsm::lsm_storage::MiniLsm;
use rustyline::DefaultEditor;
use std::fmt::Write;
use std::sync::Arc;
mod wrapper;

/*
    基本的API: put, delete, get, scan
    其它API: Init用于初始化(往LsmTree中填充一部分数据以操作), Flush, Compact, Dump和退出命令
*/
#[derive(Debug)]
enum Command {
    Init {
        begin: u64,
        end: u64,
    },
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
    Flush,
    Compact,
    Dump,
    Quit,
    Close,
}

impl Command {
    /// 使用nom包对UserInput进行解析, 参数化为命令。
    pub fn parse(input: &str) -> Result<Self> {
        use nom::branch::*;
        use nom::bytes::complete::*;
        use nom::character::complete::*;
        use nom::combinator::*;
        use nom::sequence::*;

        let uint = |i| {
            map_res(digit1::<&str, nom::error::Error<_>>, |s: &str| {
                s.parse()
                    .map_err(|_| nom::error::Error::new(s, nom::error::ErrorKind::Digit))
            })(i)
        };

        let string = |i| {
            map(take_till1(|c: char| c.is_whitespace()), |s: &str| {
                s.to_string()
            })(i)
        };

        let init = |i| {
            map(
                tuple((tag_no_case("init"), space1, uint, space1, uint)),
                |(_, _, begin, _, end)| Command::Init { begin, end },
            )(i)
        };

        let put = |i| {
            map(
                tuple((tag_no_case("put"), space1, string, space1, string)),
                |(_, _, key, _, value)| Command::Put { key, value },
            )(i)
        };

        let del = |i| {
            map(
                tuple((tag_no_case("del"), space1, string)),
                |(_, _, key)| Command::Del { key },
            )(i)
        };

        let get = |i| {
            map(
                tuple((tag_no_case("get"), space1, string)),
                |(_, _, key)| Command::Get { key },
            )(i)
        };

        let scan = |i| {
            map(
                tuple((
                    tag_no_case("scan"),
                    opt(tuple((space1, string, space1, string))),
                )),
                |(_, opt_args)| {
                    let (begin, end) = opt_args.map_or((None, None), |(_, lower, _, upper)| {
                        (Some(lower), Some(upper))
                    });
                    Command::Scan {
                        lower: begin,
                        upper: end,
                    }
                },
            )(i)
        };

        let command = |i| {
            alt((
                init,
                put,
                del,
                get,
                scan,
                map(tag_no_case("flush"), |_| Command::Flush),
                map(tag_no_case("compact"), |_| Command::Compact),
                map(tag_no_case("dump"), |_| Command::Dump),
                map(tag_no_case("quit"), |_| Command::Quit),
                map(tag_no_case("close"), |_| Command::Close),
            ))(i)
        };

        command(input)
            .map(|(_, c)| c)
            .map_err(|e| anyhow::anyhow!("{}", e))
    }
}

/// 以后写命令行工具, 首选Repl
/// Read，读取用户输入 -> Eval, 执行输入内容(放在handler里面)
/// Print 打印输出结果 -> Loop, 不断循环以上步骤
pub struct Repl {
    prompt: String,
    editor: DefaultEditor,
    handler: ReplHandler,
}

impl Repl {
    pub fn run(mut self) -> Result<()> {
        loop {
            // 读取一行
            let input = self.editor.readline(&self.prompt)?;
            // 对这行进行非空检验
            if input.trim().is_empty() {
                continue;
            }
            // 把Input解析成固定格式的命令
            let command = Command::parse(&input)?;
            // 调用.handle()方法进行处理. repeat
            self.handler.handle(&command);
        }
    }
}

struct ReplHandler {
    epoch: u64,
    lsm: Arc<MiniLsm>,
}

impl ReplHandler {
    /// 根据传入进来的不同命令, 调用lsm树的不同函数,
    /// 并将将处理结果返回.
    fn handle(&mut self, command: &Command) -> Result<()> {
        match command {
            Command::Init { begin, end } => {
                assert!(*begin <= *end);

                let mut key_format = String::new();
                let mut value_format = String::new();
                write!(&mut key_format, "{}", "{}").unwrap();
                write!(&mut value_format, "value{}@{}", "{}", self.epoch).unwrap();

                let mut success_count = 0;
                for i in *begin..=*end {
                    let key = format!("{}", i);
                    let value = format!("value{}@{}", i, self.epoch);
                    match self.lsm.put(key.as_bytes(), value.as_bytes()) {
                        Ok(()) => {
                            success_count += 1;
                        }
                        Err(e) => {
                            println!("Error inserting key {}: {:?}", key, e);
                        }
                    }
                }
                println!("{} values filled with epoch {}", success_count, self.epoch);
            }

            Command::Put { key, value } => {
                self.lsm.put(key.as_bytes(), value.as_bytes())?;
                println!("Insert a new Key-value pair: {}—{}", key, value);
            }

            Command::Del { key } => {
                self.lsm.del(key.as_bytes())?;
                println!("{} deleted", key);
            }

            Command::Get { key } => {
                if let Some(value) = self.lsm.get(key.as_bytes())? {
                    println!("{}={:?}", key, value);
                } else {
                    println!("{} not exist", key);
                }
            }
            Command::Scan { lower, upper } => match (upper, lower) {
                (None, None) => {
                    let mut iter = self
                        .lsm
                        .scan(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded)?;
                    let mut cnt = 0;
                    while iter.is_valid() {
                        println!(
                            "{:?}={:?}",
                            Bytes::copy_from_slice(iter.key()),
                            Bytes::copy_from_slice(iter.value()),
                        );
                        iter.next()?;
                        cnt += 1;
                    }
                    println!();
                    println!("{} keys scanned", cnt);
                }
                (Some(begin), Some(end)) => {
                    let mut iter = self.lsm.scan(
                        std::ops::Bound::Included(begin.as_bytes()),
                        std::ops::Bound::Included(end.as_bytes()),
                    )?;
                    let mut cnt = 0;
                    while iter.is_valid() {
                        println!(
                            "{:?}={:?}",
                            Bytes::copy_from_slice(iter.key()),
                            Bytes::copy_from_slice(iter.value()),
                        );
                        iter.next()?;
                        cnt += 1;
                    }
                    println!();
                    println!("{} keys scanned", cnt);
                }
                _ => {
                    println!("invalid command");
                }
            },
            _ => {}
        };

        self.epoch += 1;
        Ok(())
    }
}

fn main() {
    todo!()
}
