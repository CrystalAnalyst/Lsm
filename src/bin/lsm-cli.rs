#![allow(unused)]
#![allow(dead_code)]

use anyhow::Result;
use lsm::key::KeySlice;
use lsm::lsm_storage::MiniLsm;
use rustyline::DefaultEditor;
use std::sync::Arc;
mod wrapper;

/*
    暂定支持put, delete, get, scan
    定义命令 -> 把UserInput解析成命令 -> 针对不同命令调用不同方法处理.
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

        let put = |i| {
            map(
                tuple((tag_no_case("put"), space1, uint, space1, uint)),
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
                    let (begin, end) = opt_args
                        .map_or((None, None), |(_, begin, _, end)| (Some(begin), Some(end)));
                    Command::Scan {
                        lower: begin,
                        upper: end,
                    }
                },
            )(i)
        };

        let command = |i| alt((put, del, get, scan))(i);

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
    pub fn handle(&self, command: &Command) {
        todo!()
    }
}

fn main() {
    todo!()
}
