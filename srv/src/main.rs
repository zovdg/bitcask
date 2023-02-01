//! main
use std::io::{prelude::*, BufReader, Write};
use std::net::TcpStream;

use log::{error, info};
use store::storage::Storage;
use store::BitCask;

mod store;
mod utils;

use crate::store::{error::Result, OpenOptions};
use crate::utils::server::Server;
use crate::utils::threadpool::ThreadPool;

fn help(stream: &mut TcpStream) -> Result<()> {
    stream.write_all("help -- show help\\n".as_bytes())?;
    stream.write_all("get  -- get key value, by: <key>\\n".as_bytes())?;
    stream.write_all("set  -- set key value, by: <key> <value>\\n".as_bytes())?;
    stream.write_all("ls   -- list keys\\n".as_bytes())?;
    stream.write_all("rm   -- remove key value, by: <key>\\n".as_bytes())?;
    stream.write_all("exit -- exit command\\n".as_bytes())?;
    Ok(())
}

fn process_db_command(stream: &mut TcpStream, handle: &mut BitCask, cmds: &[&str]) -> Result<()> {
    match cmds[0] {
        "set" => {
            if cmds.len() != 3 {
                return Ok(());
            }
            let key = cmds[1].as_bytes().to_vec();
            let value = cmds[2].as_bytes().to_vec();
            handle.set(key, value)?;
        }
        "get" => {
            if cmds.len() != 2 {
                return Ok(());
            }
            let key = cmds[1].as_bytes().to_vec();
            match handle.get(&key)? {
                None => {}
                Some(v) => {
                    stream.write_all(&v)?;
                }
            };
        }
        "ls" => {
            let keys = handle.keys()?;
            for key in keys.iter() {
                stream.write_all(key)?;
                stream.write_all("\\n".as_bytes())?;
            }
        }
        "rm" => {
            if cmds.len() != 2 {
                return Ok(());
            }
            let key = cmds[1].as_bytes().to_vec();
            handle.delete(&key)?;
        }
        "merge" => {
            info!("Command to do compact ...");
            handle.compact()?;
        }
        &_ => todo!(),
    };

    Ok(())
}

fn empty() {}

fn handle_connection(mut stream: TcpStream, mut bitcask: BitCask) -> Result<()> {
    loop {
        let mut buf_reader = BufReader::new(&mut stream);
        let mut cmd = String::new();

        if buf_reader.read_line(&mut cmd)? == 0 {
            break;
        }

        if cmd.is_empty() {
            stream.write_all("\n".as_bytes())?;
            continue;
        }

        let cmd = cmd.strip_suffix('\n').unwrap();
        let cmds: Vec<&str> = cmd.split(' ').collect();

        match cmds[0] {
            "exit" => {
                break;
            }
            "help" => {
                help(&mut stream)?;
            }
            "set" | "get" | "ls" | "rm" | "merge" => {
                process_db_command(&mut stream, &mut bitcask, &cmds)?;
            }
            "" => empty(),
            _ => {
                stream.write_all(cmds.join("-").as_bytes())?;
            }
        };

        stream.write_all("\n".as_bytes())?;
    }

    Ok(())
}

fn main() -> Result<()> {
    // Init log config from env.
    env_logger::init();

    let addr = format!("{}:{}", "127.0.0.1", 7878);
    info!("Starting server at {addr} ...");

    let mut server = Server::new(addr);

    let pool = ThreadPool::new(4);

    let path = "database";
    let bitcask = OpenOptions::new()
        // .max_log_file_size(100)
        .open(path)
        .unwrap();

    server.running(move |stream: TcpStream| {
        info!(
            "Connection established! from {}",
            stream.peer_addr().unwrap()
        );

        let handle = bitcask.clone();

        pool.execute(move || {
            handle_connection(stream, handle).unwrap_or_else(|e| error!("{:?}", e));
        });
    })?;

    Ok(())
}
