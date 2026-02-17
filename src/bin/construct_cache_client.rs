use construct_cache::socket_interface::client_impl::ConstructCacheClient;
use construct_cache::socket_interface::socket_errors::SocketError;
use log4rs::{config::{Appender, Root}, encode::pattern::PatternEncoder};

use std::io::{self, Write};
use log::{error, info, LevelFilter};
use std::{env, process::exit};
use serde::Deserialize;
use tokio::fs;
use toml;


#[derive(Deserialize)]
struct Config {
    server_addr: ServerAddr,
    log_info: LogInfo
}

#[derive(Deserialize)]
struct ServerAddr {
    ip: String,
    port: u16
}

#[derive(Deserialize)]
struct LogInfo {
    log_file: String
}

async fn parse_config(path: &str) -> Result<Config, Box<dyn std::error::Error>> {
    let contents = match fs::read_to_string(path).await {
        Ok(c) => c,
        Err(e) => {return Err(Box::new(e)); }
    };
    let config: Config = toml::from_str(&contents).unwrap();
    Ok(config)
}


fn print_basic_help() {
    println!("\n=====How to use this=====");
    println!("c <key> <value>: Creates simple key value pair");
    println!("d <key>: Deletes key value pair");
    println!("g <key>: Gets the value of a key from the key value store");
    println!("b <backup_id>: Backs up the key value store with the specific ID");
    println!("r <backup_id>: Restores the key values store from a specified backup ID");
    println!("p <message>: Pings the key value store with a message");
    println!("u <key> <value>: Updates the key value store with new value");
    println!("x: Exits the client");
    println!("=========================\n");
}

fn setup_logging(path: &str) {
    let log_level = LevelFilter::Trace;
    let file = log4rs::append::file::FileAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{d(%Y-%m-%d %H:%M:%S)} [{l}] {m}{n}")))
        .build(&path)
        .expect("Failed to create log file!");

    let config = log4rs::config::Config::builder()
        .appender(Appender::builder().build("file", Box::new(file)))
        .build(
            Root::builder()
                .appender("file")
                .build(log_level)
        ).unwrap();
        log4rs::init_config(config).unwrap();
}

#[tokio::main]
async fn main() -> Result<(), SocketError> {
    env::set_var("RUST_LOG", "trace");
    let prompt_prefix = ">> ";
    let mut input = String::new();
    let mut exit_loop = false;
    let config_loc = "client_config.toml";
    let config = match parse_config(config_loc).await {
        Ok(c) => {
            info!("Successfully read config file: {}", config_loc);
            c
        },
        Err(e) => {
            error!("Got error: {:?} trying to read config file {}", e, config_loc);
            exit(1);
        }
    };
    let addr = config.server_addr.ip;
    let port = config.server_addr.port;
    let log_file_loc = config.log_info.log_file;
    setup_logging(&log_file_loc);
    let connect_addr = format!("{}:{}", addr, port);
    let mut client = ConstructCacheClient::new(&connect_addr).await?;
    println!(
        "KV Store client!!\n--------\nSend x to exit, h for help\n-------\n");
    while !exit_loop {
        print!("{}", prompt_prefix);
        let _ = io::stdout().flush();
        input.clear();
        io::stdin().read_line(&mut input)
            .expect("Failed to read line");
        let ip = input.trim();
        if ip.is_empty() {
            continue;
        }
        let control_char = ip.chars().nth(0).unwrap();
        let mut skip_input = false;
        match control_char {
            'x' => {
                exit_loop = true;
                skip_input = true;
            },
            'c' => {
                let mut split = ip.split(' ');
                split.next();
                let key: &str;
                let val: &str;
                match split.next() {
                    None => { 
                        eprintln!("Expected key!");
                        continue;
                    },
                    Some(x) => { key = x; }
                }
                match split.next() {
                    None => {
                        eprintln!("Expected value!");
                        continue;
                    },
                    Some(x) => { val = x; }
                }
                client.send_create(key, val).await?;
            },
            'b' => {
                let mut split = ip.split(' ');
                split.next();
                let backup_id: &str;
                match split.next() {
                    None => {
                        eprintln!("Expected backup ID!");
                        continue;
                    }
                    Some(x) => {backup_id = x; }
                }
                client.send_backup(backup_id).await?;
            },
            'p' => {
                let mut split = ip.split(' ');
                split.next();
                let ping_msg: &str;
                match split.next() {
                    None => {
                        eprintln!("Expected message to ping!");
                        continue;
                    },
                    Some(x) => { ping_msg = x; }
                }
                client.send_ping(ping_msg).await?;
            },
            'r' => {
                let mut split = ip.split(' ');
                split.next();
                let backup_id: &str;
                match split.next() {
                    None => {
                        eprintln!("Expected backup ID!");
                        continue;
                    }
                    Some(x) => {backup_id = x; }
                }
                client.send_restore(backup_id).await?;
            }
            'g' => {
                let mut split = ip.split(' ');
                split.next();
                let read_key: &str;
                match split.next() {
                    None => {
                        eprintln!("Expected key to read!");
                        continue;
                    }
                    Some(x) => {read_key = x; }
                }
                client.send_read(read_key).await?;
            },
            'u' => {
                let mut split = ip.split(' ');
                split.next();
                let key: &str;
                let val: &str;
                match split.next() {
                    None => { 
                        eprintln!("Expected key!");
                        continue;
                    },
                    Some(x) => { key = x; }
                }
                match split.next() {
                    None => {
                        eprintln!("Expected value!");
                        continue;
                    },
                    Some(x) => { val = x; }
                }
                client.send_update(key, val).await?;
            },
            'd' => {
                let mut split = ip.split(' ');
                split.next();
                let key: &str;
                match split.next() {
                    None => {
                        eprintln!("Expected key to delete!");
                        continue;
                    },
                    Some(x) => { key = x; }
                }
                client.send_delete(key).await?;
            }
            'h' => {
                print_basic_help();
                skip_input = true;
            },
            _ => {
                eprintln!("Unexpected input: {:?}", ip);
                skip_input = true;
            }
        }
        if !skip_input {
            match client.receive_resp().await {
                Ok(s) => println!("<< {}", s),
                Err(e) => eprintln!("<! {}", e)
            }
        }
    }
    Ok(())
}
