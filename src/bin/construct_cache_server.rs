use construct_cache::socket_interface::server_impl::ConstructCacheServer;
use log4rs::{
    config::{Appender, Root},
    encode::pattern::PatternEncoder,
};

use log::{error, info, trace, warn, LevelFilter};
use serde::Deserialize;
use std::io;
use std::{env, process::exit};
use tokio::fs;

#[derive(Deserialize)]
struct Config {
    net_config: NetConfig,
    log_info: LogInfo,
}

#[derive(Deserialize)]
struct NetConfig {
    ip: String,
    port: u16,
}

#[derive(Deserialize)]
struct LogInfo {
    log_file: String,
}

fn setup_logging(path: &str) {
    let log_level = LevelFilter::Trace;
    let file = log4rs::append::file::FileAppender::builder()
        .encoder(Box::new(PatternEncoder::new(
            "{d(%Y-%m-%d %H:%M:%S)} [{l}] {m}{n}",
        )))
        .build(path)
        .expect("Failed to create log file!");

    let config = log4rs::config::Config::builder()
        .appender(Appender::builder().build("file", Box::new(file)))
        .build(Root::builder().appender("file").build(log_level))
        .unwrap();
    log4rs::init_config(config).unwrap();
}

async fn parse_config(path: &str) -> Result<Config, Box<dyn std::error::Error>> {
    let contents = match fs::read_to_string(path).await {
        Ok(c) => c,
        Err(e) => {
            return Err(Box::new(e));
        }
    };
    let config: Config = toml::from_str(&contents).unwrap();
    Ok(config)
}

#[tokio::main]
async fn main() -> io::Result<()> {
    env::set_var("RUST_LOG", "trace");
    let config_loc = "server_config.toml";
    let config = match parse_config(config_loc).await {
        Ok(c) => {
            info!("Successfully read config file: {}", config_loc);
            c
        }
        Err(e) => {
            error!(
                "Got error: {:?} trying to read config file {}",
                e, config_loc
            );
            exit(1);
        }
    };
    let addr = config.net_config.ip;
    let port = config.net_config.port;
    let log_file = config.log_info.log_file;
    setup_logging(&log_file);
    let listen_addr = format!("{}:{}", addr, port);
    trace!("Hello, server!");
    let server = ConstructCacheServer::new(&listen_addr, "default", "");
    match server.main_loop().await {
        Ok(_) => {}
        Err(e) => {
            warn!("Got error {:?}", e)
        }
    };
    Ok(())
}
