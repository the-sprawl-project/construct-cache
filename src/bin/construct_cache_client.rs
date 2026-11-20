use construct_cache::socket_interface::client_impl::ConstructCacheClient;
use construct_cache::socket_interface::socket_errors::SocketError;
use log4rs::{
    config::{Appender, Root},
    encode::pattern::PatternEncoder,
};

use log::{error, info, LevelFilter};
use serde::Deserialize;
use std::io::{self, Write};
use std::{env, process::exit};
use tokio::fs;

#[derive(Deserialize)]
struct Config {
    server_addr: ServerAddr,
    log_info: LogInfo,
}

#[derive(Deserialize)]
struct ServerAddr {
    ip: String,
    port: u16,
}

#[derive(Deserialize)]
struct LogInfo {
    log_file: String,
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

fn print_basic_help() {
    println!("\n=====How to use this=====");
    println!("c <key> <value>: Creates simple key value pair");
    println!("d <key>: Deletes key value pair");
    println!("g <key>: Gets the value of a key from the key value store");
    println!("b: Backs up the key value store (creates a checkpoint)");
    println!("r <backup_id>: Restores the key values store from a specified backup ID");
    println!("p <message>: Pings the key value store with a message");
    println!("u <key> <value>: Updates the key value store with new value");
    println!("x: Exits the client");
    println!("=========================\n");
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

fn resolve_port(arg: Option<String>, default_port: u16) -> Result<u16, String> {
    match arg {
        Some(p) => p
            .parse::<u16>()
            .map_err(|_| format!("Invalid port argument: {}", p)),
        None => Ok(default_port),
    }
}

fn parse_snapshot_id(arg: Option<&str>) -> Result<i64, &'static str> {
    match arg {
        None => Err("Expected backup ID!"),
        Some(x) => x.parse::<i64>().map_err(|_| "Backup ID must be an integer!"),
    }
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
        }
        Err(e) => {
            error!(
                "Got error: {:?} trying to read config file {}",
                e, config_loc
            );
            exit(1);
        }
    };
    let addr = config.server_addr.ip;
    let port = match resolve_port(env::args().nth(1), config.server_addr.port) {
        Ok(p) => p,
        Err(msg) => {
            eprintln!("{}", msg);
            exit(1);
        }
    };
    let log_file_loc = config.log_info.log_file;
    setup_logging(&log_file_loc);
    let connect_addr = format!("{}:{}", addr, port);
    let mut client = ConstructCacheClient::new(&connect_addr).await?;
    println!("KV Store client!!\n--------\nSend x to exit, h for help\n-------\n");
    while !exit_loop {
        print!("{}", prompt_prefix);
        let _ = io::stdout().flush();
        input.clear();
        io::stdin()
            .read_line(&mut input)
            .expect("Failed to read line");
        let ip = input.trim();
        if ip.is_empty() {
            continue;
        }
        let control_char = ip.chars().next().unwrap();
        let mut skip_input = false;
        match control_char {
            'x' => {
                exit_loop = true;
                skip_input = true;
            }
            'c' => {
                let mut split = ip.split(' ');
                split.next();
                let key = match split.next() {
                    None => {
                        eprintln!("Expected key!");
                        continue;
                    }
                    Some(x) => x,
                };
                let val = match split.next() {
                    None => {
                        eprintln!("Expected value!");
                        continue;
                    }
                    Some(x) => x,
                };
                client.send_create(key, val).await?;
            }
            'b' => {
                client.send_commit().await?;
            }
            'p' => {
                let mut split = ip.split(' ');
                split.next();
                let ping_msg = match split.next() {
                    None => {
                        eprintln!("Expected message to ping!");
                        continue;
                    }
                    Some(x) => x,
                };
                client.send_ping(ping_msg).await?;
            }
            'r' => {
                let mut split = ip.split(' ');
                split.next();
                let backup_id = match parse_snapshot_id(split.next()) {
                    Ok(id) => id,
                    Err(msg) => {
                        eprintln!("{}", msg);
                        continue;
                    }
                };
                client.send_rollback(backup_id).await?;
            }
            'g' => {
                let mut split = ip.split(' ');
                split.next();
                let read_key = match split.next() {
                    None => {
                        eprintln!("Expected key to read!");
                        continue;
                    }
                    Some(x) => x,
                };
                client.send_read(read_key).await?;
            }
            'u' => {
                let mut split = ip.split(' ');
                split.next();
                let key = match split.next() {
                    None => {
                        eprintln!("Expected key!");
                        continue;
                    }
                    Some(x) => x,
                };
                let val = match split.next() {
                    None => {
                        eprintln!("Expected value!");
                        continue;
                    }
                    Some(x) => x,
                };
                client.send_update(key, val).await?;
            }
            'd' => {
                let mut split = ip.split(' ');
                split.next();
                let key = match split.next() {
                    None => {
                        eprintln!("Expected key to delete!");
                        continue;
                    }
                    Some(x) => x,
                };
                client.send_delete(key).await?;
            }
            'h' => {
                print_basic_help();
                skip_input = true;
            }
            _ => {
                eprintln!("Unexpected input: {:?}", ip);
                skip_input = true;
            }
        }
        if !skip_input {
            match client.receive_resp().await {
                Ok(s) => println!("<< {}", s),
                Err(e) => eprintln!("<! {}", e),
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{parse_snapshot_id, resolve_port};
    use construct_cache::proto::ReqType;
    use construct_cache::socket_interface::client_impl::ConstructCacheClient;
    use construct_cache::socket_interface::decode_utils::{
        parse_generic_request, parse_rollback_request,
    };
    use futures::StreamExt;
    use tokio::net::TcpListener;
    use tokio_util::codec::{Framed, LengthDelimitedCodec};

    #[test]
    fn test_resolve_port_uses_default_when_missing() {
        assert_eq!(resolve_port(None, 8080), Ok(8080));
    }

    #[test]
    fn test_resolve_port_overrides_with_valid_cli_arg() {
        assert_eq!(resolve_port(Some("9001".to_string()), 8080), Ok(9001));
    }

    #[test]
    fn test_resolve_port_rejects_invalid_and_out_of_range() {
        assert!(resolve_port(Some("abc".to_string()), 8080).is_err());
        assert!(resolve_port(Some("70000".to_string()), 8080).is_err());
    }

    #[test]
    fn test_parse_snapshot_id() {
        assert_eq!(parse_snapshot_id(Some("42")), Ok(42));
        assert_eq!(parse_snapshot_id(None), Err("Expected backup ID!"));
        assert_eq!(
            parse_snapshot_id(Some("not_an_int")),
            Err("Backup ID must be an integer!")
        );
    }

    #[tokio::test]
    async fn test_client_sends_commit_and_rollback_requests() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();

        let server_handle = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut framed = Framed::new(socket, LengthDelimitedCodec::new());

            let commit_bytes = framed.next().await.unwrap().unwrap();
            let commit_req = parse_generic_request(&commit_bytes.freeze()).unwrap();
            assert_eq!(commit_req.req_type(), ReqType::Commit);

            let rollback_bytes = framed.next().await.unwrap().unwrap();
            let rollback_gen = parse_generic_request(&rollback_bytes.freeze()).unwrap();
            assert_eq!(rollback_gen.req_type(), ReqType::Rollback);
            let rollback_req = parse_rollback_request(&rollback_gen.payload).unwrap();
            assert_eq!(rollback_req.snapshot_id, 42);
        });

        let mut client = ConstructCacheClient::new(&addr).await.unwrap();
        assert!(client.send_commit().await.unwrap());
        assert!(client.send_rollback(42).await.unwrap());

        server_handle.await.unwrap();
    }
}

