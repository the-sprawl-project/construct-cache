use crate::key_value_store::key_value_store::KeyValueStore;
use prost::Message;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use tokio::net::TcpListener;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use futures::{SinkExt, StreamExt};

use super::decode_utils::*;
use crate::proto::*;
use log::{error, info, trace, warn};

/// The main key value store server. Stores a listening address so that
/// it may be able to selectively choose the interfaces it listens on
pub struct ConstructCacheServer {
    listen_addr_: String,
    kvs_access_: RwLock<KeyValueStore>,
}

fn invalid_create_resp() -> CreateKvPairResp {
    CreateKvPairResp { success: false }
}

impl ConstructCacheServer {
    pub fn new(listening_addr: &str, name: &str) -> Arc<ConstructCacheServer> {
        Arc::new(ConstructCacheServer {
            listen_addr_: String::from_str(listening_addr).unwrap(),
            kvs_access_: RwLock::new(KeyValueStore::new(name)),
        })
    }

    pub fn handle_ping_request(&self, binary_req: &[u8]) -> Vec<u8> {
        let ping_request = match parse_ping_request(binary_req) {
            Ok(v) => v,
            Err(e) => {
                warn!("Parse error: {:?}", e);
                return vec![];
            }
        };
        let message: String = ping_request.ping_message;
        info!("Received ping: {:?}", message);
        let resp = message.clone() + " acked by server";
        let ping_resp = PingResponse {
            ping_resp_message: resp,
        };
        ping_resp.encode_to_vec()
    }

    fn add_value(&self, pair: KeyValuePair) -> bool {
        let mut store = self.kvs_access_.write().unwrap();
        let success = (*store).add(kvp_proto_to_kvp_rust(pair));
        if success {
            info!("Successfully added pair!");
        } else {
            info!("Did not add pair!");
        }
        success
    }

    fn get_value(&self, key: &str) -> Option<(String, i64)> {
        let store = self.kvs_access_.read().unwrap();
        let val = (*store).get(key);
        val.map(|kvp| (kvp.value().to_string(), kvp.timestamp()))
    }

    fn update_value(&self, pair: KeyValuePair) -> bool {
        let mut store = self.kvs_access_.write().unwrap();
        (*store).update(kvp_proto_to_kvp_rust(pair))
    }

    fn delete_value(&self, key: &str) -> bool {
        let mut store = self.kvs_access_.write().unwrap();
        (*store).delete(key)
    }

    fn backup_key_value_store(&self, backup_id: &str) -> bool {
        let store = self.kvs_access_.read().unwrap();
        match store.write_to_file(backup_id) {
            Ok(_) => true,
            Err(e) => {
                error!("Inner error in backup: {:?}", e.to_string());
                false
            }
        }
    }

    fn restore_key_value_store(&self, backup_id: &str) -> bool {
        let mut store = self.kvs_access_.write().unwrap();
        match store.read_from_file(backup_id) {
            Ok(_) => true,
            Err(e) => {
                error!("Inner error in restore: {:?}", e.to_string());
                false
            }
        }
    }

    pub fn handle_create_request(&self, binary_req: &[u8]) -> Vec<u8> {
        let create_request = match parse_create_request(binary_req) {
            Ok(v) => v,
            Err(e) => {
                warn!("Parse error: {:?}", e);
                return invalid_create_resp().encode_to_vec();
            }
        };
        if create_request.pair.is_none() {
            warn!("No pair to insert");
            return invalid_create_resp().encode_to_vec();
        }
        let insertable_pair = match create_request.pair {
            None => return invalid_create_resp().encode_to_vec(),
            Some(x) => x,
        };
        info!("Got key: {:?}", insertable_pair.key.as_str());
        info!("Got value: {:?}", insertable_pair.value.as_str());

        let success = self.add_value(insertable_pair);
        let resp = CreateKvPairResp { success };
        resp.encode_to_vec()
    }

    pub fn handle_read_request(&self, binary_req: &[u8]) -> Vec<u8> {
        let read_request = match parse_read_request(binary_req) {
            Ok(v) => v,
            Err(e) => {
                warn!("Parse error: {:?}", e);
                return ReadKvPairResp {
                    success: false,
                    pair: None,
                }
                .encode_to_vec();
            }
        };
        let key = read_request.key;
        match self.get_value(&key) {
            None => ReadKvPairResp {
                success: false,
                pair: None,
            }
            .encode_to_vec(),
            Some((v, ts)) => ReadKvPairResp {
                success: true,
                pair: Some(KeyValuePair {
                    key,
                    value: v,
                    timestamp: ts,
                }),
            }
            .encode_to_vec(),
        }
    }

    pub fn handle_update_request(&self, binary_req: &[u8]) -> Vec<u8> {
        let update_request = match parse_update_request(binary_req) {
            Ok(v) => v,
            Err(e) => {
                warn!("Parse error: {:?}", e);
                return UpdateKvPairResp { success: false }.encode_to_vec();
            }
        };
        let mut resp = UpdateKvPairResp::default();
        match update_request.pair {
            Some(x) => {
                let success = self.update_value(x);
                resp.success = success;
            }
            None => {
                resp.success = false;
            }
        }
        resp.encode_to_vec()
    }

    pub fn handle_delete_request(&self, binary_req: &[u8]) -> Vec<u8> {
        match parse_delete_request(binary_req) {
            Ok(delete_request) => {
                let key = delete_request.key;
                let success = self.delete_value(&key);
                DeleteKvPairResp { success }.encode_to_vec()
            }
            Err(e) => {
                warn!("Parse error: {:?}", e);
                DeleteKvPairResp { success: false }.encode_to_vec()
            }
        }
    }

    pub fn handle_backup_request(&self, binary_req: &[u8]) -> Vec<u8> {
        let backup_request = match parse_backup_request(binary_req) {
            Ok(v) => v,
            Err(e) => {
                warn!("Parse error: {:?}", e);
                return BackupResp { success: false }.encode_to_vec();
            }
        };
        let success = self.backup_key_value_store(&backup_request.backup_id);
        BackupResp { success }.encode_to_vec()
    }

    pub fn handle_restore_request(&self, binary_req: &[u8]) -> Vec<u8> {
        let restore_request = match parse_restore_request(binary_req) {
            Ok(v) => v,
            Err(e) => {
                warn!("Parse error: {:?}", e);
                return RestoreResp { success: false }.encode_to_vec();
            }
        };
        let success = self.restore_key_value_store(&restore_request.backup_id);
        RestoreResp { success }.encode_to_vec()
    }

    // TODO: Given that Error is a trait, we should ideally create custom
    // errors that extend it and improve our error reporting system.
    pub async fn main_loop(self: Arc<Self>) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(self.listen_addr_.as_str()).await?;
        // Create an infinite loop that waits on a connection to the socket.
        // Once a connection is hit, spawn off a handler to this connection
        // that reads the data input to the socket, handles it, and exits
        // gracefully.
        loop {
            let self_arc = self.clone();
            let (socket, addr) = listener.accept().await?;
            tokio::spawn(async move {
                let mut framed = Framed::new(socket, LengthDelimitedCodec::new());
                trace!("Received connection from: {:?}", addr);
                while let Some(Ok(bytes)) = framed.next().await {
                    if bytes.is_empty() {
                        return;
                    }
                    let req = match parse_generic_request(&bytes.freeze()) {
                        Ok(r) => r,
                        Err(e) => {
                            warn!("Parse error: {:?}", e);
                            return;
                        }
                    };
                    let req_type = req.req_type();
                    let payload = req.payload;
                    let resp = match req_type {
                        ReqType::Ping => self_arc.handle_ping_request(&payload),
                        ReqType::Create => self_arc.handle_create_request(&payload),
                        ReqType::Read => self_arc.handle_read_request(&payload),
                        ReqType::Update => self_arc.handle_update_request(&payload),
                        ReqType::Delete => self_arc.handle_delete_request(&payload),
                        ReqType::Backup => self_arc.handle_backup_request(&payload),
                        ReqType::Restore => self_arc.handle_restore_request(&payload),
                    };
                    let mut generic_resp = GenericResponse::default();
                    generic_resp.set_req_type(req_type);
                    generic_resp.payload = resp;
                    match framed.send(generic_resp.encode_to_vec().into()).await {
                        Ok(_) => {}
                        Err(e) => {
                            warn!("Error: {:?}", e);
                            break;
                        }
                    }
                }
            });
        }
    }
}
