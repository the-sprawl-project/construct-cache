use crate::key_value_store::rocksdb_catalog::{RocksDBCatalog, StoreCatalog};
use crate::key_value_store::key_value_store::KeyValueStore;
use prost::Message;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
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
    controlplane_addr_: String,
    kvs_access_: RwLock<KeyValueStore>,
    catalog_: Arc<dyn StoreCatalog>,
    read_only_: AtomicBool,
}

fn invalid_create_resp() -> CreateKvPairResp {
    CreateKvPairResp { success: false }
}

impl ConstructCacheServer {
    pub fn new(listening_addr: &str, name: &str, controlplane_addr: &str) -> Arc<ConstructCacheServer> {
        let catalog = Arc::new(
            RocksDBCatalog::new(&format!("file://tmp/warehouse_{}", listening_addr.replace(":", "_"))).expect("Cannot create catalog"),
        );
        Arc::new(ConstructCacheServer {
            listen_addr_: String::from_str(listening_addr).unwrap(),
            controlplane_addr_: String::from_str(controlplane_addr).unwrap(),
            kvs_access_: RwLock::new(KeyValueStore::new(name)),
            catalog_: catalog,
            read_only_: AtomicBool::new(false),
        })
    }
    
    async fn sync_with_controlplane(&self, req_type: ReqType, pair: Option<KeyValuePair>, key: String) -> bool {
        if self.controlplane_addr_.is_empty() {
            return true; // No control plane to sync with
        }
        
        match crate::socket_interface::client_impl::ConstructCacheClient::new(&self.controlplane_addr_).await {
            Ok(mut client) => {
                let mut sync_req = SyncReq::default();
                sync_req.set_op_type(req_type);
                if let Some(p) = pair {
                    sync_req.pair = Some(p);
                }
                sync_req.key = key;
                sync_req.sender_addr = self.listen_addr_.clone();
                if let Ok(_) = client.send_sync_req(sync_req).await {
                    if let Ok(resp) = client.receive_sync_resp().await {
                        return resp.success;
                    }
                }
            }
            Err(e) => {
                error!("Could not connect to control plane: {:?}", e);
            }
        }
        false
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

    fn is_read_only(&self) -> bool {
        self.read_only_.load(Ordering::SeqCst)
    }

    fn add_value(&self, pair: KeyValuePair) -> bool {
        if self.is_read_only() {
            warn!("Attempted write in read-only mode");
            return false;
        }
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
        if self.is_read_only() {
            warn!("Attempted write in read-only mode");
            return false;
        }
        let mut store = self.kvs_access_.write().unwrap();
        let success = (*store).update(kvp_proto_to_kvp_rust(pair));
        if success {
            info!("Successfully updated pair!");
        }
        success
    }

    fn delete_value(&self, key: &str) -> bool {
        if self.is_read_only() {
            warn!("Attempted write in read-only mode");
            return false;
        }
        let mut store = self.kvs_access_.write().unwrap();
        let success = (*store).delete(key);
        if success {
            info!("Successfully deleted pair!");
        }
        success
    }

    fn commit_key_value_store(&self) -> bool {
        let store = self.kvs_access_.read().unwrap();
        match self.catalog_.write_checkpoint(store.name(), &store) {
            Ok(_) => true,
            Err(e) => {
                error!("Inner error in commit: {:?}", e.to_string());
                false
            }
        }
    }

    fn rollback_key_value_store(&self, snapshot_id: i64) -> bool {
        let mut store = self.kvs_access_.write().unwrap();
        let table_name = store.name().to_string();
        match self.catalog_.rollback(&table_name, snapshot_id) {
            Ok(_) => {
                // After rollback, we reload the state
                if let Ok(new_store) = self.catalog_.read_latest(&table_name) {
                    *store = new_store;
                    self.read_only_.store(false, Ordering::SeqCst);
                    true
                } else {
                    false
                }
            }
            Err(e) => {
                error!("Inner error in rollback: {:?}", e.to_string());
                false
            }
        }
    }

    pub async fn handle_create_request(&self, binary_req: &[u8]) -> Vec<u8> {
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
        let insertable_pair = create_request.pair.unwrap();

        let cp_success = self.sync_with_controlplane(ReqType::Create, Some(insertable_pair.clone()), String::new()).await;
        if !cp_success {
             return CreateKvPairResp { success: false }.encode_to_vec();
        }

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

    pub async fn handle_update_request(&self, binary_req: &[u8]) -> Vec<u8> {
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
                let cp_success = self.sync_with_controlplane(ReqType::Update, Some(x.clone()), String::new()).await;
                if !cp_success {
                     return UpdateKvPairResp { success: false }.encode_to_vec();
                }
                let success = self.update_value(x);
                resp.success = success;
            }
            None => {
                resp.success = false;
            }
        }
        resp.encode_to_vec()
    }

    pub async fn handle_delete_request(&self, binary_req: &[u8]) -> Vec<u8> {
        match parse_delete_request(binary_req) {
            Ok(delete_request) => {
                let key = delete_request.key;
                let cp_success = self.sync_with_controlplane(ReqType::Delete, None, key.clone()).await;
                if !cp_success {
                    return DeleteKvPairResp { success: false }.encode_to_vec();
                }
                let success = self.delete_value(&key);
                DeleteKvPairResp { success }.encode_to_vec()
            }
            Err(e) => {
                warn!("Parse error: {:?}", e);
                DeleteKvPairResp { success: false }.encode_to_vec()
            }
        }
    }

    pub fn handle_commit_request(&self, _binary_req: &[u8]) -> Vec<u8> {
        let success = self.commit_key_value_store();
        CommitResp { success }.encode_to_vec()
    }

    pub fn handle_rollback_request(&self, binary_req: &[u8]) -> Vec<u8> {
        let rollback_request = match parse_rollback_request(binary_req) {
            Ok(v) => v,
            Err(e) => {
                warn!("Parse error: {:?}", e);
                return RollbackResp { success: false }.encode_to_vec();
            }
        };
        let success = self.rollback_key_value_store(rollback_request.snapshot_id);
        RollbackResp { success }.encode_to_vec()
    }

    pub fn handle_list_snapshots_request(&self, binary_req: &[u8]) -> Vec<u8> {
        let req = match parse_list_snapshots_request(binary_req) {
            Ok(v) => v,
            Err(e) => {
                warn!("Parse error: {:?}", e);
                return ListSnapshotsResp {
                    success: false,
                    snapshots: vec![],
                }
                .encode_to_vec();
            }
        };
        match self.catalog_.list_snapshots(&req.table_name) {
            Ok(snaps) => {
                let snapshots = snaps
                    .into_iter()
                    .map(|s| SnapshotMsg {
                        id: s.id,
                        timestamp_ms: s.timestamp_ms,
                        manifest_list: s.manifest_list,
                        summary: s.summary,
                    })
                    .collect();
                ListSnapshotsResp {
                    success: true,
                    snapshots,
                }
                .encode_to_vec()
            }
            Err(e) => {
                error!("Failed to list snapshots: {:?}", e);
                ListSnapshotsResp {
                    success: false,
                    snapshots: vec![],
                }
                .encode_to_vec()
            }
        }
    }

    pub fn handle_time_travel_request(&self, binary_req: &[u8]) -> Vec<u8> {
        let req = match parse_time_travel_request(binary_req) {
            Ok(v) => v,
            Err(e) => {
                warn!("Parse error: {:?}", e);
                return TimeTravelResp { success: false }.encode_to_vec();
            }
        };

        let mut store = self.kvs_access_.write().unwrap();
        let table_name = store.name().to_string();
        match self.catalog_.read_version(&table_name, req.snapshot_id) {
            Ok(new_store) => {
                *store = new_store;
                self.read_only_.store(true, Ordering::SeqCst);
                TimeTravelResp { success: true }.encode_to_vec()
            }
            Err(e) => {
                error!("Time travel failed: {:?}", e);
                TimeTravelResp { success: false }.encode_to_vec()
            }
        }
    }

    pub fn handle_sync_request(&self, _binary_req: &[u8]) -> Vec<u8> {
        // Obsolete given the new SyncReq flow but kept to avoid broken compilation
        SyncResp { success: false }.encode_to_vec()
    }
    
    pub fn handle_replicate_create_request(&self, binary_req: &[u8]) -> Vec<u8> {
        let req = parse_create_request(binary_req).unwrap_or_default();
        let success = if let Some(p) = req.pair { self.add_value(p) } else { false };
        CreateKvPairResp { success }.encode_to_vec()
    }
    
    pub fn handle_replicate_update_request(&self, binary_req: &[u8]) -> Vec<u8> {
        let req = parse_update_request(binary_req).unwrap_or_default();
        let success = if let Some(p) = req.pair { self.update_value(p) } else { false };
        UpdateKvPairResp { success }.encode_to_vec()
    }
    
    pub fn handle_replicate_delete_request(&self, binary_req: &[u8]) -> Vec<u8> {
        let req = parse_delete_request(binary_req).unwrap_or_default();
        let success = self.delete_value(&req.key);
        DeleteKvPairResp { success }.encode_to_vec()
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
                        ReqType::Create => self_arc.handle_create_request(&payload).await,
                        ReqType::Read => self_arc.handle_read_request(&payload),
                        ReqType::Update => self_arc.handle_update_request(&payload).await,
                        ReqType::Delete => self_arc.handle_delete_request(&payload).await,
                        ReqType::Commit => self_arc.handle_commit_request(&payload),
                        ReqType::Rollback => self_arc.handle_rollback_request(&payload),
                        ReqType::ListSnapshots => self_arc.handle_list_snapshots_request(&payload),
                        ReqType::TimeTravel => self_arc.handle_time_travel_request(&payload),
                        ReqType::Sync => self_arc.handle_sync_request(&payload),
                        ReqType::ReplicateCreate => self_arc.handle_replicate_create_request(&payload),
                        ReqType::ReplicateUpdate => self_arc.handle_replicate_update_request(&payload),
                        ReqType::ReplicateDelete => self_arc.handle_replicate_delete_request(&payload),
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
