use tokio::net::TcpListener;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use futures::{SinkExt, StreamExt};
use prost::Message;

use construct_cache::proto::*;
use construct_cache::socket_interface::server_impl::ConstructCacheServer;
use construct_cache::socket_interface::client_impl::ConstructCacheClient;
use construct_cache::socket_interface::decode_utils::{parse_sync_request, parse_generic_request};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cp_port = "9000";
    let cp_addr = format!("127.0.0.1:{}", cp_port);
    let server_ports = vec!["9001", "9002", "9003"];
    
    // 1. Start the cache servers
    for port in &server_ports {
        let listen_addr = format!("127.0.0.1:{}", port);
        // We configure each server to point to the control plane
        let server = ConstructCacheServer::new(&listen_addr, &format!("store_{}", port), &cp_addr);
        tokio::spawn(async move {
            println!("Starting server on {}", listen_addr);
            if let Err(e) = server.main_loop().await {
                println!("Server error: {:?}", e);
            }
        });
    }

    println!("Controlplane listening on {}", cp_addr);
    let listener = TcpListener::bind(&cp_addr).await?;
    
    let server_addrs: Vec<String> = server_ports.into_iter().map(|p| format!("127.0.0.1:{}", p)).collect();
    let addrs_arc = std::sync::Arc::new(server_addrs);

    // 2. Controlplane main loop
    loop {
        let (socket, _) = listener.accept().await?;
        let addrs = addrs_arc.clone();
        tokio::spawn(async move {
            let mut framed = Framed::new(socket, LengthDelimitedCodec::new());
            while let Some(Ok(bytes)) = framed.next().await {
                if let Ok(gen_req) = parse_generic_request(&bytes.freeze()) {
                    if gen_req.req_type() == ReqType::Sync {
                        if let Ok(sync_req) = parse_sync_request(&gen_req.payload) {
                            let success = replicate_to_others(&addrs, &sync_req).await;
                            let sync_resp = SyncResp { success };
                            let mut gen_resp = GenericResponse::default();
                            gen_resp.set_req_type(ReqType::Sync);
                            gen_resp.payload = sync_resp.encode_to_vec();
                            let _ = framed.send(gen_resp.encode_to_vec().into()).await;
                        }
                    }
                }
            }
        });
    }
}

async fn replicate_to_others(addrs: &[String], req: &SyncReq) -> bool {
    let mut all_success = true;
    for addr in addrs {
        // Skip sender
        if addr == &req.sender_addr || addr.ends_with(&req.sender_addr) || req.sender_addr.ends_with(addr) {
            continue;
        }
        
        // Connect to replica
        if let Ok(mut client) = ConstructCacheClient::new(addr).await {
            let op = req.op_type();
            let mut gen_req = GenericRequest::default();
            
            match op {
                ReqType::Create => {
                    let r = CreateKvPairReq { pair: req.pair.clone() };
                    gen_req.payload = r.encode_to_vec();
                    gen_req.set_req_type(ReqType::ReplicateCreate);
                }
                ReqType::Update => {
                    let r = UpdateKvPairReq { pair: req.pair.clone() };
                    gen_req.payload = r.encode_to_vec();
                    gen_req.set_req_type(ReqType::ReplicateUpdate);
                }
                ReqType::Delete => {
                    let r = DeleteKvPairReq { key: req.key.clone() };
                    gen_req.payload = r.encode_to_vec();
                    gen_req.set_req_type(ReqType::ReplicateDelete);
                }
                _ => continue,
            }
            
            if client.send_message(gen_req).await.is_ok() {
                if let Ok(resp) = client.receive_resp().await {
                    // Check if resp contains success
                    if !resp.contains("Successfully") {
                        all_success = false;
                    }
                } else {
                    all_success = false;
                }
            } else {
                all_success = false;
            }
        } else {
            all_success = false;
        }
    }
    all_success
}
