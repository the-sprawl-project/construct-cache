use super::decode_utils::parse_generic_response;
use super::socket_errors::{ErrorKind, SocketError};
use crate::proto::*;
use futures::{SinkExt, StreamExt};
use log::warn;
use prost::Message;
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

pub struct ConstructCacheClient {
    _server_addr: String,
    _framed: Framed<TcpStream, LengthDelimitedCodec>,
}

impl ConstructCacheClient {
    pub async fn new(addr: &str) -> Result<Self, SocketError> {
        let stream = TcpStream::connect(addr).await.map_err(|e| SocketError {
            kind_: ErrorKind::ConnectError,
            context_: e.to_string(),
        })?;
        let framed = Framed::new(stream, LengthDelimitedCodec::new());
        Ok(Self {
            _server_addr: String::from(addr),
            _framed: framed,
        })
    }

    pub async fn send_message(&mut self, req: GenericRequest) -> Result<(), SocketError> {
        let bytes = req.encode_to_vec();
        self._framed
            .send(bytes.into())
            .await
            .map_err(|e| SocketError {
                kind_: ErrorKind::ConnectError,
                context_: e.to_string(),
            })
    }

    pub async fn send_ping(&mut self, message: &str) -> Result<bool, SocketError> {
        let mut request = GenericRequest::default();
        let ping_request = PingRequest {
            ping_message: message.to_string(),
        };
        request.set_req_type(ReqType::Ping);
        request.payload = ping_request.encode_to_vec();
        self.send_message(request).await?;
        Ok(true)
    }

    pub async fn send_create(&mut self, key: &str, val: &str) -> Result<bool, SocketError> {
        let mut request = GenericRequest::default();
        let create_req = CreateKvPairReq {
            pair: Some(KeyValuePair {
                key: String::from(key),
                value: String::from(val),
                ..Default::default()
            }),
        };
        request.payload = create_req.encode_to_vec();
        request.set_req_type(ReqType::Create);
        self.send_message(request).await?;
        Ok(true)
    }

    pub async fn send_delete(&mut self, key: &str) -> Result<bool, SocketError> {
        let mut request = GenericRequest::default();
        let delete_req = DeleteKvPairReq {
            key: String::from(key),
        };
        request.payload = delete_req.encode_to_vec();
        request.set_req_type(ReqType::Delete);
        self.send_message(request).await?;
        Ok(true)
    }

    pub async fn send_update(&mut self, key: &str, val: &str) -> Result<bool, SocketError> {
        let mut request = GenericRequest::default();
        let update_req = UpdateKvPairReq {
            pair: Some(KeyValuePair {
                key: String::from(key),
                value: String::from(val),
                ..Default::default()
            }),
        };
        request.payload = update_req.encode_to_vec();
        request.set_req_type(ReqType::Update);
        self.send_message(request).await?;
        Ok(true)
    }

    pub async fn send_read(&mut self, key: &str) -> Result<bool, SocketError> {
        let mut request = GenericRequest::default();
        let read_req = ReadKvPairReq {
            key: key.to_string(),
        };
        request.payload = read_req.encode_to_vec();
        request.set_req_type(ReqType::Read);
        self.send_message(request).await?;
        Ok(true)
    }

    pub async fn send_rollback(&mut self, snapshot_id: i64) -> Result<bool, SocketError> {
        let mut request = GenericRequest::default();
        let rollback_req = RollbackReq { snapshot_id };
        request.payload = rollback_req.encode_to_vec();
        request.set_req_type(ReqType::Rollback);
        self.send_message(request).await?;
        Ok(true)
    }

    pub async fn send_commit(&mut self) -> Result<bool, SocketError> {
        let mut request = GenericRequest::default();
        let commit_req = CommitReq {};
        request.payload = commit_req.encode_to_vec();
        request.set_req_type(ReqType::Commit);
        self.send_message(request).await?;
        Ok(true)
    }

    pub async fn send_list_snapshots(&mut self, table_name: &str) -> Result<bool, SocketError> {
        let mut request = GenericRequest::default();
        let list_req = ListSnapshotsReq {
            table_name: table_name.to_string(),
        };
        request.payload = list_req.encode_to_vec();
        request.set_req_type(ReqType::ListSnapshots);
        self.send_message(request).await?;
        Ok(true)
    }

    pub async fn send_time_travel(&mut self, snapshot_id: i64) -> Result<bool, SocketError> {
        let mut request = GenericRequest::default();
        let tt_req = TimeTravelReq { snapshot_id };
        request.payload = tt_req.encode_to_vec();
        request.set_req_type(ReqType::TimeTravel);
        self.send_message(request).await?;
        Ok(true)
    }

    pub async fn receive_resp(&mut self) -> Result<String, SocketError> {
        if let Some(Ok(bytes)) = self._framed.next().await {
            parse_generic_response(&bytes.freeze())
        } else {
            warn!("Connection closed!");
            Ok("".to_string())
        }
    }

    pub async fn send_sync_req(&mut self, sync_req: SyncReq) -> Result<bool, SocketError> {
        let mut request = GenericRequest::default();
        request.payload = sync_req.encode_to_vec();
        request.set_req_type(ReqType::Sync);
        self.send_message(request).await?;
        Ok(true)
    }

    pub async fn receive_sync_resp(&mut self) -> Result<SyncResp, SocketError> {
        if let Some(Ok(bytes)) = self._framed.next().await {
            let parsed_response = GenericResponse::decode(bytes.freeze()).map_err(|e| SocketError {
                kind_: ErrorKind::ParseError,
                context_: e.to_string(),
            })?;
            super::decode_utils::parse_sync_response(&parsed_response.payload)
        } else {
            Err(SocketError {
                kind_: ErrorKind::ConnectError,
                context_: "Connection closed".to_string(),
            })
        }
    }
}
