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

    pub async fn send_restore(&mut self, backup_id: &str) -> Result<bool, SocketError> {
        let mut request = GenericRequest::default();
        let restore_req = RestoreReq {
            backup_id: backup_id.to_string(),
        };
        request.payload = restore_req.encode_to_vec();
        request.set_req_type(ReqType::Restore);
        self.send_message(request).await?;
        Ok(true)
    }

    pub async fn send_backup(&mut self, backup_id: &str) -> Result<bool, SocketError> {
        let mut request = GenericRequest::default();
        let backup_req = BackupReq {
            backup_id: backup_id.to_string(),
        };
        request.payload = backup_req.encode_to_vec();
        request.set_req_type(ReqType::Backup);
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
}
