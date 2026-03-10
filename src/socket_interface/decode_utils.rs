// Importing everything to keep the decode clean.
// TODO(@Skeletrox): Split into req_decoders and resp_decoders?
use super::socket_errors::{ErrorKind, SocketError};
use crate::key_value_store::key_value_pair;
use crate::proto::*;
use prost::Message;

pub fn parse_generic_request(request: &[u8]) -> Result<GenericRequest, SocketError> {
    match GenericRequest::decode(request) {
        Ok(res) => Ok(res),
        Err(e) => Err(SocketError {
            kind_: ErrorKind::ParseError,
            context_: e.to_string(),
        }),
    }
}

pub fn parse_ping_request(request: &[u8]) -> Result<PingRequest, SocketError> {
    match PingRequest::decode(request) {
        Ok(res) => Ok(res),
        Err(e) => Err(SocketError {
            kind_: ErrorKind::ParseError,
            context_: e.to_string(),
        }),
    }
}

pub fn parse_create_request(request: &[u8]) -> Result<CreateKvPairReq, SocketError> {
    match CreateKvPairReq::decode(request) {
        Ok(res) => Ok(res),
        Err(e) => Err(SocketError {
            kind_: ErrorKind::ParseError,
            context_: e.to_string(),
        }),
    }
}

pub fn parse_read_request(request: &[u8]) -> Result<ReadKvPairReq, SocketError> {
    match ReadKvPairReq::decode(request) {
        Ok(res) => Ok(res),
        Err(e) => Err(SocketError {
            kind_: ErrorKind::ParseError,
            context_: e.to_string(),
        }),
    }
}

pub fn parse_update_request(request: &[u8]) -> Result<UpdateKvPairReq, SocketError> {
    match UpdateKvPairReq::decode(request) {
        Ok(res) => Ok(res),
        Err(e) => Err(SocketError {
            kind_: ErrorKind::ParseError,
            context_: e.to_string(),
        }),
    }
}

pub fn parse_delete_request(request: &[u8]) -> Result<DeleteKvPairReq, SocketError> {
    match DeleteKvPairReq::decode(request) {
        Ok(res) => Ok(res),
        Err(e) => Err(SocketError {
            kind_: ErrorKind::ParseError,
            context_: e.to_string(),
        }),
    }
}

pub fn parse_restore_request(request: &[u8]) -> Result<RestoreReq, SocketError> {
    match RestoreReq::decode(request) {
        Ok(res) => Ok(res),
        Err(e) => Err(SocketError {
            kind_: ErrorKind::ParseError,
            context_: e.to_string(),
        }),
    }
}

pub fn parse_list_snapshots_request(request: &[u8]) -> Result<ListSnapshotsReq, SocketError> {
    match ListSnapshotsReq::decode(request) {
        Ok(res) => Ok(res),
        Err(e) => Err(SocketError {
            kind_: ErrorKind::ParseError,
            context_: e.to_string(),
        }),
    }
}

pub fn parse_time_travel_request(request: &[u8]) -> Result<TimeTravelReq, SocketError> {
    match TimeTravelReq::decode(request) {
        Ok(res) => Ok(res),
        Err(e) => Err(SocketError {
            kind_: ErrorKind::ParseError,
            context_: e.to_string(),
        }),
    }
}

fn parse_ping_response(payload: &[u8]) -> Result<String, SocketError> {
    match PingResponse::decode(payload) {
        Ok(v) => Ok(v.ping_resp_message.to_string()),
        Err(e) => Err(SocketError {
            kind_: ErrorKind::ParseError,
            context_: e.to_string(),
        }),
    }
}

fn parse_create_response(payload: &[u8]) -> Result<String, SocketError> {
    match CreateKvPairResp::decode(payload) {
        Ok(v) => {
            if v.success {
                Ok("Successfully created pair!".to_string())
            } else {
                Ok("Key already exists!".to_string())
            }
        }
        Err(e) => Err(SocketError {
            kind_: ErrorKind::ParseError,
            context_: e.to_string(),
        }),
    }
}

fn parse_read_response(payload: &[u8]) -> Result<String, SocketError> {
    match ReadKvPairResp::decode(payload) {
        Ok(v) => {
            if v.success {
                match v.pair {
                    Some(p) => Ok(p.value),
                    None => Err(SocketError {
                        kind_: ErrorKind::ParseError,
                        context_: "No pair in response".to_string(),
                    }),
                }
            } else {
                Ok("Cannot find key!".to_string())
            }
        }
        Err(e) => Err(SocketError {
            kind_: ErrorKind::ParseError,
            context_: e.to_string(),
        }),
    }
}

fn parse_update_response(payload: &[u8]) -> Result<String, SocketError> {
    match UpdateKvPairResp::decode(payload) {
        Ok(v) => {
            if v.success {
                Ok("Successfully updated pair!".to_string())
            } else {
                Ok("Key does not exist!".to_string())
            }
        }
        Err(e) => Err(SocketError {
            kind_: ErrorKind::ParseError,
            context_: e.to_string(),
        }),
    }
}

fn parse_delete_response(payload: &[u8]) -> Result<String, SocketError> {
    match DeleteKvPairResp::decode(payload) {
        Ok(v) => {
            if v.success {
                Ok("Successfully deleted entry!".to_string())
            } else {
                Ok("Key does not exist!".to_string())
            }
        }
        Err(e) => Err(SocketError {
            kind_: ErrorKind::ParseError,
            context_: e.to_string(),
        }),
    }
}

fn parse_backup_response(payload: &[u8]) -> Result<String, SocketError> {
    match BackupResp::decode(payload) {
        Ok(v) => {
            if v.success {
                Ok("Successfully created backup!".to_string())
            } else {
                Ok("Could not complete backup!".to_string())
            }
        }
        Err(e) => Err(SocketError {
            kind_: ErrorKind::ParseError,
            context_: e.to_string(),
        }),
    }
}

fn parse_restore_response(payload: &[u8]) -> Result<String, SocketError> {
    match RestoreResp::decode(payload) {
        Ok(v) => {
            if v.success {
                Ok("Successfully restored from backup!".to_string())
            } else {
                Ok("Could not restore from backup!".to_string())
            }
        }
        Err(e) => Err(SocketError {
            kind_: ErrorKind::ParseError,
            context_: e.to_string(),
        }),
    }
}

fn parse_list_snapshots_response(payload: &[u8]) -> Result<String, SocketError> {
    match ListSnapshotsResp::decode(payload) {
        Ok(v) => {
            if v.success {
                let mut res = String::from("Snapshots:\n");
                for s in v.snapshots {
                    res.push_str(&format!("ID: {}, TS: {}\n", s.id, s.timestamp_ms));
                }
                Ok(res)
            } else {
                Ok("Could not list snapshots!".to_string())
            }
        }
        Err(e) => Err(SocketError {
            kind_: ErrorKind::ParseError,
            context_: e.to_string(),
        }),
    }
}

fn parse_time_travel_response(payload: &[u8]) -> Result<String, SocketError> {
    match TimeTravelResp::decode(payload) {
        Ok(v) => {
            if v.success {
                Ok("Successfully traveled in time!".to_string())
            } else {
                Ok("Could not travel in time!".to_string())
            }
        }
        Err(e) => Err(SocketError {
            kind_: ErrorKind::ParseError,
            context_: e.to_string(),
        }),
    }
}

pub fn parse_generic_response(response: &[u8]) -> Result<String, SocketError> {
    let parsed_response = match GenericResponse::decode(response) {
        Ok(res) => res,
        Err(e) => {
            return Err(SocketError {
                kind_: ErrorKind::ParseError,
                context_: e.to_string(),
            })
        }
    };
    let returnable: String;
    let req_type = parsed_response.req_type();
    let payload = parsed_response.payload;
    match req_type {
        ReqType::Ping => {
            match parse_ping_response(&payload) {
                Ok(v) => returnable = v,
                Err(e) => return Err(e),
            };
        }
        ReqType::Create => {
            match parse_create_response(&payload) {
                Ok(v) => returnable = v,
                Err(e) => return Err(e),
            };
        }
        ReqType::Read => {
            match parse_read_response(&payload) {
                Ok(v) => returnable = v,
                Err(e) => return Err(e),
            };
        }
        ReqType::Update => match parse_update_response(&payload) {
            Ok(v) => returnable = v,
            Err(e) => return Err(e),
        },
        ReqType::Delete => match parse_delete_response(&payload) {
            Ok(v) => returnable = v,
            Err(e) => return Err(e),
        },
        ReqType::Backup => match parse_backup_response(&payload) {
            Ok(v) => returnable = v,
            Err(e) => return Err(e),
        },
        ReqType::Restore => match parse_restore_response(&payload) {
            Ok(v) => returnable = v,
            Err(e) => return Err(e),
        },
        ReqType::ListSnapshots => match parse_list_snapshots_response(&payload) {
            Ok(v) => returnable = v,
            Err(e) => return Err(e),
        },
        ReqType::TimeTravel => match parse_time_travel_response(&payload) {
            Ok(v) => returnable = v,
            Err(e) => return Err(e),
        },
    }
    Ok(returnable)
}

pub fn kvp_proto_to_kvp_rust(inp: KeyValuePair) -> key_value_pair::KeyValuePair {
    key_value_pair::KeyValuePair::new(&inp.key, &inp.value, inp.timestamp)
}
