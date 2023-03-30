use thiserror::Error;
use tokio::task::JoinError;
use tokio_tungstenite::tungstenite;

use crate::entity::Filter;

#[derive(Error, Debug)]
pub enum RelayError {
    #[error("Websocket error: {0}")]
    WsError(#[from] tungstenite::error::Error),
    #[error("Std IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Serde error: {0}")]
    SerdeError(#[from] serde_json::Error),
    #[error("Unknow nostr message: {0}")]
    ParseNostrError(String),
    #[error("Roksdb error: {0}")]
    RocksDBError(#[from] rocksdb::Error),
    #[error("Cannot find ColumnFamily: {0}")]
    ColumnFaimlyNotFound(String),
    #[error("Cannot find value with key: {0} in CF: {1}")]
    DBValueNotFoundError(String, String),
    #[error("The tag size is longer than 20: {0}")]
    TagSizeError(usize),
    #[error("Tokio join error: {0}")]
    TokioJoinError(#[from] JoinError),

    /// Validate sig
    #[error("Invalid event id: {0}")]
    EventInvalidId(String),
    #[error("secp errr: {0}")]
    SecpError(#[from] secp256k1::Error),
    #[error("Invalid event id: {0}")]
    EventInvalidPubkey(String),

    /// Validate ReqFilter
    #[error("Invalid filter request: {0:?}")]
    ReqMsgInvalid(Filter),

    /// WebRTC error
    #[error(transparent)]
    WebRTCError(#[from] webrtc::Error),
}
