use secp256k1::{Secp256k1, VerifyOnly};

pub mod config;
pub mod db;
pub(crate) mod entity;
pub mod error;
pub mod live;
pub mod server;
pub mod shutdown;
pub mod webrtc;
pub type Result<T> = std::result::Result<T, crate::error::RelayError>;

lazy_static::lazy_static! {
    /// Secp256k1 verification instance.
    pub static ref SECP: Secp256k1<VerifyOnly> = Secp256k1::verification_only();
}
