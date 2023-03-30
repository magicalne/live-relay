use std::sync::Arc;

use ::webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use dashmap::DashMap;

/// Accept webrtc connection.and accept data redirected from peer.
pub struct Server {
    hashshake: Arc<DashMap<String, RTCSessionDescription>>, // Pubkey -
}

impl Server {
    pub fn new() -> Self {
        Self {
            hashshake: Arc::new(DashMap::new()),
        }
    }
}
