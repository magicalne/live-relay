use tokio::sync::oneshot;
use tracing::{error, info};
use webrtc::{
    api::{
        interceptor_registry::register_default_interceptors, media_engine::MediaEngine, APIBuilder,
    },
    ice_transport::ice_server::RTCIceServer,
    interceptor::registry::Registry,
    media::audio::buffer::info,
    peer_connection::{
        configuration::RTCConfiguration, sdp::session_description::RTCSessionDescription,
        RTCPeerConnection,
    },
};

use crate::server::cmd::apply_notice;

pub struct Connection {
    conn: RTCPeerConnection,
}

impl Connection {
    pub async fn from_offer(offer: RTCSessionDescription) -> crate::Result<Self> {
        // Prepare the configuration
        let config = RTCConfiguration {
            ice_servers: vec![RTCIceServer {
                urls: vec!["stun:stun.l.google.com:19302".to_owned()],
                ..Default::default()
            }],
            ..Default::default()
        };

        // Create a MediaEngine object to configure the supported codec
        let mut m = MediaEngine::default();
        m.register_default_codecs()?;

        let mut registry = Registry::new();

        // Use the default set of Interceptors
        registry = register_default_interceptors(registry, &mut m)?;

        // Create the API object with the MediaEngine
        let api = APIBuilder::new()
            .with_media_engine(m)
            .with_interceptor_registry(registry)
            .build();

        // Create a new RTCPeerConnection
        let peer_connection = api.new_peer_connection(config).await?;
        peer_connection.on_track(Box::new(move |track, recv, tran| {
            Box::pin(async move {
                info!("track: {:?}", track);
            })
        }));

        peer_connection.set_remote_description(offer).await?;

        Ok(Self {
            conn: peer_connection,
        })
    }

    pub async fn create_answer(&self) -> crate::Result<oneshot::Receiver<RTCSessionDescription>> {
        let answer = self.conn.create_answer(None).await?;
        self.conn.set_local_description(answer.clone()).await?;

        let (tx, rx) = oneshot::channel();
        self.conn.on_ice_candidate(Box::new(move |candidate| {
            Box::pin(async move {
                if candidate.is_some() {
                    tx.send(self.conn.local_description());
                }
            })
        }));
        Ok(rx)
    }

    pub fn read_strem(&self) -> crate::Result<()> {
        self.conn
            .on_track(Box::new(move |track, recv, tran| Box::pin(async move {})));
        Ok(())
    }
}
