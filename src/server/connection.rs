use std::net::SocketAddr;
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

use crate::entity::NostrMessage;
use crate::error::RelayError;
use crate::Result;

/// Send and receive `Frame` values from a remote peer.
///
/// When implementing networking protocols, a message on that protocol is
/// often composed of several smaller messages known as frames. The purpose of
/// `Connection` is to read and write frames on the underlying `TcpStream`.
///
/// To read frames, the `Connection` uses an internal buffer, which is filled
/// up until there are enough bytes to create a full frame. Once this happens,
/// the `Connection` creates the frame and returns it to the caller.
///
/// When sending frames, the frame is first encoded into the write buffer.
/// The contents of the write buffer are then written to the socket.
#[derive(Debug, Clone)]
pub struct Connection {
    peer: Arc<SocketAddr>,
    stream: Arc<Mutex<WebSocketStream<TcpStream>>>,
}

impl Connection {
    /// Create a new `Connection`, backed by `socket`. Read and write buffers
    /// are initialized.
    pub fn new(stream: WebSocketStream<TcpStream>, peer: SocketAddr) -> Connection {
        Connection {
            stream: Arc::new(Mutex::new(stream)),
            peer: Arc::new(peer),
        }
    }

    /// Read a single `Frame` value from the underlying stream.
    ///
    /// The function waits until it has retrieved enough data to parse a frame.
    /// Any data remaining in the read buffer after the frame has been parsed is
    /// kept there for the next call to `read_frame`.
    ///
    /// # Returns
    ///
    /// On success, the received frame is returned. If the `TcpStream`
    /// is closed in a way that doesn't break a frame in half, it returns
    /// `None`. Otherwise, an error is returned.
    pub async fn read_frame(&mut self) -> Result<Option<NostrMessage>> {
        let mut stream = self.stream.lock().await;
        return match stream.next().await {
            Some(Ok(Message::Text(txt))) => Ok(Some(serde_json::from_str(&txt)?)),
            Some(Ok(Message::Binary(buf))) => Ok(Some(serde_json::from_slice(&buf)?)),
            None | Some(Ok(_)) => Ok(None),
            Some(Err(err)) => Err(RelayError::WsError(err)),
        };
    }

    /// Write a single `Frame` value to the underlying stream.
    ///
    /// The `Frame` value is written to the socket using the various `write_*`
    /// functions provided by `AsyncWrite`. Calling these functions directly on
    /// a `TcpStream` is **not** advised, as this will result in a large number of
    /// syscalls. However, it is fine to call these functions on a *buffered*
    /// write stream. The data will be written to the buffer. Once the buffer is
    /// full, it is flushed to the underlying socket.
    pub async fn write_frame(&self, response: String) -> Result<()> {
        let msg = Message::Text(response);
        let mut stream = self.stream.lock().await;
        stream.send(msg).await?;
        Ok(())
    }
}
