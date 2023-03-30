use serde_json::Value;
use std::{
    collections::HashMap,
    future::Future,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tracing::{debug, error, info, warn};

use tokio::{
    net::{TcpListener, TcpStream},
    sync::{broadcast, mpsc, oneshot, Semaphore},
    time,
};
use tokio_tungstenite::accept_async;
use tracing::trace;
use webrtc::{
    peer_connection::sdp::session_description::RTCSessionDescription,
    track::track_local::TrackLocal,
};

use crate::{
    config::Config,
    db::{DbDropGuard, DB},
    entity::{Event, NostrMessage, ReqMsg, EVENT},
    server::connection::Connection,
    shutdown::Shutdown,
};

use self::cmd::{LIVE_STREAMING_ANSWER_KIND, LIVE_STREAMING_OFFER_KIND};

pub mod cmd;
pub mod connection;
mod shutdown;
const MAX_CONNECTIONS: usize = 200;

pub struct Server {
    //db
    config: Config,

    /// Broadcasts a shutdown signal to all active connections.
    ///
    /// The initial `shutdown` trigger is provided by the `run` caller. The
    /// server is responsible for gracefully shutting down active connections.
    /// When a connection task is spawned, it is passed a broadcast receiver
    /// handle. When a graceful shutdown is initiated, a `()` value is sent via
    /// the broadcast::Sender. Each active connection receives it, reaches a
    /// safe terminal state, and completes the task.
    notify_shutdown: broadcast::Sender<()>,
    /// Used as part of the graceful shutdown process to wait for client
    /// connections to complete processing.
    ///
    /// Tokio channels are closed once all `Sender` handles go out of scope.
    /// When a channel is closed, the receiver receives `None`. This is
    /// leveraged to detect all connection handlers completing. When a
    /// connection handler is initialized, it is assigned a clone of
    /// `shutdown_complete_tx`. When the listener shuts down, it drops the
    /// sender held by this `shutdown_complete_tx` field. Once all handler tasks
    /// complete, all clones of the `Sender` are also dropped. This results in
    /// `shutdown_complete_rx.recv()` completing with `None`. At this point, it
    /// is safe to exit the server process.
    shutdown_complete_rx: mpsc::Receiver<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
    listener: TcpListener,
    limit_connections: Arc<Semaphore>,
    db_holder: DbDropGuard,
}

impl Server {
    pub async fn run(&mut self) -> crate::Result<()> {
        info!("accepting inbound connections");

        loop {
            // Wait for a permit to become available
            //
            // `acquire` returns a permit that is bound via a lifetime to the
            // semaphore. When the permit value is dropped, it is automatically
            // returned to the semaphore. This is convenient in many cases.
            // However, in this case, the permit must be returned in a different
            // task than it is acquired in (the handler task). To do this, we
            // "forget" the permit, which drops the permit value **without**
            // incrementing the semaphore's permits. Then, in the handler task
            // we manually add a new permit when processing completes.
            //
            // `acquire()` returns `Err` when the semaphore has been closed. We
            // don't ever close the sempahore, so `unwrap()` is safe.
            self.limit_connections.acquire().await.unwrap().forget();
            // Accept a new socket. This will attempt to perform error handling.
            // The `accept` method internally attempts to recover errors, so an
            // error here is non-recoverable.
            let (socket, peer) = self.accept().await?;

            // TODO: use accept_hdr_async_with_config with configs
            let stream = accept_async(socket).await?;
            trace!("Accept new ws conn.");
            let connection = Connection::new(stream, peer);
            let mut handler = Handler {
                connection,

                // Receive shutdown notifications.
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),

                // Notifies the receiver half once all clones are
                // dropped.
                _shutdown_complete: self.shutdown_complete_tx.clone(),
                db: self.db_holder.db(),
                filter_tasks: HashMap::new(),
            };
            tokio::spawn(async move {
                // Process the connection. If an error is encountered, log it.
                if let Err(err) = handler.run().await {
                    error!(cause = ?err, "connection error");
                }
            });
        }
    }
    /// Accept an inbound connection.
    ///
    /// Errors are handled by backing off and retrying. An exponential backoff
    /// strategy is used. After the first failure, the task waits for 1 second.
    /// After the second failure, the task waits for 2 seconds. Each subsequent
    /// failure doubles the wait time. If accepting fails on the 6th try after
    /// waiting for 64 seconds, then this function returns with an error.
    async fn accept(&mut self) -> crate::Result<(TcpStream, SocketAddr)> {
        let mut backoff = 1;

        // Try to accept a few times
        loop {
            // Perform the accept operation. If a socket is successfully
            // accepted, return it. Otherwise, save the error.
            match self.listener.accept().await {
                Ok((socket, peer)) => return Ok((socket, peer)),

                Err(err) => {
                    if backoff > 64 {
                        // Accept has failed too many times. Return the error.
                        return Err(err.into());
                    }
                }
            }

            // Pause execution until the back off period elapses.
            time::sleep(Duration::from_secs(backoff)).await;

            // Double the back off
            backoff *= 2;
        }
    }
}

pub async fn run(config: Config, listener: TcpListener, shutdown: impl Future) {
    // When the provided `shutdown` future completes, we must send a shutdown
    // message to all active connections. We use a broadcast channel for this
    // purpose. The call below ignores the receiver of the broadcast pair, and when
    // a receiver is needed, the subscribe() method on the sender is used to create
    // one.
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);
    let max_conn_limit = config.max_connection_limit.unwrap_or(MAX_CONNECTIONS);
    let db_path = &config.rocksdb_path;
    let db_holder = DbDropGuard::new(db_path).expect("Open rocksdb failed.");
    let mut server = Server {
        db_holder,
        listener,
        config,
        notify_shutdown,
        shutdown_complete_tx,
        shutdown_complete_rx,
        limit_connections: Arc::new(Semaphore::new(max_conn_limit)),
    };

    tokio::select! {
        res = server.run() => {
            // If an error is received here, accepting connections from the TCP
            // listener failed multiple times and the server is giving up and
            // shutting down.
            //
            // Errors encountered when handling individual connections do not
            // bubble up to this point.
            if let Err(err) = res {
                error!(cause = %err, "failed to accept");
            }
        }
        _ = shutdown => {
            // The shutdown signal has been received.
            info!("shutting down");
        }
    }
    // Extract the `shutdown_complete` receiver and transmitter
    // explicitly drop `shutdown_transmitter`. This is important, as the
    // `.await` below would otherwise never complete.
    let Server {
        mut shutdown_complete_rx,
        shutdown_complete_tx,
        notify_shutdown,
        ..
    } = server;

    // When `notify_shutdown` is dropped, all tasks which have `subscribe`d will
    // receive the shutdown signal and can exit
    drop(notify_shutdown);
    // Drop final `Sender` so the `Receiver` below can complete
    drop(shutdown_complete_tx);

    // Wait for all active connections to finish processing. As the `Sender`
    // handle held by the listener has been dropped above, the only remaining
    // `Sender` instances are held by connection handler tasks. When those drop,
    // the `mpsc` channel will close and `recv()` will return `None`.
    let _ = shutdown_complete_rx.recv().await;
}

/// Per-stream handler. Upgraded to websocket stream, then start to handle requests.
struct Handler {
    /// The TCP connection decorated with the nostr protocol encoder / decoder.
    ///
    /// `Connection` allows the handler to operate at the "frame" level and keep
    /// the byte level protocol parsing details encapsulated in `Connection`.
    connection: connection::Connection,
    /// Listen for shutdown notifications.
    ///
    /// A wrapper around the `broadcast::Receiver` paired with the sender in
    /// `Listener`. The connection handler processes requests from the
    /// connection until the peer disconnects **or** a shutdown notification is
    /// received from `shutdown`. In the latter case, any in-flight work being
    /// processed for the peer is continued until it reaches a safe state, at
    /// which point the connection is terminated.
    shutdown: Shutdown,

    /// Not used directly. Instead, when `Handler` is dropped...?
    _shutdown_complete: mpsc::Sender<()>,

    db: DB,
    filter_tasks: HashMap<String, oneshot::Sender<()>>, // <subscription_id, close_channel>
}

impl Handler {
    async fn run(&mut self) -> crate::Result<()> {
        // As long as the shutdown signal has not been received, try to read a
        // new request frame.
        while !self.shutdown.is_shutdown() {
            // While reading a request frame, also listen for the shutdown
            // signal.
            let nostr_msg = tokio::select! {
                res = self.connection.read_frame() => res?,
                _ = self.shutdown.recv() => {
                    // If a shutdown signal is received, return from `run`.
                    // This will result in the task terminating.
                    return Ok(());
                }
            };

            // If `None` is returned from `read_frame()` then the peer closed
            // the socket. There is no further work to do and the task can be
            // terminated.
            let msg = match nostr_msg {
                Some(msg) => msg,
                None => return Ok(()),
            };

            // Logs the `msg` object. The syntax here is a shorthand provided by
            // the `tracing` crate. It can be thought of as similar to:
            //
            // ```
            // debug!(msg = format!("{:?}", msg));
            // ```
            //
            // `tracing` provides structured logging, so information is "logged"
            // as key-value pairs.
            debug!(?msg);
            match msg {
                NostrMessage::EventMsg(event) => {
                    if let Err(err) = cmd::apply_event(&event, &self.db, &mut self.connection).await
                    {
                        error!("Apply event error: {:?}", err);
                    }
                }
                NostrMessage::ReqMsg(req) => {
                    let sub_id = req.subscription_id.clone();
                    match cmd::apply_request(
                        req.subscription_id,
                        req.filters,
                        self.db.clone(),
                        &mut self.connection,
                    )
                    .await
                    {
                        Ok(sender) => {
                            self.filter_tasks.insert(sub_id, sender);
                        }
                        Err(err) => error!("Apply req error: {:?}", err),
                    }
                }
                NostrMessage::CloseMsg(close) => {
                    if let Some(sender) = self.filter_tasks.remove(&close.subscription_id) {
                        if let Err(err) = sender.send(()) {
                            warn!(
                                "ReqTask failed to close. sub id: {}, error: {:?}",
                                &close.subscription_id, err
                            );
                        }
                    }
                }
            };

            // Perform the work needed to apply the command. This may mutate the
            // database state as a result.
            //
            // The connection is passed into the apply function which allows the
            // command to write response frames directly to the connection. In
            // the case of pub/sub, multiple frames may be send back to the
            // peer.
            //cmd.apply(&self.db, &mut self.connection, &mut self.shutdown).await?;
        }
        Ok(())
    }
}
