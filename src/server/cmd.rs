use std::time::Duration;

use serde_json::{json, Value};
use tokio::{
    spawn,
    sync::{oneshot, Mutex},
    time::interval,
};
use tracing::{debug, trace, warn};

use crate::{
    db::{Query, DB},
    entity::{Event, Filter, EVENT, OK},
};

use super::connection::Connection;

/// default limit on query result
const LIMIT: usize = 20;

/// Ephemeral Events
/// 20000 <= n < 30000
pub const LIVE_STREAMING_OFFER_KIND: u32 = 21111;
pub const LIVE_STREAMING_ANSWER_KIND: u32 = 21112;

/// https://github.com/nostr-protocol/nips/blob/master/20.md
struct CommandResult<'a> {
    event_id: &'a [u8; 32],
    valid: bool,
    message: String,
}

struct ReqTask {
    queries: Mutex<Vec<Query>>,
    rx: oneshot::Receiver<()>,
    db: DB,
    conn: Connection,
    sub_id: String,
}

impl ReqTask {
    fn new(
        sub_id: String,
        filter: Vec<Filter>,
        db: DB,
        conn: Connection,
        rx: oneshot::Receiver<()>,
    ) -> Self {
        let queries = Mutex::new(
            filter
                .into_iter()
                .map(|filter| Query::new(filter))
                .flatten()
                .collect(),
        );
        Self {
            sub_id,
            queries,
            rx,
            db,
            conn,
        }
    }
    /// The `query` function should be the most complicated one in nostr implementation. Make this
    /// one, and only one, long and hard-to-review is acceptable.
    async fn run(&mut self) -> crate::Result<()> {
        let mut interval = interval(Duration::from_millis(100));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let mut events = Vec::new();
                    let mut queries = self.queries.lock().await;
                    for q in queries.iter_mut(){
                        let mut res = q.query(&self.db)?;
                        events.append(&mut res);
                    }
                    self.send_all(events).await?;
                },
                msg = &mut self.rx => {
                    trace!("ReqTask receives close event.");
                    return Ok(())
                }
            }
        }
    }

    /// ["EVENT", <subscription_id>, <event JSON as defined above>], used to send events requested by clients.
    async fn send_all(&self, events: Vec<Event>) -> crate::Result<()> {
        let mut values: Vec<Value> = vec![
            Value::String(EVENT.to_string()),
            Value::String(self.sub_id.clone()),
        ];

        for e in events {
            let event: Value = serde_json::to_value(e)?;
            values.push(event);
        }

        let json_string = serde_json::to_string(&values)?;
        self.conn.write_frame(json_string).await?;
        Ok(())
    }
}

impl<'a> CommandResult<'a> {
    fn new(event_id: &'a [u8; 32], valid: bool, message: String) -> Self {
        Self {
            event_id,
            valid,
            message,
        }
    }

    fn serialize(&self) -> crate::Result<String> {
        let res = serde_json::to_string(&json!([
            OK,
            hex::encode(self.event_id),
            self.valid,
            self.message
        ]))?;
        Ok(res)
    }
}

pub(crate) async fn apply_event(
    event: &Event,
    db: &DB,
    conn: &mut Connection,
) -> crate::Result<()> {
    // Ephemeral events are no to be stored.
    // Handle live streaming webrtc negotiation.
    if event.kind == LIVE_STREAMING_OFFER_KIND {
        let offer = serde_json::from_str(&event.content)?;
        let pc = crate::webrtc::Connection::from_offer(offer).await?;
        pc.create_answer().await?;
        return Ok(());
    }
    let cmd_result = match event.validate_sig() {
        // TODO: spawn_blocking
        Ok(_) => match db.put_event(event) {
            Ok(_) => CommandResult::new(&event.id, true, "".to_string()),
            Err(err) => {
                trace!("db put event failed: {:?}", err);
                CommandResult::new(&event.id, false, format!("error: DB failed to put event."))
            }
        },
        Err(err) => {
            debug!("Validate sig failed: {:?}", err);
            CommandResult::new(
                &event.id,
                false,
                format!("error: Validate signature failed."),
            )
        }
    };
    let res = cmd_result.serialize()?;
    conn.write_frame(res).await?;

    Ok(())
}

pub async fn apply_notice(conn: &mut Connection, msg: String) -> crate::Result<()> {
    let value = json!(["NOTICE", msg]);
    let response = serde_json::to_string(&value)?;
    conn.write_frame(response).await?;
    Ok(())
}

pub(crate) async fn apply_request(
    sub_id: String,
    filters: Vec<Filter>,
    db: DB,
    conn: &mut Connection,
) -> crate::Result<oneshot::Sender<()>> {
    let (tx, rx) = oneshot::channel();

    spawn(async move {
        let mut task = ReqTask::new(sub_id, filters, db, conn, rx);
        if let Err(err) = task.run().await {
            warn!("ReqTask failed: {:?}", err);
        }
    });
    Ok(tx)
}
