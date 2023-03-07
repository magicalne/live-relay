use std::{path::Path, sync::Arc};

use chrono::{DateTime, Utc};
use rocksdb::{
    ColumnFamily, Direction, IteratorMode, Options, PrefixRange, ReadOptions, Transaction,
    TransactionDB, TransactionDBOptions,
};
use tempfile::tempdir;
use tracing::debug;

use crate::{
    entity::{Event, Filter},
    error::RelayError,
};

type CF = &'static str;

/// Event
/// Key: pub_key-timestamp-id[:4]
/// Value: Event JSON
const EVENT: CF = "EVENT";

/// Event Tag
/// Key: hex event id [u8; 32]-timestamp-id[:4]
/// Value: pub_key-timestamp-id[:4]
const EVENT_TAG_CF: CF = "EVENT_TAG";
const TAG_BUF_LEN: usize = 32;

/// Pubkey Tag
/// Key: hex pubkey [u8; 32]-timestamp-id[:4]
/// Value: pub_key-timestamp-id[:4]
const PUBKEY_TAG_CF: CF = "PUBKEY_TAG";

/// Tags:
const EVENT_TAG: &str = "e";
const PUBKEY_TAG: &str = "p";

/// Kind
/// Key: kind-timestamp-id[:4]
/// Value: pub_key-timestamp-id[:4]
const KIND: CF = "KIND";

/// Event ID
/// Key: id
/// Value: pub_key-timestamp-id[:4]
const EVENT_ID: CF = "EVENT_ID";

const COLUMN_FAMILIES: &[&str; 5] = &[EVENT, EVENT_TAG_CF, PUBKEY_TAG_CF, EVENT_ID, KIND];
/// default limit on query result
const LIMIT: usize = 20;

pub(crate) struct DbDropGuard {
    /// The `Db` instance that will be shut down when this `DbHolder` struct
    /// is dropped.
    db: DB,
}
#[derive(Clone)]
pub(crate) struct DB {
    db: Arc<TransactionDB>,
}

struct RangeScan<'a> {
    cf: &'a ColumnFamily,
    transaction: &'a Transaction<'a, TransactionDB>,
    lower_bound: Option<Vec<u8>>,
    upper_bound: Option<Vec<u8>>,
    limit: usize,
    seek_mode: IteratorMode<'a>,
}

enum QueryStrategy {
    SearchById,
    SearchByPubkey,
    SearchByEventTag,
    SearchByPubkeyTag,
    SearchByKind,
}

pub(crate) struct Query {
    strategy: QueryStrategy,
    key: Vec<u8>,
    last_cursor: Option<Vec<u8>>,
    filter: Filter,
}

impl Query {
    pub(crate) fn new(filter: Filter) -> Vec<Self> {
        if let Some(ids) = &filter.ids {
            let query_vec = ids
                .iter()
                .map(|id| {
                    Query {
                        strategy: QueryStrategy::SearchById,
                        key: hex::decode(&id).unwrap(), // handle prefix
                        last_cursor: None,
                        filter: filter.clone(),
                    }
                })
                .collect();
            return query_vec;
        }

        if let Some(pubkeys) = &filter.authors {
            let query_vec = pubkeys
                .iter()
                .map(|pubkey| Query {
                    strategy: QueryStrategy::SearchByPubkey,
                    key: hex::decode(&pubkey).unwrap(),
                    last_cursor: None,
                    filter: filter.clone(),
                })
                .collect();
            return query_vec;
        }

        if let Some(kinds) = &filter.kinds {
            let query_vec = kinds
                .iter()
                .map(|kind| Query {
                    strategy: QueryStrategy::SearchByKind,
                    key: kind.to_be_bytes().to_vec(),
                    last_cursor: None,
                    filter: filter.clone(),
                })
                .collect();
            return query_vec;
        }

        let mut query_vec = Vec::new();
        if let Some(event_tags) = &filter.event_tags {
            for event_tag in event_tags {
                let tag_query = Query {
                    strategy: QueryStrategy::SearchByEventTag,
                    key: event_tag.as_bytes().to_vec(),
                    last_cursor: None,
                    filter: filter.clone(),
                };
                query_vec.push(tag_query);
            }
        }
        if let Some(pubkey_tags) = &filter.pubkey_tags {
            for pubkey_tag in pubkey_tags {
                let tag_query = Query {
                    strategy: QueryStrategy::SearchByPubkeyTag,
                    key: pubkey_tag.as_bytes().to_vec(),
                    last_cursor: None,
                    filter: filter.clone(),
                };
                query_vec.push(tag_query);
            }
        }
        query_vec
    }

    pub(crate) fn query(&mut self, db: &DB) -> crate::Result<Vec<Event>> {
        match self.strategy {
            QueryStrategy::SearchById => self.get_events_by_id(db),
            QueryStrategy::SearchByPubkey => self.get_events_by_author(db),
            QueryStrategy::SearchByEventTag => self.get_events_by_event_tag(db),
            QueryStrategy::SearchByPubkeyTag => self.get_events_by_pubkey_tag(db),
            QueryStrategy::SearchByKind => self.get_events_by_kind(db),
        }
    }

    fn get_events_by_id(&mut self, db: &DB) -> crate::Result<Vec<Event>> {
        let (events, last_cursor): (Vec<Event>, Option<Vec<u8>>) = match self.key.len() {
            1..=31 => {
                let events = db.get_events_by_id_prefix(
                    &self.key,
                    get_min_limit(&self.filter.limit),
                    self.last_cursor.clone(),
                )?;
                let last_cursor = events.last().as_ref().map(|e| e.id.to_vec());
                (events, last_cursor)
            }
            32 => match db.get_event_by_id(&self.key)? {
                Some(e) => (vec![e], None),
                None => (vec![], None),
            },
            _ => return Err(RelayError::EventInvalidId(hex::encode(&self.key))),
        };
        self.last_cursor = last_cursor;
        let filtered: Vec<Event> = events
            .into_iter()
            .filter(|e| match self.filter.authors.as_ref() {
                Some(filter_authors) => filter_authors.contains(&hex::encode(e.pubkey)),
                None => true,
            })
            .filter(|e| match self.filter.kinds.as_ref() {
                Some(filter_kinds) => filter_kinds.contains(&e.kind),
                None => true,
            })
            .filter(|e| match &self.filter.event_tags {
                Some(filter_tags) => match e.tags.as_ref() {
                    Some(event_tags) => event_tags
                        .iter()
                        .find(|event_tag| {
                            event_tag.name == EVENT_TAG && filter_tags.contains(&event_tag.value)
                        })
                        .is_some(),
                    None => false, // the event has no tag
                },
                None => return true,
            })
            .filter(|e| match &self.filter.pubkey_tags {
                Some(filter_tags) => match e.tags.as_ref() {
                    Some(pubkey_tags) => pubkey_tags
                        .into_iter()
                        .find(|pubkey_tag| {
                            pubkey_tag.name == PUBKEY_TAG && filter_tags.contains(&pubkey_tag.value)
                        })
                        .is_some(),
                    None => false,
                },
                None => false,
            })
            .collect();
        Ok(filtered)
    }

    fn get_events_by_author(&mut self, db: &DB) -> crate::Result<Vec<Event>> {
        let (events, last_cursor): (Vec<Event>, Option<Vec<u8>>) = match self.key.len() {
            1..=31 => {
                let events = db.get_events_by_pubkey_prefix(
                    &self.key,
                    self.last_cursor.clone(),
                    get_min_limit(&self.filter.limit),
                )?;
                let last_cursor = events.last().as_ref().map(|e| e.id.to_vec());
                (events, last_cursor)
            }
            32 => {
                let events = db.get_events_by_pubkey(
                    &self.key,
                    &self.filter.since,
                    &self.filter.until,
                    get_min_limit(&self.filter.limit),
                    self.last_cursor.clone(),
                )?;
                (events, None)
            }
            _ => return Err(RelayError::EventInvalidPubkey(hex::encode(&self.key))),
        };
        self.last_cursor = last_cursor;
        let filtered: Vec<Event> = events
            .into_iter()
            .filter(|e| match self.filter.ids.as_ref() {
                Some(filter_ids) => filter_ids.contains(&hex::encode(e.id)),
                None => true,
            })
            .filter(|e| match self.filter.kinds.as_ref() {
                Some(filter_kinds) => filter_kinds.contains(&e.kind),
                None => true,
            })
            .filter(|e| match &self.filter.event_tags {
                Some(filter_tags) => match e.tags.as_ref() {
                    Some(event_tags) => event_tags
                        .iter()
                        .find(|event_tag| {
                            event_tag.name == EVENT_TAG && filter_tags.contains(&event_tag.value)
                        })
                        .is_some(),
                    None => false, // the event has no tag
                },
                None => return true,
            })
            .filter(|e| match &self.filter.pubkey_tags {
                Some(filter_tags) => match e.tags.as_ref() {
                    Some(pubkey_tags) => pubkey_tags
                        .into_iter()
                        .find(|pubkey_tag| {
                            pubkey_tag.name == PUBKEY_TAG && filter_tags.contains(&pubkey_tag.value)
                        })
                        .is_some(),
                    None => false,
                },
                None => false,
            })
            .collect();
        Ok(filtered)
    }

    fn get_events_by_kind(&mut self, db: &DB) -> crate::Result<Vec<Event>> {
        let (events, last_cursor) = db.get_events_by_kind(
            &self.key,
            &self.filter.since,
            &self.filter.until,
            get_min_limit(&self.filter.limit),
            self.last_cursor.clone(),
        )?;
        self.last_cursor = last_cursor;
        let events = events
            .into_iter()
            .filter(|e| match &self.filter.event_tags {
                Some(filter_tags) => match e.tags.as_ref() {
                    Some(event_tags) => event_tags
                        .iter()
                        .find(|event_tag| {
                            event_tag.name == EVENT_TAG && filter_tags.contains(&event_tag.value)
                        })
                        .is_some(),
                    None => false, // the event has no tag
                },
                None => return true,
            })
            .filter(|e| match &self.filter.pubkey_tags {
                Some(filter_tags) => match e.tags.as_ref() {
                    Some(pubkey_tags) => pubkey_tags
                        .into_iter()
                        .find(|pubkey_tag| {
                            pubkey_tag.name == PUBKEY_TAG && filter_tags.contains(&pubkey_tag.value)
                        })
                        .is_some(),
                    None => false,
                },
                None => false,
            })
            .collect();
        Ok(events)
    }

    fn get_events_by_event_tag(&mut self, db: &DB) -> crate::Result<Vec<Event>> {
        let (events, last_cursor) = db.get_events_by_event_tag(
            &self.key,
            &self.filter.since,
            &self.filter.until,
            get_min_limit(&self.filter.limit),
            self.last_cursor.clone(),
        )?;
        self.last_cursor = last_cursor;
        Ok(events)
    }

    fn get_events_by_pubkey_tag(&mut self, db: &DB) -> crate::Result<Vec<Event>> {
        let (events, last_cursor) = db.get_events_by_event_tag(
            &self.key,
            &self.filter.since,
            &self.filter.until,
            get_min_limit(&self.filter.limit),
            self.last_cursor.clone(),
        )?;
        self.last_cursor = last_cursor;
        Ok(events)
    }
}

impl DbDropGuard {
    /// Create a new `DbHolder`, wrapping a `Db` instance. When this is dropped
    /// the `Db`'s purge task will be shut down.
    pub(crate) fn new(path: &Option<impl AsRef<Path>>) -> crate::Result<DbDropGuard> {
        let db = match path {
            Some(path) => DB::open_with_path(path)?,
            None => DB::open_temp()?,
        };
        Ok(DbDropGuard { db })
    }

    /// Get the shared database. Internally, this is an
    /// `Arc`, so a clone only increments the ref count.
    pub(crate) fn db(&self) -> DB {
        self.db.clone()
    }
}

///
/// DB queries with until and limit:
/// - get event by id
/// - get events by ids
/// - get events by id prefix
/// - get events by pubkey
/// - get events by pubkeys
/// - get events by pubkey prefix_
/// - get events by tag
/// - get events by kind
impl DB {
    pub fn open_with_path(path: impl AsRef<Path>) -> crate::Result<Self> {
        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);
        let txn_db_opts = TransactionDBOptions::default();
        let db = Arc::new(TransactionDB::open_cf(
            &db_opts,
            &txn_db_opts,
            path,
            COLUMN_FAMILIES,
        )?);

        Ok(Self { db })
    }

    /// Open a temp db for testing.
    pub(crate) fn open_temp() -> crate::Result<Self> {
        let temp_dir = tempdir()?;
        DB::open_with_path(&temp_dir.path())
    }

    pub fn get_event_by_id(&self, id: &[u8]) -> crate::Result<Option<Event>> {
        let cf = self.db.cf_handle(EVENT).unwrap(); // Safe to unwrap, the cf must be created.
        let v = self.db.get_cf(cf, id)?;
        let event = v.map(|v| serde_json::from_slice(&v).map_err(RelayError::SerdeError));
        event.transpose()
    }

    pub fn get_events_by_id_prefix(
        &self,
        id: &[u8],
        limit: usize,
        last_cursor: Option<Vec<u8>>,
    ) -> crate::Result<Vec<Event>> {
        let cf = self.db.cf_handle(EVENT_ID).unwrap();
        let tx = self.db.transaction();
        let mut scan = RangeScan::new(cf, &tx, limit);
        scan.set_lower_bound(id.to_vec());

        let mode = cursor_to_iter_mod(&last_cursor);
        scan.set_seek_mode(mode); // DESC
        let mut event_keys: Vec<Vec<u8>> = Vec::new();
        if let Some(kvs) = scan.query(id)? {
            kvs.into_iter().for_each(|(_k, v)| event_keys.push(v));
        }
        let mut events = Vec::new();
        for event_key in event_keys.into_iter() {
            if let Some(event) = self.get_event_by_id(&event_key)? {
                events.push(event);
            }
        }
        Ok(events)
    }

    pub fn get_events_by_pubkey(
        &self,
        pubkey: &[u8],
        since: &DateTime<Utc>,
        until: &DateTime<Utc>,
        limit: usize,
        last_cursor: Option<Vec<u8>>,
    ) -> crate::Result<Vec<Event>> {
        let event_cf = self.db.cf_handle(EVENT).unwrap();
        let mut readopts: ReadOptions = Default::default();
        let mut lower = Vec::with_capacity(32 + 8);
        lower.extend_from_slice(pubkey);
        lower.extend_from_slice(&since.timestamp().to_be_bytes());
        readopts.set_iterate_lower_bound(lower);
        let mut upper = Vec::with_capacity(32 + 8);
        upper.extend_from_slice(pubkey);
        upper.extend_from_slice(&until.timestamp().to_be_bytes());
        readopts.set_iterate_upper_bound(upper);
        let mut events = Vec::new();
        let mode = cursor_to_iter_mod(&last_cursor);
        let tx = self.db.transaction();
        let iter = tx.iterator_cf_opt(event_cf, readopts, mode);
        for raw in iter.take(limit) {
            let r = raw?;
            let (event_id, v) = r;
            if !event_id.starts_with(pubkey) {
                break;
            }
            let event: Event = serde_json::from_slice(&v)?;
            events.push(event);
        }
        Ok(events)
    }

    pub fn get_events_by_pubkey_prefix(
        &self,
        pubkey: &[u8],
        last_cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> crate::Result<Vec<Event>> {
        let event_cf = self.db.cf_handle(EVENT).unwrap();
        let mut readopts: ReadOptions = Default::default();
        readopts.set_iterate_range(PrefixRange(pubkey));
        let tx = self.db.transaction();
        let mode = cursor_to_iter_mod(&last_cursor);
        let iter = tx.iterator_cf_opt(event_cf, readopts, mode);
        let mut events = Vec::new();
        for raw in iter.take(limit) {
            let r = raw?;
            let (event_id, v) = r;
            if !event_id.starts_with(&pubkey) {
                break;
            }
            let event: Event = serde_json::from_slice(&v)?;
            events.push(event);
        }

        Ok(events)
    }

    pub fn get_events_by_event_tag(
        &self,
        key: &[u8],
        since: &DateTime<Utc>,
        until: &DateTime<Utc>,
        limit: usize,
        last_cursor: Option<Vec<u8>>,
    ) -> crate::Result<(Vec<Event>, Option<Vec<u8>>)> {
        self.get_events_by_tag(EVENT_TAG_CF, key, since, until, limit, last_cursor)
    }

    pub fn get_events_by_pubkey_tag(
        &self,
        key: &[u8],
        since: &DateTime<Utc>,
        until: &DateTime<Utc>,
        limit: usize,
        last_cursor: Option<Vec<u8>>,
    ) -> crate::Result<(Vec<Event>, Option<Vec<u8>>)> {
        self.get_events_by_tag(PUBKEY_TAG_CF, key, since, until, limit, last_cursor)
    }
    /// Query events by tag.Return events and last cursor.
    fn get_events_by_tag(
        &self,
        tag: &str,
        key: &[u8],
        since: &DateTime<Utc>,
        until: &DateTime<Utc>,
        limit: usize,
        last_cursor: Option<Vec<u8>>,
    ) -> crate::Result<(Vec<Event>, Option<Vec<u8>>)> {
        let tag_cf = self.db.cf_handle(EVENT_TAG_CF).unwrap(); // Safe to unwrap, the cf must be created.
        let tx = self.db.transaction();
        let mut scan = RangeScan::new(tag_cf, &tx, limit);
        let mut lower_buf = Vec::with_capacity(20 + 8);
        let mut tag_buf = Vec::from(tag);
        padding_tag_buf(&mut tag_buf)?;
        lower_buf.extend_from_slice(&tag_buf);
        lower_buf.extend_from_slice(&since.timestamp().to_be_bytes());
        scan.set_lower_bound(lower_buf);

        let mut upper_buf = Vec::with_capacity(20 + 8);
        upper_buf.extend_from_slice(&tag_buf);
        upper_buf.extend_from_slice(&until.timestamp().to_be_bytes());
        scan.set_upper_bound(upper_buf);
        let mode = cursor_to_iter_mod(&last_cursor);
        scan.set_seek_mode(mode); // DESC

        let mut last_cursor = None;
        let mut event_keys: Vec<Vec<u8>> = Vec::new();
        if let Some(kvs) = scan.query(&tag_buf)? {
            kvs.into_iter().for_each(|(k, v)| {
                last_cursor = Some(k.to_vec());
                event_keys.push(v)
            });
        }
        let mut events = Vec::new();
        for event_key in event_keys.into_iter() {
            if let Some(event) = self.get_event_by_id(&event_key)? {
                events.push(event);
            }
        }
        Ok((events, last_cursor))
    }

    pub fn get_events_by_kind(
        &self,
        kind: &[u8],
        since: &DateTime<Utc>,
        until: &DateTime<Utc>,
        limit: usize,
        last_cursor: Option<Vec<u8>>,
    ) -> crate::Result<(Vec<Event>, Option<Vec<u8>>)> {
        let cf = self.get_cf(KIND);
        let transaction = self.db.transaction();
        let mut scan = RangeScan::new(cf, &transaction, limit);
        let mut lower_buf = Vec::with_capacity(4 + 8 + 4);
        lower_buf.extend_from_slice(&kind);
        lower_buf.extend_from_slice(&since.timestamp().to_be_bytes());
        scan.set_lower_bound(lower_buf);
        let mut lower_buf = Vec::with_capacity(4 + 8 + 4);
        lower_buf.extend_from_slice(&kind);
        lower_buf.extend_from_slice(&until.timestamp().to_be_bytes());
        scan.set_lower_bound(lower_buf);
        let mode = cursor_to_iter_mod(&last_cursor);
        scan.set_seek_mode(mode); // DESC
        let mut last_cursor = None;
        let mut event_keys: Vec<Vec<u8>> = Vec::new();
        if let Some(kvs) = scan.query(&kind)? {
            kvs.into_iter().for_each(|(k, v)| {
                last_cursor = Some(k.to_vec());
                event_keys.push(v)
            });
        }
        let mut events = Vec::new();
        for event_key in event_keys.into_iter() {
            if let Some(event) = self.get_event_by_id(&event_key)? {
                events.push(event);
            }
        }
        Ok((events, last_cursor))
    }

    fn get_cf(&self, cf: CF) -> &ColumnFamily {
        self.db.cf_handle(cf).unwrap()
    }

    pub fn put_event(&self, event: &Event) -> crate::Result<()> {
        let event_cf = self.db.cf_handle(EVENT).unwrap(); // Safe to unwrap, the cf must be created.
        let tx = self.db.transaction();
        // store raw event
        let event_id = &event.id;
        let create_at = event.created_at.timestamp().to_be_bytes();
        // pub key: 32, timestamp: 4, event idp[..4]
        let mut k: Vec<u8> = Vec::with_capacity(32 + 4 + 4);
        k.extend_from_slice(&event.pubkey);
        k.extend_from_slice(&create_at);
        k.extend_from_slice(&event_id[..4]);
        let v = serde_json::to_vec(event)?;
        tx.put_cf(&event_cf, &k, &v)?;

        let index_val = k;
        // save event index
        let event_id_cf = self.db.cf_handle(EVENT_ID).unwrap();
        tx.put_cf(event_id_cf, &event_id, &index_val)?;

        // save kind
        let kind_cf = self.db.cf_handle(KIND).unwrap();
        let mut buf = Vec::with_capacity(8 + 8 + 4);
        buf.extend_from_slice(&event.kind.to_be_bytes());
        buf.extend_from_slice(&event.created_at.timestamp().to_be_bytes());
        buf.extend_from_slice(&event_id[..4]);
        tx.put_cf(kind_cf, &buf, &index_val)?;

        // save tags
        let event_tag_cf = self.db.cf_handle(EVENT_TAG).unwrap();
        let pubkey_tag_cf = self.db.cf_handle(PUBKEY_TAG).unwrap();
        if let Some(tags) = &event.tags {
            for t in tags {
                match t.name.as_str() {
                    EVENT_TAG => {
                        let mut buf = t.name.as_bytes().to_vec();
                        buf.extend_from_slice(&create_at);
                        buf.extend_from_slice(&event_id[..4]);
                        tx.put_cf(event_tag_cf, buf, &index_val)?;
                    }
                    PUBKEY_TAG => {
                        let mut buf = t.name.as_bytes().to_vec();
                        buf.extend_from_slice(&create_at);
                        buf.extend_from_slice(&event_id[..4]);
                        tx.put_cf(pubkey_tag_cf, buf, &index_val)?;
                    }
                    _ => debug!("Known tag name: {}, value: {}", t.name, t.value),
                }
            }
        }

        tx.commit()?;
        Ok(())
    }
}

/// RangeScan makes query easy with different indexes. It also divids query request into small
/// batches with a less `limit` size.
/// The nostr query protocol should be used in a DESC way accroding to the field of `created_at`.
/// The client should be care more about recent events instead of older ones.
impl<'a> RangeScan<'a> {
    fn new(
        cf: &'a ColumnFamily,
        transaction: &'a Transaction<'a, TransactionDB>,
        limit: usize,
    ) -> Self {
        Self {
            cf,
            transaction,
            lower_bound: None,
            upper_bound: None,
            limit,
            seek_mode: IteratorMode::End,
        }
    }

    fn set_lower_bound(&mut self, lower_bound: Vec<u8>) {
        self.lower_bound = Some(lower_bound);
    }

    fn set_upper_bound(&mut self, upper_bound: Vec<u8>) {
        self.upper_bound = Some(upper_bound);
    }

    fn set_seek_mode(&mut self, mode: IteratorMode<'a>) {
        self.seek_mode = mode;
    }
    fn query(&self, prefix: &[u8]) -> crate::Result<Option<Vec<(Vec<u8>, Vec<u8>)>>> {
        let mut readops = ReadOptions::default();
        if let Some(lower_bound) = &self.lower_bound {
            readops.set_iterate_lower_bound(lower_bound.clone());
        }
        if let Some(upper_bound) = &self.upper_bound {
            readops.set_iterate_upper_bound(upper_bound.clone());
        }
        let iter = self
            .transaction
            .iterator_cf_opt(&self.cf, readops, self.seek_mode);
        let mut kvs: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(self.limit);
        for raw in iter.take(self.limit) {
            let (k, v) = raw?;
            if k.starts_with(prefix) {
                kvs.push((k.into(), v.into()));
            }
        }
        Ok(if kvs.is_empty() { None } else { Some(kvs) })
    }
}

/// The tag buf should be fit in a buffer with size of 20.
/// This is simply padding with 0 in the end.
fn padding_tag_buf(buf: &mut Vec<u8>) -> crate::Result<()> {
    if buf.len() > TAG_BUF_LEN {
        return Err(RelayError::TagSizeError(buf.len()));
    }
    let padding_len = TAG_BUF_LEN - buf.len();
    for _ in 0..padding_len {
        buf.push(0);
    }
    Ok(())
}

fn cursor_to_iter_mod(last_cursor: &Option<Vec<u8>>) -> IteratorMode {
    match last_cursor {
        Some(key) => IteratorMode::From(&key, Direction::Reverse),
        None => IteratorMode::End,
    }
}

fn get_min_limit(limit: &Option<usize>) -> usize {
    limit.unwrap_or(LIMIT).min(LIMIT)
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};
    use secp256k1::{rand, KeyPair, Secp256k1, XOnlyPublicKey};

    use crate::{entity::Event, Result};

    use super::DB;

    fn create_account() -> XOnlyPublicKey {
        let secp = Secp256k1::new();
        let key_pair = KeyPair::new(&secp, &mut rand::thread_rng());
        XOnlyPublicKey::from_keypair(&key_pair)
    }

    fn create_random_events(db: &DB, pubkey: &XOnlyPublicKey, num: usize) -> Result<()> {
        let datetime = Utc::now();
        for i in 0..num {
            let id: [u8; 32] = rand::random();
            let pubkey = pubkey.serialize();
            let content = hex::encode(&id);

            let event = Event {
                id: id.clone(),
                pubkey,
                delegated_by: None,
                created_at: datetime - Duration::hours(i as i64),
                kind: 1,
                tags: None,
                content,
                sig: id.to_vec(),
            };
            db.put_event(&event)?;
        }
        Ok(())
    }

    #[test]
    fn range_scan_test() -> Result<()> {
        let db = DB::open_temp()?;
        let pubkey1 = create_account();
        create_random_events(&db, &pubkey1, 10)?;

        let pubkey2 = create_account();
        create_random_events(&db, &pubkey2, 10)?;
        let pubkey3 = create_account();
        create_random_events(&db, &pubkey3, 10)?;
        let pubkey4 = create_account();
        create_random_events(&db, &pubkey4, 10)?;
        let pk = pubkey1.serialize();
        let events = db.get_events_by_pubkey_prefix(&pk[..4], None, 15)?;
        for e in &events {
            assert_eq!(e.pubkey, pk);
            println!(
                "pubkey: {}, raw ts: {:?}, ts: {:?}",
                hex::encode(&e.pubkey),
                &e.created_at.timestamp().to_be_bytes(),
                &e.created_at
            );
        }
        assert_eq!(events.len(), 10);
        Ok(())
    }
}
