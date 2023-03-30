use std::{str::FromStr, vec};

use chrono::{serde::ts_seconds, DateTime, Utc};
use secp256k1::{
    hashes::{sha256, Hash},
    schnorr, XOnlyPublicKey,
};
use serde::{
    de::{self, Visitor},
    Deserialize, Serialize,
};
use serde_json::{json, Number, Value};
use tracing::{debug, info};

use crate::{error::RelayError, SECP};

pub const EVENT: &str = "EVENT";
pub const REQ: &str = "REQ";
pub const CLOSE: &str = "CLOSE";
/// used to notify clients if an EVENT was successful
pub const OK: &str = "OK";

#[derive(PartialEq, Eq, Debug, Clone)]
pub enum NostrMessage {
    EventMsg(Event),
    ReqMsg(ReqMsg),
    CloseMsg(CloseMsg),
}
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone, Default)]
pub struct Event {
    #[serde(with = "hex")]
    pub id: [u8; 32],
    #[serde(with = "hex")]
    pub pubkey: [u8; 32],
    #[serde(skip)]
    pub delegated_by: Option<String>,
    #[serde(with = "ts_seconds")]
    pub created_at: DateTime<Utc>,
    pub kind: u32,
    pub tags: Option<Vec<Tag>>,
    pub content: String,
    #[serde(with = "hex")]
    pub sig: Vec<u8>,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct ReqMsg {
    pub subscription_id: String,
    pub filters: Vec<Filter>,
}

/// Filter for requests
///
/// Corresponds to client-provided subscription request elements.  Any
/// element can be present if it should be used in filtering, or
/// absent ([`None`]) if it should be ignored.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct Filter {
    /// Event hashes
    pub ids: Option<Vec<String>>,
    /// List of author public keys
    pub authors: Option<Vec<String>>,
    /// Events published after this time
    #[serde(with = "ts_seconds")]
    pub since: DateTime<Utc>,
    /// Events published before this time
    #[serde(with = "ts_seconds")]
    pub until: DateTime<Utc>,
    /// Limit number of results
    pub limit: Option<usize>,
    /// Event kinds
    pub kinds: Option<Vec<u32>>,
    /// Set of tags
    #[serde(rename = "#e")]
    pub event_tags: Option<Vec<String>>,
    #[serde(rename = "#p")]
    pub pubkey_tags: Option<Vec<String>>,

    /// Force no matches due to malformed data
    // we can't represent it in the req filter, so we don't want to
    // erroneously match.  This basically indicates the req tried to
    // do something invalid.
    pub force_no_match: bool,
}

#[derive(Serialize, PartialEq, Eq, Debug, Clone)]
pub struct Tag {
    pub name: String,
    pub value: String,
    pub params: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct CloseMsg {
    pub subscription_id: String,
}

impl Event {
    pub(crate) fn validate_sig(&self) -> crate::Result<()> {
        let json_str = self.to_json_string()?;
        let digest: sha256::Hash = sha256::Hash::hash(json_str.as_bytes());
        // * ensure the id matches the computed sha256sum.
        if &self.id != &digest.as_ref() {
            debug!("event id does not match digest");
            return Err(RelayError::EventInvalidId(hex::encode(&self.id)));
        }
        // * validate the message digest (sig) using the pubkey & computed sha256 message hash.
        let sig = schnorr::Signature::from_slice(&self.sig)?;
        let msg = secp256k1::Message::from_slice(&digest.as_ref())?;
        let pubkey = XOnlyPublicKey::from_slice(&self.pubkey)?;
        SECP.verify_schnorr(&sig, &msg, &pubkey)?;
        Ok(())
    }

    fn to_json_string(&self) -> crate::Result<String> {
        // create a JsonValue for each event element
        let mut c: Vec<Value> = vec![];
        // id must be set to 0
        let id = Number::from(0_u64);
        c.push(serde_json::Value::Number(id));
        // public key
        c.push(Value::String(hex::encode(&self.pubkey)));
        // creation time
        let created_at = Number::from(self.created_at.timestamp());
        c.push(serde_json::Value::Number(created_at));
        // kind
        let kind = Number::from(self.kind);
        c.push(serde_json::Value::Number(kind));
        // tags
        if let Some(tags) = &self.tags {
            let mut tag_vec = Vec::with_capacity(tags.len());
            for tag in tags {
                tag_vec.push(tag.to_json_array());
            }
            c.push(serde_json::Value::Array(tag_vec));
        }
        // content
        c.push(Value::String(self.content.clone()));
        let s = serde_json::to_string(&Value::Array(c))?;
        Ok(s)
    }
}

impl<'de> Deserialize<'de> for NostrMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct MsgVisitor;
        impl<'de> Visitor<'de> for MsgVisitor {
            type Value = NostrMessage;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("Nostr message")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let kind = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                match kind {
                    EVENT => {
                        let event = seq
                            .next_element()?
                            .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                        return Ok(NostrMessage::EventMsg(event));
                    }
                    REQ => {
                        let sub_id = seq
                            .next_element()?
                            .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                        let mut filters = vec![];
                        while let Some(filter) = seq.next_element()? {
                            filters.push(filter);
                        }
                        return Ok(NostrMessage::ReqMsg(ReqMsg {
                            subscription_id: sub_id,
                            filters,
                        }));
                    }
                    CLOSE => {
                        let sub_id = seq
                            .next_element()?
                            .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                        return Ok(NostrMessage::CloseMsg(CloseMsg {
                            subscription_id: sub_id,
                        }));
                    }
                    _ => {
                        return Err(de::Error::invalid_value(
                            de::Unexpected::Str(kind),
                            &"Unknown event type",
                        ))
                    }
                }
            }
        }
        deserializer.deserialize_seq(MsgVisitor)
    }
}

impl Tag {
    pub(crate) fn to_json_array(&self) -> Value {
        let mut v = vec![
            serde_json::Value::String(self.name.clone()),
            serde_json::Value::String(self.value.clone()),
        ];
        self.params.as_ref().map(|params| {
            params
                .iter()
                .for_each(|p| v.push(serde_json::Value::String(p.clone())))
        });
        serde_json::Value::Array(v)
    }
}
impl<'de> Deserialize<'de> for Tag {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct TagVisitor;
        impl<'de> Visitor<'de> for TagVisitor {
            type Value = Tag;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("Tag")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: de::SeqAccess<'de>,
            {
                let name = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;

                let value = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;

                let mut params: Vec<String> = vec![];
                while let Some(param) = seq.next_element()? {
                    params.push(param);
                }
                let params = if params.is_empty() {
                    None
                } else {
                    Some(params)
                };
                return Ok(Tag {
                    name,
                    value,
                    params,
                });
            }
        }

        deserializer.deserialize_seq(TagVisitor)
    }
}

pub fn serilize_event(e: &Event) -> serde_json::Value {
    return json!([EVENT, e]);
}

#[cfg(test)]
mod tests {

    use serde_json::Value;

    use crate::{
        entity::{serilize_event, Event, NostrMessage},
        Result,
    };

    #[test]
    fn event_serialize() -> Result<()> {
        let raw_json = r#"{"id":"1384757da583e6129ce831c3d7afc775a33a090578f888dd0d010328ad047d0c","pubkey":"bbbd9711d357df4f4e498841fd796535c95c8e751fa35355008a911c41265fca","created_at":1612650459,"kind":1,"tags":null,"content":"hello world","sig":"59d0cc47ab566e81f72fe5f430bcfb9b3c688cb0093d1e6daa49201c00d28ecc3651468b7938642869ed98c0f1b262998e49a05a6ed056c0d92b193f4e93bc21"}"#;
        let e: Event = serde_json::from_str(raw_json)?;
        let actual_json = serde_json::to_string(&e)?;
        assert_eq!(raw_json, actual_json);
        Ok(())
    }

    #[test]
    fn nostr_msg_serialize() -> Result<()> {
        println!("test");
        let raw_json = r#"["EVENT",{"id":"1384757da583e6129ce831c3d7afc775a33a090578f888dd0d010328ad047d0c","pubkey":"bbbd9711d357df4f4e498841fd796535c95c8e751fa35355008a911c41265fca","created_at":1612650459,"kind":1,"tags":null,"content":"hello world","sig":"59d0cc47ab566e81f72fe5f430bcfb9b3c688cb0093d1e6daa49201c00d28ecc3651468b7938642869ed98c0f1b262998e49a05a6ed056c0d92b193f4e93bc21"}]"#;
        let msg: NostrMessage = serde_json::from_str(raw_json)?;
        match &msg {
            NostrMessage::EventMsg(e) => {
                let ser = serilize_event(&e);
                let expect = {
                    let raw: Value = serde_json::from_str(raw_json)?;
                    serde_json::to_string(&raw)?
                };
                assert_eq!(ser.to_string(), expect);
            }
            _ => panic!("Should be an event msg!"),
        }
        Ok(())
    }
}
