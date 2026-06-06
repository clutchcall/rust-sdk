//! Data modality — MQTT-style typed pub/sub over MoQT.
//!
//! Hierarchical topics with `+` / `#` wildcards (top-level segment must be
//! concrete). The frame header carries the full topic + the publisher's
//! client id so subscribers MQTT-filter and attribute without out-of-band
//! lookup. Mirrors `@clutchcall/sdk/data` and `clutchcall.data`.

use crate::moqt::{MoqtClient, FramePublication, FrameSubscription};
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub struct DataError(pub String);
impl fmt::Display for DataError { fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "{}", self.0) } }
impl Error for DataError {}

// ── wire ────────────────────────────────────────────────────────────────

pub const FROM_LEN_BYTES:  usize = 1;
pub const TOPIC_LEN_BYTES: usize = 1;
pub const MAX_FROM_LEN:    usize = 0xFF;
pub const MAX_TOPIC_LEN:   usize = 0xFF;

pub fn encode_data_frame(from_client_id: &str, topic: &str, payload: &[u8]) -> Result<Vec<u8>, DataError> {
    if from_client_id.len() > MAX_FROM_LEN  { return Err(DataError(format!("from_client_id > 255 ({})", from_client_id.len()))); }
    if topic.len()          > MAX_TOPIC_LEN { return Err(DataError(format!("topic > 255 ({})", topic.len()))); }
    let mut out = Vec::with_capacity(1 + from_client_id.len() + 1 + topic.len() + payload.len());
    out.push(from_client_id.len() as u8);
    out.extend_from_slice(from_client_id.as_bytes());
    out.push(topic.len() as u8);
    out.extend_from_slice(topic.as_bytes());
    out.extend_from_slice(payload);
    Ok(out)
}

pub struct DecodedDataFrame {
    pub from_client_id: String,
    pub topic:          String,
    pub payload:        Vec<u8>,
}

pub fn decode_data_frame(buf: &[u8]) -> Result<DecodedDataFrame, DataError> {
    if buf.is_empty() { return Err(DataError("frame too short".into())); }
    let from_len = buf[0] as usize;
    let mut pos = 1;
    if buf.len() < pos + from_len + 1 { return Err(DataError("truncated (from + topic_len)".into())); }
    let from = String::from_utf8_lossy(&buf[pos..pos + from_len]).to_string();
    pos += from_len;
    let topic_len = buf[pos] as usize;
    pos += 1;
    if buf.len() < pos + topic_len { return Err(DataError("truncated (topic)".into())); }
    let topic = String::from_utf8_lossy(&buf[pos..pos + topic_len]).to_string();
    pos += topic_len;
    Ok(DecodedDataFrame {
        from_client_id: from,
        topic,
        payload: buf[pos..].to_vec(),
    })
}

pub fn topic_matches(topic: &str, topic_filter: &str) -> bool {
    if topic == topic_filter { return true; }
    let t: Vec<&str> = topic.split('/').collect();
    let f: Vec<&str> = topic_filter.split('/').collect();
    for (i, fp) in f.iter().enumerate() {
        if *fp == "#" { return i == f.len() - 1; }
        if i >= t.len() { return false; }
        if *fp == "+" { continue; }
        if *fp != t[i] { return false; }
    }
    t.len() == f.len()
}

pub fn top_level_segment(filter_or_topic: &str) -> Result<&str, DataError> {
    let head = filter_or_topic.split('/').next().unwrap_or("");
    if head == "+" || head == "#" {
        return Err(DataError(format!("top-level wildcard not supported ({filter_or_topic:?})")));
    }
    if head.is_empty() { return Err(DataError("empty topic / filter".into())); }
    Ok(head)
}

// ── message + handles ───────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct Message {
    pub topic:          String,
    pub from_client_id: String,
    pub payload:        Vec<u8>,
    pub retained:       bool,
}

pub struct Subscription {
    _sub: FrameSubscription,
    pub ns:            String,
    pub topic_filter:  String,
}

// ── client ──────────────────────────────────────────────────────────────

pub struct Data {
    relay_host: String,
    token:      String,
    client_id:  String,
    client:     Option<MoqtClient>,
    pubs:       HashMap<String, FramePublication>,
}

impl Data {
    pub fn new(token: &str, client_id: &str) -> Result<Self, DataError> {
        if token.is_empty()     { return Err(DataError("token required".into())); }
        if client_id.is_empty() { return Err(DataError("client_id required".into())); }
        Ok(Self {
            relay_host: "relay.clutchcall.dev".into(),
            token:      token.into(),
            client_id:  client_id.into(),
            client:     None,
            pubs:       HashMap::new(),
        })
    }

    pub fn with_relay_host(mut self, host: &str) -> Self { self.relay_host = host.into(); self }

    fn ensure(&mut self) -> Result<&MoqtClient, DataError> {
        if self.client.is_none() {
            let url = format!("moq://{}/data/{}", self.relay_host,
                utf8_percent_encode(&self.client_id, NON_ALPHANUMERIC));
            self.client = Some(
                MoqtClient::connect(&url, &self.token, |_| {})
                    .ok_or_else(|| DataError("moqt connect failed".into()))?,
            );
        }
        Ok(self.client.as_ref().unwrap())
    }

    pub fn publish(&mut self, topic: &str, payload: &[u8], reliable: bool, retained: bool) -> Result<(), DataError> {
        let top = top_level_segment(topic)?.to_string();
        let frame = encode_data_frame(&self.client_id, topic, payload)?;
        let priority: u8 = if reliable || retained { 30 } else { 100 };
        if !self.pubs.contains_key(&top) {
            let client = self.ensure()?;
            let tag = format!("data;top={top}");
            let p = client.publish_frame(&format!("data/{top}"), "msg",
                "data.pubsub", &tag, 100)
                .ok_or_else(|| DataError("publish_frame failed".into()))?;
            self.pubs.insert(top.clone(), p);
        }
        let ts = SystemTime::now().duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64).unwrap_or(0);
        self.pubs[&top].write(ts, &frame, priority);
        Ok(())
    }

    pub fn subscribe<F: FnMut(Message) + Send + 'static>(
        &mut self, topic_filter: &str, mut on_message: F,
    ) -> Result<Subscription, DataError> {
        let top = top_level_segment(topic_filter)?.to_string();
        let ns = format!("data/{top}");
        let client = self.ensure()?;
        let topic_filter_owned = topic_filter.to_string();
        let sub = client.subscribe_frame(&ns, "msg", move |_ts, prio, raw| {
            let Ok(f) = decode_data_frame(raw) else { return };
            if !topic_matches(&f.topic, &topic_filter_owned) { return; }
            on_message(Message {
                topic:          f.topic,
                from_client_id: f.from_client_id,
                payload:        f.payload,
                retained:       prio <= 30,
            });
        }).ok_or_else(|| DataError("subscribe_frame failed".into()))?;
        Ok(Subscription { _sub: sub, ns, topic_filter: topic_filter.into() })
    }
}
