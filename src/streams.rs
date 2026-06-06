//! Streams modality — broadcast over QUIC (MoQT) with signed playback URLs.
//!
//! Mirrors `@clutchcall/sdk/streams` and `clutchcall.streams`. The control
//! plane (`Streams`) talks the BFF tRPC over HTTPS via `ureq`; the data
//! plane (`BroadcastViewer` / `BroadcastPublisher`) wraps the [`MoqtClient`]
//! substrate so the integrator doesn't deal with relay path conventions
//! or the WT subprotocol.
//!
//! ```no_run
//! use clutchcall_sdk::streams::{Streams, BroadcastViewer};
//!
//! let s = Streams::new("https://app.clutchcall.dev", "tqs_...", "org_abc")?;
//! let inp = s.live_inputs().get("li_xyz")?;
//! let ticket = inp.signed_playback_url(3600)?;
//! let viewer = BroadcastViewer::open(&ticket.url,
//!     Box::new(|is_init, chunk| println!("chunk init={} {} B", is_init, chunk.data.len())),
//!     None)?;
//! # Ok::<_, Box<dyn std::error::Error>>(())
//! ```

use crate::moqt::{MoqtClient, FramePublication, FrameSubscription};
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

// ── error ───────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct StreamsError(pub String);

impl fmt::Display for StreamsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl Error for StreamsError {}

impl From<ureq::Error> for StreamsError {
    fn from(e: ureq::Error) -> Self { StreamsError(format!("ureq: {e}")) }
}
impl From<std::io::Error> for StreamsError {
    fn from(e: std::io::Error) -> Self { StreamsError(format!("io: {e}")) }
}
impl From<serde_json::Error> for StreamsError {
    fn from(e: serde_json::Error) -> Self { StreamsError(format!("json: {e}")) }
}

// ── control plane ───────────────────────────────────────────────────────

pub struct Streams {
    base_url: String,
    api_key:  String,
    org_id:   String,
    agent:    ureq::Agent,
}

#[derive(Deserialize)]
struct TrpcEnvelope<T> {
    result: Option<TrpcResult<T>>,
    error:  Option<TrpcError>,
}
#[derive(Deserialize)]
struct TrpcResult<T> { data: T }
#[derive(Deserialize)]
struct TrpcError { message: String }

impl Streams {
    pub fn new(base_url: &str, api_key: &str, org_id: &str) -> Result<Self, StreamsError> {
        if base_url.is_empty() { return Err(StreamsError("baseUrl required".into())); }
        if api_key.is_empty()  { return Err(StreamsError("apiKey required".into())); }
        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key:  api_key.to_string(),
            org_id:   org_id.to_string(),
            agent:    ureq::Agent::new(),
        })
    }

    pub fn live_inputs(&self) -> LiveInputs<'_> { LiveInputs { s: self } }

    fn call<T: for<'de> Deserialize<'de>, B: Serialize>(
        &self, path: &str, payload: &B, mutation: bool,
    ) -> Result<T, StreamsError> {
        let url = format!("{}/api/trpc/{}", self.base_url, path);
        let bearer = format!("Bearer {}", self.api_key);
        let body: TrpcEnvelope<T> = if mutation {
            self.agent.post(&url)
                .set("authorization", &bearer)
                .set("content-type", "application/json")
                .send_json(payload)?
                .into_json()?
        } else {
            let q = serde_json::to_string(payload)?;
            self.agent.get(&url)
                .set("authorization", &bearer)
                .query("input", &q)
                .call()?
                .into_json()?
        };
        if let Some(err) = body.error {
            return Err(StreamsError(err.message));
        }
        body.result.map(|r| r.data)
            .ok_or_else(|| StreamsError(format!("tRPC {path}: empty result")))
    }
}

// ── live inputs ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LiveInputData {
    pub id: String,
    pub external_input_id: String,
    pub name: String,
    pub status: String,
    pub ingest: Option<String>,
    #[serde(rename = "createdAt")]
    pub created_at: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SignedPlaybackUrl {
    pub url: String,
    pub kid: String,
    pub alg: String,
    pub expires_at: i64,
}

#[derive(Debug, Clone)]
pub struct LiveInputWithSecret {
    pub input: LiveInputData,
    pub stream_key: String,
}

pub struct LiveInputs<'a> { s: &'a Streams }

impl<'a> LiveInputs<'a> {
    pub fn create(&self, name: &str, ingest: Option<&str>) -> Result<LiveInputWithSecret, StreamsError> {
        if self.s.org_id.is_empty() {
            return Err(StreamsError("liveInputs.create: orgId required".into()));
        }
        #[derive(Deserialize)]
        struct Row {
            #[serde(flatten)] data: LiveInputData,
            stream_key_cleartext: String,
        }
        let row: Row = self.s.call(
            "streams.liveInputs.create",
            &serde_json::json!({
                "orgId":  self.s.org_id,
                "name":   name,
                "ingest": ingest.unwrap_or("fmp4"),
            }),
            true,
        )?;
        Ok(LiveInputWithSecret { input: row.data, stream_key: row.stream_key_cleartext })
    }

    pub fn get(&self, id: &str) -> Result<LiveInput<'a>, StreamsError> {
        if self.s.org_id.is_empty() {
            return Err(StreamsError("liveInputs.get: orgId required".into()));
        }
        let data: LiveInputData = self.s.call(
            "streams.liveInputs.get",
            &serde_json::json!({ "orgId": self.s.org_id, "id": id }),
            false,
        )?;
        Ok(LiveInput { s: self.s, data })
    }
}

pub struct LiveInput<'a> {
    s: &'a Streams,
    pub data: LiveInputData,
}

impl<'a> LiveInput<'a> {
    pub fn signed_playback_url(&self, ttl_seconds: u32) -> Result<SignedPlaybackUrl, StreamsError> {
        if self.s.org_id.is_empty() {
            return Err(StreamsError("signed_playback_url: orgId required".into()));
        }
        #[derive(Deserialize)]
        struct Resp {
            token: String, kid: String, alg: String,
            expires_at: i64, input: String,
        }
        let r: Resp = self.s.call(
            "streams.liveInputs.mintPlaybackToken",
            &serde_json::json!({
                "orgId": self.s.org_id,
                "id":    self.data.id,
                "ttlSeconds": ttl_seconds,
            }),
            true,
        )?;
        Ok(SignedPlaybackUrl {
            url: format!("moq://relay.clutchcall.dev/playback/{}?tok={}", r.input, r.token),
            kid: r.kid, alg: r.alg, expires_at: r.expires_at,
        })
    }
}

// ── viewer ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseReason {
    Complete,
    AuthFailed,
    Network,
    ClosedByCaller,
}

pub struct BroadcastChunk {
    pub data: Vec<u8>,
    pub timestamp_us: u64,
    pub priority: u8,
    pub is_init: bool,
}

pub type OnChunk  = Box<dyn FnMut(bool, BroadcastChunk) + Send>;
pub type OnClose  = Box<dyn FnMut(CloseReason, Option<String>) + Send>;

pub struct BroadcastViewer {
    client: MoqtClient,
    _sub:   FrameSubscription,
}

impl BroadcastViewer {
    pub fn open(moq_url: &str, mut on_chunk: OnChunk, _on_close: Option<OnClose>) -> Result<Self, StreamsError> {
        let (wt_url, token, namespace) = parse_playback_url(moq_url)?;
        let client = MoqtClient::connect(&wt_url, &token, |_state| {})
            .ok_or_else(|| StreamsError("moqt connect failed".into()))?;
        let mut saw_init = false;
        let sub = client.subscribe_frame(&namespace, "broadcast", move |ts, prio, data| {
            let is_init = !saw_init;
            saw_init = true;
            on_chunk(is_init, BroadcastChunk {
                data: data.to_vec(),
                timestamp_us: ts,
                priority: prio,
                is_init,
            });
        }).ok_or_else(|| StreamsError("subscribe_frame failed".into()))?;
        Ok(Self { client, _sub: sub })
    }

    pub fn close(self) { drop(self.client) }
}

fn parse_playback_url(moq_url: &str) -> Result<(String, String, String), StreamsError> {
    if !moq_url.starts_with("moq://") {
        return Err(StreamsError(format!("expected moq:// URL, got {:?}", &moq_url[..moq_url.len().min(32)])));
    }
    // Split [host/path]?[query]
    let rest = &moq_url["moq://".len()..];
    let (path_part, query_part) = match rest.split_once('?') {
        Some((a, b)) => (a, b),
        None => (rest, ""),
    };
    let mut parts = path_part.split('/');
    let _host = parts.next().ok_or_else(|| StreamsError("malformed URL".into()))?;
    let kind  = parts.next().ok_or_else(|| StreamsError("missing /playback/ segment".into()))?;
    let input = parts.next().ok_or_else(|| StreamsError("missing input id".into()))?;
    if kind != "playback" || input.is_empty() {
        return Err(StreamsError("URL must look like moq://<host>/playback/<input_id>?tok=…".into()));
    }
    let token = query_part.split('&')
        .find_map(|kv| kv.split_once('=').and_then(|(k, v)| (k == "tok").then_some(v)))
        .ok_or_else(|| StreamsError("playback URL missing ?tok=<jwt>".into()))?
        .to_string();
    Ok((moq_url.to_string(), token, format!("playback/{input}")))
}

// ── publisher ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default)]
pub struct PublisherCodecs {
    pub video: Option<String>, // e.g. "avc1.42E01F"
    pub audio: Option<String>, // e.g. "opus"
}

pub struct BroadcastPublisher {
    client:     MoqtClient,
    track:      FramePublication,
    wrote_init: bool,
}

impl BroadcastPublisher {
    pub fn open(
        input_id: &str,
        stream_key: &str,
        codecs: PublisherCodecs,
        relay_host: Option<&str>,
    ) -> Result<Self, StreamsError> {
        if input_id.is_empty()   { return Err(StreamsError("inputId required".into())); }
        if stream_key.is_empty() { return Err(StreamsError("streamKey required".into())); }
        let host = relay_host.unwrap_or("relay.clutchcall.dev");
        let encoded_key = utf8_percent_encode(stream_key, NON_ALPHANUMERIC).to_string();
        let url = format!("moq://{host}/publish/{input_id}?sk={encoded_key}");
        let client = MoqtClient::connect(&url, "", |_| {})
            .ok_or_else(|| StreamsError("moqt connect failed".into()))?;
        let schema_tag = match (&codecs.video, &codecs.audio) {
            (Some(v), Some(a)) => format!("{v},{a}"),
            (Some(v), None)    => v.clone(),
            (None, Some(a))    => a.clone(),
            (None, None)       => String::new(),
        };
        let track = client.publish_frame(
            &format!("publish/{input_id}"), "broadcast",
            "media.broadcast", &schema_tag, 0,
        ).ok_or_else(|| StreamsError("publish_frame failed".into()))?;
        Ok(Self { client, track, wrote_init: false })
    }

    pub fn write(&mut self, chunk: &[u8]) {
        let ts = SystemTime::now().duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64).unwrap_or(0);
        let priority = if self.wrote_init { 1 } else { 0 };
        if !self.wrote_init { self.wrote_init = true; }
        self.track.write(ts, chunk, priority);
    }

    pub fn close(self) {
        drop(self.track);
        drop(self.client);
    }
}

