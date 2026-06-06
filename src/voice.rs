//! Voice modality — telephony over QUIC (MoQT).
//!
//! Two primitives: `Calls` (control plane — originate, transfer, hangup
//! over the BFF tRPC via `ureq`) and `AudioBridge` (data plane —
//! bidirectional Opus / PCM / G.711 over MoQT with the
//! `voice/<sid>/{uplink,downlink}` namespace convention enforced).
//! Mirrors the TypeScript `@clutchcall/sdk/voice` and the Python
//! `clutchcall.voice` modules.

use crate::moqt::{MoqtClient, AudioPublication, AudioSubscription};
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub struct VoiceError(pub String);

impl fmt::Display for VoiceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "{}", self.0) }
}
impl Error for VoiceError {}

impl From<ureq::Error> for VoiceError {
    fn from(e: ureq::Error) -> Self { VoiceError(format!("ureq: {e}")) }
}
impl From<std::io::Error> for VoiceError {
    fn from(e: std::io::Error) -> Self { VoiceError(format!("io: {e}")) }
}
impl From<serde_json::Error> for VoiceError {
    fn from(e: serde_json::Error) -> Self { VoiceError(format!("json: {e}")) }
}

// ── client ──────────────────────────────────────────────────────────────

pub struct Voice {
    base_url:   String,
    api_key:    String,
    org_id:     String,
    pub relay_host: String,
    agent:      ureq::Agent,
}

#[derive(Deserialize)]
struct TrpcEnvelope<T> { result: Option<TrpcResult<T>>, error: Option<TrpcError> }
#[derive(Deserialize)]
struct TrpcResult<T> { data: T }
#[derive(Deserialize)]
struct TrpcError { message: String }

impl Voice {
    pub fn new(base_url: &str, api_key: &str, org_id: &str) -> Result<Self, VoiceError> {
        if base_url.is_empty() { return Err(VoiceError("baseUrl required".into())); }
        if api_key.is_empty()  { return Err(VoiceError("apiKey required".into())); }
        if org_id.is_empty()   { return Err(VoiceError("orgId required".into())); }
        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key:  api_key.to_string(),
            org_id:   org_id.to_string(),
            relay_host: "relay.clutchcall.dev".into(),
            agent:    ureq::Agent::new(),
        })
    }

    pub fn calls(&self)        -> Calls<'_>              { Calls { v: self } }
    pub fn audio_bridge(&self) -> AudioBridgeFactory<'_> { AudioBridgeFactory { v: self } }
    pub fn agents(&self)       -> Agents<'_>             { Agents { v: self } }

    fn call<T: for<'de> Deserialize<'de>, B: Serialize>(
        &self, path: &str, payload: &B, mutation: bool,
    ) -> Result<T, VoiceError> {
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
            return Err(VoiceError(err.message));
        }
        body.result.map(|r| r.data)
            .ok_or_else(|| VoiceError(format!("tRPC {path}: empty result")))
    }
}

// ── calls ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CallData {
    pub sid: String,
    pub status: String,
    pub to: String,
    pub from: String,
    #[serde(rename = "startedAt", default)]
    pub started_at: String,
    #[serde(rename = "trunkId", default)]
    pub trunk_id: Option<String>,
    #[serde(default)]
    pub agent: Option<String>,
}

#[derive(Debug, Clone)]
pub struct OriginateArgs<'a> {
    pub to: &'a str,
    pub from: &'a str,
    pub trunk_id: &'a str,
    pub agent: Option<&'a str>,
    pub ring_timeout_sec: u32,
}

impl<'a> Default for OriginateArgs<'a> {
    fn default() -> Self {
        Self { to: "", from: "", trunk_id: "", agent: None, ring_timeout_sec: 30 }
    }
}

pub struct Calls<'a> { v: &'a Voice }

impl<'a> Calls<'a> {
    pub fn originate(&self, args: OriginateArgs<'_>) -> Result<Call<'a>, VoiceError> {
        let data: CallData = self.v.call(
            "voice.calls.originate",
            &serde_json::json!({
                "orgId":  self.v.org_id,
                "to":     args.to,
                "from":   args.from,
                "trunkId": args.trunk_id,
                "agent":  args.agent,
                "ringTimeoutSec": args.ring_timeout_sec,
            }),
            true,
        )?;
        Ok(Call { v: self.v, data })
    }

    pub fn get(&self, sid: &str) -> Result<Call<'a>, VoiceError> {
        let data: CallData = self.v.call(
            "voice.calls.get",
            &serde_json::json!({ "orgId": self.v.org_id, "sid": sid }),
            false,
        )?;
        Ok(Call { v: self.v, data })
    }
}

pub struct Call<'a> {
    v: &'a Voice,
    pub data: CallData,
}

impl<'a> Call<'a> {
    /// Transfer to a PSTN E.164 number.
    pub fn transfer_to(&self, to: &str) -> Result<(), VoiceError> {
        let _: serde_json::Value = self.v.call(
            "voice.calls.transfer",
            &serde_json::json!({ "orgId": self.v.org_id, "sid": self.data.sid, "to": to }),
            true,
        )?;
        Ok(())
    }
    /// Re-attach the call to a different agent.
    pub fn transfer_agent(&self, agent: &str) -> Result<(), VoiceError> {
        let _: serde_json::Value = self.v.call(
            "voice.calls.transfer",
            &serde_json::json!({ "orgId": self.v.org_id, "sid": self.data.sid, "agent": agent }),
            true,
        )?;
        Ok(())
    }
    pub fn hangup(&self) -> Result<(), VoiceError> {
        let _: serde_json::Value = self.v.call(
            "voice.calls.hangup",
            &serde_json::json!({ "orgId": self.v.org_id, "sid": self.data.sid }),
            true,
        )?;
        Ok(())
    }
}

// ── audio bridge ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Codec { Opus, Pcm16, G711ULaw, G711ALaw }
impl Codec {
    fn as_capability(self) -> &'static str {
        match self {
            Codec::Opus     => "voice/opus",
            Codec::Pcm16    => "voice/pcm16",
            Codec::G711ULaw => "voice/g711_ulaw",
            Codec::G711ALaw => "voice/g711_alaw",
        }
    }
}

pub struct AudioBridgeOpts {
    pub codec:       Codec,
    pub sample_rate: u32,
    pub channels:    u8,
    pub frame_ms:    u16,
}

impl Default for AudioBridgeOpts {
    fn default() -> Self {
        Self { codec: Codec::Opus, sample_rate: 48000, channels: 1, frame_ms: 20 }
    }
}

pub struct AudioBridge {
    _client: MoqtClient,
    pub_:    AudioPublication,
    _sub:    AudioSubscription,
    pub call_sid: String,
}

impl AudioBridge {
    pub fn publish_downlink(&self, frame: &[u8]) {
        let ts = SystemTime::now().duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64).unwrap_or(0);
        self.pub_.write(ts, frame);
    }
}

pub struct AudioBridgeFactory<'a> { v: &'a Voice }

impl<'a> AudioBridgeFactory<'a> {
    pub fn attach<F: FnMut(&[u8], u64) + Send + 'static>(
        &self, call_sid: &str, opts: AudioBridgeOpts, mut on_uplink: F,
    ) -> Result<AudioBridge, VoiceError> {
        if call_sid.is_empty() { return Err(VoiceError("attach: call_sid required".into())); }
        let url = format!("moq://{}/voice/{}", self.v.relay_host,
            utf8_percent_encode(call_sid, NON_ALPHANUMERIC));
        let client = MoqtClient::connect(&url, &self.v.api_key, |_| {})
            .ok_or_else(|| VoiceError("moqt connect failed".into()))?;
        let sub = client.subscribe_audio(&format!("voice/{call_sid}/uplink"), "audio",
            move |ts, frame| on_uplink(frame, ts))
            .ok_or_else(|| VoiceError("subscribe_audio failed".into()))?;
        let pub_ = client.publish_audio(&format!("voice/{call_sid}/downlink"), "audio",
            opts.codec.as_capability(), opts.sample_rate, opts.channels, opts.frame_ms)
            .ok_or_else(|| VoiceError("publish_audio failed".into()))?;
        Ok(AudioBridge {
            _client: client, pub_, _sub: sub,
            call_sid: call_sid.to_string(),
        })
    }
}

// ── agents ──────────────────────────────────────────────────────────────

pub struct Agents<'a> { v: &'a Voice }

impl<'a> Agents<'a> {
    pub fn attach(&self, call_sid: &str, agent: &str) -> Result<(), VoiceError> {
        if call_sid.is_empty() { return Err(VoiceError("attach: call_sid required".into())); }
        if agent.is_empty()    { return Err(VoiceError("attach: agent required".into())); }
        let _: serde_json::Value = self.v.call(
            "voice.agents.attach",
            &serde_json::json!({ "orgId": self.v.org_id, "sid": call_sid, "agent": agent }),
            true,
        )?;
        Ok(())
    }
}
