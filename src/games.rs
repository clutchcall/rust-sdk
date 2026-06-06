//! Games modality — multiplayer rooms over MoQT.
//!
//! Three channels per room (state, input, event) mapped onto the MoQT
//! substrate with the right priority + QUIC lane intent per channel.
//! Namespaces baked in; input + event frames carry a 1-byte from-header so
//! the server's single subscribe callback can sort frames by player.

use crate::moqt::{MoqtClient, FramePublication, FrameSubscription};
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use std::error::Error;
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub struct GamesError(pub String);
impl fmt::Display for GamesError { fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "{}", self.0) } }
impl Error for GamesError {}

// ── wire ────────────────────────────────────────────────────────────────

pub const FROM_HEADER_BYTES: usize = 1;
pub const MAX_FROM_LEN:      usize = 0xFF;

pub fn encode_with_from(from_player_id: &str, payload: &[u8]) -> Result<Vec<u8>, GamesError> {
    if from_player_id.len() > MAX_FROM_LEN {
        return Err(GamesError(format!("from_player_id > 255 ({})", from_player_id.len())));
    }
    let mut out = Vec::with_capacity(1 + from_player_id.len() + payload.len());
    out.push(from_player_id.len() as u8);
    out.extend_from_slice(from_player_id.as_bytes());
    out.extend_from_slice(payload);
    Ok(out)
}

pub struct DecodedWithFrom {
    pub from_player_id: String,
    pub payload:        Vec<u8>,
}

pub fn decode_with_from(buf: &[u8]) -> Result<DecodedWithFrom, GamesError> {
    if buf.is_empty() { return Err(GamesError("frame too short".into())); }
    let n = buf[0] as usize;
    if buf.len() < 1 + n { return Err(GamesError(format!("truncated (from_len={n})"))); }
    Ok(DecodedWithFrom {
        from_player_id: String::from_utf8_lossy(&buf[1..1 + n]).to_string(),
        payload:        buf[1 + n..].to_vec(),
    })
}

// ── handles ─────────────────────────────────────────────────────────────

/// Server-only state publisher (no from-header).
pub struct StatePublisher { track: FramePublication }

impl StatePublisher {
    pub fn write(&self, state_bytes: &[u8]) {
        let ts = SystemTime::now().duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64).unwrap_or(0);
        self.track.write(ts, state_bytes, 100);
    }
}

/// Player-bound publisher — prefixes every frame with the caller's player id.
pub struct FromPublisher {
    track:           FramePublication,
    from_player_id:  String,
    default_priority: u8,
}

impl FromPublisher {
    pub fn write(&self, payload: &[u8]) -> Result<(), GamesError> {
        let frame = encode_with_from(&self.from_player_id, payload)?;
        let ts = SystemTime::now().duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64).unwrap_or(0);
        self.track.write(ts, &frame, self.default_priority);
        Ok(())
    }
}

pub struct Subscription { _sub: FrameSubscription }

// ── client ──────────────────────────────────────────────────────────────

pub struct Games {
    relay_host: String,
    token:      String,
    room_id:    String,
    player_id:  Option<String>,
    client:     Option<MoqtClient>,
}

impl Games {
    pub fn new(token: &str, room_id: &str, player_id: Option<&str>) -> Result<Self, GamesError> {
        if token.is_empty()   { return Err(GamesError("token required".into())); }
        if room_id.is_empty() { return Err(GamesError("room_id required".into())); }
        Ok(Self {
            relay_host: "relay.clutchcall.dev".into(),
            token: token.into(),
            room_id: room_id.into(),
            player_id: player_id.map(|s| s.into()),
            client: None,
        })
    }

    pub fn with_relay_host(mut self, host: &str) -> Self { self.relay_host = host.into(); self }

    pub fn state_ns(&self) -> String { format!("game/{}/state", self.room_id) }
    pub fn input_ns(&self) -> String { format!("game/{}/input", self.room_id) }
    pub fn event_ns(&self, channel: &str) -> String {
        format!("game/{}/event/{}", self.room_id,
                utf8_percent_encode(channel, NON_ALPHANUMERIC))
    }

    fn ensure(&mut self) -> Result<&MoqtClient, GamesError> {
        if self.client.is_none() {
            let pid_segment = match &self.player_id {
                Some(p) => format!("/{}", utf8_percent_encode(p, NON_ALPHANUMERIC)),
                None => "/_authority".into(),
            };
            let url = format!("moq://{}/games/{}{pid_segment}",
                self.relay_host,
                utf8_percent_encode(&self.room_id, NON_ALPHANUMERIC));
            self.client = Some(
                MoqtClient::connect(&url, &self.token, |_| {})
                    .ok_or_else(|| GamesError("moqt connect failed".into()))?,
            );
        }
        Ok(self.client.as_ref().unwrap())
    }

    pub fn publish_state(&mut self, tick_hz: Option<u32>) -> Result<StatePublisher, GamesError> {
        let ns = self.state_ns();
        let client = self.ensure()?;
        let tag = match tick_hz {
            Some(hz) => format!("game/state;tickHz={hz}"),
            None     => "game/state".into(),
        };
        let track = client.publish_frame(&ns, "tick", "game.state", &tag, 100)
            .ok_or_else(|| GamesError("publish_frame failed".into()))?;
        Ok(StatePublisher { track })
    }

    pub fn subscribe_state<F: FnMut(&[u8]) + Send + 'static>(
        &mut self, mut on_state: F,
    ) -> Result<Subscription, GamesError> {
        let ns = self.state_ns();
        let client = self.ensure()?;
        let sub = client.subscribe_frame(&ns, "tick", move |_ts, _prio, data| on_state(data))
            .ok_or_else(|| GamesError("subscribe_frame failed".into()))?;
        Ok(Subscription { _sub: sub })
    }

    pub fn publish_input(&mut self) -> Result<FromPublisher, GamesError> {
        let pid = self.player_id.clone()
            .ok_or_else(|| GamesError("publish_input: player_id required".into()))?;
        let ns = self.input_ns();
        let client = self.ensure()?;
        let track = client.publish_frame(&ns, "frame", "game.input", "game/input", 100)
            .ok_or_else(|| GamesError("publish_frame failed".into()))?;
        Ok(FromPublisher { track, from_player_id: pid, default_priority: 100 })
    }

    pub fn subscribe_inputs<F: FnMut(&str, &[u8]) + Send + 'static>(
        &mut self, mut on_input: F,
    ) -> Result<Subscription, GamesError> {
        let ns = self.input_ns();
        let client = self.ensure()?;
        let sub = client.subscribe_frame(&ns, "frame", move |_ts, _prio, data| {
            if let Ok(d) = decode_with_from(data) {
                on_input(&d.from_player_id, &d.payload);
            }
        }).ok_or_else(|| GamesError("subscribe_frame failed".into()))?;
        Ok(Subscription { _sub: sub })
    }

    pub fn publish_event(&mut self, channel: &str) -> Result<FromPublisher, GamesError> {
        if channel.is_empty() { return Err(GamesError("channel required".into())); }
        let from = self.player_id.clone().unwrap_or_else(|| "_authority".into());
        let ns = self.event_ns(channel);
        let tag = format!("game/event;channel={channel}");
        let client = self.ensure()?;
        let track = client.publish_frame(&ns, "msg", "game.event", &tag, 50)
            .ok_or_else(|| GamesError("publish_frame failed".into()))?;
        Ok(FromPublisher { track, from_player_id: from, default_priority: 50 })
    }

    pub fn subscribe_events<F: FnMut(&str, &[u8]) + Send + 'static>(
        &mut self, channel: &str, mut on_event: F,
    ) -> Result<Subscription, GamesError> {
        if channel.is_empty() { return Err(GamesError("channel required".into())); }
        let ns = self.event_ns(channel);
        let client = self.ensure()?;
        let sub = client.subscribe_frame(&ns, "msg", move |_ts, _prio, data| {
            if let Ok(d) = decode_with_from(data) {
                on_event(&d.from_player_id, &d.payload);
            }
        }).ok_or_else(|| GamesError("subscribe_frame failed".into()))?;
        Ok(Subscription { _sub: sub })
    }
}
