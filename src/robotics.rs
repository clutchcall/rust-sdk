//! Robotics modality — typed pub/sub for a robot fleet over MoQT.
//!
//! Bidirectional teleop convention baked in: telemetry on `robot/<id>`,
//! commands on `robot/<id>/ctl`. Wire format adds a u16 BE type-name
//! prefix so cross-language subscribers pick the right deserializer.
//! Mirrors `@clutchcall/sdk/robotics` and `clutchcall.robotics`.

use crate::moqt::{MoqtClient, FramePublication, FrameSubscription};
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use std::error::Error;
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

// ── error ───────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct RoboticsError(pub String);

impl fmt::Display for RoboticsError { fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "{}", self.0) } }
impl Error for RoboticsError {}

// ── wire ────────────────────────────────────────────────────────────────

pub const HEADER_BYTES: usize  = 2;
pub const MAX_TYPE_NAME: usize = 0xFFFF;

pub fn encode_frame(type_name: &str, payload: &[u8]) -> Result<Vec<u8>, RoboticsError> {
    if type_name.is_empty() { return Err(RoboticsError("type_name required".into())); }
    if type_name.len() > MAX_TYPE_NAME {
        return Err(RoboticsError(format!("type_name > 65535 ({})", type_name.len())));
    }
    let mut out = Vec::with_capacity(HEADER_BYTES + type_name.len() + payload.len());
    out.push((type_name.len() >> 8) as u8);
    out.push((type_name.len() & 0xff) as u8);
    out.extend_from_slice(type_name.as_bytes());
    out.extend_from_slice(payload);
    Ok(out)
}

#[derive(Debug, Clone)]
pub struct DecodedFrame {
    pub type_name: String,
    pub payload:   Vec<u8>,
}

pub fn decode_frame(buf: &[u8]) -> Result<DecodedFrame, RoboticsError> {
    if buf.len() < HEADER_BYTES { return Err(RoboticsError("frame too short".into())); }
    let n = ((buf[0] as usize) << 8) | (buf[1] as usize);
    let end = HEADER_BYTES + n;
    if buf.len() < end { return Err(RoboticsError(format!("truncated (type_name_len={n})"))); }
    Ok(DecodedFrame {
        type_name: String::from_utf8_lossy(&buf[HEADER_BYTES..end]).to_string(),
        payload:   buf[end..].to_vec(),
    })
}

// ── QoS ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reliability { BestEffort, Reliable }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability { Volatile, TransientLocal }

#[derive(Debug, Clone, Copy)]
pub struct QoSProfile {
    pub reliability: Reliability,
    pub durability:  Durability,
    pub depth:       u32,
}

impl Default for QoSProfile {
    fn default() -> Self {
        Self { reliability: Reliability::BestEffort, durability: Durability::Volatile, depth: 10 }
    }
}

impl QoSProfile {
    fn capability(&self) -> &'static str {
        match (self.durability, self.reliability) {
            (Durability::TransientLocal, Reliability::Reliable)   => "ros.tl_reliable",
            (Durability::TransientLocal, Reliability::BestEffort) => "ros.tl_be",
            (_, Reliability::Reliable)                            => "ros.reliable",
            (_, Reliability::BestEffort)                          => "ros.best_effort",
        }
    }
    fn default_priority(&self) -> u8 {
        match self.reliability { Reliability::Reliable => 50, Reliability::BestEffort => 100 }
    }
}

// ── handles ─────────────────────────────────────────────────────────────

pub struct Publication {
    track:            FramePublication,
    type_name:        String,
    default_priority: u8,
}

impl Publication {
    pub fn write(&self, payload: &[u8]) -> Result<(), RoboticsError> {
        self.write_with_priority(payload, self.default_priority)
    }
    pub fn write_with_priority(&self, payload: &[u8], priority: u8) -> Result<(), RoboticsError> {
        let frame = encode_frame(&self.type_name, payload)?;
        let ts = SystemTime::now().duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64).unwrap_or(0);
        self.track.write(ts, &frame, priority);
        Ok(())
    }
}

pub struct Subscription {
    _sub: FrameSubscription,
    pub ns:   String,
    pub name: String,
}

// ── client ──────────────────────────────────────────────────────────────

pub struct Robotics {
    relay_host: String,
    token:      String,
    robot_id:   String,
    client:     Option<MoqtClient>,
}

impl Robotics {
    pub fn new(token: &str, robot_id: &str) -> Result<Self, RoboticsError> {
        if token.is_empty()    { return Err(RoboticsError("token required".into())); }
        if robot_id.is_empty() { return Err(RoboticsError("robot_id required".into())); }
        Ok(Self {
            relay_host: "relay.clutchcall.dev".into(),
            token: token.into(),
            robot_id: robot_id.into(),
            client: None,
        })
    }

    pub fn with_relay_host(mut self, host: &str) -> Self { self.relay_host = host.into(); self }

    pub fn telemetry_ns(&self) -> String { format!("robot/{}", self.robot_id) }
    pub fn command_ns(&self) -> String   { format!("robot/{}/ctl", self.robot_id) }

    fn ensure(&mut self) -> Result<&MoqtClient, RoboticsError> {
        if self.client.is_none() {
            let id = utf8_percent_encode(&self.robot_id, NON_ALPHANUMERIC).to_string();
            let url = format!("moq://{}/robotics/{id}", self.relay_host);
            self.client = Some(
                MoqtClient::connect(&url, &self.token, |_| {})
                    .ok_or_else(|| RoboticsError("moqt connect failed".into()))?,
            );
        }
        Ok(self.client.as_ref().unwrap())
    }

    pub fn publish_telemetry(&mut self, topic: &str, type_name: &str, qos: QoSProfile) -> Result<Publication, RoboticsError> {
        let ns = self.telemetry_ns();
        self.publish(&ns, topic, type_name, qos)
    }
    pub fn subscribe_telemetry<F: FnMut(&str, &[u8]) + Send + 'static>(
        &mut self, topic: &str, on_message: F,
    ) -> Result<Subscription, RoboticsError> {
        let ns = self.telemetry_ns();
        self.subscribe(&ns, topic, on_message)
    }
    pub fn publish_command(&mut self, topic: &str, type_name: &str, qos: QoSProfile) -> Result<Publication, RoboticsError> {
        let ns = self.command_ns();
        self.publish(&ns, topic, type_name, qos)
    }
    pub fn subscribe_command<F: FnMut(&str, &[u8]) + Send + 'static>(
        &mut self, topic: &str, on_message: F,
    ) -> Result<Subscription, RoboticsError> {
        let ns = self.command_ns();
        self.subscribe(&ns, topic, on_message)
    }

    fn publish(&mut self, ns: &str, name: &str, type_name: &str, qos: QoSProfile) -> Result<Publication, RoboticsError> {
        let client = self.ensure()?;
        let schema = format!("ros2/cdr;type={type_name}");
        let track = client.publish_frame(ns, name, qos.capability(), &schema, 0)
            .ok_or_else(|| RoboticsError("publish_frame failed".into()))?;
        Ok(Publication { track, type_name: type_name.to_string(), default_priority: qos.default_priority() })
    }

    fn subscribe<F: FnMut(&str, &[u8]) + Send + 'static>(
        &mut self, ns: &str, name: &str, mut on_message: F,
    ) -> Result<Subscription, RoboticsError> {
        let client = self.ensure()?;
        let sub = client.subscribe_frame(ns, name, move |_ts, _prio, data| {
            if let Ok(f) = decode_frame(data) {
                on_message(&f.type_name, &f.payload);
            }
        }).ok_or_else(|| RoboticsError("subscribe_frame failed".into()))?;
        Ok(Subscription { _sub: sub, ns: ns.to_string(), name: name.to_string() })
    }
}
