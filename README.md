# ClutchCall Rust SDK

The official Rust crate for ClutchCall. **Modality-oriented**: each modality
is its own module, all riding the same MoQT substrate underneath. Mix as
needed in one binary.

| Module                  | Modality                                            | Status |
| ----------------------- | --------------------------------------------------- | ------ |
| `clutchcall_sdk::streams`  | Live broadcasts + signed playback URLs           | **GA** |
| `clutchcall_sdk::robotics` | Robotics topic pub/sub (ROS 2 CDR)               | **GA** |
| `clutchcall_sdk::games`    | Games (rooms, state/input/event channels)        | **GA** |
| `clutchcall_sdk::data`     | MQTT-style typed pub/sub (`+` / `#` filters)     | **GA** |
| `clutchcall_sdk::voice`    | Voice (calls + bidirectional audio bridge)       | **GA** |
| `clutchcall_sdk::moqt`     | Realtime tracks (audio/video/frame)              | GA     |
| `clutchcall_sdk::client`   | Legacy voice surface (`ClutchCallClient`) — kept for backwards compat | legacy |

## Add to your project

```toml
[dependencies]
clutchcall-sdk = "1.0"
tokio          = { version = "1", features = ["full"] }
```

## Streams — watch a live broadcast

```rust
use clutchcall_sdk::streams::{Streams, BroadcastViewer};

let s   = Streams::new("https://app.clutchcall.dev", &api_key, "org_abc")?;
let inp = s.live_inputs().get("li_xyz")?;
let t   = inp.signed_playback_url(3600)?;

let viewer = BroadcastViewer::open(&t.url,
    Box::new(|is_init, chunk| { /* feed chunk.data to MSE / file */ }),
    None)?;
```

## Robotics — telemetry + commands

```rust
use clutchcall_sdk::robotics::{Robotics, QoSProfile, Reliability};

let mut r = Robotics::new(&token, "turtlebot-7")?;
let odom = r.publish_telemetry("odom", "nav_msgs/msg/Odometry",
    QoSProfile { reliability: Reliability::Reliable, ..Default::default() })?;
odom.write(&cdr_bytes)?;
```

## Games — multiplayer rooms

```rust
use clutchcall_sdk::games::Games;

// Authoritative server (None player_id)
let mut auth = Games::new(&token, "duel-42", None)?;
let state = auth.publish_state(Some(30))?;
let _sub  = auth.subscribe_inputs(|pid, bytes| { /* apply */ })?;
```

## Data — MQTT-style pub/sub

```rust
use clutchcall_sdk::data::Data;

let mut d = Data::new(&token, "device-7")?;
d.publish("sensors/room1/temperature", b"23.5", false, false)?;
let _sub = d.subscribe("sensors/+/temperature", |m| {
    println!("{} ← {}: {:?}", m.topic, m.from_client_id, m.payload);
})?;
```

## Voice — calls + audio bridge

```rust
use clutchcall_sdk::voice::{Voice, OriginateArgs, AudioBridgeOpts, Codec};

let v    = Voice::new("https://app.clutchcall.dev", &api_key, "org_abc")?;
let call = v.calls().originate(OriginateArgs {
    to: "+15551234567", from: "+15558675309",
    trunk_id: "trunk_main", agent: Some("healthcare-assistant"),
    ..Default::default()
})?;

let bridge = v.audio_bridge().attach(&call.data.sid,
    AudioBridgeOpts { codec: Codec::Opus, ..Default::default() },
    |frame, ts_us| { asr.feed(frame); })?;
// … later
call.hangup()?;
```

### Legacy voice surface

The original `ClutchCallClient` (at the root) is kept for backwards compat:

```rust
use clutchcall_sdk::ClutchCallClient;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = ClutchCallClient::connect("pbx.clutchcall.com:443").await?;

    let resp = client
        .originate(
            "+1234567890",
            "wss://my-chatbot.com/media",
        )
        .await?;

    println!("call sid = {}", resp.call_sid);
    Ok(())
}
```

The native FFI core (`libclutchcall_core_ffi.{so,dylib,dll}`) is loaded at
runtime via `libloading`. Set `CLUTCHCALL_LIB_PATH` if it isn't on the default
loader path.

## Crate layout

- `src/client.rs`     — high-level `ClutchCallClient` (originate, hangup, media).
- `src/ffi.rs`        — `extern "C"` bindings to the C++ core.
- `src/method_id.rs`  — wire-format method IDs (mirrored across all SDKs).
- `tests/`            — integration tests against a mock gateway.
