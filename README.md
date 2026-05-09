# ClutchCall Rust SDK

Idiomatic Rust client for ClutchCall — telephony origination, media streaming,
and zero-trust JWT auth, built on Tokio + Quinn (QUIC).

## Add to your project

```toml
[dependencies]
clutchcall-sdk = "1.0"
tokio          = { version = "1", features = ["full"] }
```

## Quick start

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
