// Rust SDK smoke test against a live clutch relay: a publisher client and a
// subscriber client round-trip an audio track through the relay. Proves the
// Rust FFI binding end to end.  Run:
//   RUSTFLAGS="-L <ffi-dir>" LD_LIBRARY_PATH=<ffi-dir> \
//     RELAY_URL=quic://127.0.0.1:4443 cargo run --example relay_smoke
use clutchcall_sdk::moqt::MoqtClient;
use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::{thread, time::Duration};

fn main() {
    let url = env::var("RELAY_URL").unwrap_or_else(|_| "quic://127.0.0.1:4443".into());
    let (ns, name, n) = ("smoke/rust", "voice", 60u64);
    let recv = Arc::new(AtomicU64::new(0));

    let subc = MoqtClient::connect(&url, "", |_s| {}).expect("connect sub");
    let pubc = MoqtClient::connect(&url, "", |_s| {}).expect("connect pub");
    thread::sleep(Duration::from_millis(1500));

    let r2 = recv.clone();
    let bad = Arc::new(AtomicU64::new(0));
    let b2 = bad.clone();
    let _sub = subc
        .subscribe_frame(ns, name, move |_ts, prio, _data| {
            r2.fetch_add(1, Ordering::Relaxed);
            if prio != 200 {
                b2.fetch_add(1, Ordering::Relaxed);
            }
        })
        .expect("subscribe");
    let publication = pubc
        .publish_frame(ns, name, "ros.telemetry", "smoke/bin", 128)
        .expect("publish");
    thread::sleep(Duration::from_millis(500));

    for i in 0..n {
        publication.write(i * 1000, &[i as u8; 4], 200);
        thread::sleep(Duration::from_millis(20));
    }
    thread::sleep(Duration::from_millis(1500));

    let got = recv.load(Ordering::Relaxed);
    let prio_ok = bad.load(Ordering::Relaxed) == 0;
    println!("[rust] sent={} received={} priority_ok={}", n, got, prio_ok);
    if got >= n / 2 && prio_ok {
        println!(">>> RUST SDK: PASS");
    } else {
        println!(">>> RUST SDK: FAIL");
        std::process::exit(1);
    }
}
