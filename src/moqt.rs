//! Capability-aware MoQT track pub/sub for Rust, over the shared
//! `clutchcall_moqt_ffi` C ABI (core/moqt_ffi.cc) — the same C++ engine the
//! Python/Go/C++ SDKs use. A published track carries a `capability` (intent/
//! routing key, e.g. "asr"/"tts"/"media.passthrough"); the relay/gateway routes
//! it to the module that registered that capability.
//!
//! ```no_run
//! # use clutchcall::moqt::MoqtClient;
//! let c = MoqtClient::connect("quic://relay.acme.dev:4443", "tok", |_state| {}).unwrap();
//! let pub_ = c.publish_audio("voice/acme/call-1", "mic", "asr", 48000, 1, 20).unwrap();
//! pub_.write(0, &[0u8; 960]);
//! let _sub = c.subscribe_audio("voice/acme/call-1", "agent", |_ts, _pcm| {});
//! ```
//!
//! Callbacks fire from the engine's background io_thread; the boxed closures are
//! kept alive for the handle's lifetime.

use std::ffi::CString;
use std::os::raw::{c_char, c_void};

type StateCb = extern "C" fn(*mut c_void, i32);
type FrameCb = extern "C" fn(*mut c_void, u64, *const u8, usize);
// Frame-track object callback carries a per-frame priority (robot telemetry).
type FrameObjCb = extern "C" fn(*mut c_void, u64, u8, *const u8, usize);

#[link(name = "clutchcall_moqt_ffi")]
extern "C" {
    fn clutch_moqt_connect(url: *const c_char, token: *const c_char, cb: StateCb, user: *mut c_void) -> *mut c_void;
    fn clutch_moqt_client_close(h: *mut c_void);
    fn clutch_moqt_publish_audio(h: *mut c_void, ns: *const c_char, name: *const c_char, capability: *const c_char, sample_rate: u32, channels: u8, frame_ms: u16) -> *mut c_void;
    fn clutch_moqt_pub_write(h: *mut c_void, ts_us: u64, data: *const u8, len: usize);
    fn clutch_moqt_pub_subscriber_count(h: *mut c_void) -> usize;
    fn clutch_moqt_pub_close(h: *mut c_void);
    fn clutch_moqt_subscribe_audio(h: *mut c_void, ns: *const c_char, name: *const c_char, cb: FrameCb, user: *mut c_void) -> *mut c_void;
    fn clutch_moqt_sub_close(h: *mut c_void);
    fn clutch_moqt_publish_frame(h: *mut c_void, ns: *const c_char, name: *const c_char, capability: *const c_char, schema_tag: *const c_char, default_priority: u8) -> *mut c_void;
    fn clutch_moqt_frame_write(h: *mut c_void, ts_us: u64, data: *const u8, len: usize, priority: u8);
    fn clutch_moqt_frame_pub_subscriber_count(h: *mut c_void) -> usize;
    fn clutch_moqt_frame_pub_close(h: *mut c_void);
    fn clutch_moqt_subscribe_frame(h: *mut c_void, ns: *const c_char, name: *const c_char, cb: FrameObjCb, user: *mut c_void) -> *mut c_void;
    fn clutch_moqt_frame_sub_close(h: *mut c_void);
}

type StateBox = Box<dyn FnMut(i32)>;
type FrameBox = Box<dyn FnMut(u64, &[u8])>;
type FrameObjBox = Box<dyn FnMut(u64, u8, &[u8])>;

extern "C" fn state_tramp(user: *mut c_void, st: i32) {
    if user.is_null() { return; }
    let cb = unsafe { &mut *(user as *mut StateBox) };
    cb(st);
}
extern "C" fn frame_tramp(user: *mut c_void, ts: u64, data: *const u8, len: usize) {
    if user.is_null() { return; }
    let cb = unsafe { &mut *(user as *mut FrameBox) };
    let slice = if data.is_null() || len == 0 { &[][..] } else { unsafe { std::slice::from_raw_parts(data, len) } };
    cb(ts, slice);
}
extern "C" fn frame_obj_tramp(user: *mut c_void, ts: u64, prio: u8, data: *const u8, len: usize) {
    if user.is_null() { return; }
    let cb = unsafe { &mut *(user as *mut FrameObjBox) };
    let slice = if data.is_null() || len == 0 { &[][..] } else { unsafe { std::slice::from_raw_parts(data, len) } };
    cb(ts, prio, slice);
}

/// A MoQT session against the relay.
pub struct MoqtClient { h: *mut c_void, state: *mut StateBox }
/// A live published audio track.
pub struct AudioPublication { h: *mut c_void }
/// A live subscription; frames arrive on the closure passed to `subscribe_audio`.
pub struct AudioSubscription { h: *mut c_void, frame: *mut FrameBox }
/// A live published frame track (opaque binary, per-frame priority).
pub struct FramePublication { h: *mut c_void }
/// A live frame subscription; objects arrive on the `subscribe_frame` closure.
pub struct FrameSubscription { h: *mut c_void, frame: *mut FrameObjBox }

impl MoqtClient {
    pub fn connect(url: &str, token: &str, on_state: impl FnMut(i32) + 'static) -> Option<Self> {
        let cu = CString::new(url).ok()?;
        let ct = CString::new(token).ok()?;
        let state = Box::into_raw(Box::new(Box::new(on_state) as StateBox));
        let h = unsafe { clutch_moqt_connect(cu.as_ptr(), ct.as_ptr(), state_tramp, state as *mut c_void) };
        if h.is_null() {
            unsafe { drop(Box::from_raw(state)); }
            return None;
        }
        Some(MoqtClient { h, state })
    }

    pub fn publish_audio(&self, ns: &str, name: &str, capability: &str, sample_rate: u32, channels: u8, frame_ms: u16) -> Option<AudioPublication> {
        let cn = CString::new(ns).ok()?;
        let cm = CString::new(name).ok()?;
        let cc = CString::new(capability).ok()?;
        let h = unsafe { clutch_moqt_publish_audio(self.h, cn.as_ptr(), cm.as_ptr(), cc.as_ptr(), sample_rate, channels, frame_ms) };
        if h.is_null() { None } else { Some(AudioPublication { h }) }
    }

    pub fn subscribe_audio(&self, ns: &str, name: &str, on_frame: impl FnMut(u64, &[u8]) + 'static) -> Option<AudioSubscription> {
        let cn = CString::new(ns).ok()?;
        let cm = CString::new(name).ok()?;
        let frame = Box::into_raw(Box::new(Box::new(on_frame) as FrameBox));
        let h = unsafe { clutch_moqt_subscribe_audio(self.h, cn.as_ptr(), cm.as_ptr(), frame_tramp, frame as *mut c_void) };
        if h.is_null() {
            unsafe { drop(Box::from_raw(frame)); }
            return None;
        }
        Some(AudioSubscription { h, frame })
    }

    pub fn publish_frame(&self, ns: &str, name: &str, capability: &str, schema_tag: &str, default_priority: u8) -> Option<FramePublication> {
        let cn = CString::new(ns).ok()?;
        let cm = CString::new(name).ok()?;
        let cc = CString::new(capability).ok()?;
        let cs = CString::new(schema_tag).ok()?;
        let h = unsafe { clutch_moqt_publish_frame(self.h, cn.as_ptr(), cm.as_ptr(), cc.as_ptr(), cs.as_ptr(), default_priority) };
        if h.is_null() { None } else { Some(FramePublication { h }) }
    }

    pub fn subscribe_frame(&self, ns: &str, name: &str, on_frame: impl FnMut(u64, u8, &[u8]) + 'static) -> Option<FrameSubscription> {
        let cn = CString::new(ns).ok()?;
        let cm = CString::new(name).ok()?;
        let frame = Box::into_raw(Box::new(Box::new(on_frame) as FrameObjBox));
        let h = unsafe { clutch_moqt_subscribe_frame(self.h, cn.as_ptr(), cm.as_ptr(), frame_obj_tramp, frame as *mut c_void) };
        if h.is_null() {
            unsafe { drop(Box::from_raw(frame)); }
            return None;
        }
        Some(FrameSubscription { h, frame })
    }
}

impl FramePublication {
    pub fn write(&self, timestamp_us: u64, data: &[u8], priority: u8) {
        unsafe { clutch_moqt_frame_write(self.h, timestamp_us, data.as_ptr(), data.len(), priority); }
    }
    pub fn subscriber_count(&self) -> usize {
        unsafe { clutch_moqt_frame_pub_subscriber_count(self.h) }
    }
}

impl Drop for FramePublication {
    fn drop(&mut self) { unsafe { clutch_moqt_frame_pub_close(self.h); } }
}
impl Drop for FrameSubscription {
    fn drop(&mut self) {
        unsafe { clutch_moqt_frame_sub_close(self.h); drop(Box::from_raw(self.frame)); }
    }
}
unsafe impl Send for FramePublication {}
unsafe impl Send for FrameSubscription {}

impl AudioPublication {
    pub fn write(&self, timestamp_us: u64, data: &[u8]) {
        unsafe { clutch_moqt_pub_write(self.h, timestamp_us, data.as_ptr(), data.len()); }
    }
    pub fn subscriber_count(&self) -> usize {
        unsafe { clutch_moqt_pub_subscriber_count(self.h) }
    }
}

impl Drop for AudioPublication {
    fn drop(&mut self) { unsafe { clutch_moqt_pub_close(self.h); } }
}
impl Drop for AudioSubscription {
    fn drop(&mut self) {
        unsafe { clutch_moqt_sub_close(self.h); drop(Box::from_raw(self.frame)); }
    }
}
impl Drop for MoqtClient {
    fn drop(&mut self) {
        unsafe { clutch_moqt_client_close(self.h); drop(Box::from_raw(self.state)); }
    }
}

// The engine drives its own io_thread; the handles own their boxed closures.
unsafe impl Send for MoqtClient {}
unsafe impl Send for AudioPublication {}
unsafe impl Send for AudioSubscription {}
