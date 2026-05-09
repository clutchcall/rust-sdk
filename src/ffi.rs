use std::os::raw::{c_char, c_int, c_void};



#[repr(C)]
pub struct ClutchCallBuffer {
    pub data: *mut c_void,
    pub length: usize,
}

#[repr(C)]
pub struct C_AudioFrame {
    pub call_sid: *mut c_char,
    pub payload: *mut c_char,
    pub codec: *mut c_char,
    pub sequence_number: u64,
    pub end_of_stream: bool,
}

#[repr(C)]
pub struct C_CallEvent {
    pub call_sid: *mut c_char,
    pub event_type: i32,
    pub status: *mut c_char,
    pub start_timestamp_ms: i64,
    pub q850_cause: i32,
    pub recording_url: *mut c_char,
    pub duration_seconds: i32,
}
