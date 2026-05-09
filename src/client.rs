use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_void};
use libloading::{Library, Symbol};
use std::sync::Arc;
use crate::ffi::*;
use crate::method_id::*;
use quinn::{Endpoint, Connection};

type FreeBufferFn = unsafe extern "C" fn(ClutchCallBuffer);
type RpcOriginateFn = unsafe extern "C" fn(*const c_char, *const c_char, *const c_char, *const c_char, *const c_char, *const c_char, i32, *const c_char, i32, *const c_char, bool, i32, *const c_char) -> ClutchCallBuffer;
type RpcBulkFn = unsafe extern "C" fn(*const c_char, *const c_char, *const c_char, *const c_char, *const c_char, *const c_char, *const c_char, i32, i32, *const c_char, i32, i32, *const c_char, bool, i32) -> ClutchCallBuffer;
type RpcTerminateFn = unsafe extern "C" fn(*const c_char) -> ClutchCallBuffer;
type RpcAbortBulkFn = unsafe extern "C" fn(*const c_char) -> ClutchCallBuffer;
type RpcStreamEventsFn = unsafe extern "C" fn(*const c_char) -> ClutchCallBuffer;
type RpcBargeFn = unsafe extern "C" fn(*const c_char) -> ClutchCallBuffer;
type RpcSetInboundRoutingFn = unsafe extern "C" fn(*const c_char, i32, *const c_char, *const c_char, *const c_char, *const c_char) -> ClutchCallBuffer;
type RpcGetIncomingCallsFn = unsafe extern "C" fn(*const c_char) -> ClutchCallBuffer;
type RpcAnswerIncomingCallFn = unsafe extern "C" fn(*const c_char, *const c_char, *const c_char, *const c_char) -> ClutchCallBuffer;
type RpcEmptyFn = unsafe extern "C" fn() -> ClutchCallBuffer;
type RpcBucketRequestFn = unsafe extern "C" fn(*const c_char) -> ClutchCallBuffer;
type RpcBucketActionFn = unsafe extern "C" fn(*const c_char, i32) -> ClutchCallBuffer;
type SerializeAudioFrameFn = unsafe extern "C" fn(*const c_char, *const c_char, *const c_char, u64, bool) -> ClutchCallBuffer;

pub struct ClutchCallClient {
    lib: Arc<Library>,
    endpoint_url: String,
    connection: Option<Connection>,
    audio_out_stream: Option<quinn::SendStream>,
    pub on_audio_frame: Option<Arc<dyn Fn(Vec<u8>) + Send + Sync>>,
    pub on_call_event: Option<Arc<dyn Fn(Vec<u8>) + Send + Sync>>,
    client_id: String,
}

impl ClutchCallClient {
    pub fn new(endpoint_url: &str, lib_path: &str) -> std::io::Result<Self> {
        // SAFETY: `Library::new` loads a shared object from disk. Caller is
        // responsible for pointing at a trusted path; symbols loaded from it
        // are invoked via typed signatures matching the C FFI contract in ffi.rs.
        let lib = unsafe { Library::new(lib_path).expect("Failed to load native library") };
        Ok(ClutchCallClient {
            lib: Arc::new(lib),
            endpoint_url: endpoint_url.to_string(),
            connection: None,
            audio_out_stream: None,
            on_audio_frame: None,
            on_call_event: None,
            client_id: uuid::Uuid::new_v4().to_string(),
        })
    }

    pub async fn connect(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let mut endpoint = Endpoint::client("[::]:0".parse().unwrap())?;
        endpoint.set_default_client_config(quinn::ClientConfig::with_native_roots());
        
        let target = self.endpoint_url.trim_start_matches("quic://");
        let conn = endpoint.connect(target.parse().unwrap(), "h3")?.await?;
        self.connection = Some(conn.clone());
        
        let on_audio = self.on_audio_frame.clone();
        let on_event = self.on_call_event.clone();
        
        tokio::spawn(async move {
            while let Ok(mut stream) = conn.accept_uni().await {
                let aud = on_audio.clone();
                let evt = on_event.clone();
                tokio::spawn(async move {
                    loop {
                        let mut len_buf = [0u8; 4];
                        if stream.read_exact(&mut len_buf).await.is_err() { break; }
                        let total_len = u32::from_le_bytes(len_buf);
                        if total_len == 0 || total_len > 1024 * 1024 { continue; }
                        
                        let mut payload_buf = vec![0u8; total_len as usize];
                        if stream.read_exact(&mut payload_buf).await.is_err() { break; }
                        
                        if total_len >= 4 {
                            let mut id_buf = [0u8; 4];
                            id_buf.copy_from_slice(&payload_buf[0..4]);
                            let dg_id = u32::from_le_bytes(id_buf);
                            
                            if dg_id == METHOD_ID_AUDIO_FRAME {
                                if let Some(cb) = &aud { cb(payload_buf[4..].to_vec()); }
                            } else if dg_id == METHOD_ID_STREAM_EVENTS {
                                if let Some(cb) = &evt { cb(payload_buf[4..].to_vec()); }
                            }
                        }
                    }
                });
            }
        });

        unsafe {
            let func: Symbol<RpcStreamEventsFn> = self.lib.get(b"clutchcall_rpc_event_stream_request").unwrap();
            let c_client = CString::new(self.client_id.clone()).unwrap();
            let buf = func(c_client.as_ptr());
            self.send_rpc(buf).await?;
        }
        
        Ok(())
    }

    async fn send_rpc(&mut self, buf: ClutchCallBuffer) -> Result<(), Box<dyn std::error::Error>> {
        if self.connection.is_none() {
            Box::pin(self.connect()).await?;
        }

        // SAFETY: `buf.data`/`buf.length` are produced by the C core's serializer
        // which guarantees a valid contiguous allocation for `length` bytes until
        // `clutchcall_free_buffer` is called (below, after write completes).
        let payload = unsafe { std::slice::from_raw_parts(buf.data as *const u8, buf.length) };

        let conn = self.connection.as_mut().unwrap();
        let (mut send, _) = conn.open_bi().await?;
        
        send.write_all(payload).await?;
        send.finish().await?;

        unsafe {
            let free_buffer: Symbol<FreeBufferFn> = self.lib.get(b"clutchcall_free_buffer").unwrap();
            free_buffer(buf);
        }
        Ok(())
    }

    pub async fn dial(&mut self, to: &str, trunk_id: &str, auto_barge_in: bool, barge_in_patience_ms: i32, client_id: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
        unsafe {
            let func: Symbol<RpcOriginateFn> = self.lib.get(b"clutchcall_rpc_originate_request").unwrap();
            let c_to = CString::new(to).unwrap();
            let c_trunk = CString::new(trunk_id).unwrap();
            let empty = CString::new("").unwrap();
            let token = CString::new("mock_token").unwrap();
            let target_client = client_id.unwrap_or(self.client_id.as_str());
            let client = CString::new(target_client).unwrap();

            let buf = func(c_trunk.as_ptr(), c_to.as_ptr(), empty.as_ptr(), empty.as_ptr(), empty.as_ptr(), token.as_ptr(), 0, empty.as_ptr(), 1, empty.as_ptr(), auto_barge_in, barge_in_patience_ms, client.as_ptr());
            self.send_rpc(buf).await
        }
    }

    pub async fn originate_bulk(&mut self, csv_url: &str, trunk_id: &str, cps: i32, cmp_id: &str, auto_barge_in: bool, barge_in_patience_ms: i32) -> Result<(), Box<dyn std::error::Error>> {
        unsafe {
            let func: Symbol<RpcBulkFn> = self.lib.get(b"clutchcall_rpc_bulk_request").unwrap();
            let c_csv = CString::new(csv_url).unwrap();
            let c_trunk = CString::new(trunk_id).unwrap();
            let empty = CString::new("").unwrap();
            let c_cmp = CString::new(cmp_id).unwrap();
            let token = CString::new("mock_token").unwrap();
            let buf = func(c_csv.as_ptr(), c_trunk.as_ptr(), empty.as_ptr(), empty.as_ptr(), empty.as_ptr(), empty.as_ptr(), token.as_ptr(), 0, 1, empty.as_ptr(), cps, 1000, c_cmp.as_ptr(), auto_barge_in, barge_in_patience_ms);
            self.send_rpc(buf).await
        }
    }

    pub async fn terminate(&mut self, call_sid: &str) -> Result<(), Box<dyn std::error::Error>> {
        unsafe {
            let func: Symbol<RpcTerminateFn> = self.lib.get(b"clutchcall_rpc_terminate_request").unwrap();
            let c_sid = CString::new(call_sid).unwrap();
            let buf = func(c_sid.as_ptr());
            self.send_rpc(buf).await
        }
    }

    pub async fn abort_bulk(&mut self, campaign_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        unsafe {
            let func: Symbol<RpcAbortBulkFn> = self.lib.get(b"clutchcall_rpc_abort_bulk_request").unwrap();
            let c_id = CString::new(campaign_id).unwrap();
            let buf = func(c_id.as_ptr());
            self.send_rpc(buf).await
        }
    }

    pub async fn stream_events(&mut self, client_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        unsafe {
            let func: Symbol<RpcStreamEventsFn> = self.lib.get(b"clutchcall_rpc_event_stream_request").unwrap();
            let c_id = CString::new(client_id).unwrap();
            let buf = func(c_id.as_ptr());
            self.send_rpc(buf).await
        }
    }

    pub async fn barge(&mut self, call_sid: &str) -> Result<(), Box<dyn std::error::Error>> {
        unsafe {
            let func: Symbol<RpcBargeFn> = self.lib.get(b"clutchcall_rpc_barge_request").unwrap();
            let c_sid = CString::new(call_sid).unwrap();
            let buf = func(c_sid.as_ptr());
            self.send_rpc(buf).await
        }
    }

    pub async fn set_inbound_routing(&mut self, trunk_id: &str, rule: i32, audio_url: &str, webhook_url: &str, ai_ws: &str, ai_quic: &str) -> Result<(), Box<dyn std::error::Error>> {
        unsafe {
            let func: Symbol<RpcSetInboundRoutingFn> = self.lib.get(b"clutchcall_rpc_set_inbound_routing_request").unwrap();
            let c_trunk = CString::new(trunk_id).unwrap();
            let c_audio = CString::new(audio_url).unwrap();
            let c_web = CString::new(webhook_url).unwrap();
            let c_ws = CString::new(ai_ws).unwrap();
            let c_quic = CString::new(ai_quic).unwrap();

            let buf = func(c_trunk.as_ptr(), rule, c_audio.as_ptr(), c_web.as_ptr(), c_ws.as_ptr(), c_quic.as_ptr());
            self.send_rpc(buf).await
        }
    }

    pub async fn get_incoming_calls(&mut self, trunk_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        unsafe {
            let func: Symbol<RpcGetIncomingCallsFn> = self.lib.get(b"clutchcall_rpc_get_incoming_calls_request").unwrap();
            let c_trunk = CString::new(trunk_id).unwrap();
            let buf = func(c_trunk.as_ptr());
            self.send_rpc(buf).await
        }
    }

    pub async fn answer_incoming_call(&mut self, call_sid: &str, ai_ws: &str, ai_quic: &str) -> Result<(), Box<dyn std::error::Error>> {
        unsafe {
            let func: Symbol<RpcAnswerIncomingCallFn> = self.lib.get(b"clutchcall_rpc_answer_incoming_call_request").unwrap();
            let c_sid = CString::new(call_sid).unwrap();
            let c_ws = CString::new(ai_ws).unwrap();
            let c_quic = CString::new(ai_quic).unwrap();
            let c_client = CString::new(self.client_id.clone()).unwrap();
            let buf = func(c_sid.as_ptr(), c_ws.as_ptr(), c_quic.as_ptr(), c_client.as_ptr());
            self.send_rpc(buf).await
        }
    }

    pub async fn get_active_buckets(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        unsafe {
            let func: Symbol<RpcEmptyFn> = self.lib.get(b"clutchcall_rpc_empty").unwrap();
            let buf = func();
            self.send_rpc(buf).await
        }
    }

    pub async fn get_bucket_calls(&mut self, bucket_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        unsafe {
            let func: Symbol<RpcBucketRequestFn> = self.lib.get(b"clutchcall_rpc_bucket_request").unwrap();
            let c_id = CString::new(bucket_id).unwrap();
            let buf = func(c_id.as_ptr());
            self.send_rpc(buf).await
        }
    }

    pub async fn execute_bucket_action(&mut self, bucket_id: &str, action: i32) -> Result<(), Box<dyn std::error::Error>> {
        unsafe {
            let func: Symbol<RpcBucketActionFn> = self.lib.get(b"clutchcall_rpc_bucket_action_request").unwrap();
            let c_id = CString::new(bucket_id).unwrap();
            let buf = func(c_id.as_ptr(), action);
            self.send_rpc(buf).await
        }
    }

    pub async fn push_audio(&mut self, call_sid: &str, payload: &[u8], codec: &str, seq_num: u64, eos: bool) -> Result<(), Box<dyn std::error::Error>> {
        if self.connection.is_none() {
            self.connect().await?;
        }
        unsafe {
            let func: Symbol<SerializeAudioFrameFn> = self.lib.get(b"clutchcall_serialize_audio_frame").unwrap();
            let c_sid = CString::new(call_sid).unwrap();
            let c_codec = CString::new(codec).unwrap();
            let c_payload = CString::new(payload).unwrap_or_else(|_| CString::new("").unwrap());
            
            let buf = func(c_sid.as_ptr(), c_payload.as_ptr(), c_codec.as_ptr(), seq_num, eos);
            let serialized = std::slice::from_raw_parts(buf.data as *const u8, buf.length);
            
            let mut packet = Vec::with_capacity(buf.length + 8);
            let packet_len = (buf.length + 4) as u32;
            packet.extend_from_slice(&packet_len.to_le_bytes());
            packet.extend_from_slice(&(METHOD_ID_AUDIO_FRAME).to_le_bytes());
            packet.extend_from_slice(serialized);
            
            let free_buffer: Symbol<FreeBufferFn> = self.lib.get(b"clutchcall_free_buffer").unwrap();
            free_buffer(buf);
            
            if self.audio_out_stream.is_none() {
                let conn = self.connection.as_mut().unwrap();
                let send = conn.open_uni().await?;
                self.audio_out_stream = Some(send);
            }
            
            if let Some(stream) = &mut self.audio_out_stream {
                stream.write_all(&packet).await?;
            }
            
            Ok(())
        }
    }
}
