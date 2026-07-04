use async_trait::async_trait;
use js_sys::{Function, Uint8Array};
use mistlib_consensus_core::{NodeId, RaftError, RaftMessage, RaftTransport, Result};
use std::cell::RefCell;
use wasm_bindgen::prelude::*;

/// A [`RaftTransport`] whose actual send happens in JavaScript: the
/// wasm module doesn't own a socket at all, it just hands `(to, bytes)`
/// to a JS callback that the embedder wired up (e.g. to a WebSocket or
/// to `mistlib-wasm`'s own L1 transport).
#[derive(Default)]
pub struct CallbackTransport {
    send_cb: RefCell<Option<Function>>,
}

impl CallbackTransport {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_send_callback(&self, cb: Function) {
        *self.send_cb.borrow_mut() = Some(cb);
    }
}

// Single-threaded JS environment: safe to assert Send/Sync so this
// type satisfies `HostSendSync` uniformly; on wasm32 `HostSendSync` has
// no real bounds anyway, this only matters if this crate is ever built
// for a native test target.
unsafe impl Send for CallbackTransport {}
unsafe impl Sync for CallbackTransport {}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl RaftTransport for CallbackTransport {
    async fn send(&self, to: &NodeId, msg: RaftMessage) -> Result<()> {
        let bytes = bincode::serialize(&msg)?;
        let cb = self.send_cb.borrow().clone();
        match cb {
            Some(cb) => {
                let array = Uint8Array::from(bytes.as_slice());
                let this = JsValue::NULL;
                cb.call2(&this, &JsValue::from_str(&to.0), &array)
                    .map_err(|e| RaftError::Transport(format!("{e:?}")))?;
                Ok(())
            }
            None => Err(RaftError::Transport("no send callback registered".into())),
        }
    }
}
