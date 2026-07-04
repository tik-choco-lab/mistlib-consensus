use async_trait::async_trait;
use mistlib_core::runtime::{AsyncRuntime, BoxTask};
use wasm_bindgen_futures::spawn_local;
use web_time::Duration;

pub struct WasmRuntime;

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl AsyncRuntime for WasmRuntime {
    fn spawn(&self, future: BoxTask) {
        spawn_local(future);
    }

    async fn sleep(&self, duration: Duration) {
        #[cfg(target_arch = "wasm32")]
        {
            gloo_timers::future::TimeoutFuture::new(duration.as_millis() as u32).await;
        }
        // `WasmRuntime` only ever actually runs inside a wasm32 target;
        // this native branch exists solely so the crate (and therefore
        // the workspace) still builds on the host, e.g. for `cargo
        // build --workspace` / `cargo clippy --workspace` in CI.
        // gloo-timers' `Timeout` wraps a `wasm_bindgen::Closure`, which
        // is never `Send`, so it can't appear in this async fn's body
        // when async-trait needs a `Send` future (the non-wasm32 case).
        #[cfg(not(target_arch = "wasm32"))]
        {
            let _ = duration;
            std::future::ready(()).await;
        }
    }
}
