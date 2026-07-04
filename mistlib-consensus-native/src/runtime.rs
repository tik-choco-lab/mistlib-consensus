use async_trait::async_trait;
use mistlib_core::runtime::{AsyncRuntime, BoxTask};
use tokio::runtime::Handle;
use web_time::Duration;

/// [`AsyncRuntime`] backed by a Tokio [`Handle`]. Simplified from
/// `mistlib-native`'s `TokioRuntime` (no in-flight task counter).
pub struct TokioRuntime {
    handle: Handle,
}

impl TokioRuntime {
    pub fn new(handle: Handle) -> Self {
        Self { handle }
    }

    /// Convenience constructor using `Handle::current()`; must be
    /// called from within a Tokio runtime context.
    pub fn current() -> Self {
        Self::new(Handle::current())
    }
}

#[async_trait]
impl AsyncRuntime for TokioRuntime {
    fn spawn(&self, future: BoxTask) {
        self.handle.spawn(future);
    }

    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}
