//! Throughput counters shared between the MQTT loop, the writer and the stats
//! reporter.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::watch;

#[derive(Debug, Default)]
pub struct Counters {
    pub received: AtomicU64,
    pub dropped: AtomicU64,
    pub written: AtomicU64,
    pub bytes: AtomicU64,
    pub errors: AtomicU64,
}

impl Counters {
    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            received: self.received.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            written: self.written.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
        }
    }

    pub fn record_dropped(&self) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Snapshot {
    pub received: u64,
    pub dropped: u64,
    pub written: u64,
    pub bytes: u64,
    pub errors: u64,
}

impl fmt::Display for Snapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "received={} written={} bytes={} dropped={} errors={}",
            self.received, self.written, self.bytes, self.dropped, self.errors
        )
    }
}

/// Logs the counters every `every`, and once more on shutdown.
pub async fn report_loop(
    counters: Arc<Counters>,
    every: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut ticker = tokio::time::interval(every);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                tracing::info!(target: "mqtt_log::stats", "{}", counters.snapshot());
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
        }
    }
}
