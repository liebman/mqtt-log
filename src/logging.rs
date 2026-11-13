//! Diagnostics for mqtt-log itself.
//!
//! Kept strictly separate from the payload lane: received messages never go
//! through `tracing`, they are written to their own rotating sink.

use std::io::{self, Write};
use std::sync::{Arc, Mutex, MutexGuard};

use anyhow::{Context, Result};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::MakeWriter;

use crate::config::{AppLog, Schedule};
use crate::sink::{Rotation, Sink};

/// Diagnostics are rotated daily and capped, so they can never fill the disk.
const APP_LOG_MAX_FILES: usize = 5;
const APP_LOG_MAX_BYTES: u64 = 64 * 1024 * 1024;

/// Keeps the tracing sink alive for the lifetime of the process.
pub struct AppLogHandle {
    _sink: Option<Arc<Mutex<Sink>>>,
}

pub fn init(cfg: &AppLog) -> Result<AppLogHandle> {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(&cfg.level))
        .unwrap_or_else(|error| {
            eprintln!(
                "invalid log level `{}` ({error}), falling back to `info`",
                cfg.level
            );
            EnvFilter::new("info")
        });

    let Some(path) = &cfg.file else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(false)
            // Diagnostics must never share stdout with the payload lane.
            .with_writer(std::io::stderr)
            .init();
        return Ok(AppLogHandle { _sink: None });
    };

    let sink = Sink::open(
        path,
        Rotation::new(Schedule::Daily, Some(APP_LOG_MAX_BYTES)),
        APP_LOG_MAX_FILES,
    )
    .with_context(|| format!("opening application log file `{}`", path.display()))?;
    let shared = Arc::new(Mutex::new(sink));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_ansi(false)
        .with_writer(SinkWriter {
            sink: Arc::clone(&shared),
        })
        .init();

    Ok(AppLogHandle {
        _sink: Some(shared),
    })
}

#[derive(Clone)]
struct SinkWriter {
    sink: Arc<Mutex<Sink>>,
}

impl<'a> MakeWriter<'a> for SinkWriter {
    type Writer = SinkGuard<'a>;

    fn make_writer(&'a self) -> Self::Writer {
        SinkGuard(
            self.sink
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        )
    }
}

struct SinkGuard<'a>(MutexGuard<'a, Sink>);

impl Write for SinkGuard<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}
