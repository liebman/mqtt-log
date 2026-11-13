//! The single task that owns the files.
//!
//! The MQTT loop hands encoded work over a bounded channel; nothing else touches
//! a file. That gives us ordering, a place to batch flushes, and somewhere to
//! acknowledge QoS1/2 messages only once they are safely written.

use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::Result;
use rumqttc::AsyncClient;
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;

use crate::config::{Output, RotationSettings, Topic};
use crate::record::{Record, encode_line};
use crate::sink::{Rotation, Sinks};
use crate::stats::Counters;

pub struct WriterCfg {
    pub output: Output,
    pub rotation: RotationSettings,
    pub topics: Vec<Topic>,
    pub flush_interval: Duration,
}

/// Drains `rx` until the producer side is dropped, then flushes and returns.
pub async fn run(
    mut rx: mpsc::Receiver<Record>,
    cfg: WriterCfg,
    acker: Option<AsyncClient>,
    mut hangup: Option<mpsc::Receiver<()>>,
    counters: Arc<Counters>,
) -> Result<()> {
    let mut sinks = if cfg.output.stdout {
        None
    } else {
        Some(Sinks::new(
            cfg.output.dir.clone(),
            cfg.output.out_file.clone(),
            Rotation::new(cfg.rotation.schedule.clone(), cfg.rotation.max_size),
            cfg.rotation.max_files,
        ))
    };
    let templates: Vec<(String, Option<String>)> = cfg
        .topics
        .iter()
        .map(|topic| (topic.filter.clone(), topic.out_file.clone()))
        .collect();

    let mut stdout = std::io::stdout();
    let mut ticker = tokio::time::interval(cfg.flush_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticker.tick().await;

    loop {
        tokio::select! {
            biased;

            maybe = rx.recv() => {
                let Some(record) = maybe else { break };
                write_record(
                    &mut sinks,
                    &mut stdout,
                    &cfg.output,
                    &templates,
                    &record,
                    &counters,
                );
                if let Some(client) = &acker
                    && let Err(error) = client.ack(&record.msg).await
                {
                    tracing::warn!(
                        topic = record.topic(),
                        %error,
                        "failed to acknowledge message"
                    );
                }
            }

            _ = ticker.tick() => {
                if let Some(sinks) = &mut sinks
                    && let Err(error) = sinks.flush_all()
                {
                    counters.errors.fetch_add(1, Ordering::Relaxed);
                    tracing::error!(%error, "failed to flush log files");
                }
            }

            Some(()) = recv_hangup(&mut hangup) => {
                tracing::info!("SIGHUP received, rotating open log files");
                if let Some(sinks) = &mut sinks
                    && let Err(error) = sinks.reopen_all()
                {
                    counters.errors.fetch_add(1, Ordering::Relaxed);
                    tracing::error!(%error, "failed to rotate log files");
                }
            }
        }
    }

    if let Some(sinks) = &mut sinks {
        sinks.flush_all()?;
        tracing::debug!(
            files = sinks.open_files(),
            records = sinks.records(),
            "wrote records"
        );
    } else {
        stdout.flush()?;
    }
    Ok(())
}

fn write_record(
    sinks: &mut Option<Sinks>,
    stdout: &mut std::io::Stdout,
    output: &Output,
    templates: &[(String, Option<String>)],
    record: &Record,
    counters: &Counters,
) {
    let line = encode_line(record, output.format, output.json);

    let outcome = match sinks {
        Some(sinks) => {
            let template = templates
                .iter()
                .find(|(filter, _)| crate::topic::matches(filter, record.topic()))
                .and_then(|(_, template)| template.as_deref());
            sinks.write(record.topic(), template, &line)
        }
        None => stdout.write_all(&line).and_then(|()| stdout.flush()),
    };

    match outcome {
        Ok(()) => {
            counters.written.fetch_add(1, Ordering::Relaxed);
            counters
                .bytes
                .fetch_add(line.len() as u64, Ordering::Relaxed);
        }
        Err(error) => {
            counters.errors.fetch_add(1, Ordering::Relaxed);
            tracing::error!(topic = record.topic(), %error, "failed to write record");
        }
    }
}

async fn recv_hangup(hangup: &mut Option<mpsc::Receiver<()>>) -> Option<()> {
    match hangup {
        Some(rx) => rx.recv().await,
        None => std::future::pending::<Option<()>>().await,
    }
}
