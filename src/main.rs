//! mqtt-log: subscribe to MQTT topics and write the messages to rotating files.

#![forbid(unsafe_code)]

mod cli;
mod config;
mod logging;
mod mqtt;
mod record;
mod sink;
mod stats;
mod topic;
mod writer;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use tokio::sync::{mpsc, watch};

use crate::config::Settings;

fn main() -> Result<()> {
    let args = cli::Args::parse();
    let print_config = args.print_config;
    let check = args.check;
    let resolved = config::resolve(args)?;
    let settings = resolved.settings;
    let _app_log = logging::init(&settings.app_log)?;
    for warning in &resolved.warnings {
        tracing::warn!("{warning}");
    }

    if print_config {
        print!("{}", settings.to_toml(true)?);
        return Ok(());
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(run(settings, check));
    runtime.shutdown_timeout(Duration::from_secs(5));
    result
}

async fn run(settings: Settings, check: bool) -> Result<()> {
    tracing::info!(
        broker = %settings.broker.url,
        topics = ?settings.topics.iter().map(|t| t.filter.as_str()).collect::<Vec<_>>(),
        routes = ?settings
            .routes
            .iter()
            .map(|route| format!("{} -> {}", route.filter, route.out_file))
            .collect::<Vec<_>>(),
        output = if settings.output.stdout {
            "stdout".to_string()
        } else {
            settings.output.dir.display().to_string()
        },
        rotation = %settings.rotation.schedule,
        config = settings.config_path.as_ref().map(|p| p.display().to_string()),
        "starting mqtt-log"
    );

    if check {
        mqtt::check(&settings.broker, settings.runtime.ack_after_write).await?;
        print!("{}", settings.to_toml(true)?);
        tracing::info!("configuration and broker connection are valid");
        return Ok(());
    }

    let counters = Arc::new(stats::Counters::default());
    let (tx, rx) = mpsc::channel(settings.runtime.queue_capacity);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let (client, eventloop) = mqtt::connect(&settings.broker, settings.runtime.ack_after_write)?;

    // The writer acknowledges once the record is on disk; the MQTT loop must not.
    let acker = settings.runtime.ack_after_write.then(|| client.clone());
    let hangup = if settings.runtime.reopen_on_sighup {
        hangup_listener()
    } else {
        None
    };

    let writer_cfg = writer::WriterCfg {
        output: settings.output.clone(),
        rotation: settings.rotation.clone(),
        topics: settings.topics.clone(),
        routes: settings.routes.clone(),
        flush_interval: settings.runtime.flush_interval,
    };
    let writer_task = tokio::spawn(writer::run(rx, writer_cfg, acker, hangup, counters.clone()));

    let mqtt_cfg = mqtt::MqttCfg {
        broker: settings.broker.clone(),
        topics: settings.topics.clone(),
        on_full: settings.runtime.on_full,
    };
    let mqtt_task = tokio::spawn(mqtt::run(
        eventloop,
        client,
        mqtt_cfg,
        tx,
        counters.clone(),
        shutdown_rx.clone(),
    ));

    let stats_task = tokio::spawn(stats::report_loop(
        counters.clone(),
        settings.runtime.stats_interval,
        shutdown_rx,
    ));

    wait_for_shutdown_signal().await;
    tracing::info!("shutdown requested");
    let _ = shutdown_tx.send(true);

    // The MQTT task drops its sender, closing the channel; the writer then
    // flushes everything and returns.
    if let Err(error) = mqtt_task.await? {
        tracing::error!(%error, "mqtt task failed");
    }
    match tokio::time::timeout(Duration::from_secs(10), writer_task).await {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(error))) => tracing::error!(%error, "writer task failed"),
        Ok(Err(error)) => tracing::error!(%error, "writer task panicked"),
        Err(_) => tracing::error!("timed out waiting for the writer to flush"),
    }
    stats_task.abort();

    tracing::info!(target: "mqtt_log::stats", "final {}", counters.snapshot());
    Ok(())
}

#[cfg(unix)]
fn hangup_listener() -> Option<mpsc::Receiver<()>> {
    use tokio::signal::unix::{SignalKind, signal};

    match signal(SignalKind::hangup()) {
        Ok(mut sighup) => {
            let (tx, rx) = mpsc::channel(1);
            tokio::spawn(async move {
                while sighup.recv().await.is_some() {
                    let _ = tx.try_send(());
                }
            });
            Some(rx)
        }
        Err(error) => {
            tracing::warn!(%error, "cannot listen for SIGHUP");
            None
        }
    }
}

#[cfg(not(unix))]
fn hangup_listener() -> Option<mpsc::Receiver<()>> {
    None
}

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        match signal(SignalKind::terminate()) {
            Ok(mut sigterm) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = sigterm.recv() => {}
                }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
