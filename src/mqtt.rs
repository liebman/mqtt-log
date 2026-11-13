//! Connecting to the broker and turning publications into [`Record`]s.
//!
//! The filter strings are handed to the broker untouched, so MQTT wildcards
//! (`+`, `#`) behave exactly as they do in any other client.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::{Context, Result};
use rumqttc::{
    AsyncClient, Event, EventLoop, MqttOptions, Packet, QoS, SubscribeFilter, TlsConfiguration,
    Transport,
};
use tokio::sync::mpsc::{self, error::TrySendError};
use tokio::sync::watch;

use crate::config::{Broker, FullPolicy, Topic};
use crate::record::Record;
use crate::stats::Counters;

fn qos(level: u8) -> QoS {
    match level {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        _ => QoS::ExactlyOnce,
    }
}

/// Builds the client and the event loop. Both are returned: the loop is polled
/// by [`run`] while the client is cloned for subscribing and acknowledging.
pub fn connect(broker: &Broker, ack_after_write: bool) -> Result<(AsyncClient, EventLoop)> {
    let mut options = MqttOptions::new(&broker.client_id, &broker.host, broker.port);
    options.set_keep_alive(broker.keep_alive);
    options.set_clean_session(broker.clean_session);
    // Manual acks let the writer acknowledge only after the bytes are on disk.
    options.set_manual_acks(ack_after_write);

    if let Some(username) = &broker.username {
        options.set_credentials(
            username.clone(),
            broker.password.clone().unwrap_or_default(),
        );
    }

    if broker.tls {
        let tls = match &broker.ca_file {
            Some(path) => TlsConfiguration::Simple {
                ca: std::fs::read(path)
                    .with_context(|| format!("reading CA file `{}`", path.display()))?,
                alpn: None,
                client_auth: None,
            },
            None => TlsConfiguration::default(),
        };
        options.set_transport(Transport::Tls(tls));
    }

    Ok(AsyncClient::new(options, 64))
}

pub struct MqttCfg {
    pub broker: Broker,
    pub topics: Vec<Topic>,
    pub on_full: FullPolicy,
}

/// Polls the event loop until `shutdown` fires or the writer goes away.
pub async fn run(
    mut eventloop: EventLoop,
    client: AsyncClient,
    cfg: MqttCfg,
    tx: mpsc::Sender<Record>,
    counters: Arc<Counters>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut backoff = Duration::from_millis(500);

    loop {
        tokio::select! {
            biased;

            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }

            event = eventloop.poll() => {
                match event {
                    Ok(Event::Incoming(Packet::ConnAck(ack))) => {
                        backoff = Duration::from_millis(500);
                        tracing::info!(
                            host = %cfg.broker.host,
                            port = cfg.broker.port,
                            session_present = ack.session_present,
                            "connected to broker"
                        );
                        let filters: Vec<SubscribeFilter> = cfg
                            .topics
                            .iter()
                            .map(|topic| SubscribeFilter::new(topic.filter.clone(), qos(topic.qos)))
                            .collect();
                        tracing::info!(
                            filters = ?cfg.topics.iter().map(|t| t.filter.as_str()).collect::<Vec<_>>(),
                            "subscribing"
                        );
                        if let Err(error) = client.subscribe_many(filters).await {
                            tracing::error!(%error, "failed to subscribe");
                        }
                    }

                    Ok(Event::Incoming(Packet::SubAck(ack))) => {
                        tracing::info!(granted = ?ack.return_codes, "subscription acknowledged");
                    }

                    Ok(Event::Incoming(Packet::Publish(publish))) => {
                        counters.received.fetch_add(1, Ordering::Relaxed);
                        let record = Record::new(publish);
                        match cfg.on_full {
                            FullPolicy::Block => {
                                if tx.send(record).await.is_err() {
                                    break;
                                }
                            }
                            FullPolicy::Drop => match tx.try_send(record) {
                                Ok(()) => {}
                                Err(TrySendError::Full(_)) => {
                                    counters.record_dropped();
                                }
                                Err(TrySendError::Closed(_)) => break,
                            },
                        }
                    }

                    Ok(_) => {}

                    Err(error) => {
                        tracing::warn!(%error, retry_in = ?backoff, "mqtt connection problem");
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(Duration::from_secs(30));
                    }
                }
            }
        }
    }

    let _ = client.disconnect().await;
    Ok(())
}

/// Connects, waits for CONNACK, and returns. Used by `--check`.
pub async fn check(broker: &Broker, ack_after_write: bool) -> Result<()> {
    let (client, mut eventloop) = connect(broker, ack_after_write)?;

    let attempt = async {
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    return Ok::<(), rumqttc::ConnectionError>(());
                }
                Ok(_) => {}
                Err(error) => return Err(error),
            }
        }
    };

    match tokio::time::timeout(Duration::from_secs(15), attempt).await {
        Ok(Ok(())) => {
            let _ = client.disconnect().await;
            Ok(())
        }
        Ok(Err(error)) => Err(anyhow::anyhow!("could not connect to the broker: {error}")),
        Err(_) => Err(anyhow::anyhow!(
            "timed out after 15s connecting to {}:{}",
            broker.host,
            broker.port
        )),
    }
}
