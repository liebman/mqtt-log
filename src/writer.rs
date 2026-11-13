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

use crate::config::{Output, RotationSettings, Route, Topic};
use crate::record::{Record, encode_line};
use crate::sink::{Rotation, Sinks};
use crate::stats::Counters;

pub struct WriterCfg {
    pub output: Output,
    pub rotation: RotationSettings,
    pub topics: Vec<Topic>,
    /// Naming-only rules; they never reach the broker.
    pub routes: Vec<Route>,
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
    let routing = Routing::new(&cfg.routes, &cfg.topics);

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
                    &routing,
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
    routing: &Routing,
    record: &Record,
    counters: &Counters,
) {
    let line = encode_line(record, output.format, output.json);

    let outcome = match sinks {
        Some(sinks) => sinks.write(record.topic(), routing.template_for(record.topic()), &line),
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

/// Which file name applies to a topic.
///
/// `[[routes]]` are consulted first: they are pure naming rules and are never
/// subscribed, so overlapping them with `[[topics]]` cannot make the broker
/// deliver a message twice. `[[topics]]` entries come next, in file order; a
/// matching entry without an `out_file` deliberately stops the search and means
/// "use the [output] default", which is what the config has always done.
struct Routing {
    rules: Vec<(String, Option<String>)>,
}

impl Routing {
    fn new(routes: &[Route], topics: &[Topic]) -> Self {
        let mut rules = Vec::with_capacity(routes.len() + topics.len());
        rules.extend(
            routes
                .iter()
                .map(|route| (route.filter.clone(), Some(route.out_file.clone()))),
        );
        rules.extend(
            topics
                .iter()
                .map(|topic| (topic.filter.clone(), topic.out_file.clone())),
        );
        Self { rules }
    }

    fn template_for<'a>(&'a self, topic: &str) -> Option<&'a str> {
        self.rules
            .iter()
            .find(|(filter, _)| crate::topic::matches(filter, topic))
            .and_then(|(_, template)| template.as_deref())
    }
}

async fn recv_hangup(hangup: &mut Option<mpsc::Receiver<()>>) -> Option<()> {
    match hangup {
        Some(rx) => rx.recv().await,
        None => std::future::pending::<Option<()>>().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topic(filter: &str, out_file: Option<&str>) -> Topic {
        Topic {
            filter: filter.to_string(),
            qos: 0,
            out_file: out_file.map(str::to_string),
        }
    }

    fn route(filter: &str, out_file: &str) -> Route {
        Route {
            filter: filter.to_string(),
            out_file: out_file.to_string(),
        }
    }

    #[test]
    fn a_route_beats_the_topic_template_and_the_default() {
        let routes = [route(
            "logs/matrix-clock-806599ba7cb0",
            "matrix-clock-s3.log",
        )];
        let topics = [topic("logs/#", Some("{topic}.log"))];
        let routing = Routing::new(&routes, &topics);

        assert_eq!(
            routing.template_for("logs/matrix-clock-806599ba7cb0"),
            Some("matrix-clock-s3.log")
        );
        assert_eq!(routing.template_for("logs/other"), Some("{topic}.log"));
    }

    #[test]
    fn routes_are_consulted_before_topics() {
        let routes = [route("logs/a", "route.log")];
        let topics = [topic("logs/#", Some("everything.log"))];
        let routing = Routing::new(&routes, &topics);

        assert_eq!(routing.template_for("logs/a"), Some("route.log"));
        assert_eq!(routing.template_for("logs/b"), Some("everything.log"));
    }

    #[test]
    fn the_first_matching_rule_wins() {
        let routing = Routing::new(
            &[
                route("logs/device", "exact.log"),
                route("logs/device/#", "sub.log"),
            ],
            &[],
        );

        assert_eq!(routing.template_for("logs/device"), Some("exact.log"));
        assert_eq!(routing.template_for("logs/device/metrics"), Some("sub.log"));
    }

    #[test]
    fn a_wildcard_route_covers_the_bare_topic_and_its_children() {
        let routing = Routing::new(&[route("logs/device/#", "device.log")], &[]);

        assert_eq!(routing.template_for("logs/device"), Some("device.log"));
        assert_eq!(
            routing.template_for("logs/device/metrics"),
            Some("device.log")
        );
    }

    #[test]
    fn a_matching_topic_entry_without_a_name_means_the_output_default() {
        let topics = [
            topic("app/#", None),
            topic("app/special", Some("special.log")),
        ];
        let routing = Routing::new(&[], &topics);

        assert_eq!(routing.template_for("app/special"), None);
    }

    #[test]
    fn unmatched_topics_fall_through_to_the_default() {
        let routing = Routing::new(&[], &[topic("app/#", Some("app.log"))]);

        assert_eq!(routing.template_for("other/thing"), None);
    }
}
