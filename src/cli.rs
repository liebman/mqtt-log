//! Command line surface.
//!
//! Every field is optional so that "absent" can be told apart from "defaulted":
//! [`crate::config::resolve`] layers built-in defaults, the config file, the
//! environment (via clap's `env`) and finally these flags.

use std::path::PathBuf;
use std::time::Duration;

use bytesize::ByteSize;
use clap::Parser;

use crate::config::{FullPolicy, JsonMode, OutputFormat, Schedule};

#[derive(Debug, Parser)]
#[command(
    name = "mqtt-log",
    version,
    about = "Receive MQTT messages and write them to rotating log files",
    after_help = "Topic filters are passed straight to the broker, so MQTT wildcards \
                  (`+` and `#`) work -- quote them so your shell does not eat them:\n\n  \
                  mqtt-log -t 'app/#' -t 'sensors/+/log' --out-dir ./logs"
)]
pub struct Args {
    #[command(flatten)]
    pub broker: BrokerArgs,
    #[command(flatten)]
    pub topic: TopicArgs,
    #[command(flatten)]
    pub output: OutputArgs,
    #[command(flatten)]
    pub rotation: RotationArgs,
    #[command(flatten)]
    pub runtime: RuntimeArgs,
    #[command(flatten)]
    pub app_log: AppLogArgs,

    /// Path to a TOML config file
    #[arg(long, global = true, value_name = "FILE", env = "MQTT_LOG_CONFIG")]
    pub config: Option<PathBuf>,
    /// Do not load any config file
    #[arg(long, global = true)]
    pub no_config: bool,
    /// Treat unknown config file keys as errors
    #[arg(long, global = true)]
    pub strict: bool,
    /// Print the resolved configuration as TOML and exit
    #[arg(long, global = true)]
    pub print_config: bool,
    /// Connect, subscribe, print the resolved configuration and exit
    #[arg(long, global = true)]
    pub check: bool,
}

#[derive(Debug, clap::Args)]
pub struct BrokerArgs {
    /// Broker address: `mqtt://host:port` or `mqtts://host:port`
    #[arg(long, short = 'b', value_name = "URL", env = "MQTT_LOG_BROKER")]
    pub broker: Option<String>,
    /// MQTT client id (keep it stable if you disable clean sessions)
    #[arg(long, value_name = "ID", env = "MQTT_LOG_CLIENT_ID")]
    pub client_id: Option<String>,
    /// Username
    #[arg(long, short = 'u', value_name = "USER", env = "MQTT_LOG_USERNAME")]
    pub username: Option<String>,
    /// Password
    #[arg(
        long,
        short = 'p',
        value_name = "PASS",
        env = "MQTT_LOG_PASSWORD",
        hide_env_values = true
    )]
    pub password: Option<String>,
    /// Read the password from a file (trailing newline stripped)
    #[arg(long, value_name = "FILE", env = "MQTT_LOG_PASSWORD_FILE")]
    pub password_file: Option<PathBuf>,
    /// Start a fresh session instead of resuming a persistent one
    #[arg(long)]
    pub clean_session: bool,
    /// Always resume a persistent session (default)
    #[arg(long, conflicts_with = "clean_session")]
    pub no_clean_session: bool,
    /// MQTT keep alive
    #[arg(
        long,
        value_name = "DURATION",
        value_parser = humantime::parse_duration,
        env = "MQTT_LOG_KEEP_ALIVE"
    )]
    pub keep_alive: Option<Duration>,
    /// PEM bundle used to verify the broker certificate (mqtts only)
    #[arg(long, value_name = "FILE", env = "MQTT_LOG_CA_FILE")]
    pub ca_file: Option<PathBuf>,
}

#[derive(Debug, clap::Args)]
pub struct TopicArgs {
    /// Topic filter to subscribe to; repeatable and wildcard aware
    #[arg(long = "topic", short = 't', value_name = "FILTER")]
    pub topics: Vec<String>,
    /// QoS used for filters given on the command line
    #[arg(long, value_name = "0|1|2", env = "MQTT_LOG_QOS")]
    pub qos: Option<u8>,
}

#[derive(Debug, clap::Args)]
pub struct OutputArgs {
    /// Directory that log files are written to
    #[arg(long, value_name = "DIR", env = "MQTT_LOG_OUT_DIR")]
    pub out_dir: Option<PathBuf>,
    /// File name (or `{topic}` template) inside the output directory
    #[arg(long, value_name = "NAME", env = "MQTT_LOG_OUT_FILE")]
    pub out_file: Option<String>,
    /// Line format written to disk
    #[arg(long, value_name = "FORMAT", env = "MQTT_LOG_FORMAT")]
    pub format: Option<OutputFormat>,
    /// How payloads are inspected for JSON
    #[arg(long, value_name = "MODE", env = "MQTT_LOG_JSON")]
    pub json: Option<JsonMode>,
    /// Write to stdout instead of files
    #[arg(long)]
    pub stdout: bool,
    /// Write to files even if the config file enabled stdout
    #[arg(long, conflicts_with = "stdout")]
    pub no_stdout: bool,
}

#[derive(Debug, clap::Args)]
pub struct RotationArgs {
    /// Rotation schedule: `never`, `hourly`, `daily` or a duration like `15m`
    #[arg(long, value_name = "SCHEDULE", env = "MQTT_LOG_ROTATE")]
    pub rotate: Option<Schedule>,
    /// Rotate once the active file reaches this size
    #[arg(long, value_name = "SIZE", env = "MQTT_LOG_MAX_SIZE")]
    pub max_size: Option<ByteSize>,
    /// Number of rotated files to keep
    #[arg(long, value_name = "N", env = "MQTT_LOG_MAX_FILES")]
    pub max_files: Option<usize>,
}

#[derive(Debug, clap::Args)]
pub struct RuntimeArgs {
    /// Bounded queue length between the MQTT loop and the writer
    #[arg(long, value_name = "N", env = "MQTT_LOG_QUEUE_CAPACITY")]
    pub queue_capacity: Option<usize>,
    /// What to do when that queue is full
    #[arg(long, value_name = "POLICY", env = "MQTT_LOG_ON_FULL")]
    pub on_full: Option<FullPolicy>,
    /// How often buffered data is flushed to the OS
    #[arg(
        long,
        value_name = "DURATION",
        value_parser = humantime::parse_duration,
        env = "MQTT_LOG_FLUSH_INTERVAL"
    )]
    pub flush_interval: Option<Duration>,
    /// Only acknowledge QoS1/2 messages once they have been written
    #[arg(long)]
    pub ack_after_write: bool,
    /// Acknowledge as soon as the client loop sees the message
    #[arg(long, conflicts_with = "ack_after_write")]
    pub no_ack_after_write: bool,
    /// Rotate the active file when SIGHUP is received
    #[arg(long)]
    pub reopen_on_sighup: bool,
    /// Ignore SIGHUP
    #[arg(long, conflicts_with = "reopen_on_sighup")]
    pub no_reopen_on_sighup: bool,
    /// How often throughput counters are logged
    #[arg(
        long,
        value_name = "DURATION",
        value_parser = humantime::parse_duration,
        env = "MQTT_LOG_STATS_INTERVAL"
    )]
    pub stats_interval: Option<Duration>,
}

#[derive(Debug, clap::Args)]
pub struct AppLogArgs {
    /// Log level for mqtt-log's own diagnostics
    #[arg(long, value_name = "LEVEL", env = "MQTT_LOG_LEVEL")]
    pub log_level: Option<String>,
    /// Also write mqtt-log's own diagnostics to this file
    #[arg(long, value_name = "FILE", env = "MQTT_LOG_APP_LOG_FILE")]
    pub app_log_file: Option<PathBuf>,
}

/// Fold a `--flag` / `--no-flag` pair into a tri-state.
pub fn tri(pos: bool, neg: bool) -> Option<bool> {
    if pos {
        Some(true)
    } else if neg {
        Some(false)
    } else {
        None
    }
}
