//! Configuration: built-in defaults, an optional TOML file, the environment and
//! CLI flags, merged in that order into a fully resolved [`Settings`].

use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use bytesize::ByteSize;
use serde::{Deserialize, Serialize};

use crate::cli::{Args, tri};
use crate::topic;

/// Declares a simple string-valued enum that works with clap (`FromStr`), the
/// config file (`Deserialize`) and `--print-config` (`Display`/`Serialize`).
macro_rules! string_enum {
    ($name:ident { $default:ident => $default_text:literal, $($variant:ident => $text:literal),+ $(,)? }) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
        pub enum $name {
            #[default]
            $default,
            $($variant),+
        }

        impl FromStr for $name {
            type Err = String;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                match s.trim().to_ascii_lowercase().as_str() {
                    $default_text => Ok(Self::$default),
                    $($text => Ok(Self::$variant),)+
                    other => Err(format!(
                        "invalid value `{other}`, expected one of: {}",
                        [$default_text, $($text),+].join(", ")
                    )),
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(match self {
                    Self::$default => $default_text,
                    $(Self::$variant => $text),+
                })
            }
        }

        impl Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(&self.to_string())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                String::deserialize(deserializer)?
                    .parse()
                    .map_err(serde::de::Error::custom)
            }
        }
    };
}

string_enum!(OutputFormat {
    Jsonl => "jsonl",
    Raw => "raw",
});

string_enum!(JsonMode {
    Auto => "auto",
    Always => "always",
    Never => "never",
});

string_enum!(FullPolicy {
    Block => "block",
    Drop => "drop",
});

/// When the active file is rotated onto a new one.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum Schedule {
    /// Never rotate on a schedule (a `max_size` limit may still apply).
    #[default]
    Never,
    /// Rotate when the local hour changes.
    Hourly,
    /// Rotate when the local date changes.
    Daily,
    /// Rotate every `Duration`, aligned to local midnight.
    Every(Duration),
}

impl FromStr for Schedule {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "never" | "none" | "off" => Ok(Self::Never),
            "hourly" | "1h" => Ok(Self::Hourly),
            "daily" | "1d" | "midnight" => Ok(Self::Daily),
            other => {
                let duration = humantime::parse_duration(other)
                    .map_err(|e| format!("invalid rotation schedule `{other}`: {e}"))?;
                if duration.is_zero() {
                    return Err("rotation schedule must be greater than zero".to_string());
                }
                Ok(Self::Every(duration))
            }
        }
    }
}

impl fmt::Display for Schedule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Never => f.write_str("never"),
            Self::Hourly => f.write_str("hourly"),
            Self::Daily => f.write_str("daily"),
            Self::Every(d) => f.write_str(&humantime::format_duration(*d).to_string()),
        }
    }
}

impl Serialize for Schedule {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Schedule {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

// ---------------------------------------------------------------- file shape

/// The on-disk config file. Every field is optional so that a partial file can
/// layer over the built-in defaults; `--print-config` serialises this same type.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct Config {
    pub broker: BrokerCfg,
    pub topics: Vec<TopicCfg>,
    pub routes: Vec<RouteCfg>,
    pub output: OutputCfg,
    pub rotation: RotationCfg,
    pub runtime: RuntimeCfg,
    pub app_log: AppLogCfg,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct BrokerCfg {
    pub url: Option<String>,
    pub client_id: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub password_file: Option<PathBuf>,
    pub clean_session: Option<bool>,
    #[serde(with = "humantime_serde::option")]
    pub keep_alive: Option<Duration>,
    pub ca_file: Option<PathBuf>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct TopicCfg {
    pub filter: Option<String>,
    pub qos: Option<u8>,
    pub out_file: Option<String>,
}

/// A naming rule that never subscribes: it only feeds the writer's lookup, so
/// overlapping filters cannot cause duplicate delivery.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct RouteCfg {
    pub filter: Option<String>,
    pub out_file: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct OutputCfg {
    pub dir: Option<PathBuf>,
    pub out_file: Option<String>,
    pub format: Option<OutputFormat>,
    pub json: Option<JsonMode>,
    pub stdout: Option<bool>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct RotationCfg {
    pub schedule: Option<Schedule>,
    /// Accepts `"100MB"`, `"512KiB"` or a plain byte count; always rendered as
    /// an exact byte count so `--print-config` round-trips losslessly.
    #[serde(serialize_with = "serialize_byte_size")]
    pub max_size: Option<ByteSize>,
    pub max_files: Option<usize>,
}

fn serialize_byte_size<S: serde::Serializer>(
    value: &Option<ByteSize>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match value {
        Some(size) => serializer.serialize_u64(size.as_u64()),
        None => serializer.serialize_none(),
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct RuntimeCfg {
    pub queue_capacity: Option<usize>,
    pub on_full: Option<FullPolicy>,
    #[serde(with = "humantime_serde::option")]
    pub flush_interval: Option<Duration>,
    pub ack_after_write: Option<bool>,
    pub reopen_on_sighup: Option<bool>,
    #[serde(with = "humantime_serde::option")]
    pub stats_interval: Option<Duration>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct AppLogCfg {
    pub level: Option<String>,
    pub file: Option<PathBuf>,
}

// ------------------------------------------------------------- resolved view

/// Everything the runtime needs, with no `Option`s left.
#[derive(Debug, Clone)]
pub struct Settings {
    pub broker: Broker,
    pub topics: Vec<Topic>,
    pub routes: Vec<Route>,
    pub output: Output,
    pub rotation: RotationSettings,
    pub runtime: ResolvedRuntime,
    pub app_log: AppLog,
    pub config_path: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct Broker {
    pub url: String,
    pub host: String,
    pub port: u16,
    pub tls: bool,
    pub client_id: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub clean_session: bool,
    pub keep_alive: Duration,
    pub ca_file: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct Topic {
    pub filter: String,
    pub qos: u8,
    pub out_file: Option<String>,
}

/// A resolved routing rule: `filter` is matched against each message's topic and
/// `out_file` names the file. Routes are never handed to the broker.
#[derive(Debug, Clone)]
pub struct Route {
    pub filter: String,
    pub out_file: String,
}

#[derive(Debug, Clone)]
pub struct Output {
    pub dir: PathBuf,
    pub out_file: String,
    pub format: OutputFormat,
    pub json: JsonMode,
    pub stdout: bool,
}

#[derive(Debug, Clone)]
pub struct RotationSettings {
    pub schedule: Schedule,
    pub max_size: Option<u64>,
    pub max_files: usize,
}

#[derive(Debug, Clone)]
pub struct ResolvedRuntime {
    pub queue_capacity: usize,
    pub on_full: FullPolicy,
    pub flush_interval: Duration,
    pub ack_after_write: bool,
    pub reopen_on_sighup: bool,
    pub stats_interval: Duration,
}

#[derive(Debug, Clone)]
pub struct AppLog {
    pub level: String,
    pub file: Option<PathBuf>,
}

pub const DEFAULT_BROKER_URL: &str = "mqtt://localhost:1883";
pub const DEFAULT_CLIENT_ID: &str = "mqtt-log";
pub const DEFAULT_OUT_DIR: &str = "logs";
pub const DEFAULT_OUT_FILE: &str = "app.log";
pub const DEFAULT_QOS: u8 = 1;
pub const DEFAULT_MAX_FILES: usize = 9;
pub const DEFAULT_KEEP_ALIVE: Duration = Duration::from_secs(30);
pub const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(1);
pub const DEFAULT_STATS_INTERVAL: Duration = Duration::from_secs(30);
pub const DEFAULT_QUEUE_CAPACITY: usize = 10_000;
pub const DEFAULT_LOG_LEVEL: &str = "info";

// ------------------------------------------------------------------ resolving

/// The resolved configuration plus anything worth telling the user about.
///
/// Warnings are returned rather than logged because the tracing subscriber does
/// not exist until the configuration has been resolved.
pub struct Resolved {
    pub settings: Settings,
    pub warnings: Vec<String>,
}

/// Loads the config file (if any), layers the environment and CLI on top, and
/// validates the result.
pub fn resolve(args: Args) -> Result<Resolved> {
    let path = find_config(&args)?;
    let mut warnings = Vec::new();
    let file = match &path {
        Some(p) => Some(load(p, args.strict, &mut warnings)?),
        None => None,
    };
    let settings = Settings::merge(file.as_ref(), &args, path)?;
    warnings.extend(settings.validate()?);
    Ok(Resolved { settings, warnings })
}

fn find_config(args: &Args) -> Result<Option<PathBuf>> {
    if args.no_config {
        return Ok(None);
    }
    if let Some(path) = &args.config {
        if !path.is_file() {
            bail!("config file `{}` does not exist", path.display());
        }
        return Ok(Some(path.clone()));
    }
    Ok(default_config_paths()
        .into_iter()
        .find(|candidate| candidate.is_file()))
}

fn default_config_paths() -> Vec<PathBuf> {
    let mut paths = vec![PathBuf::from("mqtt-log.toml")];
    if let Ok(xdg) = std::env::var("XDG_CONFIG_HOME") {
        paths.push(Path::new(&xdg).join("mqtt-log").join("config.toml"));
    } else if let Ok(home) = std::env::var("HOME") {
        paths.push(
            Path::new(&home)
                .join(".config")
                .join("mqtt-log")
                .join("config.toml"),
        );
    }
    paths.push(PathBuf::from("/etc/mqtt-log/config.toml"));
    paths
}

fn load(path: &Path, strict: bool, warnings: &mut Vec<String>) -> Result<Config> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading config file `{}`", path.display()))?;

    let deserializer =
        toml::Deserializer::parse(&text).map_err(|e| render_toml_error(path, &text, &e))?;

    let mut unknown: Vec<String> = Vec::new();
    let config: Config =
        serde_ignored::deserialize(deserializer, |key: serde_ignored::Path<'_>| {
            unknown.push(key.to_string())
        })
        .map_err(|e: toml::de::Error| render_toml_error(path, &text, &e))?;

    if !unknown.is_empty() {
        let keys = unknown.join(", ");
        if strict {
            bail!("unknown config key(s) in `{}`: {keys}", path.display());
        }
        warnings.push(format!(
            "ignoring unknown config key(s) in `{}`: {keys}",
            path.display()
        ));
    }

    Ok(config)
}

/// Turns a `toml` error into a `file:line:col` message with a caret marker.
fn render_toml_error(path: &Path, text: &str, error: &toml::de::Error) -> anyhow::Error {
    let mut message = format!("{}: {}", path.display(), error.message());
    if let Some(span) = error.span() {
        let start = span.start.min(text.len());
        let line_start = text[..start].rfind('\n').map_or(0, |i| i + 1);
        let line_number = text[..start].matches('\n').count() + 1;
        let column = start - line_start + 1;
        let line = text[line_start..].lines().next().unwrap_or("");
        let width = span.end.saturating_sub(span.start).clamp(1, 80);
        message.push_str(&format!(
            "\n  --> {}:{line_number}:{column}\n   |\n{line_number:>3} | {line}\n   | {}{}",
            path.display(),
            " ".repeat(column - 1),
            "^".repeat(width),
        ));
    }
    anyhow::anyhow!(message)
}

fn read_secret(path: &Path) -> Result<String> {
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("reading password file `{}`", path.display()))?;
    Ok(contents.trim_end_matches(['\n', '\r']).to_string())
}

/// Splits `mqtt[s]://host:port` (or a bare `host:port`) into its parts.
fn parse_broker_url(raw: &str) -> Result<(String, u16, bool)> {
    let (scheme, rest) = match raw.split_once("://") {
        Some((scheme, rest)) => (Some(scheme.to_ascii_lowercase()), rest),
        None => (None, raw),
    };

    let tls = match scheme.as_deref() {
        None | Some("mqtt") | Some("tcp") => false,
        Some("mqtts") | Some("ssl") | Some("tls") => true,
        Some(other) => bail!("unsupported broker scheme `{other}`, expected mqtt:// or mqtts://"),
    };

    let authority = rest.split('/').next().unwrap_or(rest);
    let (host, port) = match authority.rsplit_once(':') {
        Some((host, port)) if !port.is_empty() && port.chars().all(|c| c.is_ascii_digit()) => {
            let port: u16 = port
                .parse()
                .with_context(|| format!("invalid port in broker URL `{raw}`"))?;
            (host.to_string(), port)
        }
        _ => (authority.to_string(), if tls { 8883 } else { 1883 }),
    };

    if host.is_empty() {
        bail!("broker URL `{raw}` does not contain a host");
    }
    Ok((host, port, tls))
}

/// Rejects templates that could escape the output directory or that use a token
/// we do not know about.
pub fn validate_template(template: &str) -> Result<()> {
    if template.trim().is_empty() {
        bail!("output file name must not be empty");
    }
    if Path::new(template).is_absolute() {
        bail!("output file name `{template}` must be relative to the output directory");
    }
    if template
        .split(['/', '\\'])
        .any(|component| component == "..")
    {
        bail!("output file name `{template}` must not contain `..`");
    }

    let mut remaining = template;
    while let Some(open) = remaining.find('{') {
        let after = &remaining[open + 1..];
        let close = after
            .find('}')
            .with_context(|| format!("unclosed `{{` in output file name `{template}`"))?;
        let token = &after[..close];
        if token != "topic" {
            bail!(
                "unknown template token `{{{token}}}` in `{template}`; supported tokens: {{topic}}"
            );
        }
        remaining = &after[close + 1..];
    }
    Ok(())
}

impl Settings {
    fn merge(file: Option<&Config>, args: &Args, config_path: Option<PathBuf>) -> Result<Self> {
        let file = file.cloned().unwrap_or_default();

        let url = args
            .broker
            .broker
            .clone()
            .or(file.broker.url.clone())
            .unwrap_or_else(|| DEFAULT_BROKER_URL.to_string());
        let (host, port, tls) = parse_broker_url(&url)?;

        let inline_password = args
            .broker
            .password
            .clone()
            .or(file.broker.password.clone());
        let password_file = args
            .broker
            .password_file
            .clone()
            .or(file.broker.password_file.clone());
        let password = match (inline_password, password_file) {
            (Some(_), Some(path)) => bail!(
                "both a password and a password file were configured (`{}`)",
                path.display()
            ),
            (Some(password), None) => Some(password),
            (None, Some(path)) => Some(read_secret(&path)?),
            (None, None) => None,
        };

        let broker = Broker {
            url,
            host,
            port,
            tls,
            client_id: args
                .broker
                .client_id
                .clone()
                .or(file.broker.client_id.clone())
                .unwrap_or_else(|| DEFAULT_CLIENT_ID.to_string()),
            username: args
                .broker
                .username
                .clone()
                .or(file.broker.username.clone()),
            password,
            clean_session: tri(args.broker.clean_session, args.broker.no_clean_session)
                .or(file.broker.clean_session)
                .unwrap_or(false),
            keep_alive: args
                .broker
                .keep_alive
                .or(file.broker.keep_alive)
                .unwrap_or(DEFAULT_KEEP_ALIVE),
            ca_file: args.broker.ca_file.clone().or(file.broker.ca_file.clone()),
        };

        let cli_qos = args.topic.qos;
        let mut topics: Vec<Topic> = Vec::new();
        for entry in &file.topics {
            let filter = entry
                .filter
                .clone()
                .context("each `[[topics]]` entry needs a `filter`")?;
            topics.push(Topic {
                filter,
                qos: entry.qos.or(cli_qos).unwrap_or(DEFAULT_QOS),
                out_file: entry.out_file.clone(),
            });
        }
        for filter in &args.topic.topics {
            if topics.iter().any(|existing| &existing.filter == filter) {
                continue;
            }
            topics.push(Topic {
                filter: filter.clone(),
                qos: cli_qos.unwrap_or(DEFAULT_QOS),
                out_file: None,
            });
        }

        let mut routes = Vec::with_capacity(file.routes.len());
        for entry in &file.routes {
            let filter = entry
                .filter
                .clone()
                .context("each `[[routes]]` entry needs a `filter`")?;
            let out_file = entry.out_file.clone().context(
                "each `[[routes]]` entry needs an `out_file`; a nameless route does nothing",
            )?;
            routes.push(Route { filter, out_file });
        }

        let output = Output {
            dir: args
                .output
                .out_dir
                .clone()
                .or(file.output.dir.clone())
                .unwrap_or_else(|| PathBuf::from(DEFAULT_OUT_DIR)),
            out_file: args
                .output
                .out_file
                .clone()
                .or(file.output.out_file.clone())
                .unwrap_or_else(|| DEFAULT_OUT_FILE.to_string()),
            format: args
                .output
                .format
                .or(file.output.format)
                .unwrap_or_default(),
            json: args.output.json.or(file.output.json).unwrap_or_default(),
            stdout: tri(args.output.stdout, args.output.no_stdout)
                .or(file.output.stdout)
                .unwrap_or(false),
        };

        let rotation = RotationSettings {
            schedule: args
                .rotation
                .rotate
                .clone()
                .or(file.rotation.schedule.clone())
                .unwrap_or_default(),
            max_size: args
                .rotation
                .max_size
                .or(file.rotation.max_size)
                .map(|size| size.as_u64()),
            max_files: args
                .rotation
                .max_files
                .or(file.rotation.max_files)
                .unwrap_or(DEFAULT_MAX_FILES),
        };

        let runtime = ResolvedRuntime {
            queue_capacity: args
                .runtime
                .queue_capacity
                .or(file.runtime.queue_capacity)
                .unwrap_or(DEFAULT_QUEUE_CAPACITY),
            on_full: args
                .runtime
                .on_full
                .or(file.runtime.on_full)
                .unwrap_or_default(),
            flush_interval: args
                .runtime
                .flush_interval
                .or(file.runtime.flush_interval)
                .unwrap_or(DEFAULT_FLUSH_INTERVAL),
            ack_after_write: tri(
                args.runtime.ack_after_write,
                args.runtime.no_ack_after_write,
            )
            .or(file.runtime.ack_after_write)
            .unwrap_or(false),
            reopen_on_sighup: tri(
                args.runtime.reopen_on_sighup,
                args.runtime.no_reopen_on_sighup,
            )
            .or(file.runtime.reopen_on_sighup)
            .unwrap_or(true),
            stats_interval: args
                .runtime
                .stats_interval
                .or(file.runtime.stats_interval)
                .unwrap_or(DEFAULT_STATS_INTERVAL),
        };

        let app_log = AppLog {
            level: args
                .app_log
                .log_level
                .clone()
                .or(file.app_log.level.clone())
                .unwrap_or_else(|| DEFAULT_LOG_LEVEL.to_string()),
            file: args
                .app_log
                .app_log_file
                .clone()
                .or(file.app_log.file.clone()),
        };

        Ok(Self {
            broker,
            topics,
            routes,
            output,
            rotation,
            runtime,
            app_log,
            config_path,
        })
    }
}

impl Settings {
    /// Rejects combinations that cannot work, so failures happen at startup
    /// rather than on the first message. Returns non-fatal warnings.
    pub fn validate(&self) -> Result<Vec<String>> {
        if self.topics.is_empty() {
            bail!(
                "at least one topic filter is required: pass --topic 'app/#' or add a \
                 [[topics]] entry to the config file"
            );
        }

        for topic in &self.topics {
            topic::validate_filter(&topic.filter)
                .map_err(|e| anyhow::anyhow!("invalid topic filter: {e}"))?;
            if topic.qos > 2 {
                bail!("qos for `{}` must be 0, 1 or 2", topic.filter);
            }
            if let Some(template) = &topic.out_file {
                validate_template(template)?;
            }
        }

        for route in &self.routes {
            topic::validate_filter(&route.filter)
                .map_err(|e| anyhow::anyhow!("invalid route filter: {e}"))?;
            validate_template(&route.out_file)?;
        }

        if self.rotation.max_files == 0 {
            bail!("rotation.max_files must be at least 1");
        }
        if self.runtime.queue_capacity == 0 {
            bail!("runtime.queue_capacity must be at least 1");
        }
        if self.runtime.flush_interval.is_zero() {
            bail!("runtime.flush_interval must be greater than zero");
        }
        if !self.output.stdout {
            validate_template(&self.output.out_file)?;
        }
        if self.broker.ca_file.is_some() && !self.broker.tls {
            bail!("a CA file was configured but the broker URL is not mqtts://");
        }
        let mut warnings = Vec::new();
        if self.broker.password.is_some() && self.broker.username.is_none() {
            warnings.push("a password was configured without a username".to_string());
        }
        if self.output.format == OutputFormat::Raw && self.output.json != JsonMode::Auto {
            warnings.push("the json mode has no effect with format = raw".to_string());
        }
        if !self.output.stdout
            && self.rotation.schedule == Schedule::Never
            && self.rotation.max_size.is_none()
        {
            warnings.push(
                "no rotation configured: the active log file will grow without bound".to_string(),
            );
        }

        Ok(warnings)
    }

    /// The resolved configuration as TOML; it can be fed back in with `--config`.
    pub fn to_toml(&self, redact: bool) -> Result<String> {
        toml::to_string_pretty(&self.to_config(redact))
            .context("serialising the resolved configuration")
    }

    fn to_config(&self, redact: bool) -> Config {
        let password = if redact && self.broker.password.is_some() {
            Some("***".to_string())
        } else {
            self.broker.password.clone()
        };

        Config {
            broker: BrokerCfg {
                url: Some(self.broker.url.clone()),
                client_id: Some(self.broker.client_id.clone()),
                username: self.broker.username.clone(),
                password,
                password_file: None,
                clean_session: Some(self.broker.clean_session),
                keep_alive: Some(self.broker.keep_alive),
                ca_file: self.broker.ca_file.clone(),
            },
            topics: self
                .topics
                .iter()
                .map(|topic| TopicCfg {
                    filter: Some(topic.filter.clone()),
                    qos: Some(topic.qos),
                    out_file: topic.out_file.clone(),
                })
                .collect(),
            routes: self
                .routes
                .iter()
                .map(|route| RouteCfg {
                    filter: Some(route.filter.clone()),
                    out_file: Some(route.out_file.clone()),
                })
                .collect(),
            output: OutputCfg {
                dir: Some(self.output.dir.clone()),
                out_file: Some(self.output.out_file.clone()),
                format: Some(self.output.format),
                json: Some(self.output.json),
                stdout: Some(self.output.stdout),
            },
            rotation: RotationCfg {
                schedule: Some(self.rotation.schedule.clone()),
                max_size: self.rotation.max_size.map(ByteSize),
                max_files: Some(self.rotation.max_files),
            },
            runtime: RuntimeCfg {
                queue_capacity: Some(self.runtime.queue_capacity),
                on_full: Some(self.runtime.on_full),
                flush_interval: Some(self.runtime.flush_interval),
                ack_after_write: Some(self.runtime.ack_after_write),
                reopen_on_sighup: Some(self.runtime.reopen_on_sighup),
                stats_interval: Some(self.runtime.stats_interval),
            },
            app_log: AppLogCfg {
                level: Some(self.app_log.level.clone()),
                file: self.app_log.file.clone(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    fn parse(argv: &[&str]) -> Args {
        let mut full = vec!["mqtt-log"];
        full.extend_from_slice(argv);
        Args::try_parse_from(full).expect("args should parse")
    }

    fn merged(argv: &[&str], file: Option<Config>) -> Settings {
        let args = parse(argv);
        Settings::merge(file.as_ref(), &args, None).expect("merge should succeed")
    }

    #[test]
    fn defaults_are_sane() {
        let settings = merged(&["-t", "app/#"], None);

        assert_eq!(settings.broker.url, "mqtt://localhost:1883");
        assert_eq!(settings.broker.host, "localhost");
        assert_eq!(settings.broker.port, 1883);
        assert!(!settings.broker.tls);
        assert!(!settings.broker.clean_session, "persistent by default");
        assert_eq!(settings.broker.keep_alive, Duration::from_secs(30));
        assert_eq!(settings.topics.len(), 1);
        assert!(settings.routes.is_empty());
        assert_eq!(settings.topics[0].qos, 1);
        assert_eq!(settings.output.dir, PathBuf::from("logs"));
        assert_eq!(settings.output.out_file, "app.log");
        assert_eq!(settings.output.format, OutputFormat::Jsonl);
        assert_eq!(settings.output.json, JsonMode::Auto);
        assert!(!settings.output.stdout);
        assert_eq!(settings.rotation.schedule, Schedule::Never);
        assert_eq!(settings.rotation.max_size, None);
        assert_eq!(settings.rotation.max_files, 9);
        assert_eq!(settings.runtime.queue_capacity, 10_000);
        assert_eq!(settings.runtime.on_full, FullPolicy::Block);
        assert!(settings.runtime.reopen_on_sighup);
        assert!(!settings.runtime.ack_after_write);
    }

    #[test]
    fn command_line_beats_the_config_file() {
        let file = Config {
            broker: BrokerCfg {
                url: Some("mqtt://from-file:1883".to_string()),
                client_id: Some("file-client".to_string()),
                ..BrokerCfg::default()
            },
            output: OutputCfg {
                dir: Some(PathBuf::from("/from/file")),
                format: Some(OutputFormat::Raw),
                ..OutputCfg::default()
            },
            rotation: RotationCfg {
                schedule: Some(Schedule::Daily),
                max_size: Some(ByteSize::mib(10)),
                ..RotationCfg::default()
            },
            ..Config::default()
        };

        let settings = merged(
            &[
                "-t",
                "app/#",
                "-b",
                "mqtts://cli:8883",
                "--out-dir",
                "/from/cli",
                "--rotate",
                "hourly",
            ],
            Some(file),
        );

        assert_eq!(settings.broker.url, "mqtts://cli:8883");
        assert_eq!(settings.broker.host, "cli");
        assert!(settings.broker.tls);
        assert_eq!(settings.broker.client_id, "file-client", "not overridden");
        assert_eq!(settings.output.dir, PathBuf::from("/from/cli"));
        assert_eq!(settings.output.format, OutputFormat::Raw, "not overridden");
        assert_eq!(settings.rotation.schedule, Schedule::Hourly);
        assert_eq!(settings.rotation.max_size, Some(10 * 1024 * 1024));
    }

    #[test]
    fn config_file_topics_and_cli_topics_are_combined() {
        let file = Config {
            topics: vec![
                TopicCfg {
                    filter: Some("app/#".to_string()),
                    qos: Some(2),
                    out_file: Some("app.log".to_string()),
                },
                TopicCfg {
                    filter: Some("sensors/+/log".to_string()),
                    qos: None,
                    out_file: None,
                },
            ],
            ..Config::default()
        };

        let settings = merged(&["-t", "extra/one", "-t", "app/#"], Some(file));

        let filters: Vec<&str> = settings.topics.iter().map(|t| t.filter.as_str()).collect();
        assert_eq!(filters, ["app/#", "sensors/+/log", "extra/one"]);
        assert_eq!(settings.topics[0].qos, 2);
        assert_eq!(settings.topics[1].qos, 1, "falls back to the default");
        assert_eq!(settings.topics[0].out_file.as_deref(), Some("app.log"));
        assert_eq!(settings.topics[2].out_file, None);
    }

    #[test]
    fn cli_qos_applies_to_every_filter() {
        let settings = merged(&["-t", "a/#", "-t", "b/#", "--qos", "0"], None);
        assert!(settings.topics.iter().all(|t| t.qos == 0));
    }

    #[test]
    fn negated_flags_win_over_the_config_file() {
        let file = Config {
            runtime: RuntimeCfg {
                ack_after_write: Some(true),
                reopen_on_sighup: Some(true),
                ..RuntimeCfg::default()
            },
            output: OutputCfg {
                stdout: Some(true),
                ..OutputCfg::default()
            },
            broker: BrokerCfg {
                clean_session: Some(false),
                ..BrokerCfg::default()
            },
            ..Config::default()
        };

        let settings = merged(
            &[
                "-t",
                "app/#",
                "--no-ack-after-write",
                "--no-reopen-on-sighup",
                "--no-stdout",
                "--clean-session",
            ],
            Some(file),
        );

        assert!(!settings.runtime.ack_after_write);
        assert!(!settings.runtime.reopen_on_sighup);
        assert!(!settings.output.stdout);
        assert!(settings.broker.clean_session);
    }

    #[test]
    fn broker_urls_are_parsed() {
        assert_eq!(
            parse_broker_url("mqtt://host:1884").unwrap(),
            ("host".to_string(), 1884, false)
        );
        assert_eq!(
            parse_broker_url("mqtts://host").unwrap(),
            ("host".to_string(), 8883, true)
        );
        assert_eq!(
            parse_broker_url("host:2000").unwrap(),
            ("host".to_string(), 2000, false)
        );
        assert_eq!(
            parse_broker_url("mqtt://[::1]:1885").unwrap(),
            ("[::1]".to_string(), 1885, false)
        );
        assert_eq!(
            parse_broker_url("mqtt://host:1883/ignored/path").unwrap(),
            ("host".to_string(), 1883, false)
        );
        assert!(parse_broker_url("http://host").is_err());
        assert!(parse_broker_url("mqtt://").is_err());
    }

    #[test]
    fn schedules_parse_from_strings() {
        assert_eq!("never".parse::<Schedule>().unwrap(), Schedule::Never);
        assert_eq!("DAILY".parse::<Schedule>().unwrap(), Schedule::Daily);
        assert_eq!("hourly".parse::<Schedule>().unwrap(), Schedule::Hourly);
        assert_eq!(
            "15m".parse::<Schedule>().unwrap(),
            Schedule::Every(Duration::from_secs(900))
        );
        assert!("fortnightly".parse::<Schedule>().is_err());
        assert!("0s".parse::<Schedule>().is_err());
    }

    #[test]
    fn schedule_display_round_trips_through_from_str() {
        for schedule in [
            Schedule::Never,
            Schedule::Hourly,
            Schedule::Daily,
            Schedule::Every(Duration::from_secs(900)),
        ] {
            let text = schedule.to_string();
            assert_eq!(text.parse::<Schedule>().unwrap(), schedule, "{text}");
        }
    }

    #[test]
    fn templates_reject_escapes_and_unknown_tokens() {
        assert!(validate_template("app.log").is_ok());
        assert!(validate_template("{topic}.log").is_ok());
        assert!(validate_template("sub/{topic}.log").is_ok());
        assert!(validate_template("/etc/passwd").is_err());
        assert!(validate_template("../escape.log").is_err());
        assert!(validate_template("sub/../../escape.log").is_err());
        assert!(validate_template("").is_err());
        assert!(validate_template("{host}.log").is_err());
        assert!(validate_template("{topic.log").is_err());
    }

    #[test]
    fn validation_requires_topics() {
        let settings = merged(&[], None);
        let error = settings.validate().unwrap_err().to_string();
        assert!(error.contains("topic filter is required"), "{error}");
    }

    #[test]
    fn validation_rejects_bad_filters() {
        let args = parse(&["-t", "app/#/nope"]);
        let settings = Settings::merge(None, &args, None).unwrap();
        assert!(settings.validate().is_err());
    }

    #[test]
    fn validation_rejects_a_ca_file_without_tls() {
        let mut settings = merged(&["-t", "app/#", "--broker", "mqtt://host:1883"], None);
        settings.broker.ca_file = Some(PathBuf::from("/tmp/ca.pem"));
        assert!(settings.validate().is_err());
    }

    #[test]
    fn resolved_config_round_trips_through_toml() {
        let args = parse(&[
            "-t",
            "app/#",
            "--broker",
            "mqtts://host:8883",
            "--out-dir",
            "/var/log/mqtt-log",
            "--out-file",
            "{topic}.log",
            "--format",
            "raw",
            "--json",
            "never",
            "--rotate",
            "daily",
            "--max-size",
            "100MB",
            "--max-files",
            "4",
            "--flush-interval",
            "250ms",
            "--ack-after-write",
            "-u",
            "user",
            "-p",
            "secret",
            "--client-id",
            "recv-1",
        ]);
        let mut settings = Settings::merge(None, &args, None).unwrap();
        settings.routes.push(Route {
            filter: "logs/device".to_string(),
            out_file: "device.log".to_string(),
        });
        settings.validate().unwrap();

        let rendered = settings.to_toml(true).unwrap();
        assert!(rendered.contains("password = \"***\""), "{rendered}");
        let reparsed = toml::from_str::<Config>(&rendered).expect("output must be valid TOML");

        assert_eq!(reparsed.broker.url.as_deref(), Some("mqtts://host:8883"));
        assert_eq!(reparsed.broker.username.as_deref(), Some("user"));
        assert_eq!(reparsed.broker.client_id.as_deref(), Some("recv-1"));
        assert_eq!(
            reparsed.output.dir,
            Some(PathBuf::from("/var/log/mqtt-log"))
        );
        assert_eq!(reparsed.output.out_file.as_deref(), Some("{topic}.log"));
        assert_eq!(reparsed.output.format, Some(OutputFormat::Raw));
        assert_eq!(reparsed.rotation.schedule, Some(Schedule::Daily));
        assert_eq!(reparsed.rotation.max_files, Some(4));
        assert_eq!(
            reparsed.runtime.flush_interval,
            Some(Duration::from_millis(250))
        );
        assert_eq!(reparsed.runtime.ack_after_write, Some(true));
        assert_eq!(reparsed.topics[0].filter.as_deref(), Some("app/#"));
        assert_eq!(reparsed.routes.len(), 1, "{rendered}");
        assert_eq!(reparsed.routes[0].filter.as_deref(), Some("logs/device"));
        assert_eq!(reparsed.routes[0].out_file.as_deref(), Some("device.log"));

        let size = reparsed.rotation.max_size.expect("max size kept");
        assert_eq!(size.as_u64(), settings.rotation.max_size.unwrap());
        assert_eq!(size.as_u64(), 100 * 1_000_000, "100MB is 100 decimal MB");
    }

    #[test]
    fn toml_file_accepts_human_friendly_units() {
        let config: Config = toml::from_str(
            r#"
            [broker]
            url = "mqtt://example:1883"
            keep_alive = "45s"

            [[topics]]
            filter = "app/#"
            qos = 1

            [rotation]
            schedule = "hourly"
            max_size = "512KiB"

            [runtime]
            flush_interval = "2s"
            "#,
        )
        .expect("should parse");

        assert_eq!(config.broker.keep_alive, Some(Duration::from_secs(45)));
        assert_eq!(
            config.rotation.max_size.map(|size| size.as_u64()),
            Some(512 * 1024)
        );
        assert_eq!(config.runtime.flush_interval, Some(Duration::from_secs(2)));
        assert_eq!(config.rotation.schedule, Some(Schedule::Hourly));
    }

    #[test]
    fn password_files_are_read_without_the_trailing_newline() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pass");
        std::fs::write(&path, "hunter2\n").unwrap();

        let settings = merged(
            &[
                "-t",
                "app/#",
                "-u",
                "user",
                "--password-file",
                path.to_str().unwrap(),
            ],
            None,
        );
        assert_eq!(settings.broker.password.as_deref(), Some("hunter2"));
    }

    #[test]
    fn password_files_are_literal_apart_from_trailing_line_endings() {
        let dir = tempfile::tempdir().unwrap();

        let cases = [
            ("crlf", "hunter2\r\n", "hunter2"),
            ("bare", "hunter2", "hunter2"),
            ("blank-lines", "hunter2\n\n\n", "hunter2"),
            // Quotes and spaces are part of the password: do not quote the file.
            ("quoted", "\"hunter2\"\n", "\"hunter2\""),
            ("padded", " hunter2 \n", " hunter2 "),
        ];

        for (name, on_disk, expected) in cases {
            let path = dir.path().join(name);
            std::fs::write(&path, on_disk).unwrap();
            let settings = merged(
                &[
                    "-t",
                    "app/#",
                    "-u",
                    "user",
                    "--password-file",
                    path.to_str().unwrap(),
                ],
                None,
            );
            assert_eq!(
                settings.broker.password.as_deref(),
                Some(expected),
                "case {name}"
            );
        }
    }

    #[test]
    fn a_password_and_a_password_file_conflict() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pass");
        std::fs::write(&path, "hunter2\n").unwrap();

        let args = parse(&[
            "-t",
            "app/#",
            "-p",
            "inline",
            "--password-file",
            path.to_str().unwrap(),
        ]);
        assert!(Settings::merge(None, &args, None).is_err());
    }

    #[test]
    fn unknown_config_keys_are_reported() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mqtt-log.toml");
        std::fs::write(&path, "[rotation]\nmax_file = 3\n").unwrap();

        assert!(
            load(&path, true, &mut Vec::new()).is_err(),
            "--strict fails"
        );

        let mut warnings = Vec::new();
        assert!(load(&path, false, &mut warnings).is_ok());
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].contains("rotation.max_file"), "{warnings:?}");
    }

    #[test]
    fn routes_are_merged_in_file_order() {
        let file = Config {
            routes: vec![
                RouteCfg {
                    filter: Some("logs/matrix-clock-806599ba7cb0".to_string()),
                    out_file: Some("matrix-clock-s3.log".to_string()),
                },
                RouteCfg {
                    filter: Some("logs/matrix-clock-404cca429418".to_string()),
                    out_file: Some("matrix-clock-c6.log".to_string()),
                },
            ],
            ..Config::default()
        };

        let settings = merged(&["-t", "logs/#"], Some(file));
        settings.validate().unwrap();

        let routes: Vec<(&str, &str)> = settings
            .routes
            .iter()
            .map(|route| (route.filter.as_str(), route.out_file.as_str()))
            .collect();
        assert_eq!(
            routes,
            [
                ("logs/matrix-clock-806599ba7cb0", "matrix-clock-s3.log"),
                ("logs/matrix-clock-404cca429418", "matrix-clock-c6.log"),
            ]
        );
        assert_eq!(settings.topics.len(), 1, "routes never subscribe");
    }

    #[test]
    fn a_route_without_an_out_file_is_rejected() {
        let file = Config {
            routes: vec![RouteCfg {
                filter: Some("logs/a".to_string()),
                out_file: None,
            }],
            ..Config::default()
        };

        let error = Settings::merge(Some(&file), &parse(&["-t", "logs/#"]), None).unwrap_err();
        assert!(error.to_string().contains("needs an `out_file`"), "{error}");
    }

    #[test]
    fn a_route_without_a_filter_is_rejected() {
        let file = Config {
            routes: vec![RouteCfg {
                filter: None,
                out_file: Some("a.log".to_string()),
            }],
            ..Config::default()
        };

        let error = Settings::merge(Some(&file), &parse(&["-t", "logs/#"]), None).unwrap_err();
        assert!(error.to_string().contains("needs a `filter`"), "{error}");
    }

    #[test]
    fn route_templates_cannot_escape_the_output_directory() {
        let file = Config {
            routes: vec![RouteCfg {
                filter: Some("logs/a".to_string()),
                out_file: Some("../a.log".to_string()),
            }],
            ..Config::default()
        };

        let settings = Settings::merge(Some(&file), &parse(&["-t", "logs/#"]), None).unwrap();
        let error = settings.validate().unwrap_err();
        assert!(
            error.to_string().contains("must not contain `..`"),
            "{error}"
        );
    }

    #[test]
    fn route_filters_are_validated_like_subscriptions() {
        let file = Config {
            routes: vec![RouteCfg {
                filter: Some("logs/#/x".to_string()),
                out_file: Some("x.log".to_string()),
            }],
            ..Config::default()
        };

        let settings = Settings::merge(Some(&file), &parse(&["-t", "logs/#"]), None).unwrap();
        let error = settings.validate().unwrap_err();
        assert!(
            error.to_string().contains("invalid route filter"),
            "{error}"
        );
    }
}
