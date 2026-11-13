//! Turning an MQTT `Publish` into a line on disk.
//!
//! Everything here is pure: classification and encoding are easy to reason
//! about and to unit test without a broker or a filesystem.

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::{DateTime, Utc};
use rumqttc::{Publish, QoS};
use serde::Serialize;

use crate::config::{JsonMode, OutputFormat};

/// A message received from the broker, stamped with our own receive time.
///
/// The producer's payload is kept in one piece: it is a *log line*, not
/// something we get to rewrite.
#[derive(Debug)]
pub struct Record {
    pub recv_ts: DateTime<Utc>,
    pub msg: Publish,
}

impl Record {
    pub fn new(msg: Publish) -> Self {
        Self {
            recv_ts: Utc::now(),
            msg,
        }
    }

    pub fn topic(&self) -> &str {
        &self.msg.topic
    }

    pub fn qos(&self) -> u8 {
        match self.msg.qos {
            QoS::AtMostOnce => 0,
            QoS::AtLeastOnce => 1,
            QoS::ExactlyOnce => 2,
        }
    }
}

/// What a payload turned out to be.
#[derive(Debug, PartialEq)]
pub enum Payload<'a> {
    /// A JSON object or array, parsed.
    Json(serde_json::Value),
    /// Valid UTF-8 that is not JSON.
    Text(&'a str),
    /// Not valid UTF-8.
    Binary(&'a [u8]),
}

fn first_non_whitespace(bytes: &[u8]) -> Option<u8> {
    bytes.iter().copied().find(|b| !b.is_ascii_whitespace())
}

fn text_or_binary(bytes: &[u8]) -> Payload<'_> {
    match std::str::from_utf8(bytes) {
        Ok(text) => Payload::Text(text),
        Err(_) => Payload::Binary(bytes),
    }
}

/// Decides whether a payload is structured.
///
/// Deliberately conservative: only objects and arrays count as JSON, because
/// `"123"`, `"true"` and `"null"` are valid JSON documents but are far more
/// likely to be ordinary log lines. Valid UTF-8 is required in every mode.
pub fn classify(bytes: &[u8], mode: JsonMode) -> Payload<'_> {
    if mode == JsonMode::Never {
        return text_or_binary(bytes);
    }

    let looks_like_json = matches!(first_non_whitespace(bytes), Some(b'{') | Some(b'['));
    if mode == JsonMode::Auto && !looks_like_json {
        return text_or_binary(bytes);
    }

    let parsed = std::str::from_utf8(bytes)
        .ok()
        .and_then(|text| serde_json::from_str::<serde_json::Value>(text).ok());

    match parsed {
        Some(value) if value.is_object() || value.is_array() => Payload::Json(value),
        _ => text_or_binary(bytes),
    }
}

/// The JSON Lines envelope. Field order is fixed by the struct, which keeps
/// files diffable.
#[derive(Debug, Serialize)]
struct Envelope<'a> {
    recv_ts: String,
    topic: &'a str,
    qos: u8,
    retain: bool,
    dup: bool,
    kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    encoding: Option<&'static str>,
    payload: serde_json::Value,
}

/// Encodes one record as a single line, always newline terminated.
pub fn encode_line(record: &Record, format: OutputFormat, mode: JsonMode) -> Vec<u8> {
    match format {
        OutputFormat::Raw => raw_line(record.msg.payload.as_ref()),
        OutputFormat::Jsonl => jsonl_line(record, mode),
    }
}

fn raw_line(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + 1);
    out.extend_from_slice(payload);
    if !out.ends_with(b"\n") {
        out.push(b'\n');
    }
    out
}

fn jsonl_line(record: &Record, mode: JsonMode) -> Vec<u8> {
    let (kind, encoding, payload) = match classify(record.msg.payload.as_ref(), mode) {
        Payload::Json(value) => ("json", None, value),
        Payload::Text(text) => (
            "text",
            None,
            serde_json::Value::String(text.trim_end_matches(['\r', '\n']).to_string()),
        ),
        Payload::Binary(bytes) => (
            "binary",
            Some("base64"),
            serde_json::Value::String(BASE64.encode(bytes)),
        ),
    };

    let envelope = Envelope {
        recv_ts: record
            .recv_ts
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        topic: record.topic(),
        qos: record.qos(),
        retain: record.msg.retain,
        dup: record.msg.dup,
        kind,
        encoding,
        payload,
    };

    let mut out = serde_json::to_vec(&envelope).expect("envelope is always serialisable");
    out.push(b'\n');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(payload: &'static [u8]) -> Record {
        Record::new(Publish::new("app/1", QoS::AtLeastOnce, payload.to_vec()))
    }

    fn encoded(payload: &'static [u8], format: OutputFormat, mode: JsonMode) -> String {
        String::from_utf8(encode_line(&record(payload), format, mode)).unwrap()
    }

    #[test]
    fn object_payloads_are_structured() {
        match classify(br#"{"level":"warn","msg":"disk 91%"}"#, JsonMode::Auto) {
            Payload::Json(value) => assert_eq!(value["level"], "warn"),
            other => panic!("expected json, got {other:?}"),
        }
    }

    #[test]
    fn arrays_are_structured_too() {
        assert!(matches!(
            classify(b"[1,2,3]", JsonMode::Auto),
            Payload::Json(_)
        ));
    }

    #[test]
    fn bare_scalars_stay_text() {
        for payload in [
            &b"123"[..],
            &b"true"[..],
            &b"null"[..],
            &b"\"quoted\""[..],
            &b"plain log line"[..],
        ] {
            assert!(
                matches!(classify(payload, JsonMode::Auto), Payload::Text(_)),
                "{:?} should stay text",
                String::from_utf8_lossy(payload)
            );
        }
    }

    #[test]
    fn json_mode_overrides_detection() {
        assert!(matches!(
            classify(b"123", JsonMode::Never),
            Payload::Text("123")
        ));
        assert!(matches!(
            classify(b"plain", JsonMode::Always),
            Payload::Text("plain")
        ));
        assert!(matches!(
            classify(br#" {"a":1} "#, JsonMode::Always),
            Payload::Json(_)
        ));
    }

    #[test]
    fn invalid_utf8_is_binary() {
        assert!(matches!(
            classify(&[0xff, 0xfe, 0x00], JsonMode::Auto),
            Payload::Binary(_)
        ));
    }

    #[test]
    fn raw_output_is_byte_exact() {
        assert_eq!(
            encoded(b"hello", OutputFormat::Raw, JsonMode::Auto),
            "hello\n"
        );
        assert_eq!(
            encoded(b"already\n", OutputFormat::Raw, JsonMode::Auto),
            "already\n"
        );
        assert_eq!(
            encoded(b"two\nlines", OutputFormat::Raw, JsonMode::Auto),
            "two\nlines\n"
        );
    }

    #[test]
    fn jsonl_output_is_one_line_per_record() {
        let line = encoded(b"multi\nline\r\n", OutputFormat::Jsonl, JsonMode::Auto);
        assert!(line.ends_with('\n'));
        assert_eq!(line.matches('\n').count(), 1, "newlines must be escaped");

        let value: serde_json::Value = serde_json::from_str(line.trim_end()).unwrap();
        assert_eq!(value["kind"], "text");
        assert_eq!(value["payload"], "multi\nline");
        assert_eq!(value["topic"], "app/1");
        assert_eq!(value["qos"], 1);
        assert_eq!(value["retain"], false);
        assert!(value["recv_ts"].as_str().unwrap().ends_with('Z'));
    }

    #[test]
    fn jsonl_keeps_structured_payloads_nested() {
        let line = encoded(
            br#"{"ts":"2020-01-01T00:00:00Z","level":"info"}"#,
            OutputFormat::Jsonl,
            JsonMode::Auto,
        );
        let value: serde_json::Value = serde_json::from_str(line.trim_end()).unwrap();
        assert_eq!(value["kind"], "json");
        assert_eq!(value["payload"]["ts"], "2020-01-01T00:00:00Z");
        assert_eq!(value["payload"]["level"], "info");
    }

    #[test]
    fn jsonl_encodes_binary_as_base64() {
        let line = encoded(&[0xff, 0xfe], OutputFormat::Jsonl, JsonMode::Auto);
        let value: serde_json::Value = serde_json::from_str(line.trim_end()).unwrap();
        assert_eq!(value["kind"], "binary");
        assert_eq!(value["encoding"], "base64");
        assert_eq!(value["payload"], "//4=");
    }
}
