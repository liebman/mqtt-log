# mqtt-log

Receives MQTT messages and writes them to log files on disk, with optional
rotation. Works with plain text log lines and with structured JSON payloads —
the shape of each payload is detected per message.

```
MQTT broker  ──►  rumqttc event loop  ──►  bounded queue  ──►  writer task  ──►  rotating files
```

The event loop never touches a file. It hands `Publish` packets to a single
writer task over a bounded channel, which is what gives ordered writes, a place
to batch flushes, and somewhere to acknowledge QoS1/2 messages **after** they
have been written.

## Quick start

```bash
cargo build --release

# everything under app/ into ./logs/app.log as JSON Lines
./target/release/mqtt-log --broker mqtt://localhost:1883 --topic 'app/#'

# per-topic files, verbatim payloads, rotating daily and at 100 MB
./target/release/mqtt-log -b mqtts://broker:8883 -u user -p pass \
    --topic 'app/#' --topic 'sensors/+/log' --qos 1 \
    --out-dir /var/log/mqtt-log --out-file '{topic}.log' \
    --format raw --rotate daily --max-size 100MB --max-files 9
```

Quote your topic filters: `#` and `+` are MQTT wildcards, and your shell will
otherwise treat `#` as a comment.

Check a configuration without consuming messages:

```bash
./target/release/mqtt-log --config /etc/mqtt-log/config.toml --check
./target/release/mqtt-log --config /etc/mqtt-log/config.toml --print-config
```

## Output formats

`--format jsonl` (default) — one object per line, always one physical line per
message, so multi-line payloads stay greppable:

```json
{"recv_ts":"2026-11-12T21:17:10.307Z","topic":"app/one","qos":1,"retain":false,"dup":false,"kind":"text","payload":"plain text line"}
{"recv_ts":"2026-11-12T21:17:10.317Z","topic":"app/two","qos":1,"retain":false,"dup":false,"kind":"json","payload":{"level":"warn","msg":"disk 91%"}}
```

`--format raw` — the payload bytes verbatim, plus a trailing newline if the
producer did not send one. Best when the producer already emits ready-to-read
lines and you do not want any envelope.

### How JSON is detected

| Situation | Classification |
|---|---|
| payload starts with `{` or `[` and parses | `kind: "json"`, payload nested |
| anything else | `kind: "text"` |
| `123`, `true`, `null`, `"quoted"` | **text** — valid JSON, but almost always a log line |
| invalid UTF-8 | `kind: "binary"`, `encoding: "base64"` |

`--json always` forces parsing (falling back to text), `--json never` disables
it. The producer's own timestamp is never touched: it stays inside `payload`,
and we add our own `recv_ts` alongside it.

## Rotation

| `--rotate` | `--max-size` | Behaviour |
|---|---|---|
| `never` (default) | — | append forever |
| `hourly` / `daily` / `15m` / `2h` | — | cut on window boundaries aligned to local midnight |
| `never` | `100MB` | size only |
| `daily` | `100MB` | whichever trips first |

Rotated files follow the Debian convention: `app.log`, `app.log.1` … and
`--max-files N` keeps the newest N (the oldest are deleted). `--max-files` must
be at least 1 — `rolling-file` treats 0 as 1, so we reject it outright.

Rotation is *lazy*: it is evaluated when a record is written, so an idle
subscription never produces empty files. On startup the current window is seeded
from the active file's mtime, so a restart at 11:05 does not append yesterday's
window to today's file.

## Durability

- `--clean-session false` (default) with a fixed `--client-id` lets a broker
  queue messages while the receiver is down (requires broker-side persistence).
- `--qos 1` is the default. At-least-once means you may see duplicates after a
  reconnect; the `dup` flag is included in every JSONL record.
- `--ack-after-write` switches the client to manual acks and acknowledges only
  after the record has been written and the buffer flushed. This makes "acked"
  mean "on disk" — at the cost of throughput.
- Buffered writes are flushed every `--flush-interval` (default 1s) and on
  shutdown. `flush` pushes to the OS; we do not `fsync`, so data survives a
  process crash but not a power cut.

## Configuration file

Every option is available on the command line, in a TOML file, and via
`MQTT_LOG_*` environment variables. Precedence, lowest to highest:

```
built-in defaults  <  config file  <  environment  <  command line
```

The file is looked up at `./mqtt-log.toml`,
`$XDG_CONFIG_HOME/mqtt-log/config.toml`, `~/.config/mqtt-log/config.toml`, then
`/etc/mqtt-log/config.toml`. Override with `--config FILE`; skip with
`--no-config`.

```toml
[broker]
url           = "mqtts://broker.example.com:8883"
client_id     = "log-recvr-1"
clean_session = false
keep_alive    = "30s"
username      = "logrecvr"
password_file = "/run/secrets/mqtt-pass"   # prefer this over an inline password
ca_file       = "/etc/ssl/certs/broker.pem"

# Per-topic qos and routing cannot be expressed on the command line.
[[topics]]
filter   = "app/#"
qos      = 1
out_file = "app.log"

[[topics]]
filter   = "sensors/+/log"
qos      = 0
out_file = "{topic}.log"

[output]
dir    = "/var/log/mqtt-log"
format = "jsonl"
json   = "auto"

[rotation]
schedule  = "daily"
max_size  = "100MB"     # or a plain byte count
max_files = 9

[runtime]
queue_capacity   = 10000
on_full          = "block"   # or "drop"
flush_interval   = "1s"
ack_after_write  = true
reopen_on_sighup = true
stats_interval   = "30s"

[app_log]
level = "info"
file  = "/var/log/mqtt-log/mqtt-log.log"
```

See `mqtt-log.example.toml` for a copyable version.

`--print-config` renders the merged result as a loadable TOML file with the
password redacted, which is usually the fastest way to answer "why did it do
that?".

Unknown keys are reported (`--strict` turns them into errors) and configuration
errors are shown with the offending line:

```
Error: /etc/mqtt-log/config.toml: invalid type: integer `5`, expected a string
  --> /etc/mqtt-log/config.toml:2:7
   |
  2 | url = 5
   |       ^
```

## Signals and shutdown

- `SIGTERM` / `Ctrl-C` — stop polling, disconnect, drain the queue, flush, exit.
- `SIGHUP` — rotate every open file (logrotate style), controlled by
  `--reopen-on-sighup` (default on). You can therefore either let mqtt-log rotate
  files, or let `logrotate(8)` rename them and signal us.

Diagnostics go to stderr, or to `app_log.file` when configured (daily rotation,
64 MB, 5 files). Received messages never go through the diagnostics logger — the
two lanes stay separate so payloads are never reformatted.

## Where the logs go

`--out-file` is a file name inside `--out-dir`; it may contain `{topic}`, which
is replaced with the topic where every `/`, `+`, `#` and other awkward
characters becomes `_`. So `sensors/kitchen/temp` becomes
`sensors_kitchen_temp.log` and can never escape the output directory. Absolute
paths and `..` are rejected.

`--stdout` writes the payload lane to standard output instead, which composes
nicely with `jq` and `tee`.

### Naming files per topic

`[[topics]] out_file` is per *subscription*, and every `[[topics]]` entry is also
subscribed. Adding an override above a wildcard (`logs/#`) therefore creates two
overlapping subscriptions, and MQTT delivers **one copy per matching
subscription** — both copies land in the override file:

```
{"recv_ts":"...T22:46:41.029Z","topic":"logs/device-a",...,"payload":"heartbeat"}
{"recv_ts":"...T22:46:41.029Z","topic":"logs/device-a",...,"payload":"heartbeat"}
```

`[[routes]]` avoids that. A route is a pure naming rule: the writer consults it,
the broker never sees it, so `logs/#` can stay the only subscription.

```toml
[[topics]]                                  # the only subscription
filter   = "logs/#"
qos      = 0
out_file = "{topic}.log"                    # fallback for unrouted topics

[[routes]]                                  # naming only, never subscribed
filter   = "logs/matrix-clock-806599ba7cb0"
out_file = "matrix-clock-s3.log"

[[routes]]
filter   = "logs/matrix-clock-404cca429418"
out_file = "matrix-clock-c6.log"
```

Resolution order, first match wins:

1. `[[routes]]`, in file order;
2. `[[topics]]` entries that set `out_file`, in file order;
3. `[output] out_file` (default `app.log`).

A `[[topics]]` entry that matches but sets no `out_file` still stops the search
and falls back to `[output]`, so older configs keep behaving as they always did.
Route filters are validated like subscriptions, and route templates like
`--out-file` (no absolute paths, no `..`, `{topic}` allowed). A wildcard route
such as `logs/device/#` covers both `logs/device` and its children.

## Known limitations

- No compression of rotated files (`rolling-file` does not support it; gzip would
  mean switching to `file-rotate`, which the `Sinks` seam makes possible).
- Rotated files are numbered, not date-named.
- `--on-full drop` drops the *newest* message when the queue is full; there is no
  drop-oldest queue.
- No `fsync`; see Durability above.
- MQTT 3.1.1 only for now (`rumqttc::v5` is available behind the same client if
  you need v5 properties).

## Development

```bash
cargo test                                   # 44 tests, no broker needed
cargo clippy --all-targets -- -D warnings
cargo fmt --check

# end-to-end against a local broker
mosquitto -p 18830 &
cargo run -- -b mqtt://127.0.0.1:18830 -t 'app/#' --stdout
mosquitto_pub -p 18830 -t app/one -m 'hello'
```

Rotation is deterministic in tests: `rolling-file` exposes
`write_with_datetime`, so daily/hourly/`15m`/size/restart behaviour is asserted
from synthetic clocks rather than by sleeping.

| Module | Responsibility |
|---|---|
| `cli` | clap surface, every field optional (tri-state flags use `--x`/`--no-x`) |
| `config` | defaults + TOML + env + CLI merged into `Settings`, validation |
| `mqtt` | connect, subscribe, wildcard filters, backoff, graceful stop |
| `record` | pure classification and line encoding |
| `sink` | rotation condition, single file sink, per-topic sink map |
| `writer` | the one task that owns files and acknowledges messages |
| `topic` | MQTT filter validation/matching and filename sanitisation |
| `stats` | shared counters and periodic reporting |
| `logging` | diagnostics lane (stderr or its own rotating file) |

