//! Rotating file sinks for the payload lane.
//!
//! `rolling-file` gives us the appender (Debian style `app.log`, `app.log.1`,
//! ..., `app.log.N`) and a public `RollingCondition` trait. We supply our own
//! condition so that a rotation schedule can be an arbitrary aligned window and
//! so that a restart does not glue two windows into one file.

use std::collections::HashMap;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Local, TimeZone, Timelike};
use rolling_file::{RollingCondition, RollingFileAppender};

use crate::config::Schedule;
use crate::topic;

/// Decides when the active file is full or the window has moved on.
#[derive(Debug, Clone)]
pub struct Rotation {
    schedule: Schedule,
    max_size: Option<u64>,
    /// Boundary of the window the current file belongs to.
    boundary: Option<DateTime<Local>>,
}

impl Rotation {
    pub fn new(schedule: Schedule, max_size: Option<u64>) -> Self {
        Self {
            schedule,
            max_size,
            boundary: None,
        }
    }

    /// Seeds the window from the existing file's mtime.
    ///
    /// Without this, a restart at 11:05 would keep appending to the file that
    /// belongs to the 10:00 window, because `rolling-file` only compares writes
    /// it has seen itself.
    pub fn resuming_from(mut self, mtime: Option<std::time::SystemTime>) -> Self {
        self.boundary = mtime
            .map(DateTime::<Local>::from)
            .and_then(|time| self.boundary_for(&time));
        self
    }

    /// The start of the window `now` falls into, if the schedule rotates at all.
    fn boundary_for(&self, now: &DateTime<Local>) -> Option<DateTime<Local>> {
        match self.schedule {
            Schedule::Never => None,
            Schedule::Hourly => now.with_minute(0)?.with_second(0)?.with_nanosecond(0),
            Schedule::Daily => {
                let midnight = now.date_naive().and_hms_opt(0, 0, 0)?;
                Some(
                    Local
                        .from_local_datetime(&midnight)
                        .earliest()
                        .unwrap_or(*now),
                )
            }
            Schedule::Every(interval) => {
                let seconds = interval.as_secs().max(1) as i64;
                let midnight = now.date_naive().and_hms_opt(0, 0, 0)?;
                let midnight = Local
                    .from_local_datetime(&midnight)
                    .earliest()
                    .unwrap_or(*now);
                let elapsed = now.timestamp() - midnight.timestamp();
                let slot = elapsed.div_euclid(seconds).max(0);
                Some(midnight + chrono::Duration::seconds(slot * seconds))
            }
        }
    }
}

impl RollingCondition for Rotation {
    fn should_rollover(&mut self, now: &DateTime<Local>, current_filesize: u64) -> bool {
        let current_boundary = self.boundary_for(now);
        let mut roll = false;

        match (current_boundary, self.boundary) {
            (Some(new), Some(seen)) if new != seen => roll = true,
            (Some(new), None) => self.boundary = Some(new),
            _ => {}
        }

        if let Some(max) = self.max_size
            && current_filesize > 0
            && current_filesize >= max
        {
            roll = true;
        }

        if roll {
            self.boundary = current_boundary;
        }
        roll
    }
}

/// One rotating file, with the bookkeeping the writer task needs.
pub struct Sink {
    appender: RollingFileAppender<Rotation>,
    bytes: u64,
}

impl Sink {
    pub fn open(path: &Path, rotation: Rotation, max_files: usize) -> io::Result<Self> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent)?;
        }
        let mtime = fs::metadata(path).and_then(|meta| meta.modified()).ok();
        let appender = RollingFileAppender::new(path, rotation.resuming_from(mtime), max_files)?;
        Ok(Self { appender, bytes: 0 })
    }

    /// Forces a rotation, which is what SIGHUP does (logrotate style).
    pub fn reopen(&mut self) -> io::Result<()> {
        self.appender.rollover()
    }
}

impl Write for Sink {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written = self.appender.write(buf)?;
        self.bytes += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.appender.flush()
    }
}

/// All of the file sinks this process has open, keyed by resolved path.
///
/// Opened lazily so a wildcard subscription does not create a file per topic up
/// front, and so an unexpected topic cannot fail the whole run.
pub struct Sinks {
    root: PathBuf,
    default_template: String,
    rotation: Rotation,
    max_files: usize,
    files: HashMap<PathBuf, Sink>,
    records: u64,
}

impl Sinks {
    pub fn new(
        root: PathBuf,
        default_template: String,
        rotation: Rotation,
        max_files: usize,
    ) -> Self {
        Self {
            root,
            default_template,
            rotation,
            max_files,
            files: HashMap::new(),
            records: 0,
        }
    }

    /// Writes one already-encoded line to the file that `topic` maps to.
    pub fn write(&mut self, topic: &str, template: Option<&str>, line: &[u8]) -> io::Result<()> {
        let path = self.path_for(topic, template);
        if !self.files.contains_key(&path) {
            let sink = Sink::open(&path, self.rotation.clone(), self.max_files)?;
            tracing::debug!(path = %path.display(), "opened log file");
            self.files.insert(path.clone(), sink);
        }
        if let Some(sink) = self.files.get_mut(&path) {
            sink.write_all(line)?;
        }
        self.records += 1;
        Ok(())
    }

    fn path_for(&self, topic: &str, template: Option<&str>) -> PathBuf {
        let template = template.unwrap_or(&self.default_template);
        let name = template.replace("{topic}", &topic::sanitize(topic));
        self.root.join(name)
    }

    pub fn flush_all(&mut self) -> io::Result<()> {
        let mut result = Ok(());
        for sink in self.files.values_mut() {
            if let Err(error) = sink.flush() {
                result = Err(error);
            }
        }
        result
    }

    pub fn reopen_all(&mut self) -> io::Result<()> {
        let mut result = Ok(());
        for sink in self.files.values_mut() {
            if let Err(error) = sink.reopen() {
                result = Err(error);
            }
        }
        result
    }

    pub fn open_files(&self) -> usize {
        self.files.len()
    }

    pub fn records(&self) -> u64 {
        self.records
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use std::time::SystemTime;

    fn at(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> DateTime<Local> {
        Local
            .with_ymd_and_hms(year, month, day, hour, minute, 0)
            .single()
            .expect("valid local time")
    }

    fn write_at(appender: &mut RollingFileAppender<Rotation>, line: &str, when: DateTime<Local>) {
        appender
            .write_with_datetime(line.as_bytes(), &when)
            .unwrap();
        appender.flush().unwrap();
    }

    fn read(path: &Path) -> String {
        fs::read_to_string(path).unwrap_or_default()
    }

    fn verbose_sinks(root: &Path, template: &str) -> Sinks {
        Sinks::new(
            root.to_path_buf(),
            template.to_string(),
            Rotation::new(Schedule::Never, None),
            3,
        )
    }

    #[test]
    fn rotates_when_the_day_changes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log");
        let mut appender =
            RollingFileAppender::new(&path, Rotation::new(Schedule::Daily, None), 3).unwrap();

        write_at(&mut appender, "day one\n", at(2024, 1, 1, 23, 0));
        write_at(&mut appender, "day two\n", at(2024, 1, 2, 0, 1));

        assert_eq!(read(&dir.path().join("app.log")), "day two\n");
        assert_eq!(read(&dir.path().join("app.log.1")), "day one\n");
    }

    #[test]
    fn does_not_rotate_inside_the_same_window() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log");
        let mut appender =
            RollingFileAppender::new(&path, Rotation::new(Schedule::Daily, None), 3).unwrap();

        write_at(&mut appender, "early\n", at(2024, 1, 1, 0, 5));
        write_at(&mut appender, "late\n", at(2024, 1, 1, 23, 55));

        assert_eq!(read(&dir.path().join("app.log")), "early\nlate\n");
        assert!(!dir.path().join("app.log.1").exists());
    }

    #[test]
    fn rotates_on_an_aligned_interval() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log");
        let rotation = Rotation::new(Schedule::Every(std::time::Duration::from_secs(900)), None);
        let mut appender = RollingFileAppender::new(&path, rotation, 3).unwrap();

        write_at(&mut appender, "a\n", at(2024, 6, 1, 10, 1));
        write_at(&mut appender, "b\n", at(2024, 6, 1, 10, 14));
        write_at(&mut appender, "c\n", at(2024, 6, 1, 10, 16));

        assert_eq!(read(&dir.path().join("app.log")), "c\n");
        assert_eq!(read(&dir.path().join("app.log.1")), "a\nb\n");
    }

    #[test]
    fn rotates_when_the_size_limit_is_reached() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log");
        let rotation = Rotation::new(Schedule::Never, Some(10));
        let mut appender = RollingFileAppender::new(&path, rotation, 3).unwrap();

        write_at(&mut appender, "0123456789\n", at(2024, 1, 1, 0, 0));
        write_at(&mut appender, "next\n", at(2024, 1, 1, 0, 1));

        assert_eq!(read(&dir.path().join("app.log")), "next\n");
        assert_eq!(read(&dir.path().join("app.log.1")), "0123456789\n");
    }

    #[test]
    fn never_rotates_when_the_schedule_is_never() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log");
        let mut appender =
            RollingFileAppender::new(&path, Rotation::new(Schedule::Never, None), 3).unwrap();

        write_at(&mut appender, "old\n", at(2020, 1, 1, 0, 0));
        write_at(&mut appender, "new\n", at(2024, 1, 1, 0, 0));

        assert_eq!(read(&dir.path().join("app.log")), "old\nnew\n");
        assert!(!dir.path().join("app.log.1").exists());
    }

    #[test]
    fn a_restart_rolls_into_a_new_window() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log");
        fs::write(&path, "yesterday\n").unwrap();

        // Last written yesterday at 23:30; the process starts today at 09:00.
        let mtime = SystemTime::from(at(2024, 1, 1, 23, 30));
        let rotation = Rotation::new(Schedule::Daily, None).resuming_from(Some(mtime));
        let mut appender = RollingFileAppender::new(&path, rotation, 3).unwrap();

        write_at(&mut appender, "today\n", at(2024, 1, 2, 9, 0));

        assert_eq!(read(&dir.path().join("app.log")), "today\n");
        assert_eq!(read(&dir.path().join("app.log.1")), "yesterday\n");
    }

    #[test]
    fn rotated_files_are_retained_up_to_the_limit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log");
        let mut appender =
            RollingFileAppender::new(&path, Rotation::new(Schedule::Daily, None), 2).unwrap();

        for day in 1..=5 {
            write_at(
                &mut appender,
                &format!("day {day}\n"),
                at(2024, 1, day, 12, 0),
            );
        }

        assert_eq!(read(&dir.path().join("app.log")), "day 5\n");
        assert_eq!(read(&dir.path().join("app.log.1")), "day 4\n");
        assert_eq!(read(&dir.path().join("app.log.2")), "day 3\n");
        assert!(!dir.path().join("app.log.3").exists());
    }

    #[test]
    fn sinks_route_topics_to_templates() {
        let dir = tempfile::tempdir().unwrap();
        let mut sinks = verbose_sinks(dir.path(), "{topic}.log");

        sinks.write("sensors/kitchen/temp", None, b"one\n").unwrap();
        sinks.write("sensors/hall/temp", None, b"two\n").unwrap();
        sinks
            .write("sensors/kitchen/temp", None, b"three\n")
            .unwrap();
        sinks.flush_all().unwrap();

        assert_eq!(sinks.open_files(), 2);
        assert_eq!(sinks.records(), 3);
        assert_eq!(
            read(&dir.path().join("sensors_kitchen_temp.log")),
            "one\nthree\n"
        );
        assert_eq!(read(&dir.path().join("sensors_hall_temp.log")), "two\n");
    }

    #[test]
    fn per_topic_templates_override_the_default() {
        let dir = tempfile::tempdir().unwrap();
        let mut sinks = verbose_sinks(dir.path(), "app.log");

        sinks.write("app/one", None, b"default\n").unwrap();
        sinks
            .write("app/two", Some("special.log"), b"special\n")
            .unwrap();
        sinks.flush_all().unwrap();

        assert_eq!(read(&dir.path().join("app.log")), "default\n");
        assert_eq!(read(&dir.path().join("special.log")), "special\n");
    }

    #[test]
    fn sinks_create_missing_directories() {
        let dir = tempfile::tempdir().unwrap();
        let mut sinks = Sinks::new(
            dir.path().join("nested").join("logs"),
            "app.log".to_string(),
            Rotation::new(Schedule::Never, None),
            3,
        );

        sinks.write("app/1", None, b"line\n").unwrap();
        sinks.flush_all().unwrap();

        assert_eq!(read(&dir.path().join("nested/logs/app.log")), "line\n");
    }

    #[test]
    fn reopen_forces_a_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let mut sinks = verbose_sinks(dir.path(), "app.log");

        sinks.write("app/1", None, b"before\n").unwrap();
        sinks.reopen_all().unwrap();
        sinks.write("app/1", None, b"after\n").unwrap();
        sinks.flush_all().unwrap();

        assert_eq!(read(&dir.path().join("app.log")), "after\n");
        assert_eq!(read(&dir.path().join("app.log.1")), "before\n");
    }
}
