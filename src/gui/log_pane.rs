//! Catching what the `eddn` crate traces so a window can show it
//!
//! The subscriber reports the health of its connection only through
//! [`tracing`] -- a connection lost and regained, a gateway gone quiet -- and
//! none of that comes back through the iterator. So the status the app shows is
//! read from the log rather than invented: [`LogLayer`] is a tracing layer that
//! keeps the last so many events in a [`LogBuffer`] the UI reads each frame.

use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::fmt::{Debug, Write as _};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::Layer;

/// How many log lines to keep
///
/// A window's worth many times over. Old lines fall off the front, the same as
/// the feed's own history, so a program left running does not grow behind its
/// log.
pub const CAPACITY: usize = 1000;

/// One line the crate said, and enough to show it
#[derive(Clone)]
pub struct LogLine {
    pub level: Level,
    pub target: String,
    pub message: String,
    pub at: DateTime<Utc>,
}

/// The shared, bounded store of log lines
///
/// Cloned between the tracing layer that fills it and the app that reads it, so
/// both hold the one buffer.
#[derive(Clone, Default)]
pub struct LogBuffer {
    lines: Arc<Mutex<VecDeque<LogLine>>>,
    /// A running count of warn-level lines filed. Kept apart from the ring so
    /// it survives lines falling off the front; read by the status bar's
    /// warning indicator.
    warnings: Arc<AtomicUsize>,
    /// A running count of error-level lines filed, kept apart from the ring the
    /// same as `warnings`; read by the status bar's error indicator.
    errors: Arc<AtomicUsize>,
}

impl LogBuffer {
    /// A copy of every line currently held, oldest first
    pub fn snapshot(&self) -> Vec<LogLine> {
        self.lines.lock().iter().cloned().collect()
    }

    /// How many warn-level lines have been filed since the program started
    pub fn warnings(&self) -> usize {
        self.warnings.load(Ordering::Relaxed)
    }

    /// Reset the warning count without touching the lines, for the pane's
    /// clear button: the history stays readable, only the badge is dismissed.
    pub fn clear_warnings(&self) {
        self.warnings.store(0, Ordering::Relaxed);
    }

    /// How many error-level lines have been filed since the program started
    pub fn errors(&self) -> usize {
        self.errors.load(Ordering::Relaxed)
    }

    /// Reset the error count without touching the lines, the error twin of
    /// [`clear_warnings`](Self::clear_warnings).
    pub fn clear_errors(&self) {
        self.errors.store(0, Ordering::Relaxed);
    }

    fn push(&self, line: LogLine) {
        match line.level {
            Level::WARN => {
                self.warnings.fetch_add(1, Ordering::Relaxed);
            }
            Level::ERROR => {
                self.errors.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
        let mut buffer = self.lines.lock();
        while buffer.len() >= CAPACITY {
            buffer.pop_front();
        }
        buffer.push_back(line);
    }
}

/// A [`tracing`] layer that files each event into a [`LogBuffer`]
pub struct LogLayer {
    buffer: LogBuffer,
}

impl LogLayer {
    pub fn new(buffer: LogBuffer) -> Self {
        LogLayer { buffer }
    }
}

/// Pulls an event's message out from the rest of its fields
///
/// The message is the human sentence and is what a log line leads with; every
/// other field is `name=value` after it. Kept apart so the sentence reads as
/// one whatever order the fields arrived in.
struct EventVisitor {
    message: String,
    fields: String,
}

impl Visit for EventVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        if field.name() == "message" {
            let _ = write!(self.message, "{:?}", value);
        } else {
            let _ = write!(self.fields, " {}={:?}", field.name(), value);
        }
    }
}

impl<S: Subscriber> Layer<S> for LogLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut visitor =
            EventVisitor { message: String::new(), fields: String::new() };
        event.record(&mut visitor);

        let metadata = event.metadata();
        self.buffer.push(LogLine {
            level: *metadata.level(),
            target: metadata.target().to_owned(),
            message: format!("{}{}", visitor.message, visitor.fields),
            at: Utc::now(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line(level: Level) -> LogLine {
        LogLine {
            level,
            target: "test".to_owned(),
            message: String::new(),
            at: Utc::now(),
        }
    }

    #[test]
    fn warnings_count_warn_lines_and_clear_leaves_the_history() {
        let buffer = LogBuffer::default();
        buffer.push(line(Level::INFO));
        buffer.push(line(Level::WARN));
        buffer.push(line(Level::WARN));
        assert_eq!(buffer.warnings(), 2);

        // Clearing dismisses the count but keeps the lines to read.
        buffer.clear_warnings();
        assert_eq!(buffer.warnings(), 0);
        assert_eq!(buffer.snapshot().len(), 3);
    }

    #[test]
    fn errors_count_error_lines_apart_from_warnings() {
        let buffer = LogBuffer::default();
        buffer.push(line(Level::WARN));
        buffer.push(line(Level::ERROR));
        buffer.push(line(Level::ERROR));
        assert_eq!(buffer.errors(), 2);
        assert_eq!(buffer.warnings(), 1);

        // Clearing dismisses the count but keeps the lines to read.
        buffer.clear_errors();
        assert_eq!(buffer.errors(), 0);
        assert_eq!(buffer.snapshot().len(), 3);
    }
}
