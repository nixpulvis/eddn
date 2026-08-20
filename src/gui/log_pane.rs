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
pub struct LogBuffer(Arc<Mutex<VecDeque<LogLine>>>);

impl LogBuffer {
    /// A copy of every line currently held, oldest first
    pub fn snapshot(&self) -> Vec<LogLine> {
        self.0.lock().iter().cloned().collect()
    }

    fn push(&self, line: LogLine) {
        let mut buffer = self.0.lock();
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
