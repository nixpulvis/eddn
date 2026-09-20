//! The EDDN tool: one subscription, written down for whoever wants it.
//!
//! ```sh
//! eddn record --to /var/lib/galos/spool --retain 48h
//! ```
//!
//! **Optional, and off by default.** Two tools each subscribing costs 17.5
//! KiB/s of somebody else's infrastructure apiece and works with nothing
//! installed; this is for an operator who would rather be one subscriber,
//! or who wants replay after a crash. What it buys and what it costs is
//! `doc/PLAN-EDDN-SPOOL.md`, measured.
//!
//! A verb rather than a binary a verb: the crate is `eddn`, so the command
//! is `eddn`, and there is room beside `record` for the reading verbs
//! without a second name to install.

use clap::{Parser, Subcommand};
use eddn::spool::Recorder;
use eddn::{Feed, Galaxy, Network};
use std::io::{stderr, IsTerminal};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tracing::{error, info, warn};

/// How often a running recorder says what it has taken.
///
/// A minute: long enough not to be noise in a journal, short enough that a
/// silent recorder is a recorder that has stopped rather than one that is
/// merely quiet.
const TALLY: Duration = Duration::from_secs(60);

/// Read and record the EDDN feed.
#[derive(Parser)]
#[command(name = "eddn", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Write the feed to a spool directory, one segment an hour.
    Record {
        /// Where the segments and the consumers' cursors go.
        #[arg(long, value_name = "DIR")]
        to: PathBuf,
        /// The gateway to subscribe to.
        #[arg(long, default_value = eddn::URL)]
        url: String,
        /// Seconds of silence before the connection is replaced. Omitted,
        /// the connection is left alone however long it carries nothing.
        #[arg(long, value_name = "SECS", default_value_t = 120)]
        stall: u64,
        /// How much history to keep: `48h`, `7d`, `90m`, or seconds.
        ///
        /// Whole segments are pruned on the hour roll. A consumer whose
        /// cursor names one on its way out is warned about and the
        /// segment goes anyway: a consumer that has stopped must not be
        /// able to fill a disk.
        #[arg(long, value_name = "WINDOW", value_parser = window)]
        retain: Option<Duration>,
    },
}

fn main() {
    tracing_subscriber::fmt()
        .with_ansi(stderr().is_terminal())
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    match Cli::parse().command {
        Command::Record { to, url, stall, retain } => {
            record(&to, &url, Duration::from_secs(stall), retain)
        }
    }
}

/// Subscribe, and write every frame down exactly as it arrived.
///
/// Nothing is decompressed and nothing is parsed: a message this build's
/// deserialiser could not read is still in the spool after the parser is
/// fixed, and a tap that went through the parser could not have written it
/// down.
///
/// **Flushed per frame, and never `fsync`ed.** The buffer is there to make
/// a record one `write` rather than three; holding records back would put
/// messages a consumer is waiting for behind a buffer that fills at 3.4 KB
/// a message. What a kill loses is the frame being written, which is the
/// torn tail the format is built to ignore — see `eddn::spool`.
///
/// Ctrl-C is the default action on purpose: there is no state to close
/// out, and the partial record it may leave is one every reader already
/// handles.
fn record(to: &PathBuf, url: &str, stall: Duration, retain: Option<Duration>) {
    let mut recorder = match Recorder::open(to) {
        Ok(recorder) => match retain {
            Some(window) => recorder.retaining(window),
            None => recorder,
        },
        Err(err) => {
            error!(dir = %to.display(), error = %err, "cannot record here");
            std::process::exit(1);
        }
    };
    info!(
        dir = %to.display(),
        url = %url,
        retain = ?retain,
        "recording EDDN",
    );

    // Every galaxy: the recorder writes what arrives and a reader
    // filters, so a spool cannot be missing the test messages a consumer
    // asked for later. See `eddn::spool`.
    let mut feed = Network::open(url, Some(stall)).galaxy(Galaxy::ALL);
    let (mut taken, mut bytes) = (0u64, 0u64);
    let mut said = Instant::now();
    while let Some(frame) = feed.frame() {
        let frame = match frame {
            Ok(frame) => frame,
            // The socket's own failures, which the feed has already
            // replaced the connection over. Nothing to record.
            Err(err) => {
                warn!(error = %err, "the feed faulted");
                continue;
            }
        };
        taken += 1;
        bytes += frame.bytes.len() as u64;
        if let Err(err) = recorder.write(&frame) {
            // A disk that will not take a write is not a thing to carry
            // on through: the cursors and the segments that stand are
            // still valid, and a recorder that kept running would be one
            // writing nothing down while looking busy.
            error!(dir = %to.display(), error = %err, "cannot write");
            std::process::exit(1);
        }
        if said.elapsed() >= TALLY {
            info!(messages = taken, bytes = bytes, "recorded");
            said = Instant::now();
        }
    }
}

/// A retention window: `48h`, `7d`, `90m`, or a plain count of seconds.
fn window(text: &str) -> Result<Duration, String> {
    let (count, scale) = match text.chars().last() {
        Some('h') => (&text[..text.len() - 1], 3600),
        Some('d') => (&text[..text.len() - 1], 86_400),
        Some('m') => (&text[..text.len() - 1], 60),
        Some('s') => (&text[..text.len() - 1], 1),
        _ => (text, 1),
    };
    let count: u64 = count
        .parse()
        .map_err(|_| format!("{text:?} is not a window like 48h or 7d"))?;
    match count.checked_mul(scale) {
        Some(secs) => Ok(Duration::from_secs(secs)),
        None => Err(format!("{text:?} is longer than time")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A window is spelled the way an operator would say it
    #[test]
    fn a_window_reads_as_a_person_writes_it() {
        assert_eq!(window("48h"), Ok(Duration::from_secs(48 * 3600)));
        assert_eq!(window("7d"), Ok(Duration::from_secs(7 * 86_400)));
        assert_eq!(window("90m"), Ok(Duration::from_secs(5_400)));
        assert_eq!(window("30s"), Ok(Duration::from_secs(30)));
        assert_eq!(window("3600"), Ok(Duration::from_secs(3600)));
        assert!(window("a while").is_err());
        assert!(window("48hours").is_err());
    }
}
