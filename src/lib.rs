//! Subscribe to journal and market messages from
//! [EDDN](https://github.com/EDCD/EDDN).
//!
//! [`subscribe`] connects to the gateway and returns an iterator over the
//! messages on it. That iterator does not end. A message that cannot be read
//! comes back as an [`Error`] and the next one is waited for, and a
//! connection that has stopped working is replaced.

mod connection;
mod error;
mod reporter;

pub use crate::connection::{
    HEARTBEAT_IVL_MS, HEARTBEAT_TIMEOUT_MS, POLL_INTERVAL_MS, RECONNECT_MAX_MS,
    RECONNECT_MIN_MS,
};
pub use crate::error::Error;

use crate::connection::{Connection, Stall};
use crate::reporter::Reporter;
use chrono::prelude::*;
use elite_journal::entry::{Entry, Event, Market};
use miniz_oxide::inflate;
use serde::Deserialize;
use std::thread;
use std::time::Duration;
use tracing::{info, warn, Level};

pub const URL: &'static str = "tcp://eddn.edcd.io:9500";

/// Top level EDDN message wrapper
#[derive(Debug, Deserialize)]
pub struct Envelope {
    #[serde(rename = "$schemaRef")]
    pub schema_ref: String,
    pub header: Header,
    pub message: Message,
}

/// Message uploader metadata
#[derive(Debug, Deserialize)]
pub struct Header {
    #[serde(rename = "gatewayTimestamp")]
    pub gateway_timestamp: DateTime<Utc>,
    #[serde(rename = "softwareName")]
    pub software_name: String,
    #[serde(rename = "softwareVersion")]
    pub software_version: String,
    #[serde(rename = "uploaderID")]
    pub uploader_id: String,
}

/// Payload of the message containing the parsed data
#[derive(Debug, Deserialize)]
// TODO: Don't use untagged, we need to write a custom deserialized that uses the $schemaRef.
// NOTE: [ "Docked", "FSDJump", "Scan", "Location", "SAASignalsFound", "CarrierJump" ]
//       https://github.com/EDCD/EDDN/blob/d9b5586a4ef5a5c4c1117ec4105b773697b468ac/schemas/journal-v1.0.json#L43
#[serde(untagged)]
pub enum Message {
    Journal(Entry<Event>),
    Commodity(Entry<Market>),
    // TODO
    // Shipyard,
    // Outfitting,
    // Blackmarket,

    // Untagged catchall, must be at the end.
    Other(serde_json::Value),
}

/// Subscribe to EDDN's ZMQ socket receiving all messages
///
/// `stall_timeout` is how long the gateway may publish nothing before its
/// connection is thrown away for a new one. `None` leaves the connection
/// alone however long it carries nothing, giving up the only cover there is
/// for the third case below and watching for the other two as ever.
///
/// # Connection resilience
///
/// A subscription outlives the connection carrying it. There are three ways
/// one stops working, a different thing notices each, and all three are
/// reported through [`tracing`] as they happen.
///
/// ## A connection that closes
///
/// The gateway restarts, or something between here and it drops the
/// connection and says so. libzmq sees the close, throws the connection away
/// and builds another, retrying until one takes. This has always worked.
///
/// How hard it tries is [`RECONNECT_MIN_MS`] and [`RECONNECT_MAX_MS`].
///
/// ## A connection that dies without closing
///
/// The machine suspends, or the gateway disappears without a word. Nothing
/// arrives, but nothing fails either: no close comes, and a socket that only
/// ever reads never writes anything that could fail. So it looks exactly
/// like a working connection that happens to be quiet, and a subscriber can
/// wait on it for as long as it runs.
///
/// Heartbeats tell the two apart. A ping is a write, and a ping that goes
/// unanswered is a failure libzmq can see, so it closes the connection and
/// the case above takes over from there.
///
/// How long that takes is [`HEARTBEAT_IVL_MS`] and [`HEARTBEAT_TIMEOUT_MS`].
///
/// ## A gateway that stops publishing
///
/// The connection is in good health and carries nothing. Pings are answered
/// by libzmq's own thread inside the gateway, which knows nothing about
/// whether the program above it is still publishing, so heartbeats report
/// the connection as fine and are right to. Only counting the silence finds
/// this one, which is what `stall_timeout` counts, in the gaps a receive
/// leaves by coming back empty every [`POLL_INTERVAL_MS`].
///
/// How long to give it is a question about the gateway rather than about
/// this crate, which is why there is no default. EDDN at a busy hour carried
/// 31 messages a second across ten minutes and never once went 0.85s without
/// one, so a couple of minutes there is quiet that cannot happen. Somewhere
/// thinner, it is ordinary.
pub fn subscribe(
    url: &str,
    stall_timeout: Option<Duration>,
) -> EnvelopeIterator {
    let ctx = zmq::Context::new();
    let connection =
        Connection::open(&ctx, url).expect("failed to open socket");

    // Not "connected". Connecting is asynchronous, and whether it took is
    // reported when libzmq knows, along with everything later that happens
    // to it.
    info!("Subscribed to {}", url);

    EnvelopeIterator {
        ctx,
        url: url.to_string(),
        connection,
        reports: Reporter::default(),
        stall: stall_timeout.map(Stall::new),
    }
}

/// Decompresses and parses each message from the ZMQ socket
///
/// The iterator does not end. A message that cannot be read comes back as an
/// [`Error`] and the next one is waited for, and a connection that stops
/// carrying messages is replaced.
pub struct EnvelopeIterator {
    ctx: zmq::Context,
    url: String,
    connection: Connection,
    reports: Reporter,
    /// Absent when the caller asked for no stall timeout at all.
    stall: Option<Stall>,
}

impl EnvelopeIterator {
    /// Throw the connection away and take a new one
    ///
    /// For a connection in the picture of health that carries nothing. libzmq
    /// has no complaint to make about it and so will not rebuild it, and the
    /// only way to a new one is a socket it has never seen.
    fn reconnect(&mut self, reason: &str) {
        // Not the reporter's throttle. This happens once a stall timeout at
        // the very most, and is worth hearing about every time it does.
        warn!("{}, replacing the connection", reason);

        loop {
            match Connection::open(&self.ctx, &self.url) {
                Ok(connection) => {
                    self.connection = connection;
                    if let Some(stall) = &mut self.stall {
                        stall.restart();
                    }
                    self.reports.replaced();
                    return;
                }
                Err(err) => {
                    warn!("Could not open a socket: {}", err);
                    thread::sleep(Duration::from_secs(5));
                }
            }
        }
    }
}

impl Iterator for EnvelopeIterator {
    type Item = Result<Envelope, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Before the receive, not only when one comes back empty. A
            // gateway sending steadily can still have lost and rebuilt its
            // connection, and a receive that always has something waiting
            // would never leave room to hear about it.
            let events = self.connection.events();
            for note in self.reports.observe(&events) {
                // `warn!` and `info!` want the level at compile time, so the
                // one the reporter chose is dispatched here.
                if note.level == Level::WARN {
                    warn!("{}", note.message);
                } else {
                    info!("{}", note.message);
                }
            }

            match self.connection.socket.recv_bytes(0) {
                Ok(compressed) => {
                    if let Some(stall) = &mut self.stall {
                        stall.restart();
                    }
                    return Some(
                        inflate::decompress_to_vec_zlib(&compressed)
                            .map_err(Error::Decompress)
                            .and_then(|json| {
                                serde_json::from_slice(&json)
                                    .map_err(Error::Parse)
                            }),
                    );
                }
                // Nothing arrived within the poll interval, which is the only
                // chance there is to see how long the quiet has run.
                Err(zmq::Error::EAGAIN) => {
                    let overrun = self.stall.as_ref().and_then(Stall::overrun);
                    if let Some(quiet) = overrun {
                        let reason =
                            format!("nothing for {}s", quiet.as_secs());
                        self.reconnect(&reason);
                    }
                }
                // A signal interrupted the wait, which says nothing about the
                // connection.
                Err(zmq::Error::EINTR) => {}
                // Anything else is the socket itself, and it will keep giving
                // the same answer until it is replaced.
                Err(err) => {
                    self.reconnect(&format!("socket error: {}", err));
                    return Some(Err(Error::Socket(err)));
                }
            }
        }
    }
}

// TODO: Make use of the schema service
// const SCHEMA_JOURNAL : &str = "https://eddn.edcd.io/schemas/journal/1";
