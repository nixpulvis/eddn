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
use elite_journal::entry::market::{BlackMarket, Outfitting, Shipyard};
use elite_journal::entry::{Entry, Event, Market};
use miniz_oxide::inflate;
use serde::Deserialize;
use std::thread;
use std::time::Duration;
use tracing::{info, warn, Level};

pub const URL: &'static str = "tcp://eddn.edcd.io:9500";

/// Top level EDDN message wrapper
#[derive(Debug)]
pub struct Envelope {
    pub schema_ref: String,
    pub header: Header,
    pub message: Message,
}

/// What the envelope looks like before its payload has been placed
///
/// The payload cannot be read until the `$schemaRef` above it has been, so
/// it is held as JSON for exactly as long as it takes to read the rest.
#[derive(Deserialize)]
struct RawEnvelope {
    #[serde(rename = "$schemaRef")]
    schema_ref: String,
    header: Header,
    message: serde_json::Value,
}

impl<'de> Deserialize<'de> for Envelope {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = RawEnvelope::deserialize(deserializer)?;
        let message = Message::read(&raw.schema_ref, raw.message)
            .map_err(serde::de::Error::custom)?;

        Ok(Envelope { schema_ref: raw.schema_ref, header: raw.header, message })
    }
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
///
/// Which of these a message becomes is decided by the `$schemaRef` above it
/// rather than by trying each in turn. Most of the schemas carry a journal
/// event and so arrive as [`Message::Journal`]; what separates them is the
/// event inside, which is what [`Event`] already dispatches on. The rest are
/// their own shapes and get their own variants.
///
/// Guessing was what this did before, and there were two things it could not
/// do. A schema whose payload has no `event` key at all — outfitting,
/// shipyard, blackmarket — could never be told apart from any other, because
/// the guess had nothing to go on. And a payload that failed to parse looked
/// exactly like a payload that belonged to some other variant, so it fell
/// quietly to the catchall instead of being reported.
#[derive(Debug)]
pub enum Message {
    Journal(Entry<Event>),
    Commodity(Entry<Market>),
    Outfitting(Entry<Outfitting>),
    Shipyard(Entry<Shipyard>),
    BlackMarket(Entry<BlackMarket>),

    /// A live schema this crate does not read yet
    ///
    /// Kept as JSON rather than dropped, so that what is going unread can be
    /// seen by whoever is looking.
    Unmodeled(serde_json::Value),

    /// Anything sent under a `/test` schema
    ///
    /// EDDN carries alpha and beta game data on the same socket as live data,
    /// separated only by a `/test` suffix on the `$schemaRef`. It describes a
    /// galaxy that is not this one, so it is parted from live data here and
    /// left for the consumer to discard.
    Test(serde_json::Value),
}

/// Where every EDDN `$schemaRef` splits from its name
const SCHEMAS: &str = "/schemas/";

/// The schemas whose payload is a journal entry
///
/// All of them carry an `event`, so one type reads them all and the variant
/// it lands on is chosen by that event rather than by the schema.
const JOURNAL_SCHEMAS: &[&str] = &[
    "journal",
    "approachsettlement",
    "codexentry",
    "dockingdenied",
    "dockinggranted",
    "fssallbodiesfound",
    "fssbodysignals",
    "fssdiscoveryscan",
    "fsssignaldiscovered",
    "navbeaconscan",
    "navroute",
    "scanbarycentre",
];

impl Message {
    /// Read a payload as the schema naming it says it is
    fn read(
        schema_ref: &str,
        message: serde_json::Value,
    ) -> Result<Self, serde_json::Error> {
        let Some((name, test)) = split_schema_ref(schema_ref) else {
            return Ok(Message::Unmodeled(message));
        };

        if test {
            return Ok(Message::Test(message));
        }

        Ok(match name {
            name if JOURNAL_SCHEMAS.contains(&name) => {
                Message::Journal(serde_json::from_value(message)?)
            }
            "commodity" => Message::Commodity(serde_json::from_value(message)?),
            "outfitting" => {
                Message::Outfitting(serde_json::from_value(message)?)
            }
            "shipyard" => Message::Shipyard(serde_json::from_value(message)?),
            "blackmarket" => {
                Message::BlackMarket(serde_json::from_value(message)?)
            }
            _ => Message::Unmodeled(message),
        })
    }
}

/// The schema a `$schemaRef` names, and whether it is a test schema
///
/// A reference reads `https://eddn.edcd.io/schemas/<name>/<version>`, with
/// `/test` after it where the data is not from the live game.
///
/// The version is not returned. Outfitting is sent under both 2 and 3 and they
/// do differ -- 2 names a module, 3 prices it -- but that is one field of one
/// payload, and is answered there by reading either. Handing a version back
/// would put the question in the wrong place: every caller would have to know
/// which versions of everything exist in order to ignore that they do.
fn split_schema_ref(schema_ref: &str) -> Option<(&str, bool)> {
    let (_, tail) = schema_ref.split_once(SCHEMAS)?;
    let mut parts = tail.split('/');

    let name = parts.next()?;
    // A reference with no version is not one EDDN sends.
    parts.next()?;

    Some((name, parts.next() == Some("test")))
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
