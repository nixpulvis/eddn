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
    IDLE_TIMEOUT, PING_INTERVAL, POLL_INTERVAL, RECONNECT_MAX, RECONNECT_MIN,
};
pub use crate::error::Error;

use crate::connection::{Connection, Stall};
use crate::reporter::Reporter;
use bitflags::bitflags;
use chrono::prelude::*;
use elite_journal::entry::market::{BlackMarket, Outfitting, Shipyard};
use elite_journal::entry::{Entry, Event, Market};
use miniz_oxide::inflate;
use omq_tokio::Context;
use serde::Deserialize;
use std::thread;
use std::time::Duration;
use tracing::{debug, info, warn, Level};

pub const URL: &'static str = "tcp://eddn.edcd.io:9500";

/// Top level EDDN message wrapper
#[derive(Debug)]
pub struct Envelope {
    pub schema_ref: String,
    pub header: Header,
    pub message: Message,

    /// Whether this is the galaxy everyone is in
    ///
    /// EDDN carries alpha and beta game data on the same socket as live data,
    /// separated only by a `/test` suffix on the `$schemaRef`. It parses like
    /// anything else and describes somewhere that is not the live galaxy, so
    /// recording it is a mistake rather than a choice.
    ///
    /// Here rather than on [`Message`], which says what a payload holds. This
    /// says where it came from, and the two are independent: a test message is
    /// still a journal message or a market message, and reads as one.
    ///
    /// [`subscribe`] never yields an envelope with this false. It is on the
    /// type for whoever reads an envelope some other way.
    pub live: bool,

    /// Which version of its schema the sender wrote to, as the reference
    /// spells it
    ///
    /// Nothing routes on this and nothing should: what the two live outfitting
    /// versions disagree about is one field of one payload, and is answered by
    /// reading either. It is here to be looked at. A sender still on an old
    /// version, or a version nothing here has heard of, does not announce
    /// itself any other way.
    ///
    /// [`None`] only where the reference is not one EDDN sends, which is the
    /// same case that leaves a message [`Message::Unmodeled`].
    pub version: Option<String>,

    /// The system this message names, where it names one
    ///
    /// Every EDDN schema carries the system at the top of its payload --
    /// `StarSystem` on the journal ones, `systemName` on the market ones -- so
    /// it is read here once rather than dug out of whichever event or market
    /// shape the payload became. [`None`] only where neither key is present.
    pub star_system: Option<String>,

    /// The station this message names, where it names one
    ///
    /// `StationName` on the journal schemas, `stationName` on the market ones.
    pub station: Option<String>,

    /// The body this message names, where it names one
    ///
    /// `BodyName` on most events, `Body` on the few that spell it that way.
    pub body: Option<String>,
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

        // The system, station and body named at the top of the payload. Each
        // key differs by schema, so every spelling it is sent under is tried.
        // Kept here rather than dug out of whichever event or market shape the
        // payload became.
        let (star_system, station, body) = {
            let field = |keys: &[&str]| {
                keys.iter()
                    .find_map(|key| raw.message.get(*key))
                    .and_then(|value| value.as_str())
                    .map(str::to_owned)
            };
            (
                field(&["StarSystem", "SystemName", "System", "systemName"]),
                field(&["StationName", "stationName"]),
                field(&["BodyName", "Body"]),
            )
        };

        // Everything the reference has to say, taken while it is still there
        // to borrow from.
        let (message, live, version) = {
            let schema = Schema::read(&raw.schema_ref);

            (
                Message::read(schema.map(|schema| schema.name), raw.message)
                    .map_err(serde::de::Error::custom)?,
                // A reference that cannot be read marks nothing as test data,
                // and is taken at its word for the same reason its payload is.
                schema.map(|schema| schema.live).unwrap_or(true),
                schema.map(|schema| schema.version.to_owned()),
            )
        };

        Ok(Envelope {
            schema_ref: raw.schema_ref,
            header: raw.header,
            message,
            live,
            version,
            star_system,
            station,
            body,
        })
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
/// do. A schema whose payload has no `event` key at all -- outfitting,
/// shipyard, blackmarket -- could never be told apart from any other, because
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

    /// A schema this crate does not read yet
    ///
    /// Kept as JSON rather than dropped, so that what is going unread can be
    /// seen by whoever is looking.
    Unmodeled(serde_json::Value),
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
    ///
    /// Whether the schema was a test one makes no difference here. A payload
    /// sent under `journal/1/test` is shaped exactly like one sent under
    /// `journal/1` and is read as one; what is not the same is the galaxy it
    /// describes, which is [`Envelope::live`]'s to say.
    fn read(
        name: Option<&str>,
        message: serde_json::Value,
    ) -> Result<Self, serde_json::Error> {
        let Some(name) = name else {
            return Ok(Message::Unmodeled(message));
        };

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

    /// The in-game moment the payload carries, where it carries one
    ///
    /// Every EDDN payload opens with a `timestamp`: when the game wrote the
    /// event, as against [`Header::gateway_timestamp`], when EDDN received it.
    /// The typed variants read it off their [`Entry`]; an unmodeled one is
    /// still raw JSON, so it is read from the `timestamp` key there. [`None`]
    /// only where that key is missing or unparseable.
    pub fn timestamp(&self) -> Option<DateTime<Utc>> {
        match self {
            Message::Journal(e) => Some(e.timestamp),
            Message::Commodity(e) => Some(e.timestamp),
            Message::Outfitting(e) => Some(e.timestamp),
            Message::Shipyard(e) => Some(e.timestamp),
            Message::BlackMarket(e) => Some(e.timestamp),
            Message::Unmodeled(value) => value
                .get("timestamp")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok()),
        }
    }
}

/// What a `$schemaRef` says about the message beneath it
///
/// A reference reads `https://eddn.edcd.io/schemas/<name>/<version>`, with
/// `/test` after it where the data is not from the live game. All three of
/// those answer different questions and are wanted in different places, which
/// is why they arrive named rather than in an order somebody has to remember.
///
/// Borrowed from the reference it was read out of. Nothing here outlives the
/// envelope being deserialised, and what the envelope keeps it keeps as its
/// own fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Schema<'a> {
    /// The schema itself, e.g. `fssdiscoveryscan`
    ///
    /// The only part anything routes on.
    name: &'a str,

    /// Which version of it the sender wrote to, as spelled in the reference
    ///
    /// Reported and not acted on, which is deliberate rather than an
    /// omission. Outfitting is sent under both 2 and 3 and they do differ --
    /// 2 names a module, 3 prices it -- but that is one field of one payload
    /// and is answered there by reading either. Routing on the version would
    /// put the question in the wrong place: every caller would have to know
    /// which versions of everything exist in order to ignore that they do.
    ///
    /// Worth saying all the same. It is the one thing about a message that
    /// says how old the sender's idea of a schema is.
    ///
    /// Kept as sent rather than as a number. Every version EDDN has ever used
    /// is an integer, but nothing here counts with it, and a reference that
    /// broke that habit would be worth reading rather than worth failing on.
    version: &'a str,

    /// Whether this is the galaxy everyone is in
    ///
    /// The reference says the opposite -- `/test` marks what is not live --
    /// and it is turned round here so that nobody reading it has to.
    live: bool,
}

impl<'a> Schema<'a> {
    /// Read a `$schemaRef`, or nothing where it is not one EDDN sends
    fn read(schema_ref: &'a str) -> Option<Self> {
        let (_, tail) = schema_ref.split_once(SCHEMAS)?;
        let mut parts = tail.split('/');

        let name = parts.next()?;
        // A reference with no version is not one EDDN sends.
        let version = parts.next()?;

        Some(Schema { name, version, live: parts.next() != Some("test") })
    }
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
/// connection and says so. The socket sees the close, throws the connection
/// away and builds another, retrying until one takes.
///
/// How hard it tries is [`RECONNECT_MIN`] and [`RECONNECT_MAX`].
///
/// ## A connection that dies without closing
///
/// The machine suspends, or the gateway disappears without a word. Nothing
/// arrives, but nothing fails either: no close comes, and a socket that only
/// ever reads never writes anything that could fail. So it looks exactly
/// like a working connection that happens to be quiet, and a subscriber can
/// wait on it for as long as it runs.
///
/// Heartbeats tell the two apart. A ping is a write, and a connection that
/// brings nothing back at all -- no answer to it, no data either -- runs out
/// of time and is closed, so the case above takes over from there.
///
/// How long that takes is [`PING_INTERVAL`] and [`IDLE_TIMEOUT`].
///
/// ## A gateway that stops publishing
///
/// The connection is in good health and carries nothing. Pings are answered
/// down in the gateway's socket, which knows nothing about whether the
/// program above it is still publishing, so heartbeats report the connection
/// as fine and are right to. Only counting the silence finds this one, which
/// is what `stall_timeout` counts, in the gaps a receive leaves by coming
/// back empty every [`POLL_INTERVAL`].
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
    let ctx = Context::new();
    let connection = open_retrying(&ctx, url);

    EnvelopeIterator {
        ctx,
        url: url.to_string(),
        connection,
        reports: Reporter::default(),
        stall: stall_timeout.map(Stall::new),
        galaxy: Galaxy::LIVE,
        started: false,
    }
}

/// Open a socket, waiting out failures rather than giving up on them
///
/// A subscription is an infinite thing that replaces a connection forever (see
/// [`EnvelopeIterator`]), so a socket that will not open is a wait, not a
/// failure: each attempt that fails is logged and retried after a pause. The
/// first connection and every replacement go through here alike, so the two
/// behave the same and neither panics on a gateway that happens to be down.
fn open_retrying(ctx: &Context, url: &str) -> Connection {
    loop {
        match Connection::open(ctx, url) {
            Ok(connection) => return connection,
            Err(err) => {
                warn!("Could not open a socket: {}", err);
                thread::sleep(Duration::from_secs(5));
            }
        }
    }
}

bitflags! {
    /// Which galaxy's data to hand over
    ///
    /// EDDN carries alpha and beta ("test") data on the same socket as the live
    /// galaxy, told apart by a `/test` suffix on the `$schemaRef` (see
    /// [`Envelope::live`]). Keeping the live galaxy and keeping the test one are
    /// independent choices, so this is a set of the two rather than a list of
    /// their combinations: [`LIVE`](Galaxy::LIVE), [`TEST`](Galaxy::TEST), or
    /// [`ALL`](Galaxy::ALL) for both. Only the live galaxy is handed over unless
    /// asked otherwise, so a subscriber cannot record test data by forgetting to.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct Galaxy: u8 {
        /// The live galaxy everyone plays in.
        const LIVE = 1 << 0;
        /// The alpha and beta test galaxies.
        const TEST = 1 << 1;
        /// Both the live galaxy and the test ones.
        const ALL = Self::LIVE.bits() | Self::TEST.bits();
    }
}

impl Default for Galaxy {
    /// The live galaxy alone, so test data is never recorded unasked.
    fn default() -> Self {
        Galaxy::LIVE
    }
}

impl Galaxy {
    /// Whether a message in the live-or-not galaxy belongs to this set
    ///
    /// A live message belongs where [`LIVE`](Galaxy::LIVE) is set, a test one
    /// where [`TEST`](Galaxy::TEST) is, and [`ALL`](Galaxy::ALL) holds both. The
    /// one predicate both the subscriber's frame filter and a viewer's galaxy
    /// filter read, so the two cannot disagree on what a galaxy admits.
    pub fn shows(self, live: bool) -> bool {
        self.contains(if live { Galaxy::LIVE } else { Galaxy::TEST })
    }
}

/// Decompresses and parses each message from the ZMQ socket
///
/// The iterator does not end. A message that cannot be read comes back as an
/// [`Error`] and the next one is waited for, and a connection that stops
/// carrying messages is replaced.
pub struct EnvelopeIterator {
    ctx: Context,
    url: String,
    connection: Connection,
    reports: Reporter,
    /// Absent when the caller asked for no stall timeout at all.
    stall: Option<Stall>,

    /// Which galaxy's data to hand over; the live one unless asked otherwise.
    galaxy: Galaxy,

    /// Whether the opening `subscribed` line has been logged yet. Logged on
    /// the first `next`, when the galaxy the builder chose is final.
    started: bool,
}

impl EnvelopeIterator {
    /// Throw the connection away and take a new one
    ///
    /// For a connection in the picture of health that carries nothing.
    /// Nothing about it has failed, so it will not be rebuilt on its own, and
    /// a new one is had by opening a socket in its place.
    fn reconnect(&mut self, reason: &str) {
        // Not the reporter's throttle. This happens once a stall timeout at
        // the very most, and is worth hearing about every time it does.
        warn!("{}, replacing the connection", reason);

        self.connection = open_retrying(&self.ctx, &self.url);
        if let Some(stall) = &mut self.stall {
            stall.restart();
        }
        self.reports.replaced();
    }

    /// Choose which galaxy's data to hand over
    ///
    /// [`Galaxy::LIVE`] by default, so a subscriber records only the live
    /// galaxy unless it asks for the rest.
    pub fn galaxy(mut self, galaxy: Galaxy) -> Self {
        self.galaxy = galaxy;
        self
    }
}

/// What a frame off the socket amounts to, or nothing where it is not ours
///
/// Apart from the receive, and everything a message goes through between
/// arriving and being handed over is here rather than there: it is
/// decompressed, read, and then either kept or dropped for describing a
/// galaxy that is not the live one. Which means all of it can be tried
/// without a socket for it to arrive on, including the dropping -- and a
/// frame silently going missing is exactly the sort of thing that otherwise
/// only shows up as a gap in a database months later.
///
/// [`None`] is a frame deliberately let go. A frame that could not be read at
/// all is `Some(Err(_))`, and the difference matters: alpha and beta data
/// arriving is ordinary, and a message that will not decompress is not.
fn read_frame(
    compressed: &[u8],
    galaxy: Galaxy,
) -> Option<Result<Envelope, Error>> {
    let read = inflate::decompress_to_vec_zlib(compressed)
        .map_err(Error::Decompress)
        .and_then(|json| {
            serde_json::from_slice::<Envelope>(&json).map_err(|source| {
                // Lossy because the message is being kept to be read by a
                // person, and one carrying bytes that are not UTF-8 is a
                // message worth seeing rather than one to give up on twice.
                Error::Parse {
                    source,
                    json: String::from_utf8_lossy(&json).into_owned(),
                }
            })
        });

    // Alpha and beta data arrives on this socket alongside the live galaxy.
    // Which of the two a subscriber wanted is `galaxy`'s to say; the rest is
    // dropped here rather than handed over, so it cannot be recorded by
    // forgetting to ask. See [`Envelope::live`].
    if let Ok(envelope) = &read {
        if !galaxy.shows(envelope.live) {
            debug!(schema = %envelope.schema_ref, "filtered by galaxy");
            return None;
        }
    }

    Some(read)
}

impl Iterator for EnvelopeIterator {
    type Item = Result<Envelope, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        // "subscribed", not "connected": connecting is asynchronous, so this
        // only says the socket is open and trying, and whether it took is
        // reported when the socket knows. Said on the first poll rather than
        // in `subscribe` so the galaxy the builder chose is part of it.
        if !self.started {
            self.started = true;
            let stall = self.stall.as_ref().map_or_else(
                || "off".to_owned(),
                |s| format!("{}s", s.timeout().as_secs()),
            );
            info!(
                url = %self.url,
                stall = %stall,
                galaxy = ?self.galaxy,
                "subscribed"
            );
        }

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

            match self.connection.socket.recv_timeout(POLL_INTERVAL) {
                Ok(message) => {
                    if let Some(stall) = &mut self.stall {
                        stall.restart();
                    }
                    let frame = match message.get(0) {
                        Some(frame) => frame,
                        None => {
                            debug!("a message carrying no frame");
                            continue;
                        }
                    };

                    if let Some(read) = read_frame(frame, self.galaxy) {
                        return Some(read);
                    }

                    // A `/test` message, which `read_frame` drops.
                }
                // Nothing arrived within the poll interval, which is the only
                // chance there is to see how long the quiet has run.
                Err(omq_tokio::Error::Timeout) => {
                    let overrun = self.stall.as_ref().and_then(Stall::overrun);
                    if let Some(quiet) = overrun {
                        let reason =
                            format!("nothing for {}s", quiet.as_secs());
                        self.reconnect(&reason);
                    }
                }
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

/// Placing a message by the schema it was sent under
///
/// The envelope is what decides everything here, so these go through the whole
/// of it rather than through [`Message::read`]: what a consumer gets handed is
/// an [`Envelope`], and how it got there is not the thing worth pinning down.
#[cfg(test)]
mod tests {
    use super::*;

    /// An envelope with `message` as given, and a header of no interest
    pub(super) fn envelope_json(schema_ref: &str, message: &str) -> String {
        format!(
            r#"{{
                "$schemaRef": "{}",
                "header": {{
                    "gatewayTimestamp": "2026-08-08T12:00:00Z",
                    "softwareName": "E:D Market Connector",
                    "softwareVersion": "5.11.3",
                    "uploaderID": "abc123"
                }},
                "message": {}
            }}"#,
            schema_ref, message,
        )
    }

    fn envelope(schema_ref: &str, message: &str) -> Envelope {
        serde_json::from_str(&envelope_json(schema_ref, message))
            .expect("envelope should parse")
    }

    /// A frame as it comes off the socket: the JSON, zlib'd
    pub(super) fn frame(schema_ref: &str, message: &str) -> Vec<u8> {
        miniz_oxide::deflate::compress_to_vec_zlib(
            envelope_json(schema_ref, message).as_bytes(),
            6,
        )
    }

    pub(super) const JUMP: &str = r#"{
        "timestamp": "2026-08-08T12:00:00Z",
        "event": "FSDJump",
        "StarSystem": "Sol",
        "StarPos": [0.0, 0.0, 0.0],
        "SystemAddress": 10477373803
    }"#;

    #[test]
    fn envelope_names_its_system() {
        // Journal messages carry the system as a top-level `StarSystem`.
        let jump = envelope("https://eddn.edcd.io/schemas/journal/1", JUMP);
        assert_eq!(jump.star_system.as_deref(), Some("Sol"));

        // Market messages carry it as `systemName` instead.
        let market = envelope(
            "https://eddn.edcd.io/schemas/commodity/3",
            r#"{
                "timestamp": "2026-08-08T12:00:00Z",
                "systemName": "Shinrarta Dezhra",
                "stationName": "Jameson Memorial",
                "marketId": 128666762,
                "commodities": []
            }"#,
        );
        assert_eq!(market.star_system.as_deref(), Some("Shinrarta Dezhra"));
        assert_eq!(market.station.as_deref(), Some("Jameson Memorial"));

        // FSSDiscoveryScan names it `SystemName`, not `StarSystem`.
        let fss = envelope(
            "https://eddn.edcd.io/schemas/fssdiscoveryscan/1",
            r#"{
                "timestamp": "2026-08-08T12:00:00Z",
                "event": "FSSDiscoveryScan",
                "SystemName": "M52 Sector PF-T b18-0",
                "SystemAddress": 1234,
                "BodyCount": 5,
                "NonBodyCount": 2
            }"#,
        );
        assert_eq!(fss.star_system.as_deref(), Some("M52 Sector PF-T b18-0"));
    }

    /// The schema names the payload, so a journal schema is a journal entry
    #[test]
    fn a_journal_message_is_a_journal_entry() {
        let envelope = envelope("https://eddn.edcd.io/schemas/journal/1", JUMP);

        assert!(matches!(envelope.message, Message::Journal(_)));
    }

    /// The event's own time is read, typed or not, and absent where unwritten
    #[test]
    fn a_message_gives_up_its_event_time() {
        let when: DateTime<Utc> =
            "2026-08-08T12:00:00Z".parse().expect("fixture time parses");

        // A typed journal entry reads it off the entry.
        let jump = envelope("https://eddn.edcd.io/schemas/journal/1", JUMP);
        assert_eq!(jump.message.timestamp(), Some(when));

        // An unmodeled payload is still raw JSON, read from its key.
        let unread = envelope("nonsense", JUMP);
        assert!(matches!(unread.message, Message::Unmodeled(_)));
        assert_eq!(unread.message.timestamp(), Some(when));

        // Nothing there to read leaves nothing to return.
        let bare = Message::Unmodeled(serde_json::json!({}));
        assert_eq!(bare.timestamp(), None);
    }

    /// Every schema whose payload is a journal event reads as one
    ///
    /// They are separate schemas with separate names and one shape, so the
    /// event inside is what tells them apart rather than the schema.
    #[test]
    fn the_standalone_journal_schemas_read_as_journal_entries() {
        for name in super::JOURNAL_SCHEMAS {
            let reference = format!("https://eddn.edcd.io/schemas/{}/1", name);
            let envelope = envelope(&reference, JUMP);

            assert!(
                matches!(envelope.message, Message::Journal(_)),
                "{} did not read as a journal entry",
                name,
            );
        }
    }

    /// The three that carry no `event`, which nothing could place before
    #[test]
    fn the_schemas_with_no_event_are_placed_by_their_reference() {
        let outfitting = envelope(
            "https://eddn.edcd.io/schemas/outfitting/3",
            r#"{
                "timestamp": "2026-08-08T12:00:00Z",
                "systemName": "Sol",
                "stationName": "Abraham Lincoln",
                "marketId": 128016384,
                "modules": ["Int_Engine_Size3_Class5_Fast"]
            }"#,
        );
        assert!(matches!(outfitting.message, Message::Outfitting(_)));

        let shipyard = envelope(
            "https://eddn.edcd.io/schemas/shipyard/2",
            r#"{
                "timestamp": "2026-08-08T12:00:00Z",
                "systemName": "Sol",
                "stationName": "Abraham Lincoln",
                "marketId": 128016384,
                "ships": ["SideWinder"]
            }"#,
        );
        assert!(matches!(shipyard.message, Message::Shipyard(_)));

        let black_market = envelope(
            "https://eddn.edcd.io/schemas/blackmarket/1",
            r#"{
                "timestamp": "2026-08-08T12:00:00Z",
                "systemName": "Sol",
                "stationName": "Abraham Lincoln",
                "name": "Gold",
                "sellPrice": 9432,
                "prohibited": false
            }"#,
        );
        assert!(matches!(black_market.message, Message::BlackMarket(_)));
    }

    /// Both live outfitting versions are read, though their payloads differ
    #[test]
    fn either_outfitting_version_is_read() {
        for (version, modules) in [
            ("2", r#"["Int_Engine_Size3_Class5_Fast"]"#),
            (
                "3",
                r#"[{
                    "id": 128064258,
                    "Name": "Int_Engine_Size3_Class5_Fast",
                    "BuyPrice": 5103953,
                    "BuyMercCoinsPrice": 0
                }]"#,
            ),
        ] {
            let reference =
                format!("https://eddn.edcd.io/schemas/outfitting/{}", version);
            let message = format!(
                r#"{{
                    "timestamp": "2026-08-08T12:00:00Z",
                    "systemName": "Sol",
                    "stationName": "Abraham Lincoln",
                    "marketId": 128016384,
                    "modules": {}
                }}"#,
                modules,
            );

            let envelope = envelope(&reference, &message);
            assert!(
                matches!(envelope.message, Message::Outfitting(_)),
                "outfitting/{} was not read",
                version,
            );
        }
    }

    /// Alpha and beta traffic is marked, not turned into something else
    ///
    /// It arrives on the same socket under the same schemas, separated only by
    /// `/test` on the end of the reference, and was being taken as real. The
    /// payload is a journal entry either way -- that is what the schema says
    /// it is -- and which galaxy it describes is the envelope's to say.
    ///
    /// `subscribe` drops these, so nothing downstream has to remember to.
    #[test]
    fn test_schemas_are_read_but_not_live() {
        let envelope =
            envelope("https://eddn.edcd.io/schemas/journal/1/test", JUMP);

        assert!(!envelope.live);
        assert!(matches!(envelope.message, Message::Journal(_)));
    }

    /// Everything else is the live galaxy
    #[test]
    fn an_ordinary_schema_is_live() {
        assert!(envelope("https://eddn.edcd.io/schemas/journal/1", JUMP).live);
        assert!(envelope("nonsense", JUMP).live);
    }

    /// A schema nothing reads is kept rather than dropped
    #[test]
    fn an_unmodelled_schema_is_kept() {
        let envelope = envelope(
            "https://eddn.edcd.io/schemas/fcmaterials_journal/1",
            r#"{
                "timestamp": "2026-08-08T12:00:00Z",
                "event": "FCMaterials",
                "MarketID": 3700571136,
                "CarrierID": "K7Q-BQL",
                "CarrierName": "Nomad",
                "Items": []
            }"#,
        );

        assert!(matches!(envelope.message, Message::Unmodeled(_)));
        assert_eq!(
            envelope.schema_ref,
            "https://eddn.edcd.io/schemas/fcmaterials_journal/1",
        );
    }

    /// A payload that disagrees with its schema is an error, not a shrug
    ///
    /// This is the whole difference from guessing. The schema says what the
    /// payload is, so a journal message whose event will not read is a
    /// message that is wrong, not a message of some other kind.
    #[test]
    fn a_payload_that_will_not_read_is_reported() {
        let json = r#"{
            "$schemaRef": "https://eddn.edcd.io/schemas/journal/1",
            "header": {
                "gatewayTimestamp": "2026-08-08T12:00:00Z",
                "softwareName": "E:D Market Connector",
                "softwareVersion": "5.11.3",
                "uploaderID": "abc123"
            },
            "message": {
                "timestamp": "2026-08-08T12:00:00Z",
                "event": "FSDJump",
                "StarSystem": "Sol",
                "StarPos": "nowhere",
                "SystemAddress": 10477373803
            }
        }"#;

        assert!(serde_json::from_str::<Envelope>(json).is_err());
    }

    /// A reference that is not one EDDN sends places nothing, and is kept
    #[test]
    fn a_reference_that_makes_no_sense_is_unmodelled() {
        assert_eq!(Schema::read("nonsense"), None);
        assert_eq!(Schema::read("https://eddn.edcd.io/schemas/journal"), None);

        let envelope = envelope("nonsense", JUMP);
        assert!(matches!(envelope.message, Message::Unmodeled(_)));
        // Nothing to report, rather than a version invented for the occasion.
        assert_eq!(envelope.version, None);
    }

    /// The name, the version and the galaxy, off the end of the reference
    #[test]
    fn a_reference_reads_into_what_it_says() {
        assert_eq!(
            Schema::read("https://eddn.edcd.io/schemas/commodity/3"),
            Some(Schema { name: "commodity", version: "3", live: true }),
        );
        assert_eq!(
            Schema::read("https://eddn.edcd.io/schemas/shipyard/2/test"),
            Some(Schema { name: "shipyard", version: "2", live: false }),
        );
    }

    /// The version is reported without being acted on
    ///
    /// Both live outfitting versions reach the same variant, which is the
    /// whole argument for not routing on it -- and the envelope still says
    /// which one arrived, which is the whole argument for keeping it.
    #[test]
    fn the_version_is_said_even_though_nothing_turns_on_it() {
        let old = envelope(
            "https://eddn.edcd.io/schemas/outfitting/2",
            r#"{
                "timestamp": "2026-08-08T12:00:00Z",
                "systemName": "Sol",
                "stationName": "Abraham Lincoln",
                "marketId": 128016384,
                "modules": ["Int_Engine_Size3_Class5_Fast"]
            }"#,
        );
        let new = envelope(
            "https://eddn.edcd.io/schemas/outfitting/3",
            r#"{
                "timestamp": "2026-08-08T12:00:00Z",
                "systemName": "Sol",
                "stationName": "Abraham Lincoln",
                "marketId": 128016384,
                "modules": [{
                    "id": 128064258,
                    "Name": "Int_Engine_Size3_Class5_Fast",
                    "BuyPrice": 5103953,
                    "BuyMercCoinsPrice": 0
                }]
            }"#,
        );

        assert_eq!(old.version.as_deref(), Some("2"));
        assert_eq!(new.version.as_deref(), Some("3"));

        assert!(matches!(old.message, Message::Outfitting(_)));
        assert!(matches!(new.message, Message::Outfitting(_)));
    }

    /// A test schema is still a version of something
    #[test]
    fn a_test_schema_reports_its_version_too() {
        let envelope =
            envelope("https://eddn.edcd.io/schemas/journal/1/test", JUMP);

        assert_eq!(envelope.version.as_deref(), Some("1"));
        assert!(!envelope.live);
    }
}

/// What comes off the socket, short of the socket itself
///
/// [`read_frame`] is everything between a frame arriving and a subscriber
/// being handed it, which is where the one decision lives that nothing else
/// can see: a frame deliberately let go looks exactly like a frame that never
/// arrived. These are what say the difference is on purpose.
#[cfg(test)]
mod frames {
    use super::tests::{envelope_json, frame, JUMP};
    use super::*;

    const JOURNAL: &str = "https://eddn.edcd.io/schemas/journal/1";
    const JOURNAL_TEST: &str = "https://eddn.edcd.io/schemas/journal/1/test";

    /// Live data is handed over
    #[test]
    fn a_live_frame_is_handed_over() {
        let read = read_frame(&frame(JOURNAL, JUMP), Galaxy::LIVE)
            .expect("a live frame should be handed over");

        let envelope = read.expect("and should read");
        assert!(envelope.live);
        assert!(matches!(envelope.message, Message::Journal(_)));
    }

    /// And alpha and beta data is not
    ///
    /// The whole of what `live` is for. Asserting the flag says the reference
    /// was understood; this says something acts on it.
    #[test]
    fn a_test_frame_is_let_go() {
        // Live: the test frame is dropped; All and Test keep it.
        assert!(read_frame(&frame(JOURNAL_TEST, JUMP), Galaxy::LIVE).is_none());
        let kept = read_frame(&frame(JOURNAL_TEST, JUMP), Galaxy::ALL)
            .expect("a test frame should be kept under All")
            .expect("and should read");
        assert!(!kept.live);
        // And Test drops the live galaxy.
        assert!(read_frame(&frame(JOURNAL, JUMP), Galaxy::TEST).is_none());
    }

    /// A frame that is not zlib is reported rather than let go
    ///
    /// The difference the return type is for. Beta data arriving is ordinary
    /// and worth nothing but silence; a frame that will not decompress is the
    /// gateway or this crate being wrong, and going quiet about it would
    /// leave nothing to notice.
    #[test]
    fn a_frame_that_is_not_zlib_is_reported() {
        assert!(matches!(
            read_frame(b"not zlib at all", Galaxy::LIVE),
            Some(Err(Error::Decompress(_))),
        ));
    }

    /// So is one that decompresses into something that is not an envelope
    #[test]
    fn a_frame_that_is_not_an_envelope_is_reported() {
        let rubbish = miniz_oxide::deflate::compress_to_vec_zlib(b"{}", 6);

        assert!(matches!(
            read_frame(&rubbish, Galaxy::LIVE),
            Some(Err(Error::Parse { .. }))
        ));
    }

    /// And one whose payload disagrees with its schema
    ///
    /// Which the schema settles: a journal message that will not read, not a
    /// message of some other kind.
    #[test]
    fn a_frame_whose_payload_will_not_read_is_reported() {
        let json = envelope_json(
            JOURNAL,
            r#"{
                "timestamp": "2026-08-08T12:00:00Z",
                "event": "FSDJump",
                "StarSystem": "Sol",
                "StarPos": "nowhere",
                "SystemAddress": 10477373803
            }"#,
        );
        let bad =
            miniz_oxide::deflate::compress_to_vec_zlib(json.as_bytes(), 6);

        assert!(matches!(
            read_frame(&bad, Galaxy::LIVE),
            Some(Err(Error::Parse { .. }))
        ));
    }

    /// A payload error carries the message, so the field can be found
    ///
    /// `invalid type: string "", expected i32` of a thirty field message says
    /// something in the feed is wrong and nothing about where, and a message
    /// nobody kept cannot be gone back to. There is no offset and no field path
    /// to be had here, so what makes it traceable is keeping the message.
    #[test]
    fn a_payload_error_keeps_the_message_it_could_not_read() {
        let json = envelope_json(
            "https://eddn.edcd.io/schemas/fssdiscoveryscan/1",
            r#"{
                "timestamp": "2026-08-08T12:00:00Z",
                "event": "FSSDiscoveryScan",
                "SystemName": "Sol",
                "StarPos": [0.0, 0.0, 0.0],
                "SystemAddress": 10477373803,
                "BodyCount": "",
                "NonBodyCount": 3,
                "Progress": 1.0
            }"#,
        );
        let bad =
            miniz_oxide::deflate::compress_to_vec_zlib(json.as_bytes(), 6);

        let Some(Err(err)) = read_frame(&bad, Galaxy::LIVE) else {
            panic!("a payload that will not read should be reported")
        };
        let said = err.to_string();

        assert!(said.contains("BodyCount"), "did not name the field: {}", said,);
        // Read out of a value, so there is no offset to point at and none is
        // offered. The path above is what locates it.
        assert!(err.near().is_none(), "pointed somewhere anyway: {}", said);
    }

    /// A malformed envelope does point at where reading stopped
    ///
    /// Here there is text and an offset into it, so the window is the answer.
    #[test]
    fn a_malformed_envelope_shows_where_it_stopped() {
        let truncated = miniz_oxide::deflate::compress_to_vec_zlib(
            br#"{"$schemaRef": "https://eddn.edcd.io/schemas/journal/1", "heade"#,
            6,
        );

        let Some(Err(err)) = read_frame(&truncated, Galaxy::LIVE) else {
            panic!("a malformed envelope should be reported")
        };

        let near = err.near().expect("should point at where it stopped");
        assert!(near.contains("heade"), "pointed elsewhere: {}", near);
    }
}
