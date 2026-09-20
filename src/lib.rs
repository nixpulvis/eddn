//! Subscribe to journal and market messages from
//! [EDDN](https://github.com/EDCD/EDDN), live or recorded.
//!
//! **[`Feed`] is the seam, and there are two implementations of it.**
//!
//! ```text
//!            frame()        Feed::reading()
//!  Network ──────────┐
//!                    ├──────────────────────> Reading
//!  Spool   ──────────┘
//! ```
//!
//! [`Network`] is the gateway's ZMQ socket; [`spool::Spool`] is a
//! recording of one, read back off disk. They differ in
//! [`frame`](Feed::frame) — where the bytes come from — and in nothing
//! else: the decompress, the parse, the galaxy filter and the stamping
//! are [`Feed::reading`], a default method written once, which both
//! spell their `Iterator::next` as. A directory built from a spool
//! cannot drift from one built off the wire, because there is one place
//! that reads a frame.
//!
//! A consumer holds `Box<dyn Feed>` and never learns which it has — the
//! source of messages is a constructor argument, not a shape the sinks
//! and the shutdown path are written twice for. See
//! `doc/PLAN-EDDN-SPOOL.md`.
//!
//! [`subscribe`] is `Network::open(…).envelopes()` under the name every
//! caller written before the trait still uses: the same messages, as
//! envelopes, with the receipt time dropped.
//!
//! Neither feed ends of its own accord. A message that cannot be read
//! comes back as an [`Error`] and the next one is waited for, and a
//! connection that has stopped working is replaced.

mod connection;
mod error;
mod reporter;
pub mod spool;

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

pub const URL: &str = "tcp://eddn.edcd.io:9500";

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
///
/// **The journal variant is not boxed**, though it is most of the enum's
/// 720 bytes and the rest are a fraction of that. Measured against what
/// the feed does with them: 18.6 messages a second is 17 KB/s of moves,
/// and the deepest a message is ever held is the 10,000-deep channel an
/// ingest drains — 9.4 MB of `Envelope` against 4 MB boxed. Neither number is
/// worth a `Box` in the pattern every consumer of this crate writes.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
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

/// One message, however it reached this process
///
/// The pair a consumer is handed by any [`Feed`]: what was published, and
/// when this process first held it. A live subscription stamps the moment
/// it came off the socket; a recorded one replays the moment the recorder
/// held it, unchanged, so a replay and the run it was recorded from agree
/// about when everything happened.
#[derive(Debug)]
pub struct Reading {
    pub received_at: DateTime<Utc>,
    pub envelope: Envelope,
}

/// Where a feed would resume from, for one that can say
///
/// A segment and a byte offset into it — see the spool's format. A live
/// subscription has no such thing, which is what [`Feed::resume`]
/// answering [`None`] means: not "unknown", but "there is nowhere to
/// resume from".
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Position {
    pub segment: String,
    pub offset: u64,
}

/// A source of EDDN messages, in the order they were published
///
/// **Two implementors, and a consumer never learns which it has.**
/// [`Network`] is the gateway's socket; [`Spool`](crate::spool::Spool) is
/// a recording of one, read back off disk. They carry the same messages
/// in the same order, so which a run reads is a constructor argument
/// rather than a shape the sinks, the publish beat and the shutdown path
/// have to be written twice for.
///
/// **An implementor supplies [`frame`](Feed::frame) and nothing else of
/// the reading.** Everything between a frame and a [`Reading`] — the
/// decompress, the parse, the galaxy filter — is
/// [`reading`](Feed::reading), written here once, so a spool and a
/// subscription cannot come to different answers about the same bytes.
/// Both spell their `Iterator::next` as a call to it.
pub trait Feed: Iterator<Item = Result<Reading, Error>> + Send {
    /// One frame, exactly as it arrived, with nothing read of it.
    ///
    /// What a recorder writes down, and what
    /// [`reading`](Feed::reading) reads. [`None`] ends the feed: a
    /// subscription never answers it, and a spool does only where it was
    /// opened to stop at the end of what was recorded.
    fn frame(&mut self) -> Option<Result<Frame, Error>>;

    /// Which galaxy's data this hands over.
    fn showing(&self) -> Galaxy;

    /// Where a restart would resume, for a feed that can say.
    ///
    /// [`None`] is a live subscription, which has nowhere to resume
    /// from — not "unknown", but "there is no such place".
    ///
    /// **Not `position`**, which is [`Iterator`]'s and would shadow this
    /// on every `dyn Feed` a consumer holds — the inherent method wins
    /// and the caller gets an element search.
    fn resume(&self) -> Option<Position> {
        None
    }

    /// The next message: frames taken until one is worth handing over.
    ///
    /// A frame let go for being another galaxy's is not the end of
    /// anything, so it reads on; a frame that will not read at all is
    /// handed over as the [`Error`] it is, and the feed goes on after it.
    fn reading(&mut self) -> Option<Result<Reading, Error>> {
        let galaxy = self.showing();
        loop {
            match self.frame()? {
                Ok(frame) => {
                    if let Some(read) = frame.read(galaxy) {
                        return Some(read);
                    }
                }
                Err(err) => return Some(Err(err)),
            }
        }
    }

    /// The same messages with the receipt time dropped.
    ///
    /// For a caller that does not care when a message arrived — which is
    /// what [`subscribe`] has always answered.
    fn envelopes(self) -> Envelopes<Self>
    where
        Self: Sized,
    {
        Envelopes(self)
    }
}

/// The same subscription, as an iterator of envelopes alone
///
/// What this crate has always handed back, and what a caller that does
/// not care when a message arrived still wants: `Network::open(…)
/// .envelopes()`, spelled the way it was before there was a [`Feed`].
pub fn subscribe(
    url: &str,
    stall_timeout: Option<Duration>,
) -> EnvelopeIterator {
    Network::open(url, stall_timeout).envelopes()
}

/// Open a socket, waiting out failures rather than giving up on them
///
/// A subscription is an infinite thing that replaces a connection forever
/// (see [`Network`]), so a socket that will not open is a wait, not a
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

/// One frame off the socket, and when it arrived
///
/// The bytes are the zlib blob the gateway sent, untouched: nothing here
/// has looked inside it, so a frame this build could not parse is still a
/// frame and a recorder can write it down whole.
#[derive(Clone, Debug)]
pub struct Frame {
    /// When this process first held it.
    pub received_at: DateTime<Utc>,
    /// The compressed message, as received.
    pub bytes: Vec<u8>,
}

impl Frame {
    /// Read this frame into a message, or let it go
    ///
    /// **Everything between a frame and a [`Reading`]**: the decompress,
    /// the parse, and the galaxy the caller asked for. A live
    /// subscription and a replayed spool cannot come to different answers
    /// about the same bytes, because this is the only place either of
    /// them reads any.
    ///
    /// Three answers, and the third is the one worth having:
    ///
    /// - `Some(Ok(_))` — a message, as sent.
    /// - `Some(Err(_))` — a frame that could not be read at all, which is
    ///   the gateway or this crate being wrong and is worth saying.
    /// - [`None`] — a frame deliberately let go for describing a galaxy
    ///   the caller did not ask for. Ordinary, and silent.
    ///
    /// A frame let go looks exactly like a frame that never arrived, so
    /// the difference between the last two is the one decision here that
    /// nothing downstream can see.
    pub fn read(&self, galaxy: Galaxy) -> Option<Result<Reading, Error>> {
        let read = inflate::decompress_to_vec_zlib(&self.bytes)
            .map_err(Error::Decompress)
            .and_then(|json| {
                serde_json::from_slice::<Envelope>(&json).map_err(|source| {
                    // Lossy because the message is being kept to be read
                    // by a person, and one carrying bytes that are not
                    // UTF-8 is a message worth seeing rather than one to
                    // give up on twice.
                    Error::Parse {
                        source,
                        json: String::from_utf8_lossy(&json).into_owned(),
                    }
                })
            });

        // Alpha and beta data arrives on the socket alongside the live
        // galaxy, and is written into a spool like anything else. Which
        // of the two a reader wanted is `galaxy`'s to say, and the rest
        // is dropped here rather than handed over, so it cannot be
        // recorded by forgetting to ask. See [`Envelope::live`].
        if let Ok(envelope) = &read {
            if !galaxy.shows(envelope.live) {
                debug!(schema = %envelope.schema_ref, "filtered by galaxy");
                return None;
            }
        }

        let received_at = self.received_at;
        Some(read.map(|envelope| Reading { received_at, envelope }))
    }
}

/// The gateway's socket: the live [`Feed`]
///
/// **`Network` rather than `Live`**, the live *galaxy* being a different
/// thing entirely — see [`Galaxy::LIVE`], which this filters by and
/// which a recorded feed filters by in exactly the same way.
///
/// It does not end. A socket error comes back as an [`Error`] and the
/// connection is replaced, a connection that goes quiet past the stall
/// window is replaced, and the next frame is waited for. The frames it
/// takes off the socket are read into messages by
/// [`Feed::reading`], which is the spool's road too.
pub struct Network {
    ctx: Context,
    url: String,
    connection: Connection,
    reports: Reporter,
    /// Absent when the caller asked for no stall timeout at all.
    stall: Option<Stall>,
    /// Which galaxy's data to hand over; the live one unless asked
    /// otherwise.
    galaxy: Galaxy,
    /// Whether the opening `subscribed` line has been logged yet. Logged
    /// on the first read, when the galaxy the builder chose is final.
    started: bool,
}

impl Network {
    /// Subscribe to EDDN's ZMQ socket, reading every message on it
    ///
    /// `stall_timeout` is how long the gateway may publish nothing before
    /// its connection is thrown away for a new one; `None` leaves the
    /// connection alone however long it carries nothing, giving up the
    /// only cover there is for the third case below and watching for the
    /// other two as ever.
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
    pub fn open(url: &str, stall_timeout: Option<Duration>) -> Network {
        let ctx = Context::new();
        let connection = open_retrying(&ctx, url);

        Network {
            ctx,
            url: url.to_string(),
            connection,
            reports: Reporter::default(),
            stall: stall_timeout.map(Stall::new),
            galaxy: Galaxy::LIVE,
            started: false,
        }
    }

    /// Choose which galaxy's data to hand over
    ///
    /// [`Galaxy::LIVE`] by default, so a subscriber records only the
    /// live galaxy unless it asks for the rest.
    pub fn galaxy(mut self, galaxy: Galaxy) -> Network {
        self.galaxy = galaxy;
        self
    }

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
}

impl Feed for Network {
    fn showing(&self) -> Galaxy {
        self.galaxy
    }

    /// One frame off the socket, waiting for one as long as it takes.
    ///
    /// The connection's health is looked at before every receive, not
    /// only when one comes back empty: a gateway sending steadily can
    /// still have lost and rebuilt its connection, and a receive that
    /// always has something waiting would never leave room to hear
    /// about it.
    fn frame(&mut self) -> Option<Result<Frame, Error>> {
        // "subscribed", not "connected": connecting is asynchronous, so
        // this only says the socket is open and trying, and whether it
        // took is reported when the socket knows. Said on the first read
        // rather than at the open, so the galaxy the builder chose is
        // part of it.
        if !self.started {
            self.started = true;
            let stall = self.stall.as_ref().map_or_else(
                || "off".to_owned(),
                |it| format!("{}s", it.timeout().as_secs()),
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

                    // Copied out of the socket's buffer, which is the one
                    // allocation this split costs: 3.4 KB a message at the
                    // measured 18.6 a second, and it is what lets the frame
                    // outlive the receive — a recorder holds it, and a
                    // parser that fails still leaves it whole.
                    return Some(Ok(Frame {
                        received_at: Utc::now(),
                        bytes: frame.to_vec(),
                    }));
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

impl Iterator for Network {
    type Item = Result<Reading, Error>;

    /// [`Feed::reading`], which is the spool's `next` as well.
    fn next(&mut self) -> Option<Self::Item> {
        self.reading()
    }
}

/// A feed's messages with the receipt time dropped
///
/// What [`subscribe`] hands back, kept for every caller written before
/// there was a [`Feed`] to hand one: the same messages in the same
/// order, as envelopes. Generic, so a spool can be read this way too —
/// there is nothing about dropping a timestamp that is a socket's.
pub struct Envelopes<F>(F);

impl<F: Feed> Iterator for Envelopes<F> {
    type Item = Result<Envelope, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.0.next()?.map(|reading| reading.envelope))
    }
}

/// What [`subscribe`] has always answered: [`Envelopes`] over the socket.
pub type EnvelopeIterator = Envelopes<Network>;

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

    /// A reference that is not one EDDN sends places nothing, and is kept
    #[test]
    fn a_reference_that_makes_no_sense_is_unmodelled() {
        assert_eq!(Schema::read("nonsense"), None);
        assert_eq!(Schema::read("https://eddn.edcd.io/schemas/journal"), None);

        let envelope = envelope("nonsense", JUMP);
        assert!(matches!(envelope.message, Message::Unmodeled(_)));
        // Nothing to report, rather than a version invented for the occasion.
        assert_eq!(envelope.version, None);
        // And read as the live galaxy: `/test` is the only thing that says
        // otherwise, so a reference this does not understand is not quietly
        // taken for somewhere else.
        assert!(envelope.live);
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
}

/// What comes off the socket, short of the socket itself
///
/// [`Frame::read`] is everything between a frame arriving and a
/// subscriber being handed it, which is where the one decision lives
/// that nothing else can see: a frame deliberately let go looks exactly
/// like a frame that never arrived. These are what say the difference is
/// on purpose.
#[cfg(test)]
mod frames {
    use super::tests::{envelope_json, frame, JUMP};
    use super::*;

    const JOURNAL: &str = "https://eddn.edcd.io/schemas/journal/1";
    const JOURNAL_TEST: &str = "https://eddn.edcd.io/schemas/journal/1/test";

    /// A frame as it would come off the socket at `secs` past the epoch
    fn arrived(schema_ref: &str, message: &str, secs: i64) -> Frame {
        Frame {
            received_at: DateTime::from_timestamp(secs, 0).expect("a moment"),
            bytes: frame(schema_ref, message),
        }
    }

    /// A zlib frame of `json`, whatever the json is
    fn deflated(json: &[u8]) -> Frame {
        Frame {
            received_at: Utc::now(),
            bytes: miniz_oxide::deflate::compress_to_vec_zlib(json, 6),
        }
    }

    /// Live data is handed over, with the moment it arrived
    #[test]
    fn a_live_frame_is_handed_over() {
        let read = arrived(JOURNAL, JUMP, 1_700_000_000)
            .read(Galaxy::LIVE)
            .expect("a live frame should be handed over")
            .expect("and should read");

        assert!(read.envelope.live);
        assert!(matches!(read.envelope.message, Message::Journal(_)));
        // The frame's own moment, not the moment it was read.
        assert_eq!(read.received_at.timestamp(), 1_700_000_000);
    }

    /// And alpha and beta data is not
    ///
    /// The whole of what `live` is for. Asserting the flag says the reference
    /// was understood; this says something acts on it.
    #[test]
    fn a_test_frame_is_let_go() {
        let test = arrived(JOURNAL_TEST, JUMP, 0);
        let live = arrived(JOURNAL, JUMP, 0);

        // Live: the test frame is dropped; All keeps it.
        assert!(test.read(Galaxy::LIVE).is_none());
        let kept = test
            .read(Galaxy::ALL)
            .expect("a test frame should be kept under All")
            .expect("and should read");
        assert!(!kept.envelope.live);
        // And Test drops the live galaxy.
        assert!(live.read(Galaxy::TEST).is_none());
    }

    /// A frame that will not read is reported rather than let go
    ///
    /// The difference the return type is for. Beta data arriving is
    /// ordinary and worth nothing but silence; a frame that will not read
    /// is the gateway or this crate being wrong, and going quiet about it
    /// would leave nothing to notice. Three ways one fails, and all three
    /// are `Some(Err(_))`:
    #[test]
    fn a_frame_that_will_not_read_is_reported() {
        // Not zlib at all.
        assert!(matches!(
            Frame { received_at: Utc::now(), bytes: b"not zlib".to_vec() }
                .read(Galaxy::LIVE),
            Some(Err(Error::Decompress(_))),
        ));

        // Zlib, but not an envelope.
        assert!(matches!(
            deflated(b"{}").read(Galaxy::LIVE),
            Some(Err(Error::Parse { .. })),
        ));

        // An envelope whose payload disagrees with the schema it was sent
        // under — which the schema settles: a journal message that will
        // not read, not a message of some other kind.
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
        assert!(matches!(
            deflated(json.as_bytes()).read(Galaxy::LIVE),
            Some(Err(Error::Parse { .. })),
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
        let Some(Err(err)) = deflated(json.as_bytes()).read(Galaxy::LIVE)
        else {
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
        let truncated = br#"{"$schemaRef": "https://eddn.edcd.io/schemas/journal/1", "heade"#;

        let Some(Err(err)) = deflated(truncated).read(Galaxy::LIVE) else {
            panic!("a malformed envelope should be reported")
        };

        let near = err.near().expect("should point at where it stopped");
        assert!(near.contains("heade"), "pointed elsewhere: {}", near);
    }
}

/// The seam a spool, and an offline test, hang off
#[cfg(test)]
mod feeds {
    use super::tests::{frame, JUMP};
    use super::*;

    const JOURNAL: &str = "https://eddn.edcd.io/schemas/journal/1";

    /// Frames a test wrote, read by the same thing that reads a socket's.
    ///
    /// **What a third feed costs is this struct**: hand over frames, say
    /// which galaxy, and the reading is [`Feed::reading`]'s. Nothing
    /// here decompresses, parses or filters, which is why a fixture
    /// cannot drift from the wire.
    struct Recorded(std::vec::IntoIter<Frame>);

    impl Iterator for Recorded {
        type Item = Result<Reading, Error>;

        fn next(&mut self) -> Option<Self::Item> {
            self.reading()
        }
    }

    impl Feed for Recorded {
        fn frame(&mut self) -> Option<Result<Frame, Error>> {
            self.0.next().map(Ok)
        }

        fn showing(&self) -> Galaxy {
            Galaxy::LIVE
        }
    }

    fn recorded(frames: Vec<Frame>) -> Recorded {
        Recorded(frames.into_iter())
    }

    /// A third feed is a struct and two methods, held behind `dyn Feed`
    ///
    /// **What the trait is for**, and the two properties an added
    /// generic method or a non-`Send` field would quietly take away: a
    /// consumer holds `Box<dyn Feed>`, hands it to the thread it parks
    /// on, and never learns what is behind it. That is what lets a
    /// recorded hour stand in for the wire — the one path with no
    /// offline exercise otherwise — and what makes `--from spool=DIR` a
    /// constructor rather than a second sink.
    ///
    /// What those readings *say* is
    /// [`a_spool_and_a_subscription_read_alike`]'s business, and
    /// [`Frame::read`]'s tests before it.
    #[test]
    fn a_feed_of_anything_is_held_behind_dyn_feed() {
        let frames = vec![Frame {
            received_at: Utc::now(),
            bytes: frame(JOURNAL, JUMP),
        }];
        let feed: Box<dyn Feed> = Box::new(recorded(frames));
        assert_eq!(feed.resume(), None, "a feed with no position said one");

        // `Send`, which a trait object that is not cannot be, and which
        // the whole arrangement rests on: the feed is read on a thread of
        // its own because reading it parks one.
        let counted = std::thread::spawn(move || feed.count());
        assert_eq!(counted.join().expect("the thread"), 1);
    }

    /// A spool and a subscription answer the same bytes the same way
    ///
    /// **The divergence this crate is shaped to make impossible.** Two
    /// feeds, one read off a socket and one off a disk, are two things
    /// that could drift: a galaxy filter fixed in one, a receipt time
    /// stamped differently in the other, and a directory built from a
    /// spool stops matching one built from the wire. So there is one
    /// [`Feed::reading`] and the feeds differ only in where their frames
    /// come
    /// from — this drives the same frames down both and asks for the
    /// same answer.
    #[test]
    fn a_spool_and_a_subscription_read_alike() {
        let at = |secs: i64| {
            DateTime::from_timestamp(1_700_000_000 + secs, 0).expect("a moment")
        };
        let test = "https://eddn.edcd.io/schemas/journal/1/test";
        let frames: Vec<Frame> = [JOURNAL, test, JOURNAL]
            .iter()
            .enumerate()
            .map(|(n, schema)| Frame {
                received_at: at(n as i64),
                bytes: frame(schema, JUMP),
            })
            .collect();

        // Down the wire's road: frames in, readings out.
        let wire: Vec<Reading> =
            recorded(frames.clone()).map(|it| it.expect("a reading")).collect();

        // And down the spool's: the same frames written to a segment and
        // read back through `spool`.
        let dir = std::env::temp_dir()
            .join(format!("eddn-alike-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut recorder = spool::Recorder::open(&dir).expect("a recorder");
        for frame in &frames {
            recorder.write(frame).expect("a write");
        }
        let spooled: Vec<Reading> = spool::Spool::open(
            &dir,
            spool::Start::Earliest,
            spool::Replay::ToEnd,
        )
        .expect("a spool")
        .map(|it| it.expect("a reading"))
        .collect();
        let _ = std::fs::remove_dir_all(&dir);

        let said = |read: &[Reading]| {
            read.iter()
                .map(|it| (it.received_at, it.envelope.schema_ref.clone()))
                .collect::<Vec<_>>()
        };
        assert_eq!(said(&wire), said(&spooled));
        // Both let the test-galaxy frame go, and neither restamped what
        // it kept.
        assert_eq!(
            said(&wire),
            vec![(at(0), JOURNAL.to_owned()), (at(2), JOURNAL.to_owned()),]
        );
    }
}
