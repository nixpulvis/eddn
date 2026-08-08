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

        // Both answers come off the reference, and are taken while it is
        // still there to borrow from.
        let (message, live) = {
            let named = split_schema_ref(&raw.schema_ref);

            (
                Message::read(named.map(|(name, _)| name), raw.message)
                    .map_err(serde::de::Error::custom)?,
                // A reference that cannot be read names no test schema, and
                // is taken at its word for the same reason its payload is.
                !named.map(|(_, test)| test).unwrap_or(false),
            )
        };

        Ok(Envelope {
            schema_ref: raw.schema_ref,
            header: raw.header,
            message,
            live,
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
                    let read = inflate::decompress_to_vec_zlib(&compressed)
                        .map_err(Error::Decompress)
                        .and_then(|json| {
                            serde_json::from_slice::<Envelope>(&json)
                                .map_err(Error::Parse)
                        });

                    // Alpha and beta data arrives on this socket alongside
                    // the live galaxy and is dropped here rather than handed
                    // over, so that a subscriber cannot record it by
                    // forgetting to ask. Whoever wants it can read an
                    // envelope directly and look at `live`.
                    if let Ok(envelope) = &read {
                        if !envelope.live {
                            debug!(schema = %envelope.schema_ref, "not live");
                            continue;
                        }
                    }

                    return Some(read);
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

/// Placing a message by the schema it was sent under
///
/// The envelope is what decides everything here, so these go through the whole
/// of it rather than through [`Message::read`]: what a consumer gets handed is
/// an [`Envelope`], and how it got there is not the thing worth pinning down.
#[cfg(test)]
mod tests {
    use super::*;

    /// An envelope with `message` as given, and a header of no interest
    fn envelope(schema_ref: &str, message: &str) -> Envelope {
        let json = format!(
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
        );

        serde_json::from_str(&json).expect("envelope should parse")
    }

    const JUMP: &str = r#"{
        "timestamp": "2026-08-08T12:00:00Z",
        "event": "FSDJump",
        "StarSystem": "Sol",
        "StarPos": [0.0, 0.0, 0.0],
        "SystemAddress": 10477373803
    }"#;

    /// The schema names the payload, so a journal schema is a journal entry
    #[test]
    fn a_journal_message_is_a_journal_entry() {
        let envelope = envelope("https://eddn.edcd.io/schemas/journal/1", JUMP);

        assert!(matches!(envelope.message, Message::Journal(_)));
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
    /// This is the whole difference from guessing. A journal message whose
    /// event will not read used to be indistinguishable from a message of
    /// some other kind, and went in the bin without a word.
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
        assert_eq!(split_schema_ref("nonsense"), None);
        assert_eq!(
            split_schema_ref("https://eddn.edcd.io/schemas/journal"),
            None,
        );

        let envelope = envelope("nonsense", JUMP);
        assert!(matches!(envelope.message, Message::Unmodeled(_)));
    }

    /// The name and the test flag, taken off the end of the reference
    #[test]
    fn a_reference_splits_into_a_name_and_whether_it_is_live() {
        assert_eq!(
            split_schema_ref("https://eddn.edcd.io/schemas/commodity/3"),
            Some(("commodity", false)),
        );
        assert_eq!(
            split_schema_ref("https://eddn.edcd.io/schemas/shipyard/2/test"),
            Some(("shipyard", true)),
        );
    }
}
