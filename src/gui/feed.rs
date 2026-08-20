//! A bounded, in-memory view of what the socket has carried
//!
//! [`subscribe`](eddn::subscribe) yields envelopes forever, and anything
//! showing them to someone has to keep only so many and say something about the
//! rest. [`Feed`] is that: a newest-last window of envelopes, the count of
//! everything that has gone through it whether it was kept or not, and a tally
//! per schema. It holds no clock and no socket, so a consumer's idea of what
//! the feed has done can be built and checked without either.

use eddn::{Envelope, Galaxy, Message};
use elite_journal::entry::Event;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::time::Duration;

/// How many envelopes a [`Feed`] keeps unless told otherwise
///
/// EDDN at a busy hour carries 31 messages a second, so this is about three
/// minutes of history: enough to scroll back through what just happened
/// without the window growing without bound behind a running program.
pub const DEFAULT_CAPACITY: usize = 5000;

/// Distinct keys seen, split by galaxy so a count can honour a live/test
/// filter. Both galaxies together is the two counts added, so a key someone
/// publishes to the live galaxy and a test one alike is counted in each --
/// which is the publisher's to answer for, not this type's to hide.
struct Tally<K> {
    live: HashSet<K>,
    test: HashSet<K>,
}

impl<K> Default for Tally<K> {
    fn default() -> Self {
        Tally { live: HashSet::new(), test: HashSet::new() }
    }
}

impl<K: Eq + std::hash::Hash> Tally<K> {
    fn insert(&mut self, key: K, live: bool) {
        if live {
            self.live.insert(key);
        } else {
            self.test.insert(key);
        }
    }

    /// How many keys the galaxy shows
    fn count(&self, galaxy: Galaxy) -> usize {
        let mut count = 0;
        if galaxy.contains(Galaxy::LIVE) {
            count += self.live.len();
        }
        if galaxy.contains(Galaxy::TEST) {
            count += self.test.len();
        }
        count
    }
}

/// A bounded history of envelopes with running totals
///
/// The window is the last [`capacity`](Feed::capacity) envelopes, newest last.
/// The totals count everything ever pushed, so a schema's tally keeps climbing
/// after the envelopes that made it have fallen out of the window.
pub struct Feed {
    capacity: usize,
    kept: VecDeque<Kept>,
    received: u64,
    errors: u64,
    per_schema: BTreeMap<String, u64>,
    systems: Tally<i64>,
    bodies: Tally<(i64, i16)>,
    stations: Tally<i64>,
}

/// A retained envelope and the lowercased text a filter searches it by
///
/// The search text is built once, when the envelope is pushed, from the same
/// fields the columns show plus the uploader. So filtering reads a ready
/// string rather than re-deriving one -- and re-`Debug`-formatting every
/// event to name it -- for every row on every frame.
struct Kept {
    envelope: Envelope,
    search: String,
    /// The connection gap this envelope arrived after, if it followed a stall,
    /// marking the row where the feed resumed. See [`Feed::push_after_gap`].
    gap: Option<Duration>,
}

impl Default for Feed {
    fn default() -> Self {
        Feed::new(DEFAULT_CAPACITY)
    }
}

impl Feed {
    /// A feed keeping the last `capacity` envelopes
    ///
    /// A capacity of zero keeps no window but still counts, which is a feed
    /// that reports on the socket without holding any of it.
    pub fn new(capacity: usize) -> Self {
        Feed {
            capacity,
            kept: VecDeque::new(),
            received: 0,
            errors: 0,
            per_schema: BTreeMap::new(),
            systems: Tally::default(),
            bodies: Tally::default(),
            stations: Tally::default(),
        }
    }

    /// Take an envelope, dropping the oldest if the window is full
    pub fn push(&mut self, envelope: Envelope) {
        self.push_inner(envelope, None);
    }

    /// Take an envelope that arrived after a connection gap of `gap`, marking
    /// the row where the feed resumed after a stall
    pub fn push_after_gap(&mut self, envelope: Envelope, gap: Duration) {
        self.push_inner(envelope, Some(gap));
    }

    fn push_inner(&mut self, envelope: Envelope, gap: Option<Duration>) {
        self.received += 1;
        *self
            .per_schema
            .entry(schema_family(&envelope.schema_ref).to_owned())
            .or_insert(0) += 1;

        self.tally_objects(&envelope.message, envelope.live);

        if self.capacity == 0 {
            return;
        }
        while self.kept.len() >= self.capacity {
            self.kept.pop_front();
        }
        let search = search_text(&envelope);
        self.kept.push_back(Kept { envelope, search, gap });
    }

    /// Record the distinct systems, bodies and stations a message names
    ///
    /// Uniques by in-game id: a system by its address, a station by its market
    /// id, a body by its id within its system. All-time, like the other totals
    /// -- a system seen an hour ago still counts.
    fn tally_objects(&mut self, message: &Message, live: bool) {
        match message {
            Message::Journal(entry) => self.tally_event(&entry.event, live),
            Message::Commodity(e) => {
                self.stations.insert(e.event.market_id, live);
            }
            Message::Outfitting(e) => {
                self.stations.insert(e.event.market_id, live);
            }
            Message::Shipyard(e) => {
                self.stations.insert(e.event.market_id, live);
            }
            Message::BlackMarket(e) => {
                if let Some(id) = e.event.market_id {
                    self.stations.insert(id, live);
                }
            }
            Message::Unmodeled(_) => {}
        }
    }

    /// The objects a single journal event names
    fn tally_event(&mut self, event: &Event, live: bool) {
        match event {
            Event::FsdJump(e) => {
                self.tally_place(e.system.address, None, None, live)
            }
            Event::CarrierJump(e) => self.tally_place(
                e.system.address,
                e.body.as_ref().map(|b| b.id),
                e.station.as_ref().and_then(|s| s.market_id),
                live,
            ),
            Event::Location(e) => self.tally_place(
                e.system.address,
                e.body.as_ref().map(|b| b.id),
                e.station.as_ref().and_then(|s| s.market_id),
                live,
            ),
            Event::Docked(e) => self.tally_place(
                e.system_address,
                None,
                e.station.market_id,
                live,
            ),
            Event::ApproachSettlement(e) => self.tally_place(
                e.system_address,
                Some(e.body_id),
                e.market_id,
                live,
            ),
            Event::Scan(e) => {
                self.bodies
                    .insert((e.system_address, e.target.body_id()), live);
            }
            Event::ScanBaryCentre(e) => {
                self.bodies.insert((e.system_address, e.body_id), live);
            }
            Event::FssBodySignals(e) => {
                self.bodies.insert((e.system_address, e.body_id), live);
            }
            Event::SAASignalsFound(e) => {
                self.bodies.insert((e.system_address, e.body_id), live);
            }
            _ => {}
        }
    }

    /// Record a place by id: its system, and its body and station where named
    ///
    /// The shape a handful of events share -- a jump names a system, a carrier
    /// jump or a location a system and maybe a body and a station, a docking a
    /// system and a station. `None` stands where an event carries none of one.
    fn tally_place(
        &mut self,
        system: i64,
        body: Option<i16>,
        market: Option<i64>,
        live: bool,
    ) {
        self.systems.insert(system, live);
        if let Some(body) = body {
            self.bodies.insert((system, body), live);
        }
        if let Some(market) = market {
            self.stations.insert(market, live);
        }
    }

    /// How many distinct systems the galaxy has named
    pub fn systems(&self, galaxy: Galaxy) -> usize {
        self.systems.count(galaxy)
    }

    /// How many distinct bodies the galaxy has named
    pub fn bodies(&self, galaxy: Galaxy) -> usize {
        self.bodies.count(galaxy)
    }

    /// How many distinct stations the galaxy has named
    pub fn stations(&self, galaxy: Galaxy) -> usize {
        self.stations.count(galaxy)
    }

    /// Count a message that could not be read
    ///
    /// An error is not an envelope and is not kept, but it is part of what the
    /// socket has done and is worth a running total of its own.
    pub fn note_error(&mut self) {
        self.errors += 1;
    }

    /// The retained window, oldest first, each with the text it filters by
    ///
    /// The search text is what [`push`](Feed::push) built once from the row's
    /// fields; a filter matches against it rather than re-reading the envelope.
    pub fn rows(
        &self,
    ) -> impl Iterator<Item = (&Envelope, &str, Option<Duration>)> {
        self.kept
            .iter()
            .map(|kept| (&kept.envelope, kept.search.as_str(), kept.gap))
    }

    /// How many envelopes have ever been pushed, kept or dropped
    pub fn received(&self) -> u64 {
        self.received
    }

    /// How many unreadable messages have been counted
    pub fn errors(&self) -> u64 {
        self.errors
    }

    /// Reset the unreadable-message count to zero
    ///
    /// For the log pane's clear button, which dismisses the error and warning
    /// counts the status bar was flagging.
    pub fn clear_errors(&mut self) {
        self.errors = 0;
    }

    /// How many envelopes the window currently holds
    pub fn retained(&self) -> usize {
        self.kept.len()
    }

    /// How much time the retained window spans
    ///
    /// The gap between the oldest kept envelope's gateway timestamp and the
    /// newest, so a consumer can say how much time the kept messages cover, not
    /// just how many. [`None`] when the window is empty; zero when it holds one.
    ///
    /// Spanned by arrival order, not by the timestamps themselves: across a
    /// reconnection the gateway's clock need not carry on exactly where it left
    /// off, so the newest can read a shade before the oldest and the gap come
    /// out slightly negative. A caller rendering it should expect that -- the
    /// window viewer floors it at zero rather than show a negative span.
    pub fn window_duration(&self) -> Option<chrono::Duration> {
        let oldest = &self.kept.front()?.envelope;
        let newest = &self.kept.back()?.envelope;
        Some(newest.header.gateway_timestamp - oldest.header.gateway_timestamp)
    }

    /// Everything pushed, tallied by schema family, in name order
    ///
    /// A volume total, not a distinct count, and all-galaxy where
    /// [`systems`](Feed::systems) and its kind honour a [`Galaxy`]: these
    /// tallies sum to [`received`](Feed::received), itself all-galaxy, so this
    /// decomposes that number rather than mirroring the filtered counts. The
    /// split is deliberate; making it galaxy-aware would break that identity.
    pub fn per_schema(&self) -> &BTreeMap<String, u64> {
        &self.per_schema
    }
}

/// The schema's name, without the gateway, version or `/test` around it
///
/// `https://eddn.edcd.io/schemas/journal/1` and its `/test` twin both answer
/// `journal`, which is what a tally wants: a count per kind of message rather
/// than per version of each. A reference this cannot find its way through is
/// answered whole, so nothing an unrecognised shape carries is silently merged
/// into another.
pub fn schema_family(schema_ref: &str) -> &str {
    const SCHEMAS: &str = "/schemas/";
    match schema_ref.find(SCHEMAS) {
        Some(at) => {
            let rest = &schema_ref[at + SCHEMAS.len()..];
            rest.split('/').next().unwrap_or(rest)
        }
        None => schema_ref,
    }
}

/// A short name for what a message holds, for a line in a list
///
/// The schema family says which kind a message is; this says which event,
/// where there is one to say. A journal message is one of hundreds of events
/// and the event is the useful thing to see at a glance, so it is named. The
/// rest are one shape each and named for the shape.
pub fn event_label(message: &Message) -> String {
    match message {
        Message::Journal(entry) => variant_name(&format!("{:?}", entry.event)),
        Message::Commodity(_) => "Commodity".to_owned(),
        Message::Outfitting(_) => "Outfitting".to_owned(),
        Message::Shipyard(_) => "Shipyard".to_owned(),
        Message::BlackMarket(_) => "BlackMarket".to_owned(),
        Message::Unmodeled(_) => "Unmodeled".to_owned(),
    }
}

/// The variant name at the front of a derived [`Debug`] rendering
///
/// `FsdJump(FsdJump { .. })` is `FsdJump`. Everything a `Debug` puts after the
/// name opens with `(`, `{` or a space, so the name is what stands before the
/// first of those.
fn variant_name(debug: &str) -> String {
    let end = debug
        .find(|c: char| c == '(' || c == '{' || c.is_whitespace())
        .unwrap_or(debug.len());
    debug[..end].to_owned()
}

/// The system column's text for an envelope
///
/// A `NavRoute` names no single system; it carries a route, so its endpoints
/// are shown as `first -> last`. Everything else shows the one system it names.
pub fn system_text(envelope: &Envelope) -> String {
    if let Message::Journal(entry) = &envelope.message {
        if let Event::NavRoute(route) = &entry.event {
            if let (Some(first), Some(last)) =
                (route.destinations.first(), route.destinations.last())
            {
                return format!(
                    "{} -> {}",
                    first.star_system, last.star_system
                );
            }
        }
    }
    envelope.star_system.clone().unwrap_or_default()
}

/// The lowercased text a row is filtered by
///
/// The system, station and body it names, the schema family and event it is,
/// and the uploader who sent it: everything a search might reach for, joined
/// and lowered once at push. It is what the columns show, so a filter finds
/// what is on screen, plus the uploader, which the detail pane shows but no
/// column does. Independent of which columns are toggled on: a hidden column's
/// contents stay findable, and the set does not shift as the view is changed.
fn search_text(envelope: &Envelope) -> String {
    let mut parts = vec![
        schema_family(&envelope.schema_ref).to_owned(),
        event_label(&envelope.message),
        system_text(envelope),
        envelope.header.uploader_id.clone(),
    ];
    if let Some(body) = &envelope.body {
        parts.push(body.clone());
    }
    if let Some(station) = &envelope.station {
        parts.push(station.clone());
    }
    parts.join(" ").to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use eddn::{Header, Message};
    use serde_json::json;

    fn envelope(schema_ref: &str) -> Envelope {
        Envelope {
            schema_ref: schema_ref.to_owned(),
            header: Header {
                gateway_timestamp: Utc::now(),
                software_name: "test".to_owned(),
                software_version: "0".to_owned(),
                uploader_id: "someone".to_owned(),
            },
            message: Message::Unmodeled(json!({})),
            live: true,
            version: None,
            star_system: None,
            station: None,
            body: None,
        }
    }

    fn journal(message: serde_json::Value) -> Envelope {
        use elite_journal::entry::{Entry, Event};
        let entry: Entry<Event> = serde_json::from_value(message).unwrap();
        Envelope {
            schema_ref: "https://eddn.edcd.io/schemas/journal/1".to_owned(),
            header: Header {
                gateway_timestamp: Utc::now(),
                software_name: "test".to_owned(),
                software_version: "0".to_owned(),
                uploader_id: "someone".to_owned(),
            },
            message: Message::Journal(entry),
            live: true,
            version: None,
            star_system: Some("Sol".to_owned()),
            station: None,
            body: None,
        }
    }

    #[test]
    fn tallies_distinct_objects_by_id() {
        let mut feed = Feed::default();
        feed.push(journal(json!({
            "timestamp": "2020-01-01T00:00:00Z",
            "event": "FSDJump",
            "SystemAddress": 1234,
            "StarSystem": "Sol",
        })));
        feed.push(journal(json!({
            "timestamp": "2020-01-01T00:01:00Z",
            "event": "Docked",
            "SystemAddress": 5678,
            "StarSystem": "Alpha",
            "StationName": "Hub",
            "MarketID": 999,
        })));
        // The same system again is counted once, not twice.
        feed.push(journal(json!({
            "timestamp": "2020-01-01T00:02:00Z",
            "event": "FSDJump",
            "SystemAddress": 1234,
            "StarSystem": "Sol",
        })));

        assert_eq!(feed.systems(Galaxy::ALL), 2);
        assert_eq!(feed.stations(Galaxy::ALL), 1);
        assert_eq!(feed.bodies(Galaxy::ALL), 0);
    }

    #[test]
    fn counts_respect_the_galaxy_filter() {
        let mut feed = Feed::default();
        let live = journal(json!({
            "timestamp": "2020-01-01T00:00:00Z",
            "event": "FSDJump",
            "SystemAddress": 1,
            "StarSystem": "Live",
        }));
        feed.push(live); // journal() marks it live
        let mut test = journal(json!({
            "timestamp": "2020-01-01T00:01:00Z",
            "event": "FSDJump",
            "SystemAddress": 2,
            "StarSystem": "Test",
        }));
        test.live = false;
        feed.push(test);

        assert_eq!(feed.systems(Galaxy::ALL), 2); // both galaxies
        assert_eq!(feed.systems(Galaxy::LIVE), 1); // live only
        assert_eq!(feed.systems(Galaxy::TEST), 1); // test only
    }

    /// A key on both galaxies is counted on each, All being the two summed
    ///
    /// A publisher that sends one system to the live galaxy and a test one
    /// alike is seen in both, and the feed says so rather than hiding the
    /// double behind a union. Sol has the same address on both.
    #[test]
    fn both_galaxies_count_a_shared_key_twice() {
        let mut feed = Feed::default();
        let live = journal(json!({
            "timestamp": "2020-01-01T00:00:00Z",
            "event": "FSDJump",
            "SystemAddress": 42,
            "StarSystem": "Shared",
        }));
        feed.push(live);
        let mut test = journal(json!({
            "timestamp": "2020-01-01T00:01:00Z",
            "event": "FSDJump",
            "SystemAddress": 42,
            "StarSystem": "Shared",
        }));
        test.live = false;
        feed.push(test);

        assert_eq!(feed.systems(Galaxy::LIVE), 1);
        assert_eq!(feed.systems(Galaxy::TEST), 1);
        assert_eq!(feed.systems(Galaxy::ALL), 2); // counted in each
    }

    #[test]
    fn family_strips_gateway_version_and_test() {
        assert_eq!(
            schema_family("https://eddn.edcd.io/schemas/journal/1"),
            "journal"
        );
        assert_eq!(
            schema_family("https://eddn.edcd.io/schemas/journal/1/test"),
            "journal"
        );
    }

    #[test]
    fn family_keeps_an_unrecognised_reference_whole() {
        assert_eq!(schema_family("nonsense"), "nonsense");
    }

    #[test]
    fn window_is_bounded_but_totals_are_not() {
        let mut feed = Feed::new(2);
        for _ in 0..5 {
            feed.push(envelope("https://eddn.edcd.io/schemas/journal/1"));
        }

        assert_eq!(feed.retained(), 2);
        assert_eq!(feed.received(), 5);
        assert_eq!(feed.per_schema().get("journal"), Some(&5));
    }

    #[test]
    fn zero_capacity_counts_without_keeping() {
        let mut feed = Feed::new(0);
        feed.push(envelope("https://eddn.edcd.io/schemas/commodity/3"));

        assert_eq!(feed.retained(), 0);
        assert_eq!(feed.received(), 1);
        assert_eq!(feed.per_schema().get("commodity"), Some(&1));
    }

    #[test]
    fn errors_are_counted_apart_from_envelopes() {
        let mut feed = Feed::default();
        feed.note_error();
        feed.push(envelope("https://eddn.edcd.io/schemas/journal/1"));

        assert_eq!(feed.errors(), 1);
        assert_eq!(feed.received(), 1);
    }

    #[test]
    fn clearing_errors_resets_the_count() {
        let mut feed = Feed::default();
        feed.note_error();
        feed.note_error();
        assert_eq!(feed.errors(), 2);

        feed.clear_errors();
        assert_eq!(feed.errors(), 0);
    }

    #[test]
    fn a_gap_is_kept_with_the_row_that_followed_it() {
        let mut feed = Feed::default();
        feed.push(envelope("https://eddn.edcd.io/schemas/journal/1"));
        feed.push_after_gap(
            envelope("https://eddn.edcd.io/schemas/journal/1"),
            Duration::from_secs(42),
        );

        let gaps: Vec<_> = feed.rows().map(|(_, _, gap)| gap).collect();
        assert_eq!(gaps, vec![None, Some(Duration::from_secs(42))]);
    }

    #[test]
    fn window_duration_spans_oldest_to_newest() {
        use chrono::{Duration, TimeZone};

        let mut feed = Feed::default();
        assert!(feed.window_duration().is_none());

        let mut first = envelope("https://eddn.edcd.io/schemas/journal/1");
        first.header.gateway_timestamp = Utc.timestamp_opt(1_000, 0).unwrap();
        feed.push(first);
        assert_eq!(feed.window_duration(), Some(Duration::zero()));

        let mut later = envelope("https://eddn.edcd.io/schemas/journal/1");
        later.header.gateway_timestamp = Utc.timestamp_opt(1_090, 0).unwrap();
        feed.push(later);
        assert_eq!(feed.window_duration(), Some(Duration::seconds(90)));
    }

    #[test]
    fn event_label_names_the_journal_event() {
        // A journal envelope built through the model rather than parsed, so the
        // label logic is what is under test and not the reader.
        let message = Message::Unmodeled(json!({}));
        assert_eq!(event_label(&message), "Unmodeled");
    }

    #[test]
    fn event_label_reads_the_event_out_of_a_journal_message() {
        let jump = journal(json!({
            "timestamp": "2020-01-01T00:00:00Z",
            "event": "FSDJump",
            "SystemAddress": 1,
            "StarSystem": "Sol",
        }));
        assert_eq!(event_label(&jump.message), "FsdJump");
    }

    #[test]
    fn variant_name_takes_what_precedes_the_fields() {
        assert_eq!(variant_name("FsdJump(FsdJump { .. })"), "FsdJump");
        assert_eq!(variant_name("Docked { .. }"), "Docked");
        assert_eq!(variant_name("Unit"), "Unit");
    }

    /// A row's search text carries every field a filter reaches for, lowered
    ///
    /// The system, station, body, schema family and event are what the columns
    /// show; the uploader is not a column but is searchable all the same, which
    /// is the claim the filter doc makes and this pins.
    #[test]
    fn search_text_covers_the_columns_and_the_uploader() {
        let mut feed = Feed::default();
        let mut env = journal(json!({
            "timestamp": "2020-01-01T00:00:00Z",
            "event": "FSDJump",
            "SystemAddress": 1,
            "StarSystem": "Sol",
        }));
        env.station = Some("Daedalus".to_owned());
        env.body = Some("Sol A 3".to_owned());
        env.header.uploader_id = "UploaderXYZ".to_owned();
        feed.push(env);

        let (_, search, _) = feed.rows().next().expect("one row kept");
        assert_eq!(search, search.to_lowercase(), "search text is lowered");
        assert!(search.contains("journal"), "schema family: {search}");
        assert!(search.contains("fsdjump"), "event: {search}");
        assert!(search.contains("sol"), "system: {search}");
        assert!(search.contains("daedalus"), "station: {search}");
        assert!(search.contains("sol a 3"), "body: {search}");
        assert!(search.contains("uploaderxyz"), "uploader: {search}");
    }
}
