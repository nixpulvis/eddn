//! A recorded feed: the wire written down, and read back as itself
//!
//! One subscription, any number of consumers. `galos-db` and `galos-index`
//! each subscribing costs 17.5 KiB/s of somebody else's infrastructure
//! apiece and gives the two of them *different* holes — two SUB sockets
//! have their own high-water marks and their own connect times, so a
//! restart puts a gap in one and not the other. A spool is one
//! subscription written to disk and read by both, so the holes are shared
//! and a crash replays rather than skips.
//!
//! **The recorder never decompresses and never parses.** Frames go down
//! exactly as the gateway sent them, with the receipt time beside each.
//! Fidelity rather than laziness: a message this build's deserialiser
//! cannot read is still in the spool after the parser is fixed, and a tap
//! that went through the parser could not have written it down. It costs a
//! `memcpy` and a `write` per frame.
//!
//! ```text
//! spool/
//!   2026091917.eddn      segments, named for the UTC hour they were opened
//!   2026091918.eddn
//!   cursors/galos-db     consumer positions, one file each
//!   cursors/galos-index
//! ```
//!
//! A record is `u32 len`, `i64 received_at` in nanoseconds, then the frame.
//!
//! **A torn tail is ignored, not an error.** A record whose length runs
//! past the end of a segment ends the read there and the reader comes back
//! to the same offset — which is the rule the index's own log already runs
//! on (`galos_index::checkpoint::read_frames`). Nothing on the write path
//! is `fsync`ed, so a killed recorder loses the partial record and nothing
//! else.
//!
//! Delivery is **at least once, in wire order**: a consumer commits its
//! cursor after the work it covers is durable, so a kill replays the tail
//! and both sinks are idempotent under replay.

use crate::{Error, Feed, Frame, Galaxy, Position, Reading};
use chrono::prelude::*;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tracing::{info, warn};

/// What a segment file is called: the UTC hour it was opened in.
const SEGMENT: &str = "%Y%m%d%H";

/// The suffix a segment carries, so nothing else in the directory is read
/// as one.
const SUFFIX: &str = ".eddn";

/// Where consumer positions live, one file each.
const CURSORS: &str = "cursors";

/// What a record's head holds: the frame's length, then when it was
/// received.
///
/// **Written as the types rather than as `4 + 8`.** The two numbers a
/// reader needs — how wide the head is and where the second field
/// starts — are the same fact said three times if they are spelled by
/// hand, and a record format is exactly the place where the third
/// spelling is the one that gets missed.
type Length = u32;
type Received = i64;

/// Where the receipt time starts in a head.
const RECEIVED: usize = size_of::<Length>();

/// How wide a record's head is.
const HEAD: usize = RECEIVED + size_of::<Received>();

/// The largest frame a record may claim to hold.
///
/// EDDN's mean is 3,454 bytes compressed and its largest schema is an
/// outfitting message; a megabyte is orders past either. What this is for
/// is a torn or corrupt head, which would otherwise ask for whatever
/// integer the garbage spelled.
const LARGEST: Length = 1 << 20;

/// How long a following reader waits before looking for more.
///
/// A quarter of a second: the feed carries 18.6 messages a second, so a
/// reader that has caught up is never waiting long, and a consumer being
/// asked to stop notices in about this.
const POLL: Duration = Duration::from_millis(250);

/// Where a reader starts when it is opened.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Start {
    /// From this consumer's own cursor, by name, and from the end of what
    /// is recorded where it has none.
    ///
    /// A fresh follower follows rather than replaying two days unasked.
    Cursor(String),
    /// From the oldest record still held.
    Earliest,
    /// From the end: only what arrives after this was opened.
    Latest,
    /// From the first record received at or after a moment.
    Since(DateTime<Utc>),
}

/// What a reader does when it reaches the end of what is recorded.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Replay {
    /// Wait for more, the way a subscription does. The iterator never ends.
    Follow,
    /// Stop. The iterator ends, which is what a test or a one-off pass
    /// over a recorded hour wants.
    ToEnd,
}

/// Write a live feed to a directory, one segment an hour.
///
/// The whole of the recorder: [`write`](Recorder::write) takes the frames
/// a [`Network`](crate::Network) feed hands over and nothing else looks
/// inside them.
pub struct Recorder {
    dir: PathBuf,
    /// The segment being written, and the hour it stands for.
    open: Option<(String, BufWriter<File>)>,
    /// How much history to keep, where the operator asked for a bound.
    retain: Option<Duration>,
}

impl Recorder {
    /// Open a spool directory for writing, making it where it is absent.
    pub fn open(dir: impl Into<PathBuf>) -> io::Result<Recorder> {
        let dir = dir.into();
        fs::create_dir_all(dir.join(CURSORS))?;
        Ok(Recorder { dir, open: None, retain: None })
    }

    /// Keep only what was received inside `window`.
    ///
    /// Pruning happens on the hour roll and takes whole segments, never
    /// records: an operator can see what is held by listing the directory,
    /// and a segment is either there or it is not.
    pub fn retaining(mut self, window: Duration) -> Recorder {
        self.retain = Some(window);
        self
    }

    /// Write one frame, rolling the segment where the hour has turned.
    ///
    /// **A record reaches the filesystem before this returns.** The
    /// buffer under it is there to make a record one `write` rather than
    /// three, not to hold records back: a consumer following the spool is
    /// waiting for them, and a `flush` a caller can forget is a spool
    /// that silently lags by however much it buffered.
    ///
    /// Not `fsync`, which is a different promise and not one the format
    /// needs: what a kill leaves is the partial record every reader
    /// already ignores. See the torn-tail rule above.
    pub fn write(&mut self, frame: &Frame) -> io::Result<()> {
        let hour = frame.received_at.format(SEGMENT).to_string();
        let rolled = match &self.open {
            Some((open, _)) => open != &hour,
            None => true,
        };
        if rolled {
            self.roll(&hour)?;
        }
        let (_, file) = self.open.as_mut().expect("a segment is open");
        let len = Length::try_from(frame.bytes.len()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "a frame past 4 GiB")
        })?;
        let at: Received = frame
            .received_at
            .timestamp_nanos_opt()
            .expect("a receipt time inside the nanosecond era");
        // The head before the frame, and the length first: a reader that
        // meets a short record knows it is short before it trusts a byte
        // of it.
        file.write_all(&len.to_le_bytes())?;
        file.write_all(&at.to_le_bytes())?;
        file.write_all(&frame.bytes)?;
        file.flush()
    }

    /// Close the segment being written and open `hour`'s, pruning what has
    /// aged out.
    fn roll(&mut self, hour: &str) -> io::Result<()> {
        if let Some((_, file)) = &mut self.open {
            file.flush()?;
        }
        let path = self.dir.join(format!("{hour}{SUFFIX}"));
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        info!(segment = %format!("{hour}{SUFFIX}"), "recording");
        self.open = Some((hour.to_owned(), BufWriter::new(file)));
        if let Some(window) = self.retain {
            self.prune(window)?;
        }
        Ok(())
    }

    /// Delete whole segments older than the window, saying who is about to
    /// lose one.
    ///
    /// **It never refuses.** A consumer that has stopped must not be able
    /// to fill a disk, so a cursor naming a segment on its way out is a
    /// warning and the segment goes anyway. The consumer finds out when it
    /// comes back and its cursor names nothing.
    fn prune(&self, window: Duration) -> io::Result<()> {
        let held = segments(&self.dir)?;
        let keep = match chrono::Duration::from_std(window) {
            Ok(window) => Utc::now() - window,
            Err(_) => return Ok(()),
        };
        let going: Vec<&String> = held
            .iter()
            .filter(|name| opened(name).is_some_and(|at| at < keep))
            // The segment being written is never a candidate, however the
            // clock is set.
            .filter(|name| Some(*name) != self.open.as_ref().map(|it| &it.0))
            .collect();
        if going.is_empty() {
            return Ok(());
        }
        for (who, at) in cursors(&self.dir)? {
            if going.iter().any(|name| **name == at.segment) {
                warn!(
                    consumer = %who,
                    segment = %at.segment,
                    "a consumer's cursor names a segment being pruned; it \
                     will have lost what was in it",
                );
            }
        }
        for name in going {
            fs::remove_file(self.dir.join(format!("{name}{SUFFIX}")))?;
            info!(segment = %format!("{name}{SUFFIX}"), "pruned");
        }
        Ok(())
    }
}

impl Spool {
    /// Read a recorded feed back.
    ///
    /// The same messages the live feed carried, in the same order, read
    /// by the same [`Feed::reading`] — so a consumer holding
    /// `Box<dyn Feed>` cannot tell this from a subscription except by
    /// asking [`Feed::resume`], which this answers and a socket does
    /// not.
    ///
    /// [`Network::open`](crate::Network::open) is the other one of
    /// these, and there are only these two.
    pub fn open(
        dir: impl Into<PathBuf>,
        start: Start,
        replay: Replay,
    ) -> io::Result<Spool> {
        let dir = dir.into();
        let held = segments(&dir)?;
        let since = match start {
            Start::Since(moment) => Some(moment),
            _ => None,
        };
        let (name, at) = match start {
            Start::Cursor(name) => {
                // No cursor is a fresh follower, and a fresh follower
                // follows: replaying two days of spool at somebody who
                // only asked to be current is not a kindness.
                let at = match read_cursor(&dir, &name)? {
                    Some(at) => Some(at),
                    None => Some(end(&dir, &held)?),
                };
                (Some(name), at)
            }
            Start::Earliest => (None, None),
            Start::Latest => (None, Some(end(&dir, &held)?)),
            Start::Since(moment) => {
                let from = held
                    .iter()
                    .rev()
                    .find(|name| opened(name).is_some_and(|at| at <= moment))
                    .or(held.first());
                (
                    None,
                    from.map(|name| Position {
                        segment: name.clone(),
                        offset: 0,
                    }),
                )
            }
        };
        // A cursor naming a segment that has been pruned is the one case
        // a reader must not paper over: it is a hole, and reading on
        // from the oldest segment held would hide it.
        if let Some(at) = &at {
            if !held.contains(&at.segment) && !held.is_empty() {
                let oldest = held.first().expect("a segment");
                if at.segment.as_str() < oldest.as_str() {
                    return Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        format!(
                            "the spool no longer holds {}{SUFFIX}, which \
                             is where this reader stopped; the oldest it \
                             holds is {oldest}{SUFFIX}",
                            at.segment,
                        ),
                    ));
                }
            }
        }
        Ok(Spool {
            dir,
            name,
            at,
            file: None,
            replay,
            since,
            galaxy: Galaxy::LIVE,
        })
    }
}

/// A recorded feed: the other [`Feed`]
///
/// **The same reading a subscription takes.** This answers frames where
/// a [`Network`](crate::Network) answers frames — segments to walk, a
/// position to keep, a torn tail to leave alone, an end either waited at
/// or answered — and everything past a frame is [`Feed::reading`],
/// which both spell their `next` as. A spool and a socket cannot come to
/// different answers about the same bytes because there is one place
/// that reads them.
#[derive(Debug)]
pub struct Spool {
    dir: PathBuf,
    /// This consumer's name, where it was opened against a cursor. Only a
    /// named reader may write one.
    name: Option<String>,
    /// Where the next record starts. [`None`] is "at the oldest segment
    /// there is", which is not known until one exists.
    at: Option<Position>,
    /// The segment being read, open at [`Spool::at`]'s offset.
    file: Option<File>,
    replay: Replay,
    /// Records received before this are skipped. Only [`Start::Since`]
    /// sets it, and only until the first record it keeps.
    since: Option<DateTime<Utc>>,
    /// Which galaxy's data to hand over; the live one unless asked
    /// otherwise.
    galaxy: Galaxy,
}

impl Spool {
    /// Commit this reader's position under `name`
    ///
    /// For a consumer that opened somewhere other than its cursor — a
    /// replay of a recorded hour, or a first run told to start
    /// [`Start::Earliest`] — and still wants to keep its place from here
    /// on. [`Start::Cursor`] names it already.
    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Choose which galaxy's data to hand over
    ///
    /// [`Galaxy::LIVE`] by default, and applied on the way *out*: the
    /// recorder writes test frames down like any other, so a spool cannot
    /// leak a test galaxy into a sink by having been recorded with one
    /// filter and read with another.
    pub fn galaxy(mut self, galaxy: Galaxy) -> Self {
        self.galaxy = galaxy;
        self
    }

    /// Write this consumer's cursor, for a reader that was named one.
    ///
    /// **After the work it covers is durable, never before.** The cursor
    /// is what a restart trusts, so one written ahead of a publish turns a
    /// kill into a hole where the contract says it should be a replay.
    pub fn commit(&self) -> io::Result<()> {
        let (Some(name), Some(at)) = (&self.name, &self.at) else {
            return Ok(());
        };
        write_cursor(&self.dir, name, at)
    }

    /// The next whole record at the current position, or nothing where
    /// there is not one yet.
    fn record(&mut self) -> io::Result<Option<Frame>> {
        let Some(at) = self.at.clone() else { return Ok(None) };
        if self.file.is_none() {
            let path = self.dir.join(format!("{}{SUFFIX}", at.segment));
            let mut file = match File::open(&path) {
                Ok(file) => file,
                Err(err) if err.kind() == io::ErrorKind::NotFound => {
                    return Ok(None);
                }
                Err(err) => return Err(err),
            };
            file.seek(SeekFrom::Start(at.offset))?;
            self.file = Some(file);
        }
        let file = self.file.as_mut().expect("a segment is open");

        let mut head = [0u8; HEAD];
        // A torn tail: the head is short, so there is no record here yet.
        // The offset does not move and the next look starts where this
        // one did.
        if !whole(file.read_exact(&mut head))? {
            return Ok(None);
        }
        let len = Length::from_le_bytes(
            head[..RECEIVED].try_into().expect("a length's bytes"),
        );
        let nanos = Received::from_le_bytes(
            head[RECEIVED..].try_into().expect("a receipt time's bytes"),
        );
        if len > LARGEST {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{}{SUFFIX} at {}: a record claiming {len} bytes",
                    at.segment, at.offset,
                ),
            ));
        }
        let mut bytes = vec![0u8; len as usize];
        if !whole(file.read_exact(&mut bytes))? {
            // The head landed and the frame did not. Back to where the
            // record starts, so the reader takes it whole or not at all.
            file.seek(SeekFrom::Start(at.offset))?;
            return Ok(None);
        }
        let received_at = DateTime::from_timestamp_nanos(nanos);
        self.at = Some(Position {
            segment: at.segment,
            offset: at.offset + HEAD as u64 + len as u64,
        });
        Ok(Some(Frame { received_at, bytes }))
    }

    /// Move to the segment after the one being read, where there is one.
    ///
    /// Answers whether it moved. A torn tail in a segment that is no
    /// longer being written is a partial record nothing will ever finish,
    /// and stepping past it is the only way on.
    fn next_segment(&mut self) -> io::Result<bool> {
        let held = segments(&self.dir)?;
        let next = match &self.at {
            Some(at) => held.iter().find(|name| *name > &at.segment),
            None => held.first(),
        };
        let Some(next) = next else { return Ok(false) };
        self.at = Some(Position { segment: next.clone(), offset: 0 });
        self.file = None;
        Ok(true)
    }
}

impl Iterator for Spool {
    type Item = Result<Reading, Error>;

    /// [`Feed::reading`], which is the socket's `next` as well.
    fn next(&mut self) -> Option<Self::Item> {
        self.reading()
    }
}

impl Feed for Spool {
    fn showing(&self) -> Galaxy {
        self.galaxy
    }

    fn resume(&self) -> Option<Position> {
        self.at.clone()
    }

    /// The next record, walking on to the next segment at the end of one
    /// and waiting or stopping at the end of them all.
    fn frame(&mut self) -> Option<Result<Frame, Error>> {
        loop {
            let read = match self.record() {
                Ok(read) => read,
                Err(err) => {
                    return Some(Err(Error::Spool {
                        path: self.dir.clone(),
                        source: err,
                    }));
                }
            };
            if let Some(frame) = read {
                // Skipped before anything is read *of* the frame:
                // `Since` is about when a message was received, which the
                // record says and the envelope does not.
                if self.since.is_some_and(|it| frame.received_at < it) {
                    continue;
                }
                self.since = None;
                return Some(Ok(frame));
            }

            // The end of what this segment holds. A later one existing
            // means this segment is finished — including its torn tail,
            // which nothing is going to complete.
            match self.next_segment() {
                Ok(true) => continue,
                Err(err) => {
                    return Some(Err(Error::Spool {
                        path: self.dir.clone(),
                        source: err,
                    }));
                }
                Ok(false) => match self.replay {
                    Replay::ToEnd => return None,
                    Replay::Follow => {
                        self.file = None;
                        std::thread::sleep(POLL);
                    }
                },
            }
        }
    }
}

/// Whether a read filled its buffer, an end part way through being the one
/// failure a spool treats as "not yet" rather than as an error.
fn whole(read: io::Result<()>) -> io::Result<bool> {
    match read {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => Ok(false),
        Err(err) => Err(err),
    }
}

/// Every segment a directory holds, oldest first.
///
/// The names sort as the hours do, which is the whole reason they are
/// spelled `%Y%m%d%H`: the order of a `readdir` is nobody's to rely on and
/// a string compare is the order of the feed.
fn segments(dir: &Path) -> io::Result<Vec<String>> {
    let mut held = Vec::new();
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(held),
        Err(err) => return Err(err),
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str().and_then(|it| it.strip_suffix(SUFFIX))
        else {
            continue;
        };
        if opened(name).is_some() {
            held.push(name.to_owned());
        }
    }
    held.sort_unstable();
    Ok(held)
}

/// The hour a segment was opened in, or nothing where the name is not one
/// of ours.
fn opened(name: &str) -> Option<DateTime<Utc>> {
    NaiveDateTime::parse_from_str(&format!("{name}0000"), "%Y%m%d%H%M%S")
        .ok()
        .map(|at| at.and_utc())
}

/// The end of everything recorded, which is where a fresh follower starts.
fn end(dir: &Path, held: &[String]) -> io::Result<Position> {
    let Some(last) = held.last() else {
        return Ok(Position { segment: String::new(), offset: 0 });
    };
    let path = dir.join(format!("{last}{SUFFIX}"));
    let offset = fs::metadata(&path).map(|it| it.len()).unwrap_or(0);
    Ok(Position { segment: last.clone(), offset })
}

/// Every consumer's position, by name.
fn cursors(dir: &Path) -> io::Result<Vec<(String, Position)>> {
    let mut held = Vec::new();
    let entries = match fs::read_dir(dir.join(CURSORS)) {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(held),
        Err(err) => return Err(err),
    };
    for entry in entries.flatten() {
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if let Some(at) = read_cursor(dir, &name)? {
            held.push((name, at));
        }
    }
    held.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(held)
}

/// One consumer's position, or nothing where it has never committed one.
fn read_cursor(dir: &Path, name: &str) -> io::Result<Option<Position>> {
    let path = dir.join(CURSORS).join(name);
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    };
    let read = text.split_whitespace().collect::<Vec<_>>();
    match read.as_slice() {
        [segment, offset] => {
            let segment =
                segment.strip_suffix(SUFFIX).unwrap_or(segment).to_owned();
            let offset = offset.parse().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("{}: {text:?} is not a cursor", path.display()),
                )
            })?;
            Ok(Some(Position { segment, offset }))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{}: {text:?} is not a cursor", path.display()),
        )),
    }
}

/// Write one consumer's position: a temporary file and a rename, so a
/// cursor is the one a consumer wrote or the one before it and never half
/// of each.
fn write_cursor(dir: &Path, name: &str, at: &Position) -> io::Result<()> {
    let dir = dir.join(CURSORS);
    fs::create_dir_all(&dir)?;
    let tmp = dir.join(format!(".{name}.tmp"));
    fs::write(&tmp, format!("{}{SUFFIX} {}\n", at.segment, at.offset))?;
    fs::rename(&tmp, dir.join(name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{frame, JUMP};

    const JOURNAL: &str = "https://eddn.edcd.io/schemas/journal/1";
    const JOURNAL_TEST: &str = "https://eddn.edcd.io/schemas/journal/1/test";

    /// Somewhere to spool, taken away with the value.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new(name: &str) -> Scratch {
            let at = std::env::temp_dir()
                .join(format!("eddn-spool-{}-{name}", std::process::id()));
            let _ = fs::remove_dir_all(&at);
            Scratch(at)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn at(hour: u32, second: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 9, 19, hour, 0, second).unwrap()
    }

    fn recorded(schema: &str, received_at: DateTime<Utc>) -> Frame {
        Frame { received_at, bytes: frame(schema, JUMP) }
    }

    fn read(dir: &Path, start: Start) -> Vec<Reading> {
        Spool::open(dir, start, Replay::ToEnd)
            .expect("a spool")
            .map(|it| it.expect("a reading"))
            .collect()
    }

    /// What was recorded is what is read, in order and with its own clock
    ///
    /// The whole contract in one: the same messages the socket carried, in
    /// the order it carried them, stamped with when the recorder held them
    /// rather than when they were read back — so a replay and the run it
    /// was taken from agree about when everything happened.
    #[test]
    fn a_recorded_hour_reads_back_as_itself() {
        let dir = Scratch::new("roundtrip");
        let mut recorder = Recorder::open(&dir.0).expect("a recorder");
        let when: Vec<DateTime<Utc>> = (0..5).map(|n| at(17, n * 7)).collect();
        for &moment in &when {
            recorder.write(&recorded(JOURNAL, moment)).expect("a write");
        }

        let read = read(&dir.0, Start::Earliest);
        assert_eq!(read.len(), 5);
        assert_eq!(
            read.iter().map(|it| it.received_at).collect::<Vec<_>>(),
            when,
        );
        assert!(read.iter().all(|it| it.envelope.live));
    }

    /// A record is twelve bytes of head and then the frame
    ///
    /// **The format is a promise to files that outlive this build.** The
    /// head's width is derived from the two fields rather than spelled
    /// `4 + 8`, which is the right way round — and is also a way to
    /// widen the format by changing a type and noticing nothing. This is
    /// what notices: the bytes on the disk, counted, and the length and
    /// the receipt time read back out of them by hand.
    #[test]
    fn a_record_is_its_head_and_then_the_frame() {
        assert_eq!(RECEIVED, 4);
        assert_eq!(HEAD, 12);

        let dir = Scratch::new("layout");
        let mut recorder = Recorder::open(&dir.0).expect("a recorder");
        let frame = recorded(JOURNAL, at(17, 3));
        recorder.write(&frame).expect("a write");

        let bytes = fs::read(dir.0.join("2026091917.eddn")).expect("a segment");
        assert_eq!(bytes.len(), HEAD + frame.bytes.len());
        assert_eq!(
            u32::from_le_bytes(bytes[0..4].try_into().expect("four")) as usize,
            frame.bytes.len(),
        );
        assert_eq!(
            i64::from_le_bytes(bytes[4..12].try_into().expect("eight")),
            frame.received_at.timestamp_nanos_opt().expect("a moment"),
        );
        assert_eq!(&bytes[HEAD..], &frame.bytes[..]);
    }

    /// An hour is a segment, and a reader walks them in order
    #[test]
    fn the_hour_rolls_and_the_reader_follows_it() {
        let dir = Scratch::new("hours");
        let mut recorder = Recorder::open(&dir.0).expect("a recorder");
        for hour in [17u32, 17, 18, 19, 19] {
            recorder.write(&recorded(JOURNAL, at(hour, 0))).expect("a write");
        }

        assert_eq!(
            segments(&dir.0).expect("the segments"),
            vec!["2026091917", "2026091918", "2026091919"],
        );
        assert_eq!(read(&dir.0, Start::Earliest).len(), 5);

        // And `Since` opens the hour it names rather than the galaxy's.
        let since = read(&dir.0, Start::Since(at(19, 0)));
        assert_eq!(since.len(), 2, "a since read the wrong hours");
        assert!(since.iter().all(|it| it.received_at == at(19, 0)));

        // And `Latest` is the end of what is held: a fresh follower takes
        // what arrives after it, not the two days before it.
        assert!(read(&dir.0, Start::Latest).is_empty());
    }

    /// A following reader waits at the end and takes what is appended
    ///
    /// **What a live run reads by**, and the two halves of it that a
    /// `ToEnd` read cannot show: the feed does not end where the records
    /// do, and a record is on the disk for a reader as soon as the
    /// recorder has taken it — which is why [`Recorder::write`] flushes
    /// rather than leaving a caller to remember to.
    #[test]
    fn a_following_reader_takes_what_is_appended() {
        let dir = Scratch::new("follow");
        let mut recorder = Recorder::open(&dir.0).expect("a recorder");
        recorder.write(&recorded(JOURNAL, at(17, 0))).expect("a write");

        let (tx, rx) = std::sync::mpsc::channel();
        let path = dir.0.clone();
        let reader = std::thread::spawn(move || {
            let feed = Spool::open(&path, Start::Earliest, Replay::Follow)
                .expect("a spool");
            for reading in feed {
                let at = reading.expect("a reading").received_at;
                if tx.send(at).is_err() {
                    return;
                }
            }
        });

        let waited = Duration::from_secs(5);
        assert_eq!(
            rx.recv_timeout(waited).expect("the record already written"),
            at(17, 0),
        );
        // And then it waits, rather than answering the end of the file as
        // the end of the feed.
        assert!(
            rx.recv_timeout(POLL * 3).is_err(),
            "the feed ended where the records did",
        );

        recorder.write(&recorded(JOURNAL, at(17, 1))).expect("a write");
        assert_eq!(
            rx.recv_timeout(waited).expect("the appended record"),
            at(17, 1),
        );

        // Close the channel and give the reader one more record, so it
        // notices and leaves rather than being left blocked on a spool
        // whose directory is about to go.
        drop(rx);
        recorder.write(&recorded(JOURNAL, at(17, 2))).expect("a write");
        reader.join().expect("the reader");
    }

    /// A torn tail is not an error, and is not read twice either
    ///
    /// **The rule the format rests on.** The recorder does not `fsync`, so
    /// a kill mid-write leaves a record that stops in the middle. Every
    /// whole record before it must still read, the partial one must not be
    /// handed over as though it were whole, and a reader that comes back
    /// must take up where the records end rather than inside one.
    #[test]
    fn a_torn_tail_loses_only_the_partial_record() {
        let dir = Scratch::new("torn");
        let mut recorder = Recorder::open(&dir.0).expect("a recorder");
        for n in 0..3 {
            recorder.write(&recorded(JOURNAL, at(17, n))).expect("a write");
        }
        drop(recorder);

        // The kill: the last record loses its final bytes.
        let path = dir.0.join("2026091917.eddn");
        let whole = fs::metadata(&path).expect("a segment").len();
        let file = OpenOptions::new().write(true).open(&path).expect("open");
        file.set_len(whole - 10).expect("a torn tail");

        let read = read(&dir.0, Start::Earliest);
        assert_eq!(read.len(), 2, "the torn record was handed over");

        // And a follower stops at the record boundary, not inside it: what
        // the next write appends is read, and nothing is read twice.
        let mut spool = Spool::open(&dir.0, Start::Earliest, Replay::ToEnd)
            .expect("a spool");
        let taken: Vec<Reading> =
            spool.by_ref().map(|it| it.unwrap()).collect();
        assert_eq!(taken.len(), 2);
        let at_end = spool.resume().expect("a position");
        assert!(
            at_end.offset < whole - 10,
            "the position ran into the torn record",
        );
    }

    /// Two consumers, two cursors, and a kill replays rather than skips
    ///
    /// The delivery contract: at least once, in wire order. A consumer
    /// commits after the work is durable, so what it had not committed
    /// comes round again — and one consumer's position is no business of
    /// the other's.
    #[test]
    fn a_cursor_replays_the_tail_and_is_a_consumer_of_its_own() {
        let dir = Scratch::new("cursors");
        let mut recorder = Recorder::open(&dir.0).expect("a recorder");
        for n in 0..4 {
            recorder.write(&recorded(JOURNAL, at(17, n))).expect("a write");
        }

        // One consumer takes two and commits; the kill is the drop.
        let mut db = Spool::open(
            &dir.0,
            Start::Cursor("galos-db".into()),
            Replay::ToEnd,
        )
        .expect("a spool");
        // A consumer with no cursor follows rather than replaying.
        assert_eq!(db.by_ref().count(), 0, "a fresh cursor replayed");

        let mut db = Spool::open(&dir.0, Start::Earliest, Replay::ToEnd)
            .expect("a spool")
            .named("galos-db");
        assert!(db.next().is_some());
        assert!(db.next().is_some());
        db.commit().expect("a cursor");
        drop(db);

        let back = read(&dir.0, Start::Cursor("galos-db".into()));
        assert_eq!(back.len(), 2, "the committed prefix came round again");

        // The other consumer has its own position and sees all four.
        let other = read(&dir.0, Start::Cursor("galos-index".into()));
        assert_eq!(other.len(), 0, "a fresh consumer replayed the spool");
        let held = cursors(&dir.0).expect("the cursors");
        assert_eq!(held.len(), 1);
        assert_eq!(held[0].0, "galos-db");
    }

    /// Test-galaxy frames are recorded and filtered on the way out
    ///
    /// The recorder writes what arrives, so nothing can be lost by its
    /// judgement; the galaxy a consumer asked for is applied where the
    /// live feed applies it, which is on the read.
    #[test]
    fn a_test_frame_is_recorded_and_let_go_on_the_way_out() {
        let dir = Scratch::new("galaxy");
        let mut recorder = Recorder::open(&dir.0).expect("a recorder");
        recorder.write(&recorded(JOURNAL, at(17, 0))).expect("a write");
        recorder.write(&recorded(JOURNAL_TEST, at(17, 1))).expect("a write");

        assert_eq!(read(&dir.0, Start::Earliest).len(), 1);
        let all: Vec<Reading> =
            Spool::open(&dir.0, Start::Earliest, Replay::ToEnd)
                .expect("a spool")
                .galaxy(Galaxy::ALL)
                .map(|it| it.expect("a reading"))
                .collect();
        assert_eq!(all.len(), 2, "the test frame was not recorded");
        assert!(!all[1].envelope.live);
    }

    /// Retention takes whole segments, and says who is losing one
    #[test]
    fn retention_prunes_whole_segments_and_names_the_cursors() {
        let dir = Scratch::new("retention");
        let mut recorder = Recorder::open(&dir.0)
            .expect("a recorder")
            .retaining(Duration::from_secs(3600));
        // Two hours the window has left behind, and then now — the roll
        // onto which is what prunes.
        recorder.write(&recorded(JOURNAL, at(17, 0))).expect("a write");
        recorder.write(&recorded(JOURNAL, at(18, 0))).expect("a write");
        write_cursor(
            &dir.0,
            "galos-db",
            &Position { segment: "2026091917".into(), offset: 0 },
        )
        .expect("a cursor");
        recorder.write(&recorded(JOURNAL, Utc::now())).expect("a write");

        let held = segments(&dir.0).expect("the segments");
        assert!(
            !held.contains(&"2026091917".to_owned())
                && !held.contains(&"2026091918".to_owned()),
            "an aged-out segment was kept: {held:?}",
        );
        assert_eq!(held.len(), 1, "the segment being written was pruned");

        // And the consumer whose cursor is now nowhere is told, rather
        // than reading on from the oldest segment as though nothing went.
        let refused = Spool::open(
            &dir.0,
            Start::Cursor("galos-db".into()),
            Replay::ToEnd,
        )
        .expect_err("a pruned cursor should be refused");
        assert_eq!(refused.kind(), io::ErrorKind::NotFound);
    }
}
