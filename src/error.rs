use miniz_oxide::inflate::TINFLStatus;
use std::fmt;

/// How much of the message to show either side of where reading stopped
///
/// A message runs to hundreds of fields and an outfitting one to thousands, so
/// the whole of it is no use in a log line. What is wanted is the field that
/// would not read, and that is at the offset the error carries.
const NEAR: usize = 60;

/// How much of a message to show where there is no offset to point at
///
/// Enough for the whole of a journal, scan or signal message, which is what
/// these failures are. An outfitting message runs to tens of thousands of
/// characters and is cut off, and [`Error::json`] is there for whoever wants
/// the rest of it.
const SHOWN: usize = 2000;

/// Anything that can come back in place of a message.
#[derive(Debug)]
pub enum Error {
    /// The socket itself failed.
    Socket(zmq::Error),
    /// A message arrived that is not the zlib EDDN sends.
    Decompress(TINFLStatus),
    /// A message arrived that decompressed but is not an
    /// [`Envelope`](crate::Envelope).
    Parse {
        /// Why it would not read, and where reading stopped
        source: serde_json::Error,
        /// The message itself, kept so the offset above can be looked up
        ///
        /// Without it the error names a type and a position in a message
        /// nobody has, which says that something in the feed is wrong and
        /// nothing about what. The game adds fields and changes their types,
        /// so this is the ordinary way to find the next one.
        json: String,
    },
}

impl Error {
    /// The message around where reading stopped
    ///
    /// EDDN sends a message as one line, so the error's column is an offset
    /// into it and the field that would not read sits at that offset.
    ///
    /// [`None`] for anything but a parse, and for a parse that stopped nowhere
    /// in particular. A payload is read out of a value rather than out of the
    /// text it arrived as, and a value carries no offsets, so it reports a line
    /// and column of zero. Pointing at the front of the envelope would read as
    /// an answer and is not one, so [`Error::json`] is what to go on there.
    pub fn near(&self) -> Option<&str> {
        let Error::Parse { source, json } = self else {
            return None;
        };
        if source.line() == 0 {
            return None;
        }

        // A line is 1 based and so is a column, and the offset wanted is the
        // start of the line plus the column.
        let line = source.line().saturating_sub(1);
        let sol: usize =
            json.split_inclusive('\n').take(line).map(str::len).sum();
        let at = sol
            .saturating_add(source.column().saturating_sub(1))
            .min(json.len());

        // Snapped outward to character boundaries, since a message carries
        // system and commander names in every alphabet the game allows.
        let from = (at.saturating_sub(NEAR)..=at)
            .find(|i| json.is_char_boundary(*i))
            .unwrap_or(at);
        let to = ((at + NEAR).min(json.len())..=json.len())
            .find(|i| json.is_char_boundary(*i))
            .unwrap_or(json.len());

        Some(&json[from..to])
    }

    /// The whole message that would not read
    ///
    /// [`None`] for anything but a parse. What it is for is the case
    /// [`Error::near`] cannot answer: a payload reports no offset, and the
    /// field that would not read is somewhere in here. `Entry` flattens its
    /// event, and a flattened field is read out of a buffer rather than out of
    /// the input, so neither an offset nor a field path survives to be
    /// reported. The message does.
    pub fn json(&self) -> Option<&str> {
        match self {
            Error::Parse { json, .. } => Some(json),
            _ => None,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Socket(err) => write!(f, "socket: {}", err),
            Error::Decompress(status) => {
                write!(f, "decompress: {:?}", status)
            }
            // Whichever of the two locates it: the window where there is an
            // offset to take one around, and the message itself where there is
            // not. Either way the log line is enough to find the field by,
            // which is the whole point of reporting one.
            Error::Parse { source, json } => {
                write!(f, "parse: {}", source)?;
                if let Some(near) = self.near() {
                    return write!(f, ", near: {}", near);
                }

                let to = (SHOWN..=json.len())
                    .find(|i| json.is_char_boundary(*i))
                    .unwrap_or(json.len());
                write!(f, ", message: {}", &json[..to])?;
                if to < json.len() {
                    write!(f, "... ({} characters in all)", json.len())?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Socket(err) => Some(err),
            Error::Decompress(_) => None,
            Error::Parse { source, .. } => Some(source),
        }
    }
}
