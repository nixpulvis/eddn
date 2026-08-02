use miniz_oxide::inflate::TINFLStatus;
use std::fmt;

/// Anything that can come back in place of a message.
#[derive(Debug)]
pub enum Error {
    /// The socket itself failed.
    Socket(zmq::Error),
    /// A message arrived that is not the zlib EDDN sends.
    Decompress(TINFLStatus),
    /// A message arrived that decompressed but is not an
    /// [`Envelope`](crate::Envelope).
    Parse(serde_json::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Socket(err) => write!(f, "socket: {}", err),
            Error::Decompress(status) => {
                write!(f, "decompress: {:?}", status)
            }
            Error::Parse(err) => write!(f, "parse: {}", err),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Socket(err) => Some(err),
            Error::Decompress(_) => None,
            Error::Parse(err) => Some(err),
        }
    }
}
