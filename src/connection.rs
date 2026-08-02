use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// How long a receive waits before coming back empty
///
/// Only so that a subscriber with nothing to read still gets to look at the
/// clock, and can tell how long the quiet has gone on. That is the whole of
/// how [a gateway that has stopped
/// publishing](crate::subscribe#a-gateway-that-stops-publishing) is counted
/// out.
pub const POLL_INTERVAL_MS: i32 = 1_000;

/// The shortest libzmq waits before trying to rebuild [a connection that has
/// closed](crate::subscribe#a-connection-that-closes)
///
/// The wait starts here, doubles on each attempt that fails, and stops
/// growing at [`RECONNECT_MAX_MS`]. It also starts over from here whenever a
/// connection is made, which is why the floor carries more weight than the
/// ceiling: a port that accepts and then drops, a load balancer in front of a
/// dead gateway, hands out a connection every time, so the wait is reset
/// before it can ever grow and this alone sets the rate. At libzmq's 100ms
/// default that is ten attempts a second for as long as the gateway is
/// broken. At a second it is one.
pub const RECONNECT_MIN_MS: i32 = 1_000;

/// The longest libzmq waits before trying to rebuild [a connection that has
/// closed](crate::subscribe#a-connection-that-closes)
///
/// Where the doubling that starts at [`RECONNECT_MIN_MS`] stops, and so the
/// longest a subscriber sits there after a gateway that went away has come
/// back.
pub const RECONNECT_MAX_MS: i32 = 15_000;

/// How often to ping the gateway
///
/// A ping is a write, and a write is what turns [a connection that has died
/// without closing](crate::subscribe#a-connection-that-dies-without-closing)
/// into something libzmq can see. With [`HEARTBEAT_TIMEOUT_MS`] this is how
/// long that goes unnoticed, so 15 seconds.
///
/// It can be this short because the answers do come. EDDN speaks ZMTP 3.1 and
/// returns a PONG for every PING, so a connection sitting quiet is still held
/// open by the answers to its pings, and only a broken one runs out of time.
pub const HEARTBEAT_IVL_MS: i32 = 5_000;

/// How long to wait for anything back after a ping
///
/// Nothing at all inside this and libzmq gives the connection up and builds
/// another. Counted from a ping sent every [`HEARTBEAT_IVL_MS`], so the two
/// together bound how long [a connection that has died without
/// closing](crate::subscribe#a-connection-that-dies-without-closing) is
/// mistaken for a quiet one.
pub const HEARTBEAT_TIMEOUT_MS: i32 = 10_000;

/// What to ask libzmq's socket monitor to report
///
/// [Connections that close](crate::subscribe#a-connection-that-closes) are lost and
/// rebuilt inside libzmq, which would otherwise happen with nothing said
/// about it.
const MONITORED: i32 = zmq::SocketEvent::CONNECTED as i32
    | zmq::SocketEvent::DISCONNECTED as i32
    | zmq::SocketEvent::CONNECT_RETRIED as i32
    | zmq::SocketEvent::HANDSHAKE_FAILED_NO_DETAIL as i32
    | zmq::SocketEvent::HANDSHAKE_FAILED_PROTOCOL as i32
    | zmq::SocketEvent::HANDSHAKE_FAILED_AUTH as i32;

/// Names the endpoint each socket's monitor reports on.
static MONITORS: AtomicUsize = AtomicUsize::new(0);

/// A socket subscribed to EDDN, and the monitor libzmq reports on it through
///
/// A monitor watches one socket, so the two are opened together and replaced
/// together.
pub(crate) struct Connection {
    pub(crate) socket: zmq::Socket,
    monitor: zmq::Socket,
}

impl Connection {
    /// Open a socket subscribed to everything on `url`
    ///
    /// Connecting is asynchronous, so this returns whether or not anything is
    /// listening, and libzmq goes on trying in the background.
    pub(crate) fn open(
        ctx: &zmq::Context,
        url: &str,
    ) -> Result<Self, zmq::Error> {
        let socket = ctx.socket(zmq::SUB)?;

        socket.set_reconnect_ivl(RECONNECT_MIN_MS)?;
        socket.set_reconnect_ivl_max(RECONNECT_MAX_MS)?;

        socket.set_heartbeat_ivl(HEARTBEAT_IVL_MS)?;
        socket.set_heartbeat_timeout(HEARTBEAT_TIMEOUT_MS)?;

        // Receives return on their own so that silence can be timed.
        socket.set_rcvtimeo(POLL_INTERVAL_MS)?;

        // Watch before connecting, so the first connection is reported like
        // any other. Each socket monitors on an endpoint of its own, since a
        // replaced one may still be closing as its successor opens.
        let endpoint = format!(
            "inproc://eddn-monitor-{}",
            MONITORS.fetch_add(1, Ordering::Relaxed)
        );
        socket.monitor(&endpoint, MONITORED)?;
        let monitor = ctx.socket(zmq::PAIR)?;
        monitor.connect(&endpoint)?;

        socket.connect(url)?;
        socket.set_subscribe(&[])?; // Required to subscribe to everything

        Ok(Connection { socket, monitor })
    }

    /// What libzmq has done to this connection since it was last asked
    ///
    /// Each event arrives as its number and the endpoint it happened on. The
    /// endpoint is the one we connected to and is dropped.
    pub(crate) fn events(&self) -> Vec<zmq::SocketEvent> {
        let mut events = Vec::new();
        while let Ok(frames) = self.monitor.recv_multipart(zmq::DONTWAIT) {
            match frames.first() {
                Some(frame) if frame.len() >= 2 => {
                    let id = u16::from_le_bytes([frame[0], frame[1]]);
                    events.push(zmq::SocketEvent::from_raw(id));
                }
                _ => {}
            }
        }
        events
    }
}

/// How long the connection has carried nothing, and how long is too long
pub(crate) struct Stall {
    timeout: Duration,
    /// When the quiet period being measured began.
    quiet_since: Instant,
}

impl Stall {
    pub(crate) fn new(timeout: Duration) -> Self {
        Stall { timeout, quiet_since: Instant::now() }
    }

    /// The quiet period starts again from now
    ///
    /// A message ends one. So does replacing the connection, which has not
    /// yet had the chance to carry a message.
    pub(crate) fn restart(&mut self) {
        self.quiet_since = Instant::now();
    }

    /// How long the quiet period has run, once it has run too long
    pub(crate) fn overrun(&self) -> Option<Duration> {
        let quiet = self.quiet_since.elapsed();
        (quiet >= self.timeout).then_some(quiet)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A socket to read the options off. Connecting is asynchronous, so
    /// nothing needs to be listening on the other end of this.
    fn opened() -> Connection {
        let ctx = zmq::Context::new();
        Connection::open(&ctx, "tcp://127.0.0.1:9599").unwrap()
    }

    /// Covers a gateway that stops publishing, which nothing else notices.
    #[test]
    fn quiet_for_longer_than_the_stall_timeout_is_an_overrun() {
        let mut stall = Stall::new(Duration::from_secs(30));
        assert!(stall.overrun().is_none());

        // One second short of it is still quiet worth waiting out.
        stall.quiet_since = Instant::now() - Duration::from_secs(29);
        assert!(stall.overrun().is_none());

        stall.quiet_since = Instant::now() - Duration::from_secs(31);
        assert_eq!(stall.overrun().map(|d| d.as_secs()), Some(31));

        // A message, or a connection replaced, and the count starts over.
        stall.restart();
        assert!(stall.overrun().is_none());
    }

    /// Covers a connection that dies without closing. Only a written ping
    /// going unanswered turns that into something libzmq can see.
    #[test]
    fn a_connection_pings_and_gives_up_on_an_unanswered_one() {
        let connection = opened();
        let ivl = connection.socket.get_heartbeat_ivl().unwrap();
        let timeout = connection.socket.get_heartbeat_timeout().unwrap();

        // Zero is libzmq's default and means no pings at all, which is what
        // left a dead connection looking like a quiet one.
        assert_ne!(ivl, 0);

        // Long enough to send a ping, then long enough to give up waiting on
        // an answer: the 15 seconds these two are documented to come to.
        assert_eq!(ivl + timeout, 15_000);
    }

    /// Covers a connection that closes, which libzmq rebuilds on its own,
    /// and how hard it tries while the gateway is away.
    #[test]
    fn a_connection_retries_from_the_floor_and_no_slower_than_the_ceiling() {
        let connection = opened();
        let floor = connection.socket.get_reconnect_ivl().unwrap();
        let ceiling = connection.socket.get_reconnect_ivl_max().unwrap();

        // libzmq starts the doubling over on every connection made, so a port
        // that accepts and drops never gets past the floor and the floor is
        // the whole of the rate. Its 100ms default is ten attempts a second.
        assert!(floor >= 1_000, "the floor is the rate, and it is {}", floor);
        assert!(ceiling >= floor);

        // Without this a receive never comes back on its own, and the quiet
        // period above could never be measured.
        assert_eq!(connection.socket.get_rcvtimeo(), Ok(POLL_INTERVAL_MS));
    }
}
