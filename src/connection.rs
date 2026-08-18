use omq_tokio::{
    Context, Endpoint, Error, MonitorEvent, MonitorStream, MonitorTryRecvError,
    Options, ReconnectPolicy, SocketType,
};
use std::time::{Duration, Instant};

/// How long a receive waits before coming back empty
///
/// Only so that a subscriber with nothing to read still gets to look at the
/// clock, and can tell how long the quiet has gone on. That is the whole of
/// how [a gateway that has stopped
/// publishing](crate::subscribe#a-gateway-that-stops-publishing) is counted
/// out.
pub const POLL_INTERVAL: Duration = Duration::from_secs(1);

/// The shortest wait before [a connection that has
/// closed](crate::subscribe#a-connection-that-closes) is rebuilt
///
/// The wait starts here, doubles on each attempt that fails, and stops
/// growing at [`RECONNECT_MAX`]. It also starts over from here whenever a
/// connection is made, which is why the floor carries more weight than the
/// ceiling: a port that accepts and then drops, a load balancer in front of a
/// dead gateway, hands out a connection every time, so the wait is reset
/// before it can ever grow and this alone sets the rate. At the 100ms default
/// that is ten attempts a second for as long as the gateway is broken. At a
/// second it is one.
pub const RECONNECT_MIN: Duration = Duration::from_secs(1);

/// The longest wait before [a connection that has
/// closed](crate::subscribe#a-connection-that-closes) is rebuilt
///
/// Where the doubling that starts at [`RECONNECT_MIN`] stops, and so the
/// longest a subscriber sits there after a gateway that went away has come
/// back.
pub const RECONNECT_MAX: Duration = Duration::from_secs(15);

/// How often to ping the gateway
///
/// A ping is a write, and a write is what turns [a connection that has died
/// without closing](crate::subscribe#a-connection-that-dies-without-closing)
/// into something the socket can see. With [`HEARTBEAT_TIMEOUT`] this is how
/// long that goes unnoticed, so 15 seconds.
///
/// It can be this short because the answers do come. EDDN speaks ZMTP 3.1 and
/// returns a PONG for every PING, so a connection sitting quiet is still held
/// open by the answers to its pings, and only a broken one runs out of time.
pub const HEARTBEAT_IVL: Duration = Duration::from_secs(5);

/// How long to wait for anything back after a ping
///
/// Nothing arriving inside this and the connection is given up and another
/// built. Counted from a ping sent every [`HEARTBEAT_IVL`], so the two
/// together bound how long [a connection that has died without
/// closing](crate::subscribe#a-connection-that-dies-without-closing) is
/// mistaken for a quiet one.
///
/// It is any traffic that holds a connection open here, not a PONG in
/// particular, so a gateway publishing steadily satisfies it without ever
/// answering a ping.
pub const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(10);

/// What a socket subscribed to EDDN is opened with
///
/// Everything the connection's health depends on is here rather than left to
/// a default, since the defaults are for a peer on a LAN that answers.
pub(crate) fn options() -> Options {
    Options::new()
        .reconnect(ReconnectPolicy::Exponential {
            min: RECONNECT_MIN,
            max: RECONNECT_MAX,
        })
        .heartbeat_interval(HEARTBEAT_IVL)
        .heartbeat_timeout(HEARTBEAT_TIMEOUT)
}

/// A socket subscribed to EDDN, and the monitor it reports on
///
/// A monitor watches one socket, so the two are opened together and replaced
/// together.
pub(crate) struct Connection {
    pub(crate) socket: omq_tokio::blocking::Socket,
    monitor: MonitorStream,
}

impl Connection {
    /// Open a socket subscribed to everything on `url`
    ///
    /// Connecting is asynchronous, so this returns whether or not anything is
    /// listening, and the socket goes on trying in the background.
    pub(crate) fn open(ctx: &Context, url: &str) -> Result<Self, Error> {
        let endpoint: Endpoint = url.parse()?;
        let socket = ctx.blocking_socket(SocketType::Sub, options());

        // Watch before connecting, so the first connection is reported like
        // any other.
        let monitor = socket.monitor();

        socket.connect(endpoint)?;
        socket.subscribe(&b""[..])?; // Required to subscribe to everything

        Ok(Connection { socket, monitor })
    }

    /// What has happened to this connection since it was last asked
    ///
    /// The monitor holds a fixed number of events and drops the oldest to
    /// make room, so a subscriber that is behind can miss some. They are
    /// diagnostic, and what is made of them is a count and a throttle, so
    /// missing one costs a line rather than the truth.
    pub(crate) fn events(&mut self) -> Vec<MonitorEvent> {
        let mut events = Vec::new();
        loop {
            match self.monitor.try_recv() {
                Ok(event) => events.push(event),
                // Behind by more than the monitor holds. What was dropped
                // cannot be reported on, and the ones still there can.
                Err(MonitorTryRecvError::Lagged(_)) => {}
                Err(_) => break,
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
    /// going unanswered turns that into something the socket can see.
    #[test]
    fn a_connection_pings_and_gives_up_on_an_unanswered_one() {
        let options = options();

        // Unset means no pings at all, which is what leaves a dead connection
        // looking like a quiet one.
        assert_eq!(options.heartbeat_interval, Some(HEARTBEAT_IVL));

        // Long enough to send a ping, then long enough to give up waiting on
        // an answer: the 15 seconds these two are documented to come to.
        let ivl = options.heartbeat_interval.unwrap();
        let timeout = options.heartbeat_timeout.unwrap();
        assert_eq!(ivl + timeout, Duration::from_secs(15));
    }

    /// Covers a connection that closes, which is rebuilt on its own, and how
    /// hard it tries while the gateway is away.
    #[test]
    fn a_connection_retries_from_the_floor_and_no_slower_than_the_ceiling() {
        // The doubling starts over on every connection made, so a port that
        // accepts and drops never gets past the floor and the floor is the
        // whole of the rate. The 100ms default is ten attempts a second.
        match options().reconnect {
            ReconnectPolicy::Exponential { min, max } => {
                assert!(
                    min >= Duration::from_secs(1),
                    "the floor is the rate, and it is {:?}",
                    min
                );
                assert!(max >= min);
            }
            policy => panic!(
                "a connection that closes wants rebuilding, and this is {:?}",
                policy
            ),
        }
    }
}
