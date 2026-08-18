use crate::connection::{RECONNECT_MAX, RECONNECT_MIN};
use omq_tokio::MonitorEvent;
use std::time::{Duration, Instant};
use tracing::Level;

/// Something worth saying, and how loudly
///
/// Handed back rather than emitted so that deciding what is worth saying can
/// be tested apart from the saying of it.
pub(crate) struct Note {
    pub(crate) level: Level,
    pub(crate) message: String,
}

/// How close together reports may be made
///
/// See [`Reporter`] for what this holds back and why.
const WARN_INTERVAL: Duration = Duration::from_secs(30);

/// Decides what is worth saying about what the socket does to the connection
///
/// Losing a connection is worth hearing about. A gateway that accepts and
/// drops on repeat loses one every second, and saying so every second buries
/// everything else, so a loss inside `WARN_INTERVAL` of the last one reported
/// is counted instead, and the count goes out with the next one that is.
///
/// A loss and the recovery that ends it are reported together or not at all.
/// Being told a connection went away and never told it came back reads as an
/// outage that is still going.
///
/// This throttle is for events that arrive as fast as the socket can
/// produce them. Everything the subscriber does deliberately, being rare by
/// construction, is reported without asking.
#[derive(Default)]
pub(crate) struct Reporter {
    /// When the connection was lost, while it is still gone.
    lost_at: Option<Instant>,
    /// Whether that loss was one of the ones reported.
    loss_reported: bool,
    /// When a report was last made.
    said_at: Option<Instant>,
    /// How many have gone unwarned since.
    unwarned: u64,
}

impl Reporter {
    /// What is worth saying about what the socket has done on its own
    pub(crate) fn observe(&mut self, events: &[MonitorEvent]) -> Vec<Note> {
        let mut said = Vec::new();

        for event in events {
            match event {
                MonitorEvent::Disconnected { reason, .. }
                    if self.lost_at.is_none() =>
                {
                    self.lost_at = Some(Instant::now());
                    // Said once and then held back, and nothing more is said
                    // until the connection is back, however long that takes.
                    // So it carries the cadence of the retrying, which is the
                    // only sign of life there will be until then.
                    self.loss_reported = self.warn(
                        &format!(
                            "Connection lost ({:?}), retrying every {} to {} seconds",
                            reason,
                            RECONNECT_MIN.as_secs(),
                            RECONNECT_MAX.as_secs(),
                        ),
                        &mut said,
                    );
                }
                MonitorEvent::Connected { .. } => {
                    if let Some(at) = self.lost_at.take() {
                        // Only if its loss was, so the two come as a pair.
                        // The connection is working again, so this is news
                        // rather than trouble.
                        if self.loss_reported {
                            said.push(Note {
                                level: Level::INFO,
                                message: format!(
                                    "Connected again, {}s without one",
                                    at.elapsed().as_secs()
                                ),
                            });
                        }
                    }
                }
                // Says what went wrong rather than naming the peer. A
                // handshake that does not finish is as often a middlebox
                // holding the connection open as it is the wrong port, and
                // the reason is the only thing that tells them apart.
                MonitorEvent::HandshakeFailed { reason, .. } => {
                    self.warn(
                        &format!("Handshake did not finish: {}", reason),
                        &mut said,
                    );
                }
                _ => {}
            }
        }

        said
    }

    /// Add a warning unless one was reported too recently, saying whether it
    /// was added
    fn warn(&mut self, message: &str, said: &mut Vec<Note>) -> bool {
        if self.said_at.is_some_and(|at| at.elapsed() < WARN_INTERVAL) {
            self.unwarned += 1;
            return false;
        }

        said.push(Note {
            level: Level::WARN,
            message: match self.unwarned {
                0 => message.to_string(),
                n => format!("{} ({} more went unreported)", message, n),
            },
        });
        self.unwarned = 0;
        self.said_at = Some(Instant::now());
        true
    }

    /// Start again on a connection that has been replaced by hand.
    pub(crate) fn replaced(&mut self) {
        self.lost_at = None;
        self.loss_reported = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use omq_tokio::socket::{DisconnectReason, PeerInfo};

    /// The gateway's endpoint, which none of these turn on.
    fn endpoint() -> omq_tokio::Endpoint {
        "tcp://127.0.0.1:9500".parse().unwrap()
    }

    /// A connection lost, however it was lost.
    fn lost() -> MonitorEvent {
        MonitorEvent::Disconnected {
            endpoint: endpoint(),
            peer: PeerInfo {
                connection_id: 1,
                peer_address: None,
                peer_identity: None,
                peer_properties: Default::default(),
                zmtp_version: (3, 1),
            },
            reason: DisconnectReason::PeerClosed,
        }
    }

    /// A connection made, which is what ends one being lost.
    fn found() -> MonitorEvent {
        MonitorEvent::Connected {
            endpoint: endpoint(),
            peer_ident: omq_tokio::socket::PeerIdent::Socket(
                "127.0.0.1:9500".parse().unwrap(),
            ),
            connection_id: 1,
        }
    }

    /// Move the clock back on what the reporter remembers, so a test does not
    /// have to wait out an interval to see the far side of one.
    fn age(reporter: &mut Reporter, by: Duration) {
        reporter.said_at = reporter.said_at.map(|at| at - by);
    }

    #[test]
    fn losing_a_connection_over_and_over_is_reported_once_an_interval() {
        let mut reporter = Reporter::default();

        // The first loss is worth saying, and so is the recovery ending it.
        // The loss is trouble, the recovery is only news.
        let said = reporter.observe(&[lost(), found()]);
        assert_eq!(said.len(), 2);
        assert_eq!(said[0].level, Level::WARN);
        assert!(said[0].message.starts_with("Connection lost"));
        assert_eq!(said[1].level, Level::INFO);
        assert!(said[1].message.starts_with("Connected again"));

        // Losing it again immediately is not, however many times.
        for _ in 0..50 {
            assert!(reporter.observe(&[lost(), found()]).is_empty());
        }

        // Once the interval has passed, one report covers what it missed.
        age(&mut reporter, WARN_INTERVAL);
        let said = reporter.observe(&[lost()]);
        assert_eq!(said.len(), 1);
        assert!(
            said[0].message.contains("50 more went unreported"),
            "{}",
            said[0].message
        );
    }

    #[test]
    fn a_connection_coming_back_is_reported_only_if_its_loss_was() {
        let mut reporter = Reporter::default();

        // Spend the interval on a loss that is reported.
        assert_eq!(reporter.observe(&[lost()]).len(), 1);
        assert_eq!(reporter.observe(&[found()]).len(), 1);

        // The next loss is throttled away, so its recovery goes too rather
        // than arriving on its own with no loss to explain it.
        assert!(reporter.observe(&[lost()]).is_empty());
        assert!(reporter.observe(&[found()]).is_empty());
    }

    #[test]
    fn a_connection_replaced_by_hand_is_not_waiting_on_a_recovery() {
        let mut reporter = Reporter::default();

        assert_eq!(reporter.observe(&[lost()]).len(), 1);
        reporter.replaced();

        // The new connection's first `Connected` belongs to it, not to the
        // loss the old one was in the middle of.
        assert!(reporter.observe(&[found()]).is_empty());
    }
}
