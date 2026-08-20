//! The thread that reads the socket, off the one that draws
//!
//! [`eddn::subscribe`] blocks forever, so it cannot run where the UI runs. It
//! runs here instead, on its own thread, handing each envelope across a channel
//! and waking the UI to come and take it.

use eddn::{subscribe, Envelope, Galaxy};
use std::sync::mpsc::Sender;
use std::thread;
use std::time::Duration;
use tracing::{error, warn};

/// What the subscribe thread hands the UI
///
/// An envelope, or the news that one could not be read. The error's own words
/// have already gone to the log by the time this is sent; what the UI wants
/// from it is only that the count of unreadable messages went up.
pub enum Update {
    /// A message off the socket. Boxed because an envelope is large and a
    /// channel of them should not carry the whole of one by value.
    Envelope(Box<Envelope>),
    /// A message that could not be read.
    Error,
}

/// Start reading `url` on a background thread
///
/// Each envelope goes down `tx` and then `ctx` is asked to repaint, so the UI
/// wakes for a message rather than polling for one. The thread ends when the
/// receiver is dropped, which is the window closing.
pub fn spawn(
    url: String,
    stall: Option<Duration>,
    include_test: bool,
    tx: Sender<Update>,
    ctx: egui::Context,
) {
    let spawned = thread::Builder::new()
        .name("eddn-subscribe".to_owned())
        .spawn(move || {
            let galaxy = if include_test { Galaxy::ALL } else { Galaxy::LIVE };
            for result in subscribe(&url, stall).galaxy(galaxy) {
                let update = match result {
                    Ok(envelope) => Update::Envelope(Box::new(envelope)),
                    Err(err) => {
                        warn!(error = %err, "unreadable message");
                        Update::Error
                    }
                };

                if tx.send(update).is_err() {
                    // The window is gone; nothing left to read for. Return
                    // rather than break so the stream-ended report below is
                    // reached only when subscribe() itself stops.
                    return;
                }
                ctx.request_repaint();
            }

            // subscribe() is documented infinite, so the loop ending is not a
            // clean shutdown -- that path is the send failure above, taken when
            // the window closes. Reaching here means the stream itself stopped
            // and the feed is now frozen with no arrivals to show it; say so,
            // or a dead thread reads as a merely quiet gateway.
            error!("subscribe stream ended; feed stopped");
        });
    if let Err(err) = spawned {
        // Spawning fails only when the OS is out of threads. Nothing to read
        // for then, but a logged line beats crashing the window on startup.
        error!(error = %err, "could not spawn the subscribe thread; no feed");
    }
}
