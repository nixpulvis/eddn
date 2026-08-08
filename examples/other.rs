//! Print the `$schemaRef` of every message [`eddn::Message`] does not read,
//! to see what is going by unread.
//!
//! The schema is the useful thing to name now rather than the event. A
//! message is placed by the schema it was sent under, so a schema turning up
//! here is exactly the work of adding it, and the ones with no `event` key at
//! all -- outfitting, shipyard, blackmarket -- could not have been named any
//! other way.

use eddn::{subscribe, Message, URL};

fn main() {
    // Without this the crate traces into the void.
    tracing_subscriber::fmt::init();

    for result in subscribe(URL, None) {
        let envelope = match result {
            Ok(envelope) => envelope,
            Err(err) => {
                eprintln!("{}", err);
                continue;
            }
        };

        if let Message::Unmodeled(_) = envelope.message {
            println!("{}", envelope.schema_ref);
        }
    }
}
