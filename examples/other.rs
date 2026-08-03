//! Print the event name of every message that is not one of the kinds
//! [`eddn::Message`] parses, to see what is going by unrecognised.

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

        if let Message::Other(value) = envelope.message {
            if let Some(event) = value.get("event") {
                println!("{}", event);
            }
        }
    }
}
