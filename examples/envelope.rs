use eddn::{subscribe, URL};

fn main() {
    // Without this the crate traces into the void.
    tracing_subscriber::fmt::init();

    for result in subscribe(URL, None) {
        match result {
            Ok(envelope) => {
                dbg!(envelope);
            }
            Err(err) => eprintln!("{}", err),
        }
    }
}
