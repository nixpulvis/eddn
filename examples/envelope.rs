use eddn::{subscribe, Message, URL};

fn main() {
    // Without this the crate traces into the void.
    tracing_subscriber::fmt::init();

    for envelop in subscribe(URL, None) {
        dbg!(envelop);
    }
}
