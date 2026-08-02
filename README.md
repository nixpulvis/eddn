# eddn

Subscribe to journal events from the [EDDN](https://github.com/EDCD/EDDN) ZMQ
event stream.

### Usage

To get started, add the following to your `Cargo.toml`:
```toml
eddn = "*"
```

An example live stream can be demonstrated with the `envelope` example:
```sh
cargo run --example envelope
```

### Disconnects

A subscription outlives the connection carrying it. `subscribe` documents the
three ways one stops working and what notices each. In short: libzmq rebuilds
a connection that closes, heartbeats find one that has died without closing
within 15 seconds, and a gateway that answers heartbeats while publishing
nothing is waited out for the `stall_timeout` given to `subscribe`. All three
are traced as they happen, so install a [`tracing`](https://docs.rs/tracing)
subscriber or none of it will be heard.

That timeout is the caller's to pick, from how quiet the gateway is expected
to go, and `None` says not to watch for it at all. There is no default
because the answer belongs to the gateway rather than to this crate.

Watching any of it happen means standing in for the gateway, since EDDN
cannot be asked to have a bad day on cue. A ZeroMQ `PUB` socket publishing
compressed envelopes is enough for the first case: kill it and a subscriber
picks the next connection up. Leave it running while it stops publishing and
the stall timeout is what ends the wait. The second case needs something
between the two that stops passing messages while holding the connection
open, which is the one thing neither end can do to itself.
