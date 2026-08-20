//! A minimal desktop viewer for the [EDDN](https://eddn.edcd.io) feed
//!
//! Subscribes to the gateway the [`eddn`] crate reads and shows what comes off
//! it: a scrolling list of messages, the detail of any one of them, and the
//! crate's own log of how the connection is holding up.

// Release builds are a windowed program with no console behind them. Debug
// builds keep the console so `cargo run` still shows a panic.
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod app;
mod feed;
mod log_pane;
mod worker;

use clap::Parser;
use eddn::URL;
use log_pane::{LogBuffer, LogLayer};
use std::sync::mpsc;
use std::time::Duration;
use tracing_subscriber::filter::{LevelFilter, Targets};
use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt, registry, EnvFilter};

/// How long the gateway may carry nothing before its connection is replaced
///
/// The same two minutes the CLI subscriber uses: a busy hour of EDDN never
/// goes a second without a message, so this is quiet that does not happen.
const STALL: Duration = Duration::from_secs(120);

/// A minimal desktop viewer for the EDDN feed
#[derive(Parser)]
#[command(name = "eddn_gui", version)]
struct Args {
    /// Gateway address to subscribe to
    #[arg(long, default_value_t = URL.to_owned())]
    url: String,

    /// Seconds of silence before the connection is replaced; 0 leaves it alone
    #[arg(long, default_value_t = STALL.as_secs())]
    stall: u64,

    /// Show non-live (alpha/beta) events too
    #[arg(long)]
    test: bool,
}

fn main() -> eframe::Result {
    let Args { url, stall, test: include_test } = Args::parse();
    // `--stall 0` leaves the connection alone however long it carries nothing.
    let stall = (stall != 0).then(|| Duration::from_secs(stall));

    // The log goes to two places, each with its own audience and filter. The
    // pane is for the person watching the feed: a whitelist of our own crates,
    // so a noisy dependency added later cannot reach it without being named.
    // stderr is for whoever is running it from a console: a warn baseline that
    // still surfaces a real failure from anywhere -- a lost GPU surface, a
    // socket error -- while our crates stay at info. RUST_LOG overrides stderr.
    let log = LogBuffer::default();
    let pane_filter = Targets::new()
        .with_target("eddn", LevelFilter::INFO)
        .with_target("eddn_gui", LevelFilter::INFO);
    let stderr_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            // wgpu_hal at error drops the benign Vulkan-loader warning seen on
            // every start while keeping its real errors (a lost device or
            // surface); everything else stays at warn so nothing genuine is
            // hidden.
            "warn,eddn=info,eddn_gui=info,wgpu_hal=error".into()
        });

    registry()
        .with(fmt::layer().with_filter(stderr_filter))
        .with(LogLayer::new(log.clone()).with_filter(pane_filter))
        .init();

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1100.0, 720.0])
            .with_min_inner_size([640.0, 400.0])
            .with_title(if include_test { "EDDN (test)" } else { "EDDN" }),
        ..Default::default()
    };

    let (tx, rx) = mpsc::channel();
    eframe::run_native(
        "EDDN",
        options,
        Box::new(move |cc| {
            // Spawned here because the worker needs the egui context to wake
            // the UI, and this is the first place it exists.
            worker::spawn(url, stall, include_test, tx, cc.egui_ctx.clone());
            Ok(Box::new(app::App::new(rx, log, include_test)))
        }),
    )
}
