//! The window: a list of what has come off the socket, a look at any one of
//! it, the crate's log, and a line of totals across the top.

use crate::cadence::Cadence;
use crate::feed::{event_label, schema_family, system_text, Feed};
use crate::log_pane::{LogBuffer, LogLine};
use crate::worker::Update;
use chrono::{DateTime, Local, Utc};
use eddn::{Envelope, Galaxy, Message};
use egui::{Color32, RichText};
use egui_extras::{Column, TableBuilder};
use serde_json::to_string_pretty;
use std::collections::VecDeque;
use std::fmt::Write as _;
use std::sync::mpsc::{Receiver, TryRecvError};
use std::time::{Duration, Instant};
use tracing::Level;

/// One line's height in the feed list, for the scroll area's row maths
const ROW_HEIGHT: f32 = 18.0;

/// How a UTC timestamp is spelled wherever the window shows one: the live
/// clock and every row's gateway time, so the two read the same and line up.
const TIMESTAMP_FORMAT: &str = "%Y-%m-%d %H:%M:%SZ";

/// The same format in local time: no `Z`, since the value is no longer UTC.
const LOCAL_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

/// A UTC instant spelled for display: UTC with a `Z`, or the machine's local
/// time without one when `local` is set. The wall clock and both time columns
/// all read through here, so flipping the toggle moves them together.
fn format_time(when: DateTime<Utc>, local: bool) -> String {
    if local {
        when.with_timezone(&Local).format(LOCAL_FORMAT).to_string()
    } else {
        when.format(TIMESTAMP_FORMAT).to_string()
    }
}

/// The window's state: everything a frame draws from and remembers between them.
pub struct App {
    feed: Feed,
    updates: Receiver<Update>,
    log: LogBuffer,

    /// Substring matched against a row's schema, event, system, body, station
    /// and uploader; independent of which columns are shown.
    filter: String,
    /// Whether test data is enabled (the `--test` flag): the subscription
    /// carries both galaxies, the live column and the live/test filters show,
    /// and non-live events appear. Without it only live events arrive.
    test_enabled: bool,
    /// Which galaxy's rows to show. Only meaningful when `test_enabled`: the
    /// subscription carries both galaxies under `--test`, and this filters the
    /// view of them down to the live galaxy, the test one, or both. Starts on
    /// the test galaxy under `--test`, since watching test data is the reason
    /// to have asked for it; [`ALL`](Galaxy::ALL) otherwise, where every row is
    /// live anyway.
    view: Galaxy,
    /// The rendered detail of whatever row was last clicked.
    selected: Option<String>,
    /// The table's columns in display order, each with whether it is shown.
    /// Any of them can be toggled from the status bar.
    columns: Vec<ColumnState>,

    /// Whether the log pane is shown. Hidden by default; opened from the
    /// status bar button and closed from the pane's own header.
    show_log: bool,

    /// Set when the follow button is pressed, to scroll back to the newest row
    /// and resume tailing. Consumed the frame it is read.
    follow_latest: bool,

    /// Whether timestamps read in local time instead of UTC. On by default for
    /// casual reading; the toggle switches to UTC, which matches the gateway.
    /// Flips the wall clock and both time columns together so they line up.
    local_time: bool,

    /// While scrolled up from the live edge, the top-of-view message by
    /// sequence number and the pixels it sits scrolled past
    ///
    /// The one piece of scroll state that has to outlive a frame. egui keeps
    /// the raw pixel offset itself, but the feed is a ring: once full, each
    /// arrival drops the oldest and every row index shifts down one, so that
    /// raw offset slides the view over the messages. A sequence number
    /// survives eviction where a row index does not, so the top message is
    /// remembered by its sequence and the offset re-derived each frame from
    /// where it now sits. The re-derivation is cheap (see
    /// [`anchor_offset`]/[`top_anchor`]); it is stored rather than recomputed
    /// only because nothing in the fresh frame remembers the pre-eviction
    /// layout. [`None`] while tailing the bottom, where sticking to the bottom
    /// already follows the newest row.
    feed_anchor: Option<(u64, f32)>,

    /// The feed's arrival timing: the rate shown in the status bar and the
    /// statistics behind the connection dot. See [`Cadence`].
    cadence: Cadence,

    /// Set once the worker reports the stream ended (or its channel drops), so
    /// the status dot shows a dead feed apart from a merely quiet one.
    stream_ended: bool,

    /// The frame the feed and log were last taken in
    ///
    /// egui may run [`ui`](eframe::App::ui) more than once a frame to settle a
    /// layout, and taking new messages on the second pass would move rows out
    /// from under the widget ids assigned on the first. So the channel and the
    /// log are read once a frame, and the passes of that frame all draw the
    /// same thing.
    last_frame: Option<u64>,
    /// The log as it stood when this frame began.
    log_lines: Vec<LogLine>,

    /// The detail panel's opening width, measured once and kept
    ///
    /// It is the width of the widest fixed line laid out in the monospace
    /// font, which does not change under us, so it is worth laying out once
    /// rather than every frame the panel is open. [`None`] until first needed.
    detail_width: Option<f32>,
}

impl App {
    /// Build the app around the worker's channel, the shared log buffer, and
    /// the size of the feed's scrollback window.
    pub fn new(
        updates: Receiver<Update>,
        log: LogBuffer,
        test_enabled: bool,
        capacity: usize,
    ) -> Self {
        App {
            feed: Feed::new(capacity),
            updates,
            log,
            filter: String::new(),
            test_enabled,
            view: if test_enabled { Galaxy::TEST } else { Galaxy::ALL },
            selected: None,
            columns: default_columns(test_enabled),
            show_log: false,
            follow_latest: false,
            local_time: true,
            feed_anchor: None,
            cadence: Cadence::default(),
            stream_ended: false,
            last_frame: None,
            log_lines: Vec::new(),
            detail_width: None,
        }
    }

    /// Take everything the worker has handed over since the last frame
    fn drain(&mut self) {
        loop {
            match self.updates.try_recv() {
                Ok(envelope) => match self.cadence.record(Instant::now()) {
                    Some(gap) => self.feed.push_after_gap(*envelope, gap),
                    None => self.feed.push(*envelope),
                },
                Err(TryRecvError::Empty) => break,
                // Every sender gone means the worker thread has stopped. It only
                // ends by unwinding, since subscribe() never returns, so the
                // feed is dead: the dot goes red.
                Err(TryRecvError::Disconnected) => {
                    self.stream_ended = true;
                    break;
                }
            }
        }

        self.cadence.tick(Instant::now());
    }

    /// The health of the feed's connection, as the status dot shows it.
    fn connection(&self) -> Connection {
        if self.stream_ended {
            Connection::Stopped
        } else if self.cadence.last().is_none() {
            Connection::Connecting
        } else if self.cadence.stalled() {
            Connection::Stalling
        } else {
            Connection::Online
        }
    }
}

/// The feed connection's health, from green to red
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Connection {
    /// No message has arrived yet; the socket is still coming up.
    Connecting,
    /// Messages are arriving about as often as the rate predicts.
    Online,
    /// The feed has gone quiet for longer than its rate predicts.
    Stalling,
    /// The stream stopped; no more messages are coming.
    Stopped,
}

impl Connection {
    /// The dot's colour for the active theme, readable on light and dark
    ///
    /// Stalling and stopped borrow egui's own warn/error colours so they track
    /// the theme. Online and connecting are tuned per mode, since a dark
    /// theme's light-green and grey wash out on a light background.
    fn color(self, visuals: &egui::Visuals) -> Color32 {
        match self {
            Connection::Connecting => visuals.weak_text_color(),
            Connection::Online if visuals.dark_mode => Color32::LIGHT_GREEN,
            Connection::Online => Color32::from_rgb(0x1a, 0x7f, 0x37),
            Connection::Stalling => visuals.warn_fg_color,
            Connection::Stopped => visuals.error_fg_color,
        }
    }

    /// The word beside the dot.
    fn label(self) -> &'static str {
        match self {
            Connection::Connecting => "connecting",
            Connection::Online => "online",
            Connection::Stalling => "stalling",
            Connection::Stopped => "stopped",
        }
    }

    /// What the dot means, on hover.
    fn tooltip(self) -> &'static str {
        match self {
            Connection::Connecting => "waiting for the first message",
            Connection::Online => "messages arriving as expected",
            Connection::Stalling => "quieter than the feed's rate predicts",
            Connection::Stopped => "the stream stopped; the feed is frozen",
        }
    }
}

impl eframe::App for App {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        // Read the socket and the log once a frame, not once a pass, so a
        // second layout pass draws the same rows the first laid out.
        let frame_nr = ui.ctx().cumulative_frame_nr();
        if self.last_frame != Some(frame_nr) {
            self.last_frame = Some(frame_nr);
            self.drain();
            // Only when the pane is shown: a hidden log needs no copy, and
            // this runs every frame, at least once a second while idle.
            self.log_lines =
                if self.show_log { self.log.snapshot() } else { Vec::new() };
            // Clear the follow request once a frame, not once a pass. A pass
            // that consumed it can be discarded by egui's multi-pass layout,
            // which throws away its scroll_to_row but not this write -- so the
            // re-run pass would scroll nowhere. Held across the frame's passes,
            // it is applied on whichever one is kept, then cleared next frame.
            self.follow_latest = false;
        }
        // Keep the age and rate honest while no messages arrive to repaint us.
        ui.ctx().request_repaint_after(Duration::from_secs(1));

        // Scroll instantly rather than animating. The follow button jumps to
        // the newest row, and a smooth scroll never lands there because new
        // rows keep extending the bottom past a moving target. Set every frame
        // because eframe rebuilds the style after startup.
        //
        // The same call silences egui's debug-only "widget id changed between
        // passes" lint: egui_extras keys each cell by its virtualized row index
        // and relays a different row window across its own two-pass layout, so
        // the id under a fixed rect shifts and the lint fires once per cell. We
        // cannot pin it from outside the widget (no per-row id API); it is
        // harmless (clicks resolve within a frame) and absent from release.
        //
        // TODO: upstream -- egui_extras should keep cell ids stable across its
        // own multi-pass layout, or expose a per-row id salt. Track/file at
        // https://github.com/emilk/egui/issues
        ui.ctx().all_styles_mut(|style| {
            style.scroll_animation = egui::style::ScrollAnimation::none();
            #[cfg(debug_assertions)]
            {
                style.debug.warn_if_rect_changes_id = false;
            }
        });

        self.status_bar(ui);
        self.log_panel(ui);
        self.detail_panel(ui);
        self.feed_list(ui);
    }
}

/// A compact line of the recent message rate, beside the numeric rate
///
/// One-per-second samples scaled to the tallest in view, newest at the right --
/// a bare polyline, no axes or interaction, since it is a glance not a plot.
fn rate_sparkline(ui: &mut egui::Ui, samples: &VecDeque<(f32, bool)>) {
    let (rect, response) = ui.allocate_exact_size(
        egui::vec2(72.0, ROW_HEIGHT),
        egui::Sense::hover(),
    );
    if samples.len() < 2 {
        return;
    }
    let peak = samples.iter().map(|&(r, _)| r).fold(0.0_f32, f32::max).max(1.0);
    let last = (samples.len() - 1) as f32;
    let x = |i: usize| rect.left() + rect.width() * (i as f32 / last);

    // Wash the stalled spans yellow behind the line, so a dip reads as a stall
    // rather than a quiet feed. One rect per stalled step; contiguous ones
    // abut into a band.
    let warn = ui.visuals().warn_fg_color;
    let wash =
        egui::Color32::from_rgba_unmultiplied(warn.r(), warn.g(), warn.b(), 48);
    for i in 0..samples.len() - 1 {
        if samples[i].1 {
            let band = egui::Rect::from_x_y_ranges(
                egui::Rangef::new(x(i), x(i + 1)),
                rect.y_range(),
            );
            ui.painter().rect_filled(band, egui::CornerRadius::ZERO, wash);
        }
    }

    let points: Vec<egui::Pos2> = samples
        .iter()
        .enumerate()
        .map(|(i, &(r, _))| {
            egui::pos2(x(i), rect.bottom() - rect.height() * (r / peak))
        })
        .collect();
    let stroke = egui::Stroke::new(1.0_f32, ui.visuals().weak_text_color());
    ui.painter().add(egui::Shape::line(points, stroke));
    response.on_hover_text(format!("peak {peak:.0}/s over {}s", samples.len()));
}

impl App {
    fn status_bar(&mut self, ui: &mut egui::Ui) {
        egui::Panel::top("status").show_inside(ui, |ui| {
            ui.horizontal_wrapped(|ui| {
                // The wall clock, in the same zone as the feed's time columns
                // -- local by default, or UTC with the toggle off -- so the two
                // read the same and how current the feed is shows at a glance.
                // The frame repaints once a second (see `ui`), so it ticks
                // without a clock of its own.
                ui.monospace(format_time(Utc::now(), self.local_time));
                ui.checkbox(&mut self.local_time, "local time");
                ui.separator();
                // Connection health at a glance, so a stall or a dead stream
                // shows without opening the log. Painted rather than a glyph,
                // to match the live/test dots and render the same in any font.
                // After the clock so the two stay lined up as the state changes.
                let conn = self.connection();
                let color = conn.color(ui.visuals());
                let (rect, dot) = ui.allocate_exact_size(
                    egui::vec2(10.0, 10.0),
                    egui::Sense::hover(),
                );
                ui.painter().circle_filled(rect.center(), 4.0, color);
                let word = ui.label(RichText::new(conn.label()).color(color));
                dot.on_hover_text(conn.tooltip());
                word.on_hover_text(conn.tooltip());
                if self.log.errors() > 0 {
                    let text =
                        RichText::new(format!("errors {}", self.log.errors()))
                            .color(ui.visuals().error_fg_color);
                    if log_link(ui, text) {
                        self.show_log = true;
                    }
                }
                if self.log.warnings() > 0 {
                    let text = RichText::new(format!(
                        "warnings {}",
                        self.log.warnings()
                    ))
                    .color(ui.visuals().warn_fg_color);
                    if log_link(ui, text) {
                        self.show_log = true;
                    }
                }
                ui.separator();
                rate_sparkline(ui, self.cadence.samples());
                ui.monospace(format!("{:>5.1}/s", self.cadence.rate()));
                if let Some(span) = self.feed.window_duration() {
                    ui.label(format!("span {}", format_span(span)));
                }
                ui.label(match self.cadence.last() {
                    Some(at) => {
                        format!("last {:.0}s ago", at.elapsed().as_secs_f64())
                    }
                    None => "waiting...".to_owned(),
                });
                ui.separator();
                let received =
                    ui.label(format!("received {}", self.feed.received()));
                if !self.feed.per_schema().is_empty() {
                    received
                        .on_hover_cursor(egui::CursorIcon::Help)
                        .on_hover_ui(|ui| {
                            for (family, count) in self.feed.per_schema() {
                                ui.label(
                                    RichText::new(format!(
                                        "{count:>6}  {family}"
                                    ))
                                    .monospace(),
                                );
                            }
                        });
                }
                let software = ui.label(format!(
                    "software {}",
                    self.feed.per_software().len()
                ));
                if !self.feed.per_software().is_empty() {
                    software
                        .on_hover_cursor(egui::CursorIcon::Help)
                        .on_hover_ui(|ui| {
                            for (name, count) in self.feed.per_software() {
                                ui.label(
                                    RichText::new(format!(
                                        "{count:>6}  {name}"
                                    ))
                                    .monospace(),
                                );
                            }
                        });
                }
                ui.separator();
                if ui.button("follow").clicked() {
                    self.follow_latest = true;
                }
                if ui.button("log").clicked() {
                    self.show_log = !self.show_log;
                }
            });

            ui.horizontal(|ui| {
                ui.label("filter");
                ui.text_edit_singleline(&mut self.filter);
                if self.test_enabled {
                    ui.separator();
                    ui.label("show:");
                    ui.selectable_value(&mut self.view, Galaxy::LIVE, "live");
                    ui.selectable_value(&mut self.view, Galaxy::TEST, "test");
                    ui.selectable_value(&mut self.view, Galaxy::ALL, "both");
                }
                ui.separator();
                for column in &mut self.columns {
                    let label = column.field.label();
                    ui.checkbox(&mut column.visible, label);
                }
            });
            // A little breathing room before the feed table butts up below.
            ui.add_space(ui.spacing().item_spacing.y);
        });
    }

    fn log_panel(&mut self, ui: &mut egui::Ui) {
        // Hidden until asked for from the status bar: the feed is the point,
        // and the log is there for when a connection needs looking into.
        if !self.show_log {
            return;
        }
        // A dark background sets the log apart from the feed above it.
        let frame = egui::Frame::side_top_panel(ui.style())
            .fill(Color32::from_gray(12));
        egui::Panel::bottom("log")
            .frame(frame)
            .resizable(true)
            .default_size(140.0)
            .show_inside(ui, |ui| {
                let (clear, close) = ui
                    .horizontal(|ui| {
                        ui.label(
                            RichText::new("log")
                                .strong()
                                .color(Color32::from_gray(200)),
                        );
                        let clear = ui
                            .button("clear counts")
                            .on_hover_text("reset the error and warning counts")
                            .clicked();
                        let close = ui.button("×").clicked();
                        (clear, close)
                    })
                    .inner;
                if clear {
                    self.log.clear_errors();
                    self.log.clear_warnings();
                }
                egui::ScrollArea::vertical()
                    .auto_shrink([false, false])
                    .stick_to_bottom(true)
                    .show(ui, |ui| {
                        for line in &self.log_lines {
                            let color = level_color(line.level);
                            ui.label(
                                RichText::new(format!(
                                    "{}  {:>5}  {}  {}",
                                    line.at.format("%H:%M:%SZ"),
                                    line.level,
                                    line.target,
                                    line.message,
                                ))
                                .monospace()
                                .color(color),
                            );
                        }
                    });
                if close {
                    self.show_log = false;
                }
            });
    }

    fn detail_panel(&mut self, ui: &mut egui::Ui) {
        // Closed until a row is clicked, and closable from its header.
        if self.selected.is_none() {
            return;
        }
        // Open just wide enough for the widest fixed line -- the 64-hex
        // uploader id -- so it reads without wrapping or a scrollbar. Measured
        // in the detail's own monospace font, plus the vertical scrollbar and a
        // little margin; the divider is draggable and remembered after that.
        // Laid out once and kept: the font and spacing do not change under us.
        let default_width = *self.detail_width.get_or_insert_with(|| {
            let sample = "uploader: \
                0000000000000000000000000000000000000000000000000000000000000000";
            let font = egui::TextStyle::Monospace.resolve(ui.style());
            let text_width = ui.ctx().fonts_mut(|fonts| {
                fonts
                    .layout_no_wrap(sample.to_owned(), font, Color32::PLACEHOLDER)
                    .rect
                    .width()
            });
            let frame = egui::Frame::side_top_panel(ui.style());
            text_width
                + frame.inner_margin.sum().x
                + ui.spacing().scroll.bar_width
                + ui.spacing().item_spacing.x
        });
        egui::Panel::right("detail")
            .resizable(true)
            .default_size(default_width)
            .show_inside(ui, |ui| {
                let close =
                    panel_header(ui, RichText::new("detail").strong(), "close");
                if let Some(text) = &self.selected {
                    egui::ScrollArea::both().auto_shrink([false, false]).show(
                        ui,
                        |ui| {
                            ui.label(RichText::new(text).monospace());
                        },
                    );
                }
                if close {
                    self.selected = None;
                }
            });
    }

    fn feed_list(&mut self, ui: &mut egui::Ui) {
        egui::CentralPanel::default().show_inside(ui, |ui| {
            // Disjoint field borrows: the filtered view borrows the feed and
            // the columns while clicks write the selection, and the three are
            // different fields.
            let feed = &self.feed;
            let selected = &mut self.selected;
            let filter = &self.filter;
            let columns = &self.columns;
            let feed_anchor = &mut self.feed_anchor;
            let view = self.view;
            let follow = self.follow_latest;
            let local = self.local_time;

            // The columns actually drawn. The table, its header and its rows
            // are all built by walking this one list, so they never fall out
            // of step however the toggles leave it.
            let shown: Vec<Field> = columns
                .iter()
                .filter(|column| column.visible)
                .map(|column| column.field)
                .collect();
            // Every column hidden: an empty table leaves egui_extras with
            // nothing to lay out, so say as much and stop.
            if shown.is_empty() {
                ui.weak("all columns hidden");
                return;
            }

            let needle = filter.to_lowercase();
            // Each row carries the sequence number of its envelope -- the count
            // of envelopes pushed before it -- so a row keeps its identity as
            // the ring drops old ones out from under the shifting indices.
            let evicted = feed.received() - feed.retained() as u64;
            let mut rows: Vec<(u64, &Envelope)> = Vec::new();
            let mut gaps: Vec<Option<Duration>> = Vec::new();
            for (index, (envelope, search, gap)) in feed.rows().enumerate() {
                if view.shows(envelope.live)
                    && (needle.is_empty() || search.contains(&needle))
                {
                    rows.push((evicted + index as u64, envelope));
                    gaps.push(gap);
                }
            }

            // Labels are selectable by default, and the text selection senses
            // the click before the cell does, so a click on a cell's text never
            // reaches the row. The feed is for reading and clicking, not
            // selecting text, so turn it off for this table.
            ui.style_mut().interaction.selectable_labels = false;

            // The pitch egui_extras scrolls by: a row plus the gap under it.
            let pitch = ROW_HEIGHT + ui.spacing().item_spacing.y;

            let mut table = TableBuilder::new(ui)
                .striped(true)
                .resizable(true)
                // Tail the feed: newest at the bottom, following new messages
                // while at the live edge.
                .stick_to_bottom(true)
                // The feed is clicked, not dragged. With drag-to-scroll on, a
                // click that shifts a pixel is read as a scroll: the view moves
                // and the anchor that holds it still is lost, so a click near
                // the live edge drops out of follow. Off, a click stays a click.
                .drag_to_scroll(false)
                .sense(egui::Sense::click())
                .cell_layout(egui::Layout::left_to_right(egui::Align::Center));
            for &field in &shown {
                table = table.column(field.column());
            }
            if follow {
                // Scroll animation is off (set in `ui`), so this jumps straight
                // to the newest row; stick_to_bottom then keeps tailing.
                table = table.scroll_to_row(
                    rows.len().saturating_sub(1),
                    Some(egui::Align::BOTTOM),
                );
            } else if let Some((anchor, frac)) = *feed_anchor {
                // Scrolled up: hold the remembered message where it was, so
                // eviction cannot slide the view over the feed.
                table = table.vertical_scroll_offset(anchor_offset(
                    &rows, anchor, frac, pitch,
                ));
            }

            let output = table
                .header(20.0, |mut header| {
                    for &field in &shown {
                        header.col(|ui| {
                            ui.strong(field.header(feed, view));
                        });
                    }
                })
                .body(|body| {
                    body.rows(ROW_HEIGHT, rows.len(), |mut row| {
                        let index = row.index();
                        let (_, envelope) = rows[index];
                        let gap = gaps[index];
                        for &field in &shown {
                            row.col(|ui| {
                                // A row that opens after a connection gap gets a
                                // line under its time: the same stall the status
                                // dot flags, drawn where the feed picked back up.
                                // Painted in the time cell, in the scroll
                                // content, so it tracks the rows rather than
                                // lagging a frame behind as a separate layer
                                // would.
                                if gap.is_some() && field == Field::GatewayTime
                                {
                                    let rect = ui.max_rect();
                                    let stroke = egui::Stroke::new(
                                        2.0_f32,
                                        ui.visuals().warn_fg_color,
                                    );
                                    ui.painter().hline(
                                        rect.x_range(),
                                        rect.top(),
                                        stroke,
                                    );
                                }
                                field.cell(ui, envelope, local);
                            });
                        }
                        // The cells already sense clicks (Sense::click on the
                        // table), so the row's unioned response carries them.
                        // `interact` would reuse that response's id at the full
                        // row rect and clash, drawing egui's id-clash overlay.
                        let response = row.response();
                        if response.clicked() {
                            *selected = Some(detail(envelope));
                        }
                        let response = match gap {
                            Some(gap) => response.on_hover_text(format!(
                                "connection gap {}",
                                format_span(
                                    chrono::Duration::from_std(gap)
                                        .unwrap_or_else(|_| {
                                            chrono::Duration::zero()
                                        }),
                                ),
                            )),
                            None => response,
                        };
                        response
                            .on_hover_cursor(egui::CursorIcon::PointingHand);
                    });
                });

            // Remember which message is at the top and how far past it the view
            // sits, for the next frame to re-anchor to. None at the bottom,
            // where sticking to the bottom already follows the newest row.
            let settled = output.state.offset.y;
            let max_offset =
                (output.content_size.y - output.inner_rect.height()).max(0.0);
            *feed_anchor = top_anchor(&rows, settled, max_offset, pitch);
        });
    }
}

/// A feed column and whether it is currently shown
struct ColumnState {
    field: Field,
    visible: bool,
}

/// One column of the feed table
///
/// A column's name, width, header and cell all live on the one value rather
/// than spread through [`feed_list`](App::feed_list): the table is built, its
/// header laid out, its cells filled and the filter matched by walking the same
/// set, so a column is added, dropped or reordered in one place and every part
/// of the table follows.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Field {
    /// When EDDN received the message: the gateway's own timestamp.
    GatewayTime,
    /// When the game wrote the event: the message's own timestamp. Off by
    /// default.
    JournalTime,
    /// Gateway-received time less the event's own time: how long the message
    /// took to reach EDDN. Off by default.
    Delta,
    /// The live/test dot; offered only when `--test` mixes the two galaxies.
    Live,
    System,
    Body,
    Station,
    Schema,
    Event,
}

impl Field {
    /// The name shown on the column's toggle
    fn label(self) -> &'static str {
        match self {
            Field::GatewayTime => "gateway time",
            Field::JournalTime => "journal time",
            Field::Delta => "delta",
            Field::Live => "live",
            Field::System => "system",
            Field::Body => "body",
            Field::Station => "station",
            Field::Schema => "schema",
            Field::Event => "event",
        }
    }

    /// How wide the column sits
    fn column(self) -> Column {
        match self {
            Field::GatewayTime => Column::exact(160.0),
            Field::JournalTime => Column::exact(160.0),
            Field::Delta => Column::exact(70.0),
            Field::Live => Column::exact(18.0),
            Field::System => Column::initial(160.0).at_least(100.0).clip(true),
            Field::Body => Column::initial(150.0).at_least(90.0).clip(true),
            Field::Station => Column::initial(150.0).at_least(90.0).clip(true),
            Field::Schema => Column::initial(130.0).at_least(90.0),
            Field::Event => Column::initial(200.0).at_least(150.0).clip(true),
        }
    }

    /// The header's text, with a running count where the feed keeps one
    fn header(self, feed: &Feed, galaxy: Galaxy) -> String {
        match self {
            Field::GatewayTime => "gateway time".to_owned(),
            Field::JournalTime => "journal time".to_owned(),
            Field::Delta => "delta".to_owned(),
            Field::Live => String::new(),
            Field::System => {
                format!("systems ({})", feed.systems(galaxy))
            }
            Field::Body => {
                format!("bodies ({})", feed.bodies(galaxy))
            }
            Field::Station => {
                format!("stations ({})", feed.stations(galaxy))
            }
            Field::Schema => "schema".to_owned(),
            Field::Event => "event".to_owned(),
        }
    }

    /// The cell's text, or `None` for a column that paints itself
    ///
    /// This is the single reading of a column, used both to fill its cell and
    /// to match the filter, so the two never drift: what the filter searches is
    /// exactly what a column shows. [`GatewayTime`](Field::GatewayTime),
    /// [`JournalTime`](Field::JournalTime) and [`Live`](Field::Live) draw
    /// themselves and carry no searchable text.
    fn text(self, envelope: &Envelope) -> Option<String> {
        match self {
            Field::GatewayTime
            | Field::JournalTime
            | Field::Live
            | Field::Delta => None,
            Field::System => Some(system_text(envelope)),
            Field::Body => envelope.body.clone(),
            Field::Station => envelope.station.clone(),
            Field::Schema => {
                Some(schema_family(&envelope.schema_ref).to_owned())
            }
            Field::Event => Some(event_label(&envelope.message)),
        }
    }

    /// Draw one cell of this column
    fn cell(self, ui: &mut egui::Ui, envelope: &Envelope, local: bool) {
        match self {
            Field::GatewayTime => {
                ui.monospace(format_time(
                    envelope.header.gateway_timestamp,
                    local,
                ));
            }
            Field::JournalTime => {
                let text = envelope
                    .message
                    .timestamp()
                    .map(|event| format_time(event, local))
                    .unwrap_or_default();
                ui.monospace(text);
            }
            Field::Delta => {
                let text = envelope
                    .message
                    .timestamp()
                    .map(|event| {
                        format_delta(envelope.header.gateway_timestamp - event)
                    })
                    .unwrap_or_default();
                ui.monospace(text);
            }
            Field::Live => {
                // A filled dot painted rather than a glyph, so it renders the
                // same whatever the font has.
                let color = if envelope.live {
                    Color32::LIGHT_GREEN
                } else {
                    Color32::DARK_GRAY
                };
                let (rect, _) = ui.allocate_exact_size(
                    egui::vec2(ui.available_width(), ROW_HEIGHT),
                    egui::Sense::hover(),
                );
                ui.painter().circle_filled(rect.center(), 4.0, color);
            }
            Field::Schema | Field::Event => {
                // The tint keys off the very text the cell shows, which is the
                // same string the filter searches, so colour, text and filter
                // are one reading and cannot drift.
                let text = self.text(envelope).unwrap_or_default();
                let tint = category_tint(&text, ui.visuals().dark_mode);
                ui.painter().rect_filled(
                    ui.max_rect(),
                    egui::CornerRadius::ZERO,
                    tint,
                );
                ui.label(text);
            }
            _ => {
                ui.label(self.text(envelope).unwrap_or_default());
            }
        }
    }
}

/// The feed's columns in display order, every one shown but the journal time
/// and the gateway delta
///
/// The live/test dot is offered only when `--test` mixes the two galaxies;
/// without it every message is live and the column would say nothing. The
/// journal time and the delta are diagnostics most runs do not want, so they
/// ship present but off.
fn default_columns(test_enabled: bool) -> Vec<ColumnState> {
    let mut columns = vec![
        ColumnState { field: Field::GatewayTime, visible: true },
        ColumnState { field: Field::JournalTime, visible: false },
        ColumnState { field: Field::Delta, visible: false },
    ];
    if test_enabled {
        columns.push(ColumnState { field: Field::Live, visible: true });
    }
    columns.extend(
        [
            Field::Schema,
            Field::Event,
            Field::System,
            Field::Body,
            Field::Station,
        ]
        .map(|field| ColumnState { field, visible: true }),
    );
    columns
}

/// The scroll offset that puts the message `anchor` back at the top of the
/// view, `frac` pixels above its first line
///
/// The rows are newest-last and their sequence numbers only climb, so the
/// anchored message sits at the first row whose sequence has caught up to it.
/// A message already evicted has no row and lands at the top, which is where
/// reading the oldest kept message leaves you anyway.
fn anchor_offset<T>(
    rows: &[(u64, T)],
    anchor: u64,
    frac: f32,
    pitch: f32,
) -> f32 {
    let top = rows.partition_point(|(seq, _)| *seq < anchor);
    top as f32 * pitch + frac
}

/// The message at the top of a view scrolled to `settled`, and the pixels it
/// sits past, or [`None`] when the view is at the bottom
///
/// At the bottom there is nothing to anchor: sticking to the bottom already
/// follows the newest row, and anchoring would fight it. `max_offset` is the
/// furthest the view can scroll, so the last pixel counts as the bottom within
/// the rounding a row's height allows.
fn top_anchor<T>(
    rows: &[(u64, T)],
    settled: f32,
    max_offset: f32,
    pitch: f32,
) -> Option<(u64, f32)> {
    if rows.is_empty() || settled >= max_offset - 1.0 {
        return None;
    }
    let top = ((settled / pitch).floor() as usize).min(rows.len() - 1);
    Some((rows[top].0, settled - top as f32 * pitch))
}

/// The full rendering of an envelope for the detail pane
fn detail(envelope: &Envelope) -> String {
    let mut text = String::new();
    let _ = writeln!(text, "schema:   {}", envelope.schema_ref);
    let _ = writeln!(text, "live:     {}", envelope.live);
    if let Some(system) = &envelope.star_system {
        let _ = writeln!(text, "system:   {}", system);
    }
    if let Some(version) = &envelope.version {
        let _ = writeln!(text, "version:  {}", version);
    }
    let _ = writeln!(
        text,
        "software: {} {}",
        envelope.header.software_name, envelope.header.software_version
    );
    let _ = writeln!(text, "uploader: {}", envelope.header.uploader_id);
    let _ = writeln!(
        text,
        "gateway:  {}",
        envelope.header.gateway_timestamp.to_rfc3339()
    );
    let _ = writeln!(text);

    match &envelope.message {
        Message::Unmodeled(value) => {
            let _ = writeln!(
                text,
                "{}",
                to_string_pretty(value)
                    .unwrap_or_else(|_| format!("{:?}", value))
            );
        }
        modeled => {
            let _ = writeln!(text, "{:#?}", modeled);
        }
    }
    text
}

/// A status-bar count, coloured by its severity, that opens the log on click
///
/// Errors and warnings both lead only to the log -- their details never go
/// anywhere else -- so the count is the way in rather than a dead end. Returns
/// whether it was clicked.
fn log_link(ui: &mut egui::Ui, text: RichText) -> bool {
    ui.add(egui::Label::new(text).sense(egui::Sense::click()))
        .on_hover_cursor(egui::CursorIcon::PointingHand)
        .on_hover_text("open the log")
        .clicked()
}

/// A compact rendering of how long the retained window spans, e.g. `2m41s`
fn format_span(span: chrono::Duration) -> String {
    let secs = span.num_seconds().max(0);
    if secs >= 3600 {
        format!("{}h{}m", secs / 3600, (secs % 3600) / 60)
    } else if secs >= 60 {
        format!("{}m{}s", secs / 60, secs % 60)
    } else {
        format!("{}s", secs)
    }
}

/// The gateway-minus-event span, e.g. `2m41s`, or `-3s` where the sender's
/// clock runs ahead of the gateway's
///
/// [`format_span`] floors at zero, so the sign is carried here: without it a
/// clock skew that makes the gateway look earlier than the event would read as
/// `0s` rather than showing that it happened.
fn format_delta(delta: chrono::Duration) -> String {
    if delta.num_seconds() < 0 {
        format!("-{}", format_span(-delta))
    } else {
        format_span(delta)
    }
}

/// A panel's header: its title, and a button beside it to close the panel.
///
/// Returns whether that button was clicked, left for the caller to act on once
/// it has finished borrowing the panel's own contents.
fn panel_header(ui: &mut egui::Ui, title: RichText, close: &str) -> bool {
    ui.horizontal(|ui| {
        ui.label(title);
        ui.button(close).clicked()
    })
    .inner
}

/// The colour a log line is drawn in, chosen by its level.
fn level_color(level: Level) -> Color32 {
    match level {
        Level::ERROR => Color32::LIGHT_RED,
        Level::WARN => Color32::from_rgb(255, 200, 80),
        Level::INFO => Color32::LIGHT_GREEN,
        Level::DEBUG => Color32::LIGHT_BLUE,
        Level::TRACE => Color32::GRAY,
    }
}

/// The colour group a schema family or event name belongs to
///
/// Grouping is what lets a run of kindred messages read as one band: every name
/// in a group takes the one hue (see [`category_tint`]), and the cell's own
/// text says which member it is. Matching is by substring and case-blind, so a
/// PascalCase event and a lower-case schema family fall in alike --
/// `FSSDiscoveryScan`, `SAAScanComplete` and the `fssdiscoveryscan` schema all
/// group on a scan, while the `journal` schema keeps a colour of its own. The
/// first keyword found, in the order below, places the name; the exploration
/// words come before `nav` so a beacon scan reads as a scan, and `fss` catches
/// the scanner schemas that name no scan outright (`fssbodysignals`). A name no
/// keyword matches is `"other"`: an event or schema the crate cannot place,
/// which [`category_tint`] paints like the unreadable. The table is a starting
/// set, meant to grow as the feed turns up more.
fn category_group(key: &str) -> &'static str {
    const KEYWORDS: &[(&str, &str)] = &[
        ("journal", "journal"),
        ("fss", "exploration"),
        ("scan", "exploration"),
        ("signal", "exploration"),
        ("codex", "exploration"),
        ("discovery", "exploration"),
        ("jump", "travel"),
        ("dock", "travel"),
        ("supercruise", "travel"),
        ("location", "travel"),
        ("liftoff", "travel"),
        ("touchdown", "travel"),
        ("approach", "travel"),
        ("nav", "travel"),
        ("interdict", "combat"),
        ("bounty", "combat"),
        ("kill", "combat"),
        ("died", "combat"),
        ("damage", "combat"),
        ("attack", "combat"),
        ("market", "trade"),
        ("trade", "trade"),
        ("commodity", "trade"),
        ("mission", "missions"),
        ("engineer", "engineering"),
        ("module", "outfitting"),
        ("shipyard", "outfitting"),
        ("outfitting", "outfitting"),
        ("powerplay", "powerplay"),
        ("wing", "social"),
        ("crew", "social"),
        ("squadron", "social"),
        ("friends", "social"),
    ];
    let lower = key.to_ascii_lowercase();
    for (needle, group) in KEYWORDS {
        if lower.contains(needle) {
            return group;
        }
    }
    "other"
}

/// The hue a colour group is drawn at, or [`None`] for one with no colour
///
/// The named clusters take fixed, well-spread hues -- combat red, exploration
/// green, trade blue -- so no two read as the same basic colour and the ones
/// that turn up together (a scan and a commodity) sit a long way apart on the
/// wheel. A group that is none of them (`"other"`) has no hue; the caller
/// paints it with [`UNKNOWN_TINT`] instead.
fn group_hue(group: &str) -> Option<f32> {
    const CLUSTERS: &[(&str, f32)] = &[
        ("combat", 0.00),      // red
        ("engineering", 0.08), // orange
        ("outfitting", 0.15),  // amber
        ("exploration", 0.33), // green
        ("social", 0.44),      // teal
        ("travel", 0.50),      // cyan
        ("journal", 0.60),     // azure
        ("trade", 0.70),       // blue
        ("missions", 0.82),    // violet
        ("powerplay", 0.92),   // magenta
    ];
    CLUSTERS.iter().find(|(name, _)| *name == group).map(|(_, hue)| *hue)
}

/// The tint for anything the crate cannot place
///
/// An unreadable `Unmodeled` payload and a schema family under no known
/// category share the one loud red, so the unknown is what catches the eye
/// rather than something to hunt for. Louder and more opaque than the muted
/// cluster tints, and the same on either theme. (sRGBA 235, 45, 45 at ~45%,
/// premultiplied so it can be `const`.)
const UNKNOWN_TINT: Color32 =
    Color32::from_rgba_premultiplied(106, 20, 20, 115);

/// A background tint for a category cell, keyed by its [group](category_group)
///
/// Kindred schemas and events land on one hue, so a run of them reads as a
/// single band; the cell's own text says which member it is. A group with no
/// hue -- the unreadable and the uncategorised alike -- gets [`UNKNOWN_TINT`].
/// The cluster tints are kept muted and keyed off the theme, tinting the row's
/// stripe rather than fighting it.
fn category_tint(key: &str, dark_mode: bool) -> Color32 {
    let Some(hue) = group_hue(category_group(key)) else {
        return UNKNOWN_TINT;
    };
    let hsva = if dark_mode {
        egui::ecolor::Hsva::new(hue, 0.70, 0.95, 0.22)
    } else {
        egui::ecolor::Hsva::new(hue, 0.85, 0.55, 0.30)
    };
    Color32::from(hsva)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feed::DEFAULT_CAPACITY;
    use crate::log_pane::LogBuffer;
    use chrono::Utc;
    use eddn::Header;
    use serde_json::{from_value, json};
    use std::sync::mpsc;

    fn envelope(live: bool) -> Envelope {
        Envelope {
            schema_ref: "https://eddn.edcd.io/schemas/journal/1".to_owned(),
            header: Header {
                gateway_timestamp: Utc::now(),
                software_name: "test".to_owned(),
                software_version: "0".to_owned(),
                uploader_id: "cmdr".to_owned(),
            },
            message: Message::Unmodeled(json!({})),
            live,
            version: None,
            star_system: None,
            station: None,
            body: None,
        }
    }

    #[test]
    fn test_mode_starts_on_the_test_galaxy() {
        // --test is asked for to watch test data, so that is what shows first.
        let (_tx, rx) = mpsc::channel();
        let app = App::new(rx, LogBuffer::default(), true, DEFAULT_CAPACITY);
        assert_eq!(app.view, Galaxy::TEST);
    }

    #[test]
    fn without_test_mode_every_row_shows() {
        // Only live events arrive, so the galaxy filter admits all of them.
        let (_tx, rx) = mpsc::channel();
        let app = App::new(rx, LogBuffer::default(), false, DEFAULT_CAPACITY);
        assert_eq!(app.view, Galaxy::ALL);
    }

    #[test]
    fn drain_moves_updates_into_the_feed() {
        let (tx, rx) = mpsc::channel();
        let mut app =
            App::new(rx, LogBuffer::default(), false, DEFAULT_CAPACITY);

        tx.send(Box::new(envelope(true))).unwrap();
        tx.send(Box::new(envelope(false))).unwrap();
        app.drain();

        // Both envelopes counted and kept, and the arrivals recorded in the
        // cadence. Unreadable messages no longer travel this channel; the log
        // alone counts them.
        assert_eq!(app.feed.received(), 2);
        assert_eq!(app.feed.retained(), 2);
        assert!(app.cadence.last().is_some());
        assert!(app.cadence.rate() > 0.0);
    }

    #[test]
    fn a_dropped_worker_channel_stops_the_stream() {
        let (tx, rx) = mpsc::channel::<Update>();
        let mut app =
            App::new(rx, LogBuffer::default(), false, DEFAULT_CAPACITY);

        drop(tx);
        app.drain();

        assert!(app.stream_ended);
    }

    #[test]
    fn a_single_message_reads_live_not_connecting() {
        let (tx, rx) = mpsc::channel();
        let mut app =
            App::new(rx, LogBuffer::default(), false, DEFAULT_CAPACITY);

        // Connecting only holds while nothing has arrived.
        assert_eq!(app.connection(), Connection::Connecting);

        // One message is enough to leave it: record() sets the last arrival, so
        // the connection reads live even before there are gaps to judge a stall.
        tx.send(Box::new(envelope(true))).unwrap();
        app.drain();
        assert_eq!(app.connection(), Connection::Online);
    }

    #[test]
    fn connection_reflects_stream_and_cadence() {
        let (_tx, rx) = mpsc::channel::<Update>();
        let mut app =
            App::new(rx, LogBuffer::default(), false, DEFAULT_CAPACITY);

        // No message yet: still coming up.
        assert_eq!(app.connection(), Connection::Connecting);

        // A steady 1s cadence, last message just now: live.
        let steady = [1.0, 1.0, 1.0, 1.0];
        app.cadence = Cadence::from_parts(steady, Some(Instant::now()));
        assert_eq!(app.connection(), Connection::Online);

        // The same cadence gone quiet for 5s: an outlier, so stalling.
        app.cadence = Cadence::from_parts(
            steady,
            Some(Instant::now() - Duration::from_secs(5)),
        );
        assert_eq!(app.connection(), Connection::Stalling);

        // A dead stream wins over everything else.
        app.stream_ended = true;
        assert_eq!(app.connection(), Connection::Stopped);
    }

    #[test]
    fn nav_route_system_shows_endpoints() {
        use eddn::Header;
        use elite_journal::entry::{Entry, Event};

        let entry: Entry<Event> = from_value(json!({
            "timestamp": "2020-01-01T00:00:00Z",
            "event": "NavRoute",
            "Route": [
                {"StarSystem": "Sol", "SystemAddress": 1,
                 "StarPos": [0.0, 0.0, 0.0], "StarClass": "G"},
                {"StarSystem": "Wolf 359", "SystemAddress": 2,
                 "StarPos": [1.0, 1.0, 1.0], "StarClass": "M"},
                {"StarSystem": "Sirius", "SystemAddress": 3,
                 "StarPos": [2.0, 2.0, 2.0], "StarClass": "A"}
            ]
        }))
        .unwrap();
        let envelope = Envelope {
            schema_ref: "https://eddn.edcd.io/schemas/navroute/1".to_owned(),
            header: Header {
                gateway_timestamp: Utc::now(),
                software_name: "t".to_owned(),
                software_version: "0".to_owned(),
                uploader_id: "u".to_owned(),
            },
            message: Message::Journal(entry),
            live: true,
            version: None,
            star_system: None,
            station: None,
            body: None,
        };

        assert_eq!(system_text(&envelope), "Sol -> Sirius");
    }

    /// Rows as the table holds them, sequence numbers with a stand-in payload
    fn rows(seqs: &[u64]) -> Vec<(u64, ())> {
        seqs.iter().map(|&seq| (seq, ())).collect()
    }

    #[test]
    fn anchor_holds_a_message_still_as_the_ring_evicts() {
        let pitch = 22.0;
        // Message 12 sits third from the top, five pixels scrolled past it.
        let before = rows(&[10, 11, 12, 13, 14]);
        assert_eq!(anchor_offset(&before, 12, 5.0, pitch), 2.0 * pitch + 5.0);

        // Two arrivals drop 10 and 11; 12 is now the top row. The offset falls
        // by exactly those two rows, so 12 stays where it was on screen.
        let after = rows(&[12, 13, 14, 15, 16]);
        assert_eq!(anchor_offset(&after, 12, 5.0, pitch), 5.0);
    }

    #[test]
    fn anchor_to_an_evicted_message_lands_at_the_top() {
        // Message 8 is already gone; nothing to hold, so the view sits at the
        // oldest kept row.
        let after = rows(&[12, 13, 14, 15, 16]);
        assert_eq!(anchor_offset(&after, 8, 3.0, 22.0), 3.0);
    }

    #[test]
    fn top_anchor_reads_the_top_row_and_its_remainder() {
        let pitch = 22.0;
        let view = rows(&[12, 13, 14, 15, 16]);
        // Scrolled 49px down a 200px-tall content: two whole rows and 5 over,
        // so message 14 is at the top with a five-pixel remainder.
        assert_eq!(top_anchor(&view, 49.0, 200.0, pitch), Some((14, 5.0)));
    }

    #[test]
    fn top_anchor_is_none_within_a_pixel_of_the_bottom() {
        let view = rows(&[12, 13, 14, 15, 16]);
        // At and just shy of the furthest scroll: tailing, no anchor.
        assert_eq!(top_anchor(&view, 100.0, 100.0, 22.0), None);
        assert_eq!(top_anchor(&view, 99.5, 100.0, 22.0), None);
        // A row up from the bottom: scrolled away, so anchored.
        assert!(top_anchor(&view, 95.0, 100.0, 22.0).is_some());
    }

    #[test]
    fn top_anchor_is_none_for_an_empty_view() {
        assert_eq!(top_anchor::<()>(&[], 0.0, 0.0, 22.0), None);
    }

    #[test]
    fn delta_shows_the_span_and_its_sign() {
        use chrono::Duration;
        // Gateway later than the event: how long it took to arrive.
        assert_eq!(format_delta(Duration::seconds(161)), "2m41s");
        assert_eq!(format_delta(Duration::seconds(0)), "0s");
        // A sender's clock running ahead reads as negative, not floored to 0s.
        assert_eq!(format_delta(Duration::seconds(-3)), "-3s");
    }

    #[test]
    fn the_delta_column_ships_present_but_off() {
        let column = default_columns(false)
            .into_iter()
            .find(|c| c.field == Field::Delta)
            .expect("delta column offered");
        assert!(!column.visible);
    }

    #[test]
    fn a_category_tint_bands_a_group_and_moves_with_the_theme() {
        // One key, one colour, every frame and run.
        assert_eq!(
            category_tint("FSDJump", true),
            category_tint("FSDJump", true)
        );
        // Same group, same colour: a run of travel reads as one band, even
        // across events that share no name.
        assert_eq!(
            category_tint("FSDJump", true),
            category_tint("Docked", true)
        );
        // Distinct clusters take distinct hues, and the ones that share a feed
        // stay well apart: a commodity is blue, a scan green, a jump cyan.
        assert_ne!(
            category_tint("FSDJump", true),
            category_tint("Commodity", true)
        );
        assert_ne!(
            category_tint("Commodity", true),
            category_tint("Scan", true)
        );
        // The theme moves the colour, matching the light/dark split.
        assert_ne!(
            category_tint("FSDJump", true),
            category_tint("FSDJump", false)
        );
    }

    #[test]
    fn kindred_names_share_a_colour_group() {
        // A bare event and its dressed-up kin share a substring, not a prefix,
        // yet group together all the same.
        assert_eq!(category_group("Scan"), category_group("FSSDiscoveryScan"));
        assert_eq!(category_group("Scan"), category_group("SAAScanComplete"));
        // A jump and a dock share no word at all, but both are travel.
        assert_eq!(category_group("FSDJump"), category_group("Docked"));
        // Unrelated kinds keep their distance.
        assert_ne!(category_group("Scan"), category_group("Bounty"));
    }

    #[test]
    fn schema_families_take_the_cluster_colours() {
        // A lower-case schema family colours like its kindred event: the
        // scanner schemas green, a commodity schema like a commodity.
        assert_eq!(category_group("fssdiscoveryscan"), category_group("Scan"));
        assert_eq!(category_group("fssbodysignals"), category_group("Scan"));
        assert_eq!(category_group("commodity"), category_group("Commodity"));
        // The journal schema keeps a colour of its own, apart from the scans
        // it carries.
        assert_ne!(
            category_tint("journal", true),
            category_tint("fssdiscoveryscan", true)
        );
    }

    #[test]
    fn the_unplaceable_share_one_loud_red() {
        // An unreadable payload and a schema under no category get the one
        // red, so the unknown is the thing that catches the eye.
        assert_eq!(category_tint("Unmodeled", true), UNKNOWN_TINT);
        assert_eq!(category_tint("some_future_schema", true), UNKNOWN_TINT);
        // It is red, and unlike any placed cluster.
        assert!(UNKNOWN_TINT.r() > UNKNOWN_TINT.g());
        assert!(UNKNOWN_TINT.r() > UNKNOWN_TINT.b());
        assert_ne!(category_tint("Scan", true), UNKNOWN_TINT);
    }

    #[test]
    fn a_time_reads_utc_with_a_z_and_local_without() {
        let when: DateTime<Utc> =
            "2026-08-20T12:00:00Z".parse().expect("fixture parses");
        assert_eq!(format_time(when, false), "2026-08-20 12:00:00Z");
        // Local's digits depend on the machine's zone; the dropped Z does not.
        assert!(!format_time(when, true).ends_with('Z'));
    }
}
