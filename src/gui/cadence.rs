//! The feed's arrival timing: the rate, the age of the last message, and
//! whether the quiet since it counts as a stall
//!
//! Everything read off the clock as messages land lives here, so the app holds
//! one [`Cadence`] rather than four fields it must keep in step. The rate drives
//! the status bar; the stall statistics drive the connection dot.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// How long an arrival counts toward the rate shown
const RATE_WINDOW: Duration = Duration::from_secs(5);

/// The shortest span the rate is averaged over
///
/// Until [`RATE_WINDOW`] of history exists the rate is averaged over how long
/// the feed has actually been running, which starts near zero. This floors that
/// span at one second: the reading fills smoothly from zero to the true rate
/// over the first second rather than dividing a message or two by almost
/// nothing and jumping around before it settles.
const RATE_WINDOW_MIN: Duration = Duration::from_secs(1);

/// How many recent inter-arrival gaps feed the stall statistics
///
/// The mean and spread are taken over the last this-many inter-arrival gaps --
/// a count, not a time window, so a slow schema still gathers enough samples to
/// judge by. Old gaps fall off the front as new ones arrive, so the statistics
/// track the feed's current cadence.
const INTERVAL_SAMPLES: usize = 128;

/// How many standard deviations past the mean gap counts as a stall
///
/// The dot goes yellow once the current silence runs more than this many
/// standard deviations beyond the recent inter-arrival mean -- a z-score
/// outlier test, so how far past normal counts as a stall follows the feed's
/// own regularity rather than a fixed number. A float: raise it to tolerate
/// burstier feeds, lower it to warn sooner.
const STALL_SIGMAS: f64 = 4.0;

/// The shortest silence that can count as a stall, whatever the statistics say
///
/// The outlier test degenerates when arrivals are near-metronomic: the spread
/// collapses toward zero and the threshold falls to about the mean, so ordinary
/// sub-second jitter reads as a stall. This floor keeps a stall a real pause,
/// so the same test drives the status dot and the feed's gap marks without
/// either tripping on a burst of near-simultaneous messages.
const MIN_GAP: Duration = Duration::from_secs(1);

/// How many one-second rate samples the sparkline keeps -- about three minutes
const RATE_SAMPLES: usize = 180;

/// The feed's arrival timing and the statistics the stall warning reads
#[derive(Default)]
pub struct Cadence {
    /// When each of the last [`RATE_WINDOW`] of arrivals came in, for the rate.
    arrivals: VecDeque<Instant>,
    /// The most recent inter-arrival gaps, in seconds, bounded to
    /// [`INTERVAL_SAMPLES`]. Their mean and spread decide a stall.
    intervals: VecDeque<f64>,
    /// When the most recent message arrived, for the age shown and the stall.
    last: Option<Instant>,
    /// When the first message ever arrived, for scaling the rate window.
    first: Option<Instant>,
    /// One-per-second samples of the rate and whether the feed was stalled at
    /// that moment, newest last, bounded to [`RATE_SAMPLES`]. Feeds the
    /// status-bar sparkline and its stall shading.
    samples: VecDeque<(f32, bool)>,
    /// When the rate was last sampled into `samples`.
    last_sample: Option<Instant>,
}

impl Cadence {
    /// Record a message arriving at `now`, returning the gap it followed when
    /// that gap was a connection stall
    ///
    /// Files the arrival for the rate and, once there is a previous one to
    /// measure from, the gap since it for the stall statistics -- bounded to
    /// the most recent samples so the mean and spread follow the feed's
    /// current cadence. The gap is judged against the distribution as it stood
    /// before this arrival -- the same outlier test the status dot applies to
    /// the ongoing silence -- and returned when it is an outlier, so a resumed
    /// feed can be marked where it stalled.
    pub fn record(&mut self, now: Instant) -> Option<Duration> {
        self.arrivals.push_back(now);
        let gap = self.last.map(|prev| now - prev);
        let connection_gap = gap.filter(|g| self.is_stall(g.as_secs_f64()));
        if let Some(g) = gap {
            self.intervals.push_back(g.as_secs_f64());
            while self.intervals.len() > INTERVAL_SAMPLES {
                self.intervals.pop_front();
            }
        }
        self.last = Some(now);
        self.first.get_or_insert(now);
        connection_gap
    }

    /// Advance the cadence to `now`: age out old arrivals and sample the rate
    ///
    /// Called every frame, not only on arrival, so the rate decays toward zero
    /// through a silence rather than freezing at the value it last held, and so
    /// the sparkline gets a sample about once a second whether or not messages
    /// are arriving.
    pub fn tick(&mut self, now: Instant) {
        let cutoff = now - RATE_WINDOW;
        while self.arrivals.front().is_some_and(|at| *at < cutoff) {
            self.arrivals.pop_front();
        }
        let due = self.last_sample.map_or(true, |at| {
            now.duration_since(at) >= Duration::from_secs(1)
        });
        if due {
            self.last_sample = Some(now);
            self.samples.push_back((self.rate() as f32, self.stalled()));
            while self.samples.len() > RATE_SAMPLES {
                self.samples.pop_front();
            }
        }
    }

    /// The recent one-per-second (rate, stalled) samples, oldest first
    pub fn samples(&self) -> &VecDeque<(f32, bool)> {
        &self.samples
    }

    /// When the most recent message arrived, if any has
    pub fn last(&self) -> Option<Instant> {
        self.last
    }

    /// Messages a second
    ///
    /// Averaged over how long the feed has been running, growing from the first
    /// message until it reaches [`RATE_WINDOW`]. Dividing by the full window
    /// before that much history exists understates the rate at startup.
    pub fn rate(&self) -> f64 {
        let Some(first) = self.first else {
            return 0.0;
        };
        let window =
            first.elapsed().clamp(RATE_WINDOW_MIN, RATE_WINDOW).as_secs_f64();
        self.arrivals.len() as f64 / window
    }

    /// Whether the silence since the last arrival counts as a stall
    ///
    /// The same test the feed's gap marks use, so the dot and the marks agree.
    /// [`false`] before any message arrives; see [`Cadence::is_stall`].
    pub fn stalled(&self) -> bool {
        self.last
            .is_some_and(|last| self.is_stall(last.elapsed().as_secs_f64()))
    }

    /// Whether a silence of `secs` counts as a stall
    ///
    /// At least [`MIN_GAP`] and a z-score outlier past the recent inter-arrival
    /// distribution -- the mean plus [`STALL_SIGMAS`] standard deviations. The
    /// floor keeps the degenerate near-zero-spread case from flagging
    /// sub-second jitter. [`false`] until two gaps give a spread.
    fn is_stall(&self, secs: f64) -> bool {
        secs >= MIN_GAP.as_secs_f64()
            && self.stats().is_some_and(|(mean, stddev)| {
                secs > mean + STALL_SIGMAS * stddev
            })
    }

    /// Mean and sample standard deviation of the recent inter-arrival gaps
    ///
    /// [`None`] until at least two gaps have been seen, since a spread needs
    /// two points.
    fn stats(&self) -> Option<(f64, f64)> {
        let n = self.intervals.len();
        if n < 2 {
            return None;
        }
        let mean = self.intervals.iter().sum::<f64>() / n as f64;
        let variance =
            self.intervals.iter().map(|gap| (gap - mean).powi(2)).sum::<f64>()
                / (n as f64 - 1.0);
        Some((mean, variance.sqrt()))
    }
}

#[cfg(test)]
impl Cadence {
    /// Build a cadence with known statistics, for tests that need a specific
    /// rate or stall state without waiting on the wall clock.
    pub fn from_parts(
        intervals: impl IntoIterator<Item = f64>,
        last: Option<Instant>,
    ) -> Self {
        let mut cadence = Cadence::default();
        cadence.intervals.extend(intervals);
        cadence.last = last;
        cadence
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_scales_until_the_window_saturates() {
        let now = Instant::now();
        let mut cadence = Cadence::default();
        for _ in 0..30 {
            cadence.arrivals.push_back(now);
        }

        // ~1s into the feed: averaged over the second actually observed, not the
        // full window, so 30 messages read as ~30/s rather than 30/5 = 6/s.
        cadence.first = Some(now - Duration::from_secs(1));
        let ramping = cadence.rate();
        assert!((25.0..=31.0).contains(&ramping), "ramping rate was {ramping}");

        // Past RATE_WINDOW: averaged over the full window, so 30 / 5 = 6/s.
        cadence.first = Some(now - Duration::from_secs(30));
        let saturated = cadence.rate();
        assert!(
            (5.5..=6.5).contains(&saturated),
            "saturated rate was {saturated}"
        );
    }

    #[test]
    fn record_files_the_gap_between_arrivals() {
        let now = Instant::now();
        let mut cadence = Cadence::default();

        // The first arrival has nothing to measure from, so no gap yet.
        cadence.record(now);
        assert!(cadence.last().is_some());
        assert_eq!(cadence.stats(), None);

        // The second yields one gap; a spread still needs a second gap.
        cadence.record(now + Duration::from_secs(1));
        assert_eq!(cadence.intervals.len(), 1);
        assert_eq!(cadence.stats(), None);
    }

    #[test]
    fn intervals_are_bounded_to_the_sample_count() {
        let now = Instant::now();
        let mut cadence = Cadence::default();
        for i in 0..(INTERVAL_SAMPLES + 50) {
            cadence.record(now + Duration::from_millis(i as u64));
        }
        assert_eq!(cadence.intervals.len(), INTERVAL_SAMPLES);
    }

    #[test]
    fn stats_are_the_mean_and_sample_stddev() {
        let cadence = Cadence::from_parts([1.0, 3.0], None);

        // mean 2; sample variance ((1)^2 + (1)^2) / (2 - 1) = 2, so sd = √2.
        let (mean, stddev) = cadence.stats().unwrap();
        assert!((mean - 2.0).abs() < 1e-9, "mean was {mean}");
        assert!((stddev - 2.0_f64.sqrt()).abs() < 1e-9, "stddev was {stddev}");
    }

    #[test]
    fn a_steady_feed_gone_quiet_reads_as_stalled() {
        // A steady 1s cadence: mean 1, zero spread, so any real overrun is an
        // outlier at once.
        let steady = [1.0, 1.0, 1.0, 1.0];

        let live = Cadence::from_parts(steady, Some(Instant::now()));
        assert!(!live.stalled());

        let quiet = Cadence::from_parts(
            steady,
            Some(Instant::now() - Duration::from_secs(5)),
        );
        assert!(quiet.stalled());
    }

    #[test]
    fn nothing_is_stalled_before_two_gaps() {
        assert!(!Cadence::default().stalled());
        let one_gap = Cadence::from_parts([1.0], Some(Instant::now()));
        assert!(!one_gap.stalled());
    }

    #[test]
    fn record_returns_a_gap_only_when_it_is_an_outlier() {
        let mut cadence = Cadence::default();
        let start = Instant::now();
        // A steady one-second cadence: mean 1, no spread.
        for i in 0..5u64 {
            assert_eq!(cadence.record(start + Duration::from_secs(i)), None);
        }

        // An eleven-second jump is an outlier and comes back as the gap.
        let resumed = start + Duration::from_secs(15);
        assert_eq!(cadence.record(resumed), Some(Duration::from_secs(11)));

        // A one-second step after it is ordinary again.
        assert_eq!(cadence.record(resumed + Duration::from_secs(1)), None);
    }

    #[test]
    fn a_sub_second_gap_is_never_a_stall() {
        let mut cadence = Cadence::default();
        let start = Instant::now();
        // A fast, near-metronomic cadence: 30ms gaps, so the spread is tiny and
        // the outlier threshold sits just above the mean.
        for i in 0..10u64 {
            cadence.record(start + Duration::from_millis(30 * i));
        }

        // A 300ms gap is many times the mean -- a statistical outlier -- but
        // under the floor, so it is jitter, not a connection gap.
        let bumped = start + Duration::from_millis(30 * 9 + 300);
        assert_eq!(cadence.record(bumped), None);
    }
}
