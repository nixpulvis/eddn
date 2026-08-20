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
}

impl Cadence {
    /// Record a message arriving at `now`
    ///
    /// Files the arrival for the rate and, once there is a previous one to
    /// measure from, the gap since it for the stall statistics -- bounded to
    /// the most recent samples so the mean and spread follow the feed's current
    /// cadence.
    pub fn record(&mut self, now: Instant) {
        self.arrivals.push_back(now);
        if let Some(prev) = self.last {
            self.intervals.push_back((now - prev).as_secs_f64());
            while self.intervals.len() > INTERVAL_SAMPLES {
                self.intervals.pop_front();
            }
        }
        self.last = Some(now);
        self.first.get_or_insert(now);
    }

    /// Drop arrivals that have aged out of the rate window as of `now`
    ///
    /// Called every frame, not only on arrival, so the rate decays toward zero
    /// through a silence rather than freezing at the value it last held.
    pub fn prune(&mut self, now: Instant) {
        let cutoff = now - RATE_WINDOW;
        while self.arrivals.front().is_some_and(|at| *at < cutoff) {
            self.arrivals.pop_front();
        }
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

    /// Whether the silence since the last arrival is a statistical outlier
    ///
    /// The gap since the last message is compared to the recent inter-arrival
    /// mean plus [`STALL_SIGMAS`] standard deviations -- a z-score outlier
    /// test, so how far past normal counts as a stall follows the feed's own
    /// regularity rather than a fixed number. [`false`] before any message and
    /// until two gaps have been seen, since a spread needs two points.
    pub fn stalled(&self) -> bool {
        let (Some(last), Some((mean, stddev))) = (self.last, self.stats())
        else {
            return false;
        };
        last.elapsed().as_secs_f64() > mean + STALL_SIGMAS * stddev
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
        let variance = self
            .intervals
            .iter()
            .map(|gap| (gap - mean).powi(2))
            .sum::<f64>()
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
}
