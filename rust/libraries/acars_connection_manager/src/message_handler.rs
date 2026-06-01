// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

//! Per-protocol message handler: decode -> freq accounting -> skew check
//! -> dedupe -> rewrite -> fan-out.
//!
//! State shape (post-PR8):
//! - Counters: `Arc<AtomicU64>`; no locking on the hot path.
//! - Frequency table: `Arc<std::sync::Mutex<HashMap<String, u64>>>`; one
//!   short critical section per message, sorted only when stats are
//!   printed.
//! - Dedupe cache: `Arc<std::sync::Mutex<HashMap<u64, u64>>>` keyed on
//!   message hash, value is the cleared message time in unix seconds.
//!   Lookups are O(1); pruning still walks the map but only every
//!   `dedupe_window` seconds.
//!
//! We use `std::sync::Mutex` rather than `tokio::sync::Mutex` because no
//! `.await` is ever held across a guard in this file; sync mutexes are
//! materially cheaper for these short critical sections.

use acars_config::Input;
use acars_vdlm2_parser::{AcarsVdlm2Message, DecodeMessage, MessageResult};
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::env;
use std::fmt::Write as _;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;
use tokio::sync::mpsc::Receiver;
use tokio::time::{Duration, sleep};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace};

/// One (frequency, count) pair as rendered in stats output. Kept as a
/// named struct so the snapshot-and-sort step in [`print_stats`] stays
/// readable.
pub struct FrequencyCount {
    pub freq: String,
    pub count: u64,
}

/// Hash-based dedupe configuration. Presence on
/// [`MessageHandlerConfig::dedupe`] enables dedupe; absence disables it.
#[derive(Clone, Debug)]
pub struct DedupeOpts {
    /// Window within which a repeat hash is considered a duplicate.
    pub window: Duration,
}

/// Periodic stats configuration. Always present (we always print
/// totals); the `verbose` flag adds the per-frequency table.
#[derive(Clone, Debug)]
pub struct StatsOpts {
    /// Interval between stats lines. Already in seconds.
    pub every: Duration,
    /// Include per-frequency breakdown in the periodic stats line.
    pub verbose: bool,
}

/// Per-protocol message handler configuration. Mirrors the relevant
/// subset of CLI flags in [`acars_config::Input`]; instances are built
/// from CLI via [`Self::new`] or directly constructed in tests.
#[derive(Clone, Debug)]
pub struct MessageHandlerConfig {
    /// Stamp the message's `app` field with `acars_router` + crate
    /// version before forwarding.
    pub add_proxy_id: bool,
    /// Dedupe configuration. `None` disables dedupe entirely.
    pub dedupe: Option<DedupeOpts>,
    /// Reject messages whose timestamp is more than this far in the
    /// past or future relative to local wall-clock.
    pub skew_window: Duration,
    /// Free-form label for log lines (typically `"acars"` / `"vdlm2"`).
    pub queue_type: String,
    /// `Some(name)` rewrites `station_id`/`vdl2.station` to `name`;
    /// `None` leaves the field untouched.
    pub station_name_override: Option<String>,
    /// Periodic stats printer configuration.
    pub stats: StatsOpts,
}

impl MessageHandlerConfig {
    pub(crate) fn new(args: &Input, queue_type: &str) -> Self {
        Self {
            add_proxy_id: args.add_proxy_id,
            dedupe: args.enable_dedupe.then(|| DedupeOpts {
                window: Duration::from_secs(args.dedupe_window),
            }),
            skew_window: Duration::from_secs(args.skew_window),
            queue_type: queue_type.to_string(),
            station_name_override: args.override_station_name.clone(),
            stats: StatsOpts {
                every: Duration::from_secs(args.stats_every * 60),
                verbose: args.stats_verbose,
            },
        }
    }

    #[tracing::instrument(
        name = "message_handler",
        skip_all,
        fields(proto = %self.queue_type),
    )]
    pub(crate) async fn watch_message_queue(
        self,
        mut input_queue: Receiver<String>,
        output_queue: broadcast::Sender<AcarsVdlm2Message>,
        shutdown: CancellationToken,
    ) {
        let total_messages_processed = Arc::new(AtomicU64::new(0));
        let total_messages_since_last = Arc::new(AtomicU64::new(0));
        let all_frequencies_logged: Arc<Mutex<HashMap<String, u64>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let dedupe_cache: Arc<Mutex<HashMap<u64, u64>>> = Arc::new(Mutex::new(HashMap::new()));

        let stats_every: Duration = self.stats.every;
        let version: &str = env!("CARGO_PKG_VERSION");

        // Spawn the periodic stats printer. It reads (relaxed) atomics and
        // takes a short snapshot of the frequency table; no contention on
        // the hot path.
        {
            let total_all = Arc::clone(&total_messages_processed);
            let total_since = Arc::clone(&total_messages_since_last);
            let freqs = self
                .stats
                .verbose
                .then(|| Arc::clone(&all_frequencies_logged));
            let queue_type = self.queue_type.clone();
            let stats_shutdown = shutdown.clone();
            tokio::spawn(async move {
                print_stats(
                    total_all,
                    total_since,
                    freqs,
                    stats_every,
                    &queue_type,
                    stats_shutdown,
                )
                .await;
            });
        }

        // Spawn the dedupe-cache pruner only when dedupe is enabled.
        if let Some(dedupe) = self.dedupe.as_ref() {
            let cache = Arc::clone(&dedupe_cache);
            let dedupe_window = dedupe.window;
            let queue_type = self.queue_type.clone();
            let dedupe_shutdown = shutdown.clone();
            tokio::spawn(async move {
                clean_up_dedupe_cache(cache, dedupe_window, &queue_type, dedupe_shutdown).await;
            });
        }

        loop {
            let message_content = tokio::select! {
                () = shutdown.cancelled() => {
                    info!(
                        "[Message Handler {}] shutdown requested; closing input loop",
                        self.queue_type
                    );
                    return;
                }
                msg = input_queue.recv() => match msg {
                    Some(m) => m,
                    None => return,
                },
            };

            total_messages_processed.fetch_add(1, Ordering::Relaxed);
            total_messages_since_last.fetch_add(1, Ordering::Relaxed);

            let parsed: MessageResult<AcarsVdlm2Message> = message_content.decode_message();
            trace!("[Message Handler {}] GOT: {:?}", self.queue_type, parsed);
            let mut message = match parsed {
                Ok(m) => m,
                Err(e) => {
                    error!(
                        "[Message Handler {}] Failed to parse received message: {e}\nReceived: {message_content}",
                        self.queue_type
                    );
                    continue;
                }
            };

            let current_time = unix_time_secs();

            // Frequency accounting. The previous implementation had this
            // block copy-pasted five times across the five message
            // variants; the helper collapses all of them into one
            // HashMap::entry call.
            if let Some(freq) = message_frequency(&message) {
                if let Ok(mut map) = all_frequencies_logged.lock() {
                    *map.entry(freq).or_insert(0) += 1;
                }
            }

            let Some(mut message_time) = message.get_time() else {
                error!(
                    "[Message Handler {}] Message has no timestamp field. Skipping message.",
                    self.queue_type
                );
                continue;
            };

            let skew = self.skew_window;
            if message_time > current_time {
                if Duration::from_secs_f64(message_time - current_time) > skew {
                    error!(
                        "[Message Handler {}] Message is from the future. Skipping. Current time {current_time}, Message time {message_time}. Difference {}",
                        self.queue_type,
                        message_time - current_time,
                    );
                    continue;
                }
                trace!(
                    "[Message Handler {}] Message is from the future. Current time {current_time}, Message time {message_time}. Difference {}",
                    self.queue_type,
                    message_time - current_time,
                );
                message_time = current_time;
            } else if Duration::from_secs_f64(current_time - message_time) > skew {
                error!(
                    "[Message Handler {}] Message is {} seconds old. Time in message {message_time}. Skipping message. {}",
                    self.queue_type,
                    current_time - message_time,
                    skew.as_secs()
                );
                continue;
            }

            let hashed_value = match hash_message(message.clone()) {
                Ok(h) => h,
                Err(e) => {
                    error!(
                        "[Message Handler {}] Failed to create hash of message: {e}",
                        self.queue_type
                    );
                    continue;
                }
            };

            if let Some(dedupe) = self.dedupe.as_ref()
                && is_duplicate(
                    &dedupe_cache,
                    hashed_value,
                    Duration::from_secs_f64(message_time).as_secs(),
                    dedupe.window,
                )
            {
                info!(
                    "[Message Handler {}] Message is a duplicate. Skipping message.",
                    self.queue_type
                );
                continue;
            }

            if let Some(station_name) = self.station_name_override.as_deref() {
                trace!(
                    "[Message Handler {}] Overriding station name to {station_name}",
                    self.queue_type
                );
                message.set_station_name(station_name);
            }

            if self.add_proxy_id {
                trace!(
                    "[Message Handler {}] Adding proxy_id to message",
                    self.queue_type
                );
                message.set_proxy_details("acars_router", version);
            }

            debug!("[Message Handler {}] SENDING: {message:?}", self.queue_type);
            trace!(
                "[Message Handler {}] Hashed value: {hashed_value}",
                self.queue_type
            );

            if let Ok(n) = output_queue.send(message) {
                debug!(
                    "[Message Handler {}] Message sent to {n} sender(s)",
                    self.queue_type
                );
            } else {
                // `Err` only when no receivers are subscribed; common
                // during startup (no outputs configured) — debug, not
                // error.
                debug!(
                    "[Message Handler {}] No active senders; message dropped",
                    self.queue_type
                );
            }
        }
    }
}

/// Look up `freq`-style accessor for every supported message variant.
/// Centralises the per-variant accessor knowledge that the message
/// handler previously re-implemented five times.
fn message_frequency(message: &AcarsVdlm2Message) -> Option<String> {
    match message {
        AcarsVdlm2Message::Vdlm2Message(m) => Some(m.vdl2.freq.to_string()),
        AcarsVdlm2Message::AcarsMessage(m) => Some(m.freq.to_string()),
        AcarsVdlm2Message::HfdlMessage(m) => Some(m.hfdl.freq.to_string()),
        AcarsVdlm2Message::ImslMessage(m) => m.freq.clone(),
        AcarsVdlm2Message::IrdmMessage(m) => m.freq.as_ref().map(ToString::to_string),
    }
}

/// Returns `true` if `hash` has been observed within `window_secs` and
/// records the current observation either way. The previous implementation
/// scanned a `VecDeque` linearly; this is an O(1) `HashMap` probe.
fn is_duplicate(
    cache: &Mutex<HashMap<u64, u64>>,
    hash: u64,
    message_time_secs: u64,
    window: Duration,
) -> bool {
    let Ok(mut map) = cache.lock() else {
        // A poisoned mutex means another task panicked while holding the
        // lock. We conservatively treat the current message as new
        // (failing open is preferable to silently dropping every message
        // for the rest of the process lifetime).
        return false;
    };
    let window_secs = window.as_secs();
    match map.get(&hash) {
        Some(&saved) if message_time_secs.saturating_sub(saved) < window_secs => true,
        _ => {
            map.insert(hash, message_time_secs);
            false
        }
    }
}

fn unix_time_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0.0, |n| n.as_secs_f64())
}

/// Render the periodic stats line for a single protocol queue. Pulled
/// out of [`print_stats`] so the formatting can be unit-tested without
/// touching the broadcast / timer machinery.
///
/// `has_counted_freqs` is sticky in the caller: once any frequency has
/// ever been counted for this queue the verbose section keeps printing
/// even if the current snapshot is empty (otherwise the table would
/// disappear during a quiet minute).
#[must_use]
pub fn print_formatted_stats(
    total_all_time: u64,
    total_since_last: u64,
    frequencies: Option<Vec<FrequencyCount>>,
    stats_minutes: u64,
    queue_type: &str,
    has_counted_freqs: bool,
) -> String {
    let mut output = String::new();
    let _ = write!(
        output,
        "{queue_type} in the last {stats_minutes} minute(s):\nTotal messages processed: {total_all_time}\nTotal messages processed since last update: {total_since_last}\n"
    );

    match (frequencies, has_counted_freqs) {
        (Some(freqs), true) => {
            let _ = writeln!(
                output,
                "{queue_type} Frequencies logged since container start:"
            );
            // Counts are bounded in practice by traffic over a single
            // container lifetime; the precision loss for the percentage
            // display is acceptable.
            #[allow(clippy::cast_precision_loss)]
            let total_f = total_all_time as f64;
            for freq in &freqs {
                let percentage = if total_f > 0.0 {
                    #[allow(clippy::cast_precision_loss)]
                    let count_f = freq.count as f64;
                    (count_f / total_f) * 100.0
                } else {
                    0.0
                };
                let _ = write!(
                    output,
                    "\n{queue_type} {}: {}/{total_all_time} ({percentage:.2}%)",
                    freq.freq, freq.count
                );
            }
        }
        (Some(_), false) => {
            let _ = writeln!(output, "{queue_type} No frequencies logged.");
        }
        (None, _) => {}
    }

    output
}

async fn print_stats(
    total_all_time: Arc<AtomicU64>,
    total_since_last: Arc<AtomicU64>,
    frequencies: Option<Arc<Mutex<HashMap<String, u64>>>>,
    stats_every: Duration,
    queue_type: &str,
    shutdown: CancellationToken,
) {
    let stats_minutes = stats_every.as_secs() / 60;
    let mut has_counted_freqs = false;
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return,
            () = sleep(stats_every) => {}
        }

        // `since_last` is cleared atomically; readers see a consistent
        // value via swap rather than a separate load+store race.
        let total_all = total_all_time.load(Ordering::Relaxed);
        let total_since = total_since_last.swap(0, Ordering::Relaxed);

        let freqs_snapshot = frequencies.as_ref().and_then(|arc| {
            let mut v: Vec<FrequencyCount> = arc
                .lock()
                .ok()?
                .iter()
                .map(|(f, c)| FrequencyCount {
                    freq: f.clone(),
                    count: *c,
                })
                .collect();
            v.sort_by_key(|b| std::cmp::Reverse(b.count));
            Some(v)
        });

        if !has_counted_freqs && freqs_snapshot.as_ref().is_some_and(|v| !v.is_empty()) {
            has_counted_freqs = true;
        }

        info!(
            "{}",
            print_formatted_stats(
                total_all,
                total_since,
                freqs_snapshot,
                stats_minutes,
                queue_type,
                has_counted_freqs
            )
        );
    }
}

async fn clean_up_dedupe_cache(
    cache: Arc<Mutex<HashMap<u64, u64>>>,
    dedupe_window: Duration,
    queue_type: &str,
    shutdown: CancellationToken,
) {
    let window_secs = dedupe_window.as_secs();
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return,
            () = sleep(dedupe_window) => {}
        }
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |n| n.as_secs());

        let remaining = match cache.lock() {
            Ok(mut map) => {
                map.retain(|_, &mut ts| current_time.saturating_sub(ts) <= window_secs);
                map.len()
            }
            Err(_) => continue,
        };
        debug!("[Message Handler {queue_type}] dedupe cache size after pruning {remaining}");
    }
}

fn hash_message(mut message: AcarsVdlm2Message) -> MessageResult<u64> {
    let mut hasher = DefaultHasher::new();
    message.clear_proxy_details(); // Clears out "app"
    message.clear_error(); // Clears out "error"
    message.clear_level(); // Clears out "level"
    message.clear_station_name(); // Clears out "vdl2.station" or "station_id"
    message.clear_time(); // Clears out "timestamp" or "vdl2.t"
    message.clear_channel(); // Clears out "channel"
    message.clear_freq_skew(); // Clears out "vdl2.freq_skew"
    message.clear_hdr_bits_fixed(); // Clears out "vdl2.hdr_bits_fixed"
    message.clear_noise_level(); // Clears out "vdl2.noise_level"
    message.clear_octets_corrected_by_fec(); // Clears out "vdl2.octets_corrected_by_fec"
    message.clear_sig_level(); // Clears out "vdl2.sig_level"
    let msg = message.to_string()?;
    trace!("[Hasher] Message to be hashed: {msg}");
    msg.hash(&mut hasher);
    Ok(hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn print_formatted_stats_with_freqs() {
        let frequencies = vec![
            FrequencyCount {
                freq: "134.525".to_string(),
                count: 70,
            },
            FrequencyCount {
                freq: "131.550".to_string(),
                count: 20,
            },
            FrequencyCount {
                freq: "131.525".to_string(),
                count: 10,
            },
        ];
        let output = print_formatted_stats(100, 100, Some(frequencies), 1, "TEST", true);
        assert_eq!(
            output,
            "TEST in the last 1 minute(s):\nTotal messages processed: 100\nTotal messages processed since last update: 100\nTEST Frequencies logged since container start:\n\nTEST 134.525: 70/100 (70.00%)\nTEST 131.550: 20/100 (20.00%)\nTEST 131.525: 10/100 (10.00%)"
        );
    }

    #[test]
    fn print_formatted_stats_without_freqs() {
        let output = print_formatted_stats(100, 100, None, 1, "TEST", false);
        assert_eq!(
            output,
            "TEST in the last 1 minute(s):\nTotal messages processed: 100\nTotal messages processed since last update: 100\n"
        );
    }

    #[test]
    fn is_duplicate_detects_repeat_within_window() {
        let cache: Mutex<HashMap<u64, u64>> = Mutex::new(HashMap::new());
        let window = Duration::from_secs(60);
        assert!(!is_duplicate(&cache, 0xdead_beef, 100, window));
        // Same hash, 10s later: within 60s window -> duplicate.
        assert!(is_duplicate(&cache, 0xdead_beef, 110, window));
    }

    #[test]
    fn is_duplicate_allows_repeat_after_window() {
        let cache: Mutex<HashMap<u64, u64>> = Mutex::new(HashMap::new());
        let window = Duration::from_secs(60);
        assert!(!is_duplicate(&cache, 1, 100, window));
        // Same hash, 61s later: outside the window -> not a duplicate.
        // The cache stores the *new* observation timestamp.
        assert!(!is_duplicate(&cache, 1, 161, window));
        let stored = *cache.lock().unwrap().get(&1).unwrap();
        assert_eq!(stored, 161);
    }
}

#[cfg(test)]
mod corpus_tests {
    //! End-to-end pipeline + hash-stability tests driven by the recorded
    //! ACARS / VDLM2 fixture corpora. These replace the deleted
    //! `test_data/data_feeder_*.py` harnesses.

    use super::*;
    use serde_json::Value;
    use tokio::sync::{broadcast as bcast, mpsc};
    use tokio_util::sync::CancellationToken;

    const ACARS: &str = include_str!("../tests/fixtures/acars.jsonl");
    const VDLM2: &str = include_str!("../tests/fixtures/vdlm2.jsonl");

    /// Default-ish skew window (matches the CLI default of 5s). Tests
    /// exercise the real freshness check rather than disabling it.
    const SKEW: u64 = 5;

    /// Build a config with realistic skew/dedupe windows. Corpus lines
    /// have to be rewritten to a fresh timestamp before they reach this
    /// pipeline (see [`rewrite_timestamp`]).
    fn config(queue_type: &str, dedupe: bool) -> MessageHandlerConfig {
        MessageHandlerConfig {
            add_proxy_id: false,
            dedupe: dedupe.then(|| DedupeOpts {
                window: Duration::from_secs(300),
            }),
            skew_window: Duration::from_secs(SKEW),
            queue_type: queue_type.to_string(),
            station_name_override: None,
            stats: StatsOpts {
                every: Duration::from_secs(60),
                verbose: false,
            },
        }
    }

    /// Rewrite the recorded timestamp in `line` to `target` (unix seconds
    /// as f64). Handles both the flat ACARS shape (`"timestamp": f64`)
    /// and the nested VDLM2 shape (`"vdl2": { "t": { "sec", "usec" } }`).
    /// Other variants (hfdl/imsl/irdm) are not present in the active
    /// corpus fixtures; extend here when those land.
    fn rewrite_timestamp(line: &str, target: f64) -> String {
        let mut v: Value = serde_json::from_str(line).expect("corpus line is valid JSON");
        if v.get("timestamp").is_some() {
            v["timestamp"] = serde_json::json!(target);
        }
        if let Some(t) = v.get_mut("vdl2").and_then(|v| v.get_mut("t")) {
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let sec = target.trunc() as i64;
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let usec = (target.fract() * 1_000_000.0).round() as i64;
            *t = serde_json::json!({ "sec": sec, "usec": usec });
        }
        v.to_string()
    }

    fn now_secs() -> f64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_secs_f64()
    }

    /// Drive `watch_message_queue` with `lines` repeated `repeats` times.
    /// Returns (`input_count`, `emitted_count`).
    async fn drive_pipeline(
        cfg: MessageHandlerConfig,
        corpus: &str,
        repeats: usize,
        timestamp: f64,
    ) -> (usize, usize) {
        let lines: Vec<String> = corpus
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| rewrite_timestamp(l, timestamp))
            .collect();
        let input_count = lines.len() * repeats;

        let (tx_in, rx_in) = mpsc::channel::<String>(64);
        let (tx_out, mut rx_out) = bcast::channel::<AcarsVdlm2Message>(input_count.max(1));
        let shutdown = CancellationToken::new();

        let handler_shutdown = shutdown.clone();
        let handler = tokio::spawn(async move {
            cfg.watch_message_queue(rx_in, tx_out, handler_shutdown)
                .await;
        });

        // Feed corpus. Dropping `tx_in` closes the channel and the
        // handler loop returns naturally; no need to cancel for this.
        for _ in 0..repeats {
            for line in &lines {
                tx_in.send(line.clone()).await.expect("input send");
            }
        }
        drop(tx_in);
        handler.await.expect("handler task");

        let mut received = 0;
        loop {
            match rx_out.try_recv() {
                Ok(_) => received += 1,
                Err(bcast::error::TryRecvError::Empty | bcast::error::TryRecvError::Closed) => {
                    break;
                }
                Err(bcast::error::TryRecvError::Lagged(_)) => {
                    panic!("broadcast lagged in pipeline test; capacity too small")
                }
            }
        }
        (input_count, received)
    }

    #[tokio::test]
    async fn acars_corpus_passes_through_without_dedupe() {
        let (sent, got) = drive_pipeline(config("acars", false), ACARS, 1, now_secs()).await;
        assert_eq!(
            sent, got,
            "every corpus line should emerge from the pipeline"
        );
    }

    #[tokio::test]
    async fn vdlm2_corpus_passes_through_without_dedupe() {
        let (sent, got) = drive_pipeline(config("vdlm2", false), VDLM2, 1, now_secs()).await;
        assert_eq!(sent, got);
    }

    #[tokio::test]
    async fn dedupe_collapses_immediate_duplicates() {
        // Some payloads in the corpus collapse to the same hash once
        // the volatile fields are cleared, so we can't compare against
        // the raw line count. Instead, drive the pipeline once to
        // discover the unique-payload count, then drive it twice and
        // assert the second pass adds nothing.
        let now = now_secs();
        let (_, baseline) = drive_pipeline(config("acars", true), ACARS, 1, now).await;
        let (_, doubled) = drive_pipeline(config("acars", true), ACARS, 2, now).await;
        assert!(baseline > 0);
        assert_eq!(doubled, baseline, "second pass must add no new messages");
    }

    #[tokio::test]
    async fn stale_messages_are_rejected_by_skew_window() {
        // Push the timestamps comfortably outside the skew window and
        // assert that the freshness check drops every message.
        let stale = now_secs() - f64::from(u32::try_from(SKEW).unwrap()) - 30.0;
        let (sent, got) = drive_pipeline(config("acars", false), ACARS, 1, stale).await;
        assert!(sent > 0);
        assert_eq!(got, 0, "stale corpus must be rejected by the skew check");
    }

    #[test]
    fn hash_is_stable_across_calls() {
        // Pick the first non-empty line from each corpus and assert that
        // hashing it twice in a row yields the same digest. Catches
        // accidental dependence on HashMap iteration order or random
        // state inside DefaultHasher (which is, in fact, deterministic
        // within a process — the test guards against future drift).
        for (label, corpus) in [("acars", ACARS), ("vdlm2", VDLM2)] {
            let line = corpus
                .lines()
                .find(|l| !l.trim().is_empty())
                .expect("non-empty corpus");
            let msg: AcarsVdlm2Message = line.decode_message().expect("decode corpus line");
            let h1 = hash_message(msg.clone()).expect("hash 1");
            let h2 = hash_message(msg).expect("hash 2");
            assert_eq!(h1, h2, "{label}: hash must be stable across calls");
        }
    }

    #[test]
    fn hash_ignores_proxy_and_station_fields() {
        // hash_message clears proxy/station/etc. before hashing so that
        // two copies of the same payload that differ only in those
        // bookkeeping fields collide and dedupe properly.
        let line = ACARS
            .lines()
            .find(|l| !l.trim().is_empty())
            .expect("non-empty corpus");
        let mut a: AcarsVdlm2Message = line.decode_message().expect("decode");
        let mut b: AcarsVdlm2Message = line.decode_message().expect("decode");
        a.set_proxy_details("acars_router", "test-a");
        b.set_proxy_details("acars_router", "test-b");
        a.set_station_name("station-A");
        b.set_station_name("station-B");
        assert_eq!(
            hash_message(a).expect("hash a"),
            hash_message(b).expect("hash b"),
            "hash must ignore proxy_id and station_name"
        );
    }
}
