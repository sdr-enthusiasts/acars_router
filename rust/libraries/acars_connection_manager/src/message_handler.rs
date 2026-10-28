// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

//! Per-protocol message handler: decode -> freq accounting -> skew check
//! -> dedupe -> rewrite -> fan-out.
//!
//! When a protocol's [`ValidationMode`] is `Bypass`, a message that fails
//! to decode but is still a complete JSON object skips the pipeline and is
//! forwarded byte-for-byte (newline-terminated) instead of being dropped.
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

use crate::OutboundMessage;
use crate::packet_handler::is_json_object;
use acars_config::{Input, ValidationMode};
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
    /// `Bypass` forwards messages that fail shape validation unchanged
    /// (if they are complete JSON objects); `Enforce` drops them.
    pub validation: ValidationMode,
}

impl MessageHandlerConfig {
    pub(crate) fn new(args: &Input, queue_type: &str, validation: ValidationMode) -> Self {
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
            validation,
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
        output_queue: broadcast::Sender<OutboundMessage>,
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
            let dedupe_shutdown = shutdown.clone();
            tokio::spawn(async move {
                clean_up_dedupe_cache(cache, dedupe_window, dedupe_shutdown).await;
            });
        }

        loop {
            let message_content = tokio::select! {
                () = shutdown.cancelled() => {
                    info!(
                        "shutdown requested; closing input loop"
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
            let mut message = match parsed {
                Ok(m) => {
                    trace!("GOT: {m:?}");
                    m
                }
                // Bypassed messages must not produce any log about the
                // validation failure, so nothing is logged before this
                // branch decides.
                Err(e) => {
                    if self.try_bypass(&message_content, &output_queue) {
                        continue;
                    }
                    error!("Failed to parse received message: {e}\nReceived: {message_content}");
                    continue;
                }
            };

            let current_time = unix_time_secs();

            // Frequency accounting. The previous implementation had this
            // block copy-pasted five times across the five message
            // variants; the helper collapses all of them into one
            // HashMap::entry call.
            if let Some(freq) = message_frequency(&message)
                && let Ok(mut map) = all_frequencies_logged.lock()
            {
                *map.entry(freq).or_insert(0) += 1;
            }

            // A decoded message with no usable timestamp is a validation
            // failure too: in bypass mode forward the original text.
            let Some(mut message_time) = message.get_time() else {
                if self.try_bypass(&message_content, &output_queue) {
                    continue;
                }
                error!("Message has no timestamp field. Skipping message.");
                continue;
            };

            let skew = self.skew_window;
            if message_time > current_time {
                if Duration::from_secs_f64(message_time - current_time) > skew {
                    error!(
                        "Message is from the future. Skipping. Current time {current_time}, Message time {message_time}. Difference {}",
                        message_time - current_time,
                    );
                    continue;
                }
                trace!(
                    "Message is from the future. Current time {current_time}, Message time {message_time}. Difference {}",
                    message_time - current_time,
                );
                message_time = current_time;
            } else if Duration::from_secs_f64(current_time - message_time) > skew {
                error!(
                    "Message is {} seconds old. Time in message {message_time}. Skipping message. {}",
                    current_time - message_time,
                    skew.as_secs()
                );
                continue;
            }

            let hashed_value = match hash_message(message.clone()) {
                Ok(h) => h,
                Err(e) => {
                    error!("Failed to create hash of message: {e}");
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
                info!("Message is a duplicate. Skipping message.");
                continue;
            }

            if let Some(station_name) = self.station_name_override.as_deref() {
                trace!("Overriding station name to {station_name}");
                message.set_station_name(station_name);
            }

            if self.add_proxy_id {
                trace!("Adding proxy_id to message");
                message.set_proxy_details("acars_router", version);
            }

            debug!("SENDING: {message:?}");
            trace!("Hashed value: {hashed_value}");

            let line = match message.to_string_newline() {
                Ok(line) => line,
                Err(e) => {
                    error!("Failed to serialize message: {e}");
                    continue;
                }
            };

            broadcast_line(&output_queue, OutboundMessage::from(line));
        }
    }

    /// In bypass mode, forward the original `text` of a message that failed
    /// validation and return `true`. Returns `false` (nothing sent, nothing
    /// logged) in enforce mode or when `text` is not a JSON object, leaving
    /// the caller to log and drop it.
    fn try_bypass(&self, text: &str, output_queue: &broadcast::Sender<OutboundMessage>) -> bool {
        if self.validation != ValidationMode::Bypass {
            return false;
        }
        let Some(line) = raw_passthrough_line(text) else {
            return false;
        };
        broadcast_line(output_queue, line);
        true
    }
}

/// Fan `line` out to every subscribed sender.
fn broadcast_line(output_queue: &broadcast::Sender<OutboundMessage>, line: OutboundMessage) {
    if let Ok(n) = output_queue.send(line) {
        debug!("Message sent to {n} sender(s)");
    } else {
        // `Err` only when no receivers are subscribed; common during
        // startup (no outputs configured) — debug, not error.
        debug!("No active senders; message dropped");
    }
}

/// Build the byte-for-byte passthrough line for a message that failed shape
/// validation. Strips any trailing `\r\n` / `\n` and appends exactly one
/// `\n`. Returns `None` when the text is not a complete JSON object (it is
/// not a message at all and must still be dropped).
fn raw_passthrough_line(text: &str) -> Option<OutboundMessage> {
    let trimmed = text
        .strip_suffix("\r\n")
        .or_else(|| text.strip_suffix('\n'))
        .unwrap_or(text);
    if !is_json_object(trimmed) {
        return None;
    }
    let mut line = String::with_capacity(trimmed.len() + 1);
    line.push_str(trimmed);
    line.push('\n');
    Some(OutboundMessage::from(line))
}

/// Look up `freq`-style accessor for every supported message variant.
/// Centralises the per-variant accessor knowledge that the message
/// handler previously re-implemented five times.
fn message_frequency(message: &AcarsVdlm2Message) -> Option<String> {
    match message {
        AcarsVdlm2Message::Vdlm2Message(m) => Some(m.vdl2.freq.to_string()),
        AcarsVdlm2Message::AcarsMessage(m) => Some(m.freq.to_string()),
        AcarsVdlm2Message::HfdlMessage(m) => Some(m.hfdl.freq.to_string()),
        AcarsVdlm2Message::ImslMessage(m) => m.freq_string(),
        AcarsVdlm2Message::IrdmMessage(m) => m.freq.as_ref().map(ToString::to_string),
        AcarsVdlm2Message::XngMessage(m) => Some(m.frequency_hz.to_string()),
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
        debug!("dedupe cache size after pruning {remaining}");
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
    trace!("Message to be hashed: {msg}");
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
    const XNG: &str = include_str!("../tests/fixtures/xng.jsonl");

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
            validation: ValidationMode::Enforce,
        }
    }

    /// Rewrite the recorded timestamp in `line` to `target` (unix seconds
    /// as f64). Handles the flat ACARS shape (`"timestamp": f64`), the
    /// nested VDLM2 shape (`"vdl2": { "t": { "sec", "usec" } }`) and the
    /// xng envelope shape (`"timestamp": "<RFC3339>"` alongside an object
    /// `body` and a numeric `frequency_hz`), the IMSL shape (integer
    /// `"timestamp"` alongside `msg_name`) and the IRDM shape
    /// (`"acars": { "timestamp": "<RFC3339>" }`). hfdl is not present in
    /// the active corpus fixtures; extend here when it lands.
    fn rewrite_timestamp(line: &str, target: f64) -> String {
        let mut v: Value = serde_json::from_str(line).expect("corpus line is valid JSON");
        let is_xng = v.get("timestamp").is_some_and(Value::is_string)
            && v.get("body").is_some_and(Value::is_object)
            && v.get("frequency_hz").is_some_and(Value::is_number);
        let is_imsl = v.get("timestamp").is_some_and(Value::is_number)
            && v.get("msg_name").is_some_and(Value::is_string);
        if let Some(ts) = v
            .get_mut("acars")
            .and_then(|a| a.get_mut("timestamp"))
            .filter(|t| t.is_string())
        {
            #[allow(clippy::cast_possible_truncation)]
            let secs = target.trunc() as i64;
            let stamp = chrono::DateTime::from_timestamp(secs, 0)
                .expect("target is a valid unix time")
                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
            *ts = serde_json::json!(stamp);
        }
        if is_imsl {
            #[allow(clippy::cast_possible_truncation)]
            let secs = target.trunc() as i64;
            v["timestamp"] = serde_json::json!(secs);
        } else if is_xng {
            #[allow(clippy::cast_possible_truncation)]
            let nanos = (target * 1e9).round() as i64;
            let stamp = chrono::DateTime::from_timestamp_nanos(nanos)
                .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
            v["timestamp"] = serde_json::json!(stamp);
        } else if v.get("timestamp").is_some() {
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
    /// Returns (`input_count`, `emitted_count`). Every emitted payload is
    /// checked to be a newline-terminated line that decodes back into an
    /// [`AcarsVdlm2Message`].
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
        let (input_count, emitted) = drive_lines(cfg, &lines, repeats).await;
        for payload in &emitted {
            assert!(
                payload.ends_with('\n'),
                "broadcast payload must be newline-terminated"
            );
            let _: AcarsVdlm2Message = payload.decode_message().expect("broadcast payload decodes");
        }
        (input_count, emitted.len())
    }

    /// Feed the already-prepared `lines` (repeated `repeats` times) through
    /// `watch_message_queue` and collect every broadcast payload.
    async fn drive_lines(
        cfg: MessageHandlerConfig,
        lines: &[String],
        repeats: usize,
    ) -> (usize, Vec<OutboundMessage>) {
        let input_count = lines.len() * repeats;

        let (tx_in, rx_in) = mpsc::channel::<String>(64);
        let (tx_out, mut rx_out) = bcast::channel::<OutboundMessage>(input_count.max(1));
        let shutdown = CancellationToken::new();

        let handler_shutdown = shutdown.clone();
        let handler = tokio::spawn(async move {
            cfg.watch_message_queue(rx_in, tx_out, handler_shutdown)
                .await;
        });

        // Feed corpus. Dropping `tx_in` closes the channel and the
        // handler loop returns naturally; no need to cancel for this.
        for _ in 0..repeats {
            for line in lines {
                tx_in.send(line.clone()).await.expect("input send");
            }
        }
        drop(tx_in);
        handler.await.expect("handler task");

        let mut received = Vec::new();
        loop {
            match rx_out.try_recv() {
                Ok(payload) => received.push(payload),
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

    #[tokio::test]
    async fn broadcast_payload_matches_legacy_per_sender_serialization() {
        // The handler now serializes once; the payload on the channel must
        // be byte-identical to what each sender used to produce by calling
        // `to_string_newline()` on the processed message itself.
        let now = now_secs();
        let version = env!("CARGO_PKG_VERSION");
        for (label, corpus) in [("acars", ACARS), ("vdlm2", VDLM2)] {
            let line = rewrite_timestamp(
                corpus
                    .lines()
                    .find(|l| !l.trim().is_empty())
                    .expect("non-empty corpus"),
                now,
            );
            // Station override only: fully deterministic, so the expected
            // bytes can be built independently of the handler.
            let mut cfg = config(label, false);
            cfg.station_name_override = Some("TEST-STATION".to_string());

            let (_, emitted) = drive_lines(cfg, std::slice::from_ref(&line), 1).await;
            assert_eq!(emitted.len(), 1, "{label}: exactly one payload expected");

            let mut decoded: AcarsVdlm2Message = line.decode_message().expect("decode");
            decoded.set_station_name("TEST-STATION");
            let expected = decoded.to_string_newline().expect("serialize");

            assert_eq!(
                &*emitted[0], expected,
                "{label}: broadcast payload must match legacy serialization"
            );

            // With proxy details the parser stamps a fresh random UUID, so
            // instead assert the payload is exactly the serialization of the
            // message it carries.
            let mut cfg = config(label, false);
            cfg.station_name_override = Some("TEST-STATION".to_string());
            cfg.add_proxy_id = true;

            let (_, emitted) = drive_lines(cfg, std::slice::from_ref(&line), 1).await;
            assert_eq!(emitted.len(), 1, "{label}: exactly one payload expected");

            let carried: AcarsVdlm2Message = emitted[0].decode_message().expect("decode payload");
            assert_eq!(
                &*emitted[0],
                carried.to_string_newline().expect("serialize"),
                "{label}: proxied payload must match legacy serialization"
            );
            assert!(
                emitted[0].contains(&format!("\"acars_router_version\":\"{version}\"")),
                "{label}: proxy details must be present"
            );
        }
    }

    fn xng_lines(timestamp: f64) -> Vec<String> {
        XNG.lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| rewrite_timestamp(l, timestamp))
            .collect()
    }

    #[test]
    fn xng_fixture_decodes_as_xng_variant() {
        let lines = xng_lines(now_secs());
        assert_eq!(lines.len(), 20);
        for line in &lines {
            let msg: AcarsVdlm2Message = line.decode_message().expect("xng line decodes");
            assert!(
                matches!(msg, AcarsVdlm2Message::XngMessage(_)),
                "expected XngMessage variant, got {msg:?}"
            );
            assert!(
                message_frequency(&msg).is_some(),
                "xng carries frequency_hz"
            );
        }
    }

    #[test]
    fn rewrite_timestamp_sets_rfc3339_for_xng() {
        let target = 1_700_000_000.25;
        let line = xng_lines(target).remove(0);
        let msg: AcarsVdlm2Message = line.decode_message().expect("decode");
        let got = msg.get_time().expect("xng timestamp parses");
        assert!((got - target).abs() < 1e-6, "got {got}, want {target}");
    }

    #[tokio::test]
    async fn xng_corpus_passes_through_without_dedupe() {
        let (sent, got) = drive_pipeline(config("xng", false), XNG, 1, now_secs()).await;
        assert_eq!(sent, 20);
        assert_eq!(sent, got, "every xng corpus line should emerge");
    }

    #[tokio::test]
    async fn xng_dedupe_collapses_immediate_duplicate() {
        let line = xng_lines(now_secs()).remove(0);
        let (_, emitted) = drive_lines(config("xng", true), &[line.clone(), line], 1).await;
        assert_eq!(emitted.len(), 1, "duplicate xng message must be dropped");
    }

    #[tokio::test]
    async fn xng_stale_messages_are_rejected_by_skew_window() {
        let stale = now_secs() - f64::from(u32::try_from(SKEW).unwrap()) - 30.0;
        let (sent, got) = drive_pipeline(config("xng", false), XNG, 1, stale).await;
        assert!(sent > 0);
        assert_eq!(
            got, 0,
            "stale xng corpus must be rejected by the skew check"
        );
    }

    #[tokio::test]
    async fn xng_proxy_id_and_station_override_are_applied() {
        let version = env!("CARGO_PKG_VERSION");
        let line = xng_lines(now_secs()).remove(0);
        let mut cfg = config("xng", false);
        cfg.add_proxy_id = true;
        cfg.station_name_override = Some("TEST-STATION".to_string());

        let (_, emitted) = drive_lines(cfg, std::slice::from_ref(&line), 1).await;
        assert_eq!(emitted.len(), 1);

        let payload: Value = serde_json::from_str(&emitted[0]).expect("payload is JSON");
        assert_eq!(
            payload["source"]["app"]["acars_router_version"],
            Value::from(version)
        );
        assert_eq!(
            payload["source"]["station"]["ident"],
            Value::from("TEST-STATION")
        );

        let carried: AcarsVdlm2Message = emitted[0].decode_message().expect("decode payload");
        assert!(matches!(carried, AcarsVdlm2Message::XngMessage(_)));
    }

    const IMSL: &str = include_str!("../tests/fixtures/imsl.jsonl");
    const IRDM: &str = include_str!("../tests/fixtures/irdm.jsonl");

    fn corpus_lines(corpus: &str, timestamp: f64) -> Vec<String> {
        corpus
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| rewrite_timestamp(l, timestamp))
            .collect()
    }

    /// Return `line` with each `(pointer, value)` pair written into it.
    fn with_fields(line: &str, fields: &[(&str, Value)]) -> String {
        let mut v: Value = serde_json::from_str(line).expect("line is valid JSON");
        for (pointer, value) in fields {
            *v.pointer_mut(pointer).expect("field exists") = value.clone();
        }
        v.to_string()
    }

    #[test]
    fn imsl_fixture_decodes_as_imsl_variant() {
        let lines = corpus_lines(IMSL, now_secs());
        assert_eq!(lines.len(), 15);
        for line in &lines {
            let msg: AcarsVdlm2Message = line.decode_message().expect("imsl line decodes");
            assert!(
                matches!(msg, AcarsVdlm2Message::ImslMessage(_)),
                "expected ImslMessage variant, got {msg:?}"
            );
            assert!(message_frequency(&msg).is_some(), "imsl carries freq");
        }
    }

    #[test]
    fn irdm_fixture_decodes_as_irdm_variant() {
        let lines = corpus_lines(IRDM, now_secs());
        assert_eq!(lines.len(), 7);
        for line in &lines {
            let msg: AcarsVdlm2Message = line.decode_message().expect("irdm line decodes");
            assert!(
                matches!(msg, AcarsVdlm2Message::IrdmMessage(_)),
                "expected IrdmMessage variant, got {msg:?}"
            );
            assert!(message_frequency(&msg).is_some(), "irdm carries freq");
        }
    }

    #[test]
    fn rewrite_timestamp_sets_integer_seconds_for_imsl_and_rfc3339_for_irdm() {
        let target = 1_700_000_000.75;
        let imsl: Value =
            serde_json::from_str(&corpus_lines(IMSL, target).remove(0)).expect("JSON");
        assert_eq!(imsl["timestamp"], Value::from(1_700_000_000_i64));
        let irdm: Value =
            serde_json::from_str(&corpus_lines(IRDM, target).remove(0)).expect("JSON");
        assert_eq!(
            irdm["acars"]["timestamp"],
            Value::from("2023-11-14T22:13:20Z")
        );
        for corpus in [IMSL, IRDM] {
            let msg: AcarsVdlm2Message = corpus_lines(corpus, target)
                .remove(0)
                .decode_message()
                .expect("decode");
            assert_eq!(msg.get_time(), Some(1_700_000_000.0));
        }
    }

    #[tokio::test]
    async fn imsl_corpus_passes_through_without_dedupe() {
        let (sent, got) = drive_pipeline(config("imsl", false), IMSL, 1, now_secs()).await;
        assert_eq!(sent, 15);
        assert_eq!(sent, got, "every imsl corpus line should emerge");
    }

    #[tokio::test]
    async fn irdm_corpus_passes_through_without_dedupe() {
        let (sent, got) = drive_pipeline(config("irdm", false), IRDM, 1, now_secs()).await;
        assert_eq!(sent, 7);
        assert_eq!(sent, got, "every irdm corpus line should emerge");
    }

    #[tokio::test]
    async fn imsl_and_irdm_stale_messages_are_rejected_by_skew_window() {
        let stale = now_secs() - f64::from(u32::try_from(SKEW).unwrap()) - 30.0;
        for (label, corpus) in [("imsl", IMSL), ("irdm", IRDM)] {
            let (sent, got) = drive_pipeline(config(label, false), corpus, 1, stale).await;
            assert!(sent > 0);
            assert_eq!(got, 0, "{label}: stale corpus must be rejected by skew");
        }
    }

    #[tokio::test]
    async fn imsl_and_irdm_dedupe_collapses_immediate_duplicate() {
        for (label, corpus) in [("imsl", IMSL), ("irdm", IRDM)] {
            let line = corpus_lines(corpus, now_secs()).remove(0);
            let (_, emitted) = drive_lines(config(label, true), &[line.clone(), line], 1).await;
            assert_eq!(emitted.len(), 1, "{label}: duplicate must be dropped");
        }
    }

    #[tokio::test]
    async fn imsl_dedupe_collapses_copies_from_different_stations() {
        let line = corpus_lines(IMSL, now_secs()).remove(0);
        let other = with_fields(
            &line,
            &[
                ("/source/station_id", Value::from("OTHER-STATION")),
                ("/level", Value::from("-22.5")),
            ],
        );
        assert_ne!(line, other);
        let (_, emitted) = drive_lines(config("imsl", true), &[line, other], 1).await;
        assert_eq!(emitted.len(), 1, "cross-station imsl copy must be dropped");
    }

    #[tokio::test]
    async fn irdm_dedupe_collapses_copies_from_different_stations() {
        let line = corpus_lines(IRDM, now_secs()).remove(0);
        let other = with_fields(
            &line,
            &[
                ("/source/station_id", Value::from("OTHER-STATION")),
                ("/level", Value::from(-12.5)),
                ("/freq", Value::from(1_625_712_000.5)),
                ("/acars/errors", Value::from(2)),
            ],
        );
        assert_ne!(line, other);
        let (_, emitted) = drive_lines(config("irdm", true), &[line, other], 1).await;
        assert_eq!(emitted.len(), 1, "cross-station irdm copy must be dropped");
    }

    #[tokio::test]
    async fn imsl_and_irdm_proxy_id_and_station_override_are_applied() {
        let version = env!("CARGO_PKG_VERSION");
        for (label, corpus, app_pointer) in [
            ("imsl", IMSL, "/source/app/acars_router_version"),
            ("irdm", IRDM, "/app/acars_router_version"),
        ] {
            let line = corpus_lines(corpus, now_secs()).remove(0);
            let mut cfg = config(label, false);
            cfg.add_proxy_id = true;
            cfg.station_name_override = Some("TEST-STATION".to_string());

            let (_, emitted) = drive_lines(cfg, std::slice::from_ref(&line), 1).await;
            assert_eq!(emitted.len(), 1, "{label}: exactly one payload expected");

            let payload: Value = serde_json::from_str(&emitted[0]).expect("payload is JSON");
            assert_eq!(
                payload.pointer(app_pointer),
                Some(&Value::from(version)),
                "{label}: proxy details must be present"
            );
            assert_eq!(
                payload["source"]["station_id"],
                Value::from("TEST-STATION"),
                "{label}: station override must be applied"
            );
        }
    }

    #[tokio::test]
    async fn imsl_unknown_fields_survive_routing() {
        let line = corpus_lines(IMSL, now_secs())
            .into_iter()
            .find(|l| l.contains("\"libacars\""))
            .expect("imsl fixture has a libacars line");
        let input: Value = serde_json::from_str(&line).expect("JSON");

        let (_, emitted) = drive_lines(config("imsl", false), std::slice::from_ref(&line), 1).await;
        assert_eq!(emitted.len(), 1);

        let payload: Value = serde_json::from_str(&emitted[0]).expect("payload is JSON");
        for field in ["level", "libacars"] {
            assert!(input.get(field).is_some(), "fixture line has {field}");
            assert_eq!(payload.get(field), input.get(field), "{field} must survive");
        }
    }

    fn bypass_config(queue_type: &str, dedupe: bool) -> MessageHandlerConfig {
        let mut cfg = config(queue_type, dedupe);
        cfg.validation = ValidationMode::Bypass;
        cfg
    }

    fn lines_of(texts: &[&str]) -> Vec<String> {
        texts.iter().map(|t| (*t).to_string()).collect()
    }

    fn first_fresh_line(corpus: &str) -> String {
        rewrite_timestamp(
            corpus
                .lines()
                .find(|l| !l.trim().is_empty())
                .expect("non-empty corpus"),
            now_secs(),
        )
    }

    #[tokio::test]
    async fn enforce_drops_invalid_shape_object() {
        let (_, emitted) =
            drive_lines(config("acars", false), &lines_of(&[r#"{"foo":"bar"}"#]), 1).await;
        assert_eq!(emitted.len(), 0);
    }

    #[tokio::test]
    async fn bypass_forwards_invalid_shape_object_verbatim() {
        for input in [
            r#"{"foo":"bar"}"#,
            "{\"foo\":\"bar\"}\n",
            "{\"foo\":\"bar\"}\r\n",
        ] {
            let (_, emitted) =
                drive_lines(bypass_config("acars", false), &lines_of(&[input]), 1).await;
            assert_eq!(emitted.len(), 1, "input {input:?}");
            assert_eq!(&*emitted[0], "{\"foo\":\"bar\"}\n", "input {input:?}");
        }
    }

    #[tokio::test]
    async fn bypass_forwards_nested_unknown_fields_verbatim() {
        let input = r#"{"freq":"not-a-number","x":{"y":1}}"#;
        let (_, emitted) = drive_lines(bypass_config("acars", false), &lines_of(&[input]), 1).await;
        assert_eq!(emitted.len(), 1);
        assert_eq!(&*emitted[0], format!("{input}\n"));
    }

    #[tokio::test]
    async fn bypass_drops_non_objects() {
        for input in ["not json", "[1,2]"] {
            let (_, emitted) =
                drive_lines(bypass_config("acars", false), &lines_of(&[input]), 1).await;
            assert_eq!(emitted.len(), 0, "input {input:?}");
        }
    }

    #[tokio::test]
    async fn bypass_valid_message_still_takes_normal_path() {
        let version = env!("CARGO_PKG_VERSION");
        let line = first_fresh_line(ACARS);
        let mut cfg = bypass_config("acars", false);
        cfg.add_proxy_id = true;
        let (_, emitted) = drive_lines(cfg, std::slice::from_ref(&line), 1).await;
        assert_eq!(emitted.len(), 1);
        assert!(
            emitted[0].contains(&format!("\"acars_router_version\":\"{version}\"")),
            "proxy details must be applied to valid messages in bypass mode"
        );
    }

    #[tokio::test]
    async fn bypass_valid_message_is_still_deduped() {
        let line = first_fresh_line(ACARS);
        let (_, emitted) =
            drive_lines(bypass_config("acars", true), &[line.clone(), line], 1).await;
        assert_eq!(emitted.len(), 1, "duplicate valid message must be dropped");
    }

    /// First ACARS fixture line with its `timestamp` field removed: still
    /// decodes, but `get_time()` has nothing to return.
    fn acars_line_without_timestamp() -> String {
        let line = ACARS
            .lines()
            .find(|l| !l.trim().is_empty())
            .expect("non-empty corpus");
        let mut v: Value = serde_json::from_str(line).expect("corpus line is valid JSON");
        v.as_object_mut()
            .expect("corpus line is an object")
            .remove("timestamp")
            .expect("corpus line has a timestamp");
        let text = v.to_string();
        let decoded: AcarsVdlm2Message = text.decode_message().expect("still decodes");
        assert!(decoded.get_time().is_none(), "timestamp must be unusable");
        text
    }

    #[tokio::test]
    async fn bypass_forwards_decoded_message_without_timestamp_verbatim() {
        let line = acars_line_without_timestamp();
        let (_, emitted) = drive_lines(
            bypass_config("acars", false),
            std::slice::from_ref(&line),
            1,
        )
        .await;
        assert_eq!(emitted.len(), 1);
        assert_eq!(&*emitted[0], format!("{line}\n"));
    }

    #[tokio::test]
    async fn enforce_drops_decoded_message_without_timestamp() {
        let line = acars_line_without_timestamp();
        let (_, emitted) =
            drive_lines(config("acars", false), std::slice::from_ref(&line), 1).await;
        assert_eq!(emitted.len(), 0);
    }

    #[test]
    fn raw_passthrough_line_normalises_trailing_newline() {
        let expected = Some("{\"a\":1}\n");
        for input in ["{\"a\":1}", "{\"a\":1}\n", "{\"a\":1}\r\n"] {
            assert_eq!(
                raw_passthrough_line(input).as_deref(),
                expected,
                "input {input:?}"
            );
        }
        assert!(raw_passthrough_line("123").is_none());
        assert!(raw_passthrough_line("[1,2]").is_none());
        assert!(raw_passthrough_line("{\"a\":").is_none());
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
