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
use log::{debug, error, info, trace};
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

/// One (frequency, count) pair as rendered in stats output. Kept as a
/// named struct so the snapshot-and-sort step in [`print_stats`] stays
/// readable.
pub struct FrequencyCount {
    pub freq: String,
    pub count: u64,
}

#[derive(Clone, Debug, Default)]
pub struct MessageHandlerConfig {
    pub add_proxy_id: bool,
    pub dedupe: bool,
    pub dedupe_window: u64,
    pub skew_window: u64,
    pub queue_type: String,
    pub should_override_station_name: bool,
    pub station_name: String,
    pub stats_every: u64,
    pub stats_verbose: bool,
}

impl MessageHandlerConfig {
    pub(crate) fn new(args: &Input, queue_type: &str) -> Self {
        let (should_override_station_name, station_name) = args
            .override_station_name
            .as_ref()
            .map_or_else(|| (false, String::default()), |s| (true, s.clone()));
        Self {
            add_proxy_id: args.add_proxy_id,
            dedupe: args.enable_dedupe,
            dedupe_window: args.dedupe_window,
            skew_window: args.skew_window,
            queue_type: queue_type.to_string(),
            should_override_station_name,
            station_name,
            stats_every: args.stats_every,
            stats_verbose: args.stats_verbose,
        }
    }

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

        let stats_every: u64 = self.stats_every * 60; // minutes -> seconds
        let version: &str = env!("CARGO_PKG_VERSION");

        // Spawn the periodic stats printer. It reads (relaxed) atomics and
        // takes a short snapshot of the frequency table; no contention on
        // the hot path.
        {
            let total_all = Arc::clone(&total_messages_processed);
            let total_since = Arc::clone(&total_messages_since_last);
            let freqs = self
                .stats_verbose
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
        if self.dedupe {
            let cache = Arc::clone(&dedupe_cache);
            let dedupe_window = self.dedupe_window;
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

            let skew = Duration::from_secs(self.skew_window);
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
                    self.skew_window
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

            if self.dedupe
                && is_duplicate(
                    &dedupe_cache,
                    hashed_value,
                    Duration::from_secs_f64(message_time).as_secs(),
                    self.dedupe_window,
                )
            {
                info!(
                    "[Message Handler {}] Message is a duplicate. Skipping message.",
                    self.queue_type
                );
                continue;
            }

            if self.should_override_station_name {
                trace!(
                    "[Message Handler {}] Overriding station name to {}",
                    self.queue_type, self.station_name
                );
                message.set_station_name(&self.station_name);
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

            match output_queue.send(message) {
                Ok(n) => debug!(
                    "[Message Handler {}] Message sent to {n} sender(s)",
                    self.queue_type
                ),
                // `Err` only when no receivers are subscribed; common during
                // startup (no outputs configured) — debug, not error.
                Err(_) => debug!(
                    "[Message Handler {}] No active senders; message dropped",
                    self.queue_type
                ),
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
    window_secs: u64,
) -> bool {
    let Ok(mut map) = cache.lock() else {
        // A poisoned mutex means another task panicked while holding the
        // lock. We conservatively treat the current message as new
        // (failing open is preferable to silently dropping every message
        // for the rest of the process lifetime).
        return false;
    };
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
    stats_every: u64,
    queue_type: &str,
    shutdown: CancellationToken,
) {
    let stats_minutes = stats_every / 60;
    let mut has_counted_freqs = false;
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return,
            () = sleep(Duration::from_secs(stats_every)) => {}
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
    dedupe_window: u64,
    queue_type: &str,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return,
            () = sleep(Duration::from_secs(dedupe_window)) => {}
        }
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |n| n.as_secs());

        let remaining = match cache.lock() {
            Ok(mut map) => {
                map.retain(|_, &mut ts| current_time.saturating_sub(ts) <= dedupe_window);
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
        assert!(!is_duplicate(&cache, 0xdead_beef, 100, 60));
        // Same hash, 10s later: within 60s window -> duplicate.
        assert!(is_duplicate(&cache, 0xdead_beef, 110, 60));
    }

    #[test]
    fn is_duplicate_allows_repeat_after_window() {
        let cache: Mutex<HashMap<u64, u64>> = Mutex::new(HashMap::new());
        assert!(!is_duplicate(&cache, 1, 100, 60));
        // Same hash, 61s later: outside the window -> not a duplicate.
        // The cache stores the *new* observation timestamp.
        assert!(!is_duplicate(&cache, 1, 161, 60));
        let stored = *cache.lock().unwrap().get(&1).unwrap();
        assert_eq!(stored, 161);
    }
}
