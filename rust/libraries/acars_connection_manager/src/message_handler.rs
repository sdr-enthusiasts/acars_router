// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

use acars_config::Input;
use acars_vdlm2_parser::{AcarsVdlm2Message, DecodeMessage, MessageResult};
use std::collections::hash_map::DefaultHasher;
use std::collections::vec_deque::VecDeque;
use std::env;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

pub struct FrequencyCount {
    freq: String,
    count: u32,
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
        if let Some(station_name) = &args.override_station_name {
            Self {
                add_proxy_id: args.add_proxy_id,
                dedupe: args.enable_dedupe,
                dedupe_window: args.dedupe_window,
                skew_window: args.skew_window,
                queue_type: queue_type.to_string(),
                should_override_station_name: args.override_station_name.is_some(),
                station_name: station_name.to_string(),
                stats_every: args.stats_every,
                stats_verbose: args.stats_verbose,
            }
        } else {
            Self {
                add_proxy_id: args.add_proxy_id,
                dedupe: args.enable_dedupe,
                dedupe_window: args.dedupe_window,
                skew_window: args.skew_window,
                queue_type: queue_type.to_string(),
                should_override_station_name: false,
                station_name: Default::default(),
                stats_every: args.stats_every,
                stats_verbose: args.stats_verbose,
            }
        }
    }

    pub(crate) async fn watch_message_queue(
        self,
        mut input_queue: Receiver<String>,
        output_queue: Sender<AcarsVdlm2Message>,
    ) {
        let dedupe_queue: Arc<Mutex<VecDeque<(u64, u64)>>> =
            Arc::new(Mutex::new(VecDeque::with_capacity(100)));
        let total_messages_processed: Arc<Mutex<i32>> = Arc::new(Mutex::new(0));
        let total_messages_since_last: Arc<Mutex<i32>> = Arc::new(Mutex::new(0));
        let all_frequencies_logged: Arc<Mutex<Vec<FrequencyCount>>> =
            Arc::new(Mutex::new(Vec::new()));
        let queue_type_stats: String = self.queue_type.clone();
        let queue_type_dedupe: String = self.queue_type.clone();
        let stats_every: u64 = self.stats_every * 60; // Value has to be in seconds. Input is in minutes.
        let version: &str = env!("CARGO_PKG_VERSION");

        // Generate an async loop that sleeps for the requested stats print duration and then logs
        // Give it the context for the counters
        // The stats values to the console.

        let stats_total_messages_context: Arc<Mutex<i32>> = Arc::clone(&total_messages_processed);
        let stats_total_messages_since_last_context: Arc<Mutex<i32>> =
            Arc::clone(&total_messages_since_last);
        let stats_frequency_context: Option<Arc<Mutex<Vec<FrequencyCount>>>> = if self.stats_verbose
        {
            Some(Arc::clone(&all_frequencies_logged))
        } else {
            None
        };

        tokio::spawn(async move {
            print_stats(
                stats_total_messages_context,
                stats_total_messages_since_last_context,
                stats_frequency_context,
                stats_every,
                queue_type_stats.as_str(),
            )
            .await;
        });

        // Generate an async loop that sleeps for the requested dedupe window and then cleans the queue
        // Give it the context for the dedupe queue
        // The dedupe queue to be cleaned.
        if self.dedupe {
            let dedupe_queue_context: Arc<Mutex<VecDeque<(u64, u64)>>> = Arc::clone(&dedupe_queue);
            let dedupe_window: u64 = self.dedupe_window;

            tokio::spawn(async move {
                clean_up_dedupe_queue(
                    dedupe_queue_context,
                    dedupe_window,
                    queue_type_dedupe.as_str(),
                )
                .await;
            });
        }

        while let Some(message_content) = input_queue.recv().await {
            // Grab the mutexes for the stats counter and increment the total messages processed by the message handler.
            let parse_message: MessageResult<AcarsVdlm2Message> = message_content.decode_message();
            let stats_total_loop_context: Arc<Mutex<i32>> = Arc::clone(&total_messages_processed);
            let stats_total_loop_since_last_context: Arc<Mutex<i32>> =
                Arc::clone(&total_messages_since_last);
            let dedupe_queue_loop: Arc<Mutex<VecDeque<(u64, u64)>>> = Arc::clone(&dedupe_queue);
            *stats_total_loop_since_last_context.lock().await += 1;
            *stats_total_loop_context.lock().await += 1;

            trace!(
                "[Message Handler {}] GOT: {:?}",
                self.queue_type,
                parse_message
            );

            match parse_message {
                Err(parse_error) => error!(
                    "[Message Handler {}] Failed to parse received message: {}\nReceived: {}",
                    self.queue_type, parse_error, message_content
                ),
                Ok(mut message) => {
                    let current_time: f64 = match SystemTime::now().duration_since(UNIX_EPOCH) {
                        Ok(n) => n.as_secs_f64(),
                        Err(_) => f64::default(),
                    };

                    // See if the frequency is in the list of frequencies we've seen
                    // If not, add it to the list and log it
                    // match the message type

                    match &message {
                        AcarsVdlm2Message::Vdlm2Message(m) => {
                            // get the freq from Vdlm2Message::Vdlm2Body
                            let frequency: String = m.vdl2.freq.to_string();
                            // check and see if we have the frequency in all_frequencies_logged. If so, increment the count.
                            // if not, add it
                            let mut found: bool = false;
                            for freq in all_frequencies_logged.lock().await.iter_mut() {
                                if freq.freq == frequency {
                                    freq.count += 1;
                                    found = true;
                                    break;
                                }
                            }

                            if !found {
                                let new_frequency: FrequencyCount = FrequencyCount {
                                    freq: frequency,
                                    count: 1,
                                };
                                all_frequencies_logged.lock().await.push(new_frequency);
                            }
                        }
                        AcarsVdlm2Message::AcarsMessage(m) => {
                            // get the freq from AcarsMessage::AcarsBody
                            let frequency: String = m.freq.to_string();

                            let mut found: bool = false;
                            for freq in all_frequencies_logged.lock().await.iter_mut() {
                                if freq.freq == frequency {
                                    freq.count += 1;
                                    found = true;
                                    break;
                                }
                            }

                            if !found {
                                let new_frequency: FrequencyCount = FrequencyCount {
                                    freq: frequency,
                                    count: 1,
                                };
                                all_frequencies_logged.lock().await.push(new_frequency);
                            }
                        }
                        AcarsVdlm2Message::HfdlMessage(m) => {
                            // get the freq from HfdlMessage::HfdlBody
                            let frequency: String = m.hfdl.freq.to_string();

                            let mut found: bool = false;
                            for freq in all_frequencies_logged.lock().await.iter_mut() {
                                if freq.freq == frequency {
                                    freq.count += 1;
                                    found = true;
                                    break;
                                }
                            }

                            if !found {
                                let new_frequency: FrequencyCount = FrequencyCount {
                                    freq: frequency,
                                    count: 1,
                                };
                                all_frequencies_logged.lock().await.push(new_frequency);
                            }
                        }
                    }

                    let get_message_time: Option<f64> = message.get_time();

                    match get_message_time {
                        None => {
                            error!("[Message Handler {}] Message has no timestamp field. Skipping message.", self.queue_type);
                            continue;
                        }
                        Some(mut message_time) => {
                            if message_time > current_time {
                                if (message_time - current_time) > self.skew_window as f64 {
                                    error!("[Message Handler {}] Message is from the future. Skipping. Current time {}, Message time {}. Difference {}", self.queue_type, current_time, message_time, message_time - current_time);
                                    continue;
                                }
                                trace!("[Message Handler {}] Message is from the future. Current time {}, Message time {}. Difference {}", self.queue_type, current_time, message_time, message_time - current_time);
                                message_time = current_time;
                            }
                            // If the message is older than the skew window, reject it
                            else if (current_time - message_time) > self.skew_window as f64 {
                                error!("[Message Handler {}] Message is {} seconds old. Time in message {}. Skipping message. {}", self.queue_type, current_time - message_time, message_time, self.skew_window);
                                continue;
                            }

                            // Time to hash the message
                            let hash_message: MessageResult<u64> = hash_message(message.clone());

                            match hash_message {
                                Err(hash_parsing_error) => error!(
                                    "[Message Handler {}] Failed to create hash of message: {}",
                                    self.queue_type, hash_parsing_error
                                ),
                                Ok(hashed_value) => {
                                    if self.dedupe {
                                        let mut rejected: bool = false;
                                        for (time, hashed_value_saved) in
                                            dedupe_queue_loop.lock().await.iter()
                                        {
                                            let f64_time: f64 = *time as f64;
                                            if *hashed_value_saved == hashed_value
                                                && current_time - f64_time
                                                    < self.dedupe_window as f64
                                            // Both the time and hash have to be equal to reject the message
                                            {
                                                info!("[Message Handler {}] Message is a duplicate. Skipping message.", self.queue_type);
                                                rejected = true;
                                                break;
                                            }
                                        }

                                        if rejected {
                                            continue;
                                        } else {
                                            dedupe_queue_loop
                                                .lock()
                                                .await
                                                .push_back((message_time as u64, hashed_value));
                                        }
                                    }

                                    if self.should_override_station_name {
                                        trace!(
                                            "[Message Handler {}] Overriding station name to {}",
                                            self.queue_type,
                                            self.station_name
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

                                    debug!(
                                        "[Message Handler {}] SENDING: {:?}",
                                        self.queue_type, message
                                    );
                                    trace!(
                                        "[Message Handler {}] Hashed value: {}",
                                        self.queue_type,
                                        hashed_value
                                    );
                                    trace!(
                                        "[Message Handler {}] Final message: {:?}",
                                        self.queue_type,
                                        message
                                    );

                                    match output_queue.send(message).await {
                                        Ok(_) => debug!("[Message Handler {}] Message sent to output queue", self.queue_type),
                                        Err(e) => error!("[Message Handler {}] Error sending message to output queue: {}", self.queue_type, e)
                                    }
                                    // let parse_final_message: MessageResult<String> = message.to_string();
                                    // match parse_final_message {
                                    //     Err(parse_error) => error!("{}", parse_error),
                                    //     Ok(final_message) => {
                                    //         // Send to the output methods for emitting on the network
                                    //         debug!("[Message Handler {}] Message to be sent: {}", self.queue_type, &final_message);
                                    //         match output_queue.send(final_message).await {
                                    //             Ok(_) => debug!("[Message Handler {}] Message sent to output queue", self.queue_type),
                                    //             Err(e) => error!("[Message Handler {}] Error sending message to output queue: {}", self.queue_type, e)
                                    //         };
                                    //     }
                                    // }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

pub fn print_formatted_stats(
    total_all_time: i32,
    total_since_last: i32,
    frequencies: Option<Vec<FrequencyCount>>,
    stats_minutes: u64,
    queue_type: &str,
    has_counted_freqs: bool,
) -> String {
    let mut output: String = String::new();
    output.push_str(&format!(
            "{} in the last {} minute(s):\nTotal messages processed: {}\nTotal messages processed since last update: {}\n",
            queue_type, stats_minutes, total_all_time, total_since_last
        ));

    if has_counted_freqs && frequencies.is_some() {
        output.push_str(
            format!("{} Frequencies logged since container start:\n", queue_type).as_str(),
        );
        if let Some(freqs) = frequencies {
            for freq in freqs.iter() {
                let percentage: f64 = (freq.count as f64 / total_all_time as f64) * 100.0;
                output.push_str(
                    format!(
                        "\n{} {}: {}/{} ({:.2}%)",
                        queue_type, freq.freq, freq.count, total_all_time, percentage
                    )
                    .as_str(),
                );
            }
        }
    } else if frequencies.is_some() {
        output.push_str(format!("{} No frequencies logged.\n", queue_type).as_str());
    }

    output
}

pub async fn print_stats(
    total_all_time: Arc<Mutex<i32>>,
    total_since_last: Arc<Mutex<i32>>,
    frequencies: Option<Arc<Mutex<Vec<FrequencyCount>>>>,
    stats_every: u64,
    queue_type: &str,
) {
    let stats_minutes = stats_every / 60;
    let mut has_counted_freqs = false;
    loop {
        sleep(Duration::from_secs(stats_every)).await;
        let total_all_time_locked = *total_all_time.lock().await;
        let total_since_last_locked = *total_since_last.lock().await;

        *total_since_last.lock().await = 0;

        // now print the frequencies, and show each as a percentage of the total_all_time

        if let Some(f) = &frequencies {
            // sort the frequencies by count
            if let Some(f) = &frequencies {
                f.lock().await.sort_by(|a, b| b.count.cmp(&a.count));
            }

            if !has_counted_freqs && f.lock().await.len() > 0 {
                has_counted_freqs = true;
            }
        }

        let mut freqs_locked = None;

        // TODO: This thing is ugly and bad
        if let Some(f) = &frequencies {
            // copy the contents of the frequencies vector into a new vector
            let mut freqs: Vec<FrequencyCount> = Vec::new();
            for freq in f.lock().await.iter() {
                freqs.push(FrequencyCount {
                    freq: freq.freq.clone(),
                    count: freq.count,
                });
            }

            freqs_locked = Some(freqs);
        }

        info!(
            "{}",
            print_formatted_stats(
                total_all_time_locked,
                total_since_last_locked,
                freqs_locked,
                stats_minutes,
                queue_type,
                has_counted_freqs
            )
        );
    }
}

pub async fn clean_up_dedupe_queue(
    dedupe_queue: Arc<Mutex<VecDeque<(u64, u64)>>>,
    dedupe_window: u64,
    queue_type: &str,
) {
    loop {
        sleep(Duration::from_secs(dedupe_window)).await;
        // Remove old messages from the dedupe_queue
        if !dedupe_queue.lock().await.is_empty() {
            // iterate through dedupe_que and remove old messages
            let current_time = match SystemTime::now().duration_since(UNIX_EPOCH) {
                Ok(n) => n.as_secs(),
                Err(_) => 0,
            };

            dedupe_queue.lock().await.retain(|message| {
                let (timestamp, _) = message;
                let diff = current_time - timestamp;
                diff <= dedupe_window
            });

            debug!(
                "[Message Handler {}] dedupe queue size after pruning {}",
                queue_type,
                dedupe_queue.lock().await.len()
            );
        }
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
    let parse_msg: MessageResult<String> = message.to_string();
    match parse_msg {
        Err(parse_error) => Err(parse_error),
        Ok(msg) => {
            trace!("[Hasher] Message to be hashed: {}", msg);
            msg.hash(&mut hasher);
            let hashed_value = hasher.finish();
            Ok(hashed_value)
        }
    }
}

// create a test for print_stats

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_print_stats() {
        let total_all_time: i32 = 100;
        let total_since_last: i32 = 100;
        let mut frequencies: Vec<FrequencyCount> = Vec::new();
        let stats_every: u64 = 1;
        let queue_type: &str = "TEST";

        // fill the frequencies with some data

        let freq_three: FrequencyCount = FrequencyCount {
            freq: "134.525".to_string(),
            count: 70,
        };

        frequencies.push(freq_three);

        let freq_one: FrequencyCount = FrequencyCount {
            freq: "131.550".to_string(),
            count: 20,
        };

        frequencies.push(freq_one);

        let freq_two: FrequencyCount = FrequencyCount {
            freq: "131.525".to_string(),
            count: 10,
        };

        frequencies.push(freq_two);

        let output_with_extra_stats = print_formatted_stats(
            total_all_time,
            total_since_last,
            Some(frequencies),
            stats_every,
            queue_type,
            true,
        );

        assert_eq!(
            output_with_extra_stats,
            "TEST in the last 1 minute(s):\nTotal messages processed: 100\nTotal messages processed since last update: 100\nTEST Frequencies logged since container start:\n\nTEST 134.525: 70/100 (70.00%)\nTEST 131.550: 20/100 (20.00%)\nTEST 131.525: 10/100 (10.00%)"
        );

        let output_without_extra_stats = print_formatted_stats(
            total_all_time,
            total_since_last,
            None,
            stats_every,
            queue_type,
            false,
        );

        assert_eq!(
            output_without_extra_stats,
            "TEST in the last 1 minute(s):\nTotal messages processed: 100\nTotal messages processed since last update: 100\n"
        );
    }
}
