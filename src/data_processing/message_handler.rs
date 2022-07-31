// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

use crate::hasher::hash_message;
use crate::Input;
use log::{debug, error, info, trace};
use std::collections::VecDeque;
use std::env;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use acars_vdlm2_parser::{AcarsVdlm2Message, DecodeMessage, MessageResult};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

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
}

impl MessageHandlerConfig {
    pub fn new(args: &Input, queue_type: &str) -> Self {
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
            }
        }
    }
}

pub async fn print_stats(
    total_all_time: Arc<Mutex<i32>>,
    total_since_last: Arc<Mutex<i32>>,
    stats_every: u64,
    queue_type: &str,
) {
    let stats_minutes = stats_every / 60;
    loop {
        sleep(Duration::from_secs(stats_every)).await;
        info!("{} in the last {} minute(s):\nTotal messages processed: {}\nTotal messages processed since last update: {}",
            queue_type, stats_minutes, total_all_time.lock().await, total_since_last.lock().await);
        *total_since_last.lock().await = 0;
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

            debug!("[Message Handler {}] dedupe queue size after pruning {}", queue_type, dedupe_queue.lock().await.len());
        }
    }
}

pub async fn watch_received_message_queue(
    mut input_queue: Receiver<String>,
    output_queue: Sender<String>,
    config: MessageHandlerConfig,
) {
    let dedupe_queue: Arc<Mutex<VecDeque<(u64, u64)>>> =
        Arc::new(Mutex::new(VecDeque::with_capacity(100)));
    let total_messages_processed = Arc::new(Mutex::new(0));
    let total_messages_since_last = Arc::new(Mutex::new(0));
    let queue_type_stats = config.queue_type.clone();
    let queue_type_dedupe = config.queue_type.clone();
    let stats_every = config.stats_every * 60; // Value has to be in seconds. Input is in minutes.
    let version = env!("CARGO_PKG_VERSION");

    // Generate an async loop that sleeps for the requested stats print duration and then logs
    // Give it the context for the counters
    // The stats values to the console.

    let stats_total_messages_context = Arc::clone(&total_messages_processed);
    let stats_total_messages_since_last_context = Arc::clone(&total_messages_since_last);

    tokio::spawn(async move {
        print_stats(
            stats_total_messages_context,
            stats_total_messages_since_last_context,
            stats_every,
            queue_type_stats.as_str(),
        ).await;
    });

    // Generate an async loop that sleeps for the requested dedupe window and then cleans the queue
    // Give it the context for the dedupe queue
    // The dedupe queue to be cleaned.
    if config.dedupe {
        let dedupe_queue_context = Arc::clone(&dedupe_queue);
        let dedupe_window = config.dedupe_window;

        tokio::spawn(async move {
            clean_up_dedupe_queue(
                dedupe_queue_context,
                dedupe_window,
                queue_type_dedupe.as_str(),
            ).await;
        });
    }

    while let Some(message_content) = input_queue.recv().await {
        // Grab the mutexes for the stats counter and increment the total messages processed by the message handler.
        let parse_message: MessageResult<AcarsVdlm2Message> = message_content.decode_message();
        let stats_total_loop_context = Arc::clone(&total_messages_processed);
        let stats_total_loop_since_last_context = Arc::clone(&total_messages_since_last);
        let dedupe_queue_loop = Arc::clone(&dedupe_queue);
        *stats_total_loop_since_last_context.lock().await += 1;
        *stats_total_loop_context.lock().await += 1;

        trace!("[Message Handler {}] GOT: {:?}", config.queue_type, parse_message);

        match parse_message {
            Err(parse_error) => error!("[Message Handler {}] Failed to parse received message: {}\nReceived: {}", config.queue_type, parse_error, message_content),
            Ok(mut message) => {

                let current_time = match SystemTime::now().duration_since(UNIX_EPOCH) {
                    Ok(n) => n.as_secs_f64(),
                    Err(_) => f64::default(),
                };

                // acarsdec/vdlm2dec use floating point message times. dumpvdl2 uses ints.
                // Determine if the message is dumpvdl2 or not, and handle correctly
                // FIXME: There is some stupid bug here, and it'll probably be fixed by working out the todo below
                // Basically I'll see the test suite fail super occasionally with an incorrect number of messages sent back,
                // and a rerun of the test suite will show no issues. I suspect this is due to a message either NOT being marked as dupe when
                // it should or a message not being marked as dupe when it should. Really this boils down to a) when the dupe queue is cleared,
                // and b) the fact that we're only being precise as a second.
                // In practice what this means is we'll probably see some messages get improperly handled, but it's such an edge case
                // that it's likely this only really is a concern in super-high message rate envs (like the test suite).

                let get_message_time = message.get_time();

                match get_message_time {
                    None => {
                        error!("[Message Handler {}] Message has no timestamp field. Skipping message.", config.queue_type);
                        continue;
                    },
                    Some(mut message_time) => {
                        if message_time > current_time {
                            if (message_time - current_time) > config.skew_window as f64 {
                                error!("[Message Handler {}] Message is from the future. Skipping. Current time {}, Message time {}. Difference {}", config.queue_type, current_time, message_time, message_time - current_time);
                                continue;
                            }
                            trace!("[Message Handler {}] Message is from the future. Current time {}, Message time {}. Difference {}", config.queue_type, current_time, message_time, message_time - current_time);
                            message_time = current_time;
                        }
                        // If the message is older than the skew window, reject it
                        else if (current_time - message_time) > config.skew_window as f64 {
                            error!("[Message Handler {}] Message is {} seconds old. Time in message {}. Skipping message. {}", config.queue_type, current_time - message_time, message_time, config.skew_window);
                            continue;
                        }

                        // Time to hash the message
                        let hash_message = hash_message(message.clone());

                        match hash_message {
                            Err(hash_parsing_error) => error!("{}", hash_parsing_error),
                            Ok(hashed_value) => {
                                if config.dedupe {
                                    let mut rejected = false;
                                    for (time, hashed_value_saved) in dedupe_queue_loop.lock().await.iter() {
                                        let f64_time = *time as f64;
                                        if *hashed_value_saved == hashed_value && current_time - f64_time < config.dedupe_window as f64
                                        // Both the time and hash have to be equal to reject the message
                                        {
                                            info!("[Message Handler {}] Message is a duplicate. Skipping message.", config.queue_type);
                                            rejected = true;
                                            break;
                                        }
                                    }

                                    if rejected {
                                        continue;
                                    } else {
                                        dedupe_queue_loop.lock().await.push_back((message_time as u64, hashed_value));
                                    }
                                }

                                match config.should_override_station_name {
                                    false => message.clear_station_name(), // We'll nuke it again, just in case.
                                    true => {
                                        trace!("[Message Handler {}] Overriding station name to {}", config.queue_type, config.station_name);
                                        message.set_station_name(&config.station_name);
                                    }
                                }

                                match config.add_proxy_id {
                                    false => message.clear_proxy_details(), // This shouldn't already be set, but we'll blow it away just in case
                                    true => {
                                        trace!("[Message Handler {}] Adding proxy_id to message", config.queue_type);
                                        message.set_proxy_details("acars_router", version);
                                    }
                                }

                                debug!("[Message Handler {}] SENDING: {:?}", config.queue_type, message);
                                trace!("[Message Handler {}] Hashed value: {}", config.queue_type, hashed_value);
                                trace!("[Message Handler {}] Final message: {:?}", config.queue_type, message);

                                let parse_final_message: MessageResult<String> = message.to_string();
                                match parse_final_message {
                                    Err(parse_error) => error!("{}", parse_error),
                                    Ok(final_message) => {
                                        // Send to the output methods for emitting on the network
                                        match output_queue.send(final_message).await {
                                            Ok(_) => debug!("[Message Handler {}] Message sent to output queue", config.queue_type),
                                            Err(e) => error!("[Message Handler {}] Error sending message to output queue: {}", config.queue_type, e)
                                        };
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
