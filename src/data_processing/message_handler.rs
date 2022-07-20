// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

use crate::hasher::hash_message;
use log::{debug, error, info, trace};
use std::collections::VecDeque;
use std::env;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

#[derive(Clone, Debug)]
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

pub async fn print_stats(
    total_all_time: Arc<Mutex<i32>>,
    total_since_last: Arc<Mutex<i32>>,
    stats_every: u64,
    queue_type: &str,
) {
    let stats_minutes = stats_every / 60;
    loop {
        sleep(Duration::from_secs(stats_every)).await;
        info!("{queue_type} in the last {stats_minutes} minute(s):");
        info!(
            "Total {} messages processed: {}",
            &queue_type,
            total_all_time.lock().await
        );

        info!(
            "Total {} messages processed since last update: {}",
            &queue_type,
            total_since_last.lock().await
        );

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
                if diff > dedupe_window {
                    false
                } else {
                    true
                }
            });

            debug!(
                "[Message Handler {}] dedupe queue size after pruning {}",
                queue_type,
                dedupe_queue.lock().await.len()
            );
        }
    }
}

pub async fn watch_received_message_queue(
    mut input_queue: Receiver<serde_json::Value>,
    output_queue: Sender<serde_json::Value>,
    config: &MessageHandlerConfig,
) {
    let dedupe_queue: Arc<Mutex<VecDeque<(u64, u64)>>> =
        Arc::new(Mutex::new(VecDeque::with_capacity(100)));
    let total_messages_processed = Arc::new(Mutex::new(0));
    let total_messages_since_last = Arc::new(Mutex::new(0));
    let queue_type_stats = config.queue_type.clone();
    let queue_type_dedupe = config.queue_type.clone();
    let stats_every = config.stats_every.clone() * 60; // Value has to be in seconds. Input is in minutes.
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
        )
        .await;
    });

    // Generate an async loop that sleeps for the requested dedupe window and then cleans the queue
    // Give it the context for the dedupe queue
    // The dedupe queue to be cleaned.
    if config.dedupe {
        let dedupe_queue_context = Arc::clone(&dedupe_queue);
        let dedupe_window = config.dedupe_window.clone();

        tokio::spawn(async move {
            clean_up_dedupe_queue(
                dedupe_queue_context,
                dedupe_window,
                queue_type_dedupe.as_str(),
            )
            .await;
        });
    }

    while let Some(mut message) = input_queue.recv().await {
        // Grab the mutexes for the stats counter and increment the total messages processed by the message handler.
        let stats_total_loop_context = Arc::clone(&total_messages_processed);
        let stats_total_loop_since_last_context = Arc::clone(&total_messages_since_last);
        let dedupe_queue_loop = Arc::clone(&dedupe_queue);
        *stats_total_loop_since_last_context.lock().await += 1;
        *stats_total_loop_context.lock().await += 1;

        trace!("[Message Handler {}] GOT: {}", config.queue_type, message);

        // Create a copy of what we received. Not really used anywhere in the program but it's useful for debugging
        let original_message = message.clone();

        let current_time = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(n) => n.as_secs(),
            Err(_) => 0,
        };

        // acarsdec/vdlm2dec use floating point message times. dumpvdl2 uses ints.
        // Determine if the message is dumpvdl2 or not, and handle correctly
        let mut message_time = match message.get("vdl2") {
            Some(_) => message["vdl2"]["t"]["sec"].as_u64().unwrap_or(0),
            // TODO: I'd like to do this better. The as_u64() function did not give a correct value
            // So we take the f64 value, round it, then cast it to u64.
            // ALSO we seem to truncate the timestamp if the timestamp is too long.
            // We're losing like.....pico seconds of precision or something. I don't care
            // But we probably should? It is in fact mangling the output either here
            // Or somewhere along the way. Perhaps it's the conversion from Value back to string....
            None => message["timestamp"].as_f64().unwrap_or(0.0).round() as u64,
        };

        // Sanity check to verify the message has a time stamp
        // If no time stamp, reject the message
        if message_time == 0 {
            error!(
                "[Message Handler {}] Message has no timestamp field. Skipping message.",
                config.queue_type
            );
            continue;
        }

        // We can end up in a condition with rounding of the message time we end up thinking the message is
        // from the future. It will cause the next check to panic. As a work around we'll set message_time to current time
        // if the message time is in the future.
        // TODO: This gives me the willies and seems like a terrible hack. Do better.

        if message_time > current_time {
            if (message_time - current_time) > config.skew_window {
                error!(
                    "[Message Handler {}] Message is from the future. Skipping. Current time {}, Message time {}. Difference {}",
                    config.queue_type, current_time, message_time, message_time - current_time
                );
                continue;
            }

            trace!(
                "[Message Handler {}] Message is from the future. Current time {}, Message time {}. Difference {}",
                config.queue_type, current_time, message_time, message_time - current_time
            );
            message_time = current_time;
        }
        // If the message is older than the skew window, reject it
        else if (current_time - message_time) > config.skew_window {
            error!(
                "[Message Handler {}] Message is {} seconds old. Time in message {}. Skipping message. {}",
                config.queue_type,
                current_time - message_time,
                message_time,
                config.skew_window
            );
            continue;
        }

        // Time to hash the message
        let (hashed_value, hashed_message) = hash_message(message.clone());

        if config.dedupe {
            let mut rejected = false;
            for (_, hashed_value_saved) in dedupe_queue_loop.lock().await.iter() {
                if *hashed_value_saved == hashed_value {
                    info!(
                        "[Message Handler {}] Message is a duplicate. Skipping message.",
                        config.queue_type
                    );
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
                    .push_back((message_time, hashed_value.clone()));
            }
        }

        if config.should_override_station_name {
            trace!(
                "[Message Handler {}] Overriding station name to {}",
                config.queue_type,
                config.station_name
            );

            match message["vdl2"].get("app") {
                // dumpvdl2 message
                Some(_) => {
                    message["vdl2"]["station"] =
                        serde_json::Value::String(config.station_name.clone());
                }
                // acarsdec or vdlm2dec message
                None => {
                    message["station_id"] = serde_json::Value::String(config.station_name.clone());
                }
            }
        }

        if config.add_proxy_id {
            trace!(
                "[Message Handler {}] Adding proxy_id to message",
                config.queue_type
            );
            match message["vdl2"].get("app") {
                // dumpvdl2 message
                Some(_) => {
                    message["vdl2"]["app"]["proxied"] = serde_json::Value::Bool(true);
                    message["vdl2"]["app"]["proxied_by"] =
                        serde_json::Value::String("acars_router".to_string());
                    message["vdl2"]["app"]["acars_router_version"] =
                        serde_json::Value::String(version.to_string());
                }
                // acarsdec or vdlm2dec message
                None => match message.get("app") {
                    Some(_) => {
                        message["app"]["proxied"] = serde_json::Value::Bool(true);
                        message["app"]["proxied_by"] =
                            serde_json::Value::String("acars_router".to_string());
                        message["app"]["acars_router_version"] =
                            serde_json::Value::String(version.to_string());
                    }
                    None => {
                        // insert app in to message
                        // This is a legacy implementation to handle old versions of acarsdec and vdlm2dec
                        message["app"] = serde_json::Value::Object(serde_json::map::Map::new());
                        message["app"]["proxied"] = serde_json::Value::Bool(true);
                        message["app"]["proxied_by"] =
                            serde_json::Value::String("acars_router".to_string());
                        message["app"]["acars_router_version"] =
                            serde_json::Value::String(version.to_string());
                    }
                },
            }
        }

        debug!(
            "[Message Handler {}] SENDING: {}",
            config.queue_type, message
        );
        let mut final_message = serde_json::Value::Object(serde_json::map::Map::new());

        // Set up the final json object following the pythonic convention
        // "data" stores the input JSON
        // "hash" stores the hashed value
        // "msg_time" stores the message time (I added this, not in the pythonic JSON format)
        // "out_json" stores the output JSON
        // We are not following the pythonic convention fully. There is a "JSON" field where a bunch of internal structures are stored.
        // I'll just store the message that was hashed here

        final_message["msg_time"] =
            serde_json::Value::Number(serde_json::Number::from(message_time));
        final_message["hash"] = serde_json::Value::Number(serde_json::Number::from(hashed_value));
        final_message["out_json"] = message;
        final_message["data"] = original_message;
        final_message["json"] = hashed_message;

        trace!(
            "[Message Handler {}] Final message: {}",
            config.queue_type,
            final_message
        );

        // Send to the output methods for emitting on the network
        match output_queue.send(final_message).await {
            Ok(_) => {
                debug!(
                    "[Message Handler {}] Message sent to output queue",
                    config.queue_type
                );
            }
            Err(e) => {
                error!(
                    "[Message Handler {}] Error sending message to output queue: {}",
                    config.queue_type, e
                );
            }
        };
    }
}
