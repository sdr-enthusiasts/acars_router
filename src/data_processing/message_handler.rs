// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

use crate::hasher::hash_message;
use log::{debug, error, info, trace};
use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::{Receiver, Sender};
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

pub async fn watch_received_message_queue(
    mut input_queue: Receiver<serde_json::Value>,
    output_queue: Sender<serde_json::Value>,
    config: &MessageHandlerConfig,
) {
    let mut dedupe_queue: VecDeque<(u64, u64)> = VecDeque::with_capacity(100);
    let mut total_messages_processed = 0;
    let mut total_messages_since_last = 0;
    let queue_type = config.queue_type.clone();
    let stats_every = config.stats_every.clone() * 60; // Value has to be in seconds. Input is in minutes.

    // Generate an async loop that sleeps for the requested stats print duration and then logs
    // The stats values to the console.

    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(stats_every)).await;
            info!(
                "Total {} messages processed: {}",
                &queue_type, total_messages_processed
            );

            info!(
                "Total {} messages processed since last update: {}",
                &queue_type, total_messages_since_last
            );

            total_messages_since_last = 0;
        }
    });

    while let Some(mut message) = input_queue.recv().await {
        total_messages_since_last += 1;
        total_messages_processed += 1;

        trace!("[Message Handler {}] GOT: {}", config.queue_type, message);

        let original_message = message.clone();

        let current_time = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(n) => n.as_secs(),
            Err(_) => 0,
        };

        // Clean up the dedupe queue
        // TODO: Can this be done....better?

        if !dedupe_queue.is_empty() {
            // iterate through dedupe_que and remove old messages
            // TODO: Can this be done in one shot using remove while iterating the queue?
            let mut indexes_to_remove = Vec::with_capacity(dedupe_queue.len());
            for (i, (time, _)) in dedupe_queue.iter().enumerate() {
                if current_time - time > config.dedupe_window {
                    indexes_to_remove.push(i);
                }
            }

            trace!(
                "[Message Handler {}] Removing {} old messages from dedupe queue",
                config.queue_type,
                indexes_to_remove.len()
            );

            for index in indexes_to_remove.iter().rev() {
                dedupe_queue.remove(*index);
            }
        }

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
            for (_, hashed_value_saved) in dedupe_queue.iter() {
                if *hashed_value_saved == hashed_value {
                    info!(
                        "[Message Handler {}] Message is a duplicate. Skipping message.",
                        config.queue_type
                    );
                    rejected = true;
                    continue;
                }
            }

            if rejected {
                continue;
            } else {
                dedupe_queue.push_back((message_time, hashed_value.clone()));
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
                }
                // acarsdec or vdlm2dec message
                None => match message.get("app") {
                    Some(_) => {
                        message["app"]["proxied"] = serde_json::Value::Bool(true);
                        message["app"]["proxied_by"] =
                            serde_json::Value::String("acars_router".to_string());
                    }
                    None => {
                        // insert app in to message
                        // This is a legacy implementation to handle old versions of acarsdec and vdlm2dec
                        message["app"] = serde_json::Value::Object(serde_json::map::Map::new());
                        message["app"]["proxied"] = serde_json::Value::Bool(true);
                        message["app"]["proxied_by"] =
                            serde_json::Value::String("acars_router".to_string());
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

        // Send to the output methods for emitting on the network
        output_queue.send(final_message).await.unwrap();
    }
}
