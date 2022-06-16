// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

use crate::hasher::hash_message;
use log::debug;
use log::error;
use log::trace;
use log::warn;
use queue::Queue;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::{Receiver, Sender};

#[derive(Clone, Debug)]
pub struct MessageHandlerConfig {
    pub add_proxy_id: bool,
    pub dedupe: bool,
    pub dedupe_window: u64,
    pub skew_window: u64,
    pub queue_type: String,
}

pub async fn watch_received_message_queue(
    mut input_queue: Receiver<serde_json::Value>,
    output_queue: Sender<serde_json::Value>,
    config: &MessageHandlerConfig,
) {
    let mut q = Queue::with_capacity(100);

    while let Some(mut message) = input_queue.recv().await {
        trace!("[Message Handler {}] GOT: {}", config.queue_type, message);

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
            warn!(
                "[Message Handler {}] Message is from the future. Current time {}, Message time {}. Difference {}",
                config.queue_type, current_time, message_time, message_time - current_time
            );
            message_time = current_time;
        }

        // If the message is older than the skew window, reject it
        if (current_time - message_time) > config.skew_window {
            error!(
                "[Message Handler {}] Message is {} seconds old. Skipping message. {}",
                config.queue_type,
                current_time - message_time,
                config.skew_window
            );
            continue;
        }

        // Hash the message, but only if we have deduping on
        // No need to spend the cpu cycles hashing the message if we are not deduping

        if config.dedupe {
            // TODO: after supporting the pythonic convention for hashing, remove old messages outside of the window from the queue
            if (current_time - message_time) < config.dedupe_window {
                trace!(
                    "[Message Handler {}] Message Within DeDuplication Window. Hashing.",
                    config.queue_type
                );
                let hashed_message = hash_message(message.clone());
                if q.vec().contains(&hashed_message) {
                    debug!(
                        "[Message Handler {}] DUPLICATE: {}",
                        config.queue_type, message
                    );
                    continue;
                } else {
                    q.force_queue(hashed_message);
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

        // TODO: to follow acars_router pythonic conventions
        // Add in the hashed message to the message
        // We'll need the hasher to return both the hashed value and the hashed message
        // And move the actual message we're sending to the appropriate field
        debug!(
            "[Message Handler {}] SENDING: {}",
            config.queue_type, message
        );
        // Send to the output methods for emitting on the network
        output_queue.send(message).await.unwrap();
    }
}
