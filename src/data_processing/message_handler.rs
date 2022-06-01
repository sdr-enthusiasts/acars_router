// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

use log::debug;
use log::error;
use log::trace;
use queue::Queue;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::Receiver;

#[path = "./hasher.rs"]
mod hasher;
use hasher::hash_message;

pub struct MessageHandlerConfig {
    pub add_proxy_id: bool,
    pub dedupe: bool,
    pub dedupe_window: u64,
    pub skew_window: u64,
}

pub async fn watch_message_queue(
    mut queue: Receiver<serde_json::Value>,
    config: &MessageHandlerConfig,
) {
    let mut q = Queue::with_capacity(100);
    while let Some(mut message) = queue.recv().await {
        debug!("[Message Handler] GOT: {}", message);

        let current_time = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(n) => n.as_secs(),
            Err(_) => 0,
        };

        let message_time = match message.get("vdl2") {
            Some(_) => message["vdl2"]["t"]["sec"].as_u64().unwrap_or(0),
            // TODO: I'd like to do this better. The as_u64() function did not give a correct value
            // So we take the f64 value, round it, then cast it to u64.
            None => message["timestamp"].as_f64().unwrap_or(0.0).round() as u64,
        };

        // Sanity check to verify the message has a time stamp
        // If no time stamp, reject the message
        if message_time == 0 {
            error!("Message has no timestamp field. Skipping message.");
            continue;
        }

        // If the message is older than the skew window, reject it
        if (current_time - message_time) > config.skew_window {
            error!(
                "[Message Handler] Message is {} seconds old. Skipping message.",
                current_time - message_time
            );
            continue;
        }

        // Hash the message, but only if we have deduping on
        // No need to spend the cpu cycles hashing the message if we are not deduping

        if config.dedupe {
            if (current_time - message_time) < config.dedupe_window {
                trace!("[Message Handler] Message Within DeDuplication Window. Hashing.");
                let hashed_message = hash_message(message.clone());
                if q.vec().contains(&hashed_message) {
                    debug!("[Message Handler] DUPLICATE: {}", message);
                    continue;
                } else {
                    q.force_queue(hashed_message);
                }
            }
        }

        if config.add_proxy_id {
            trace!("[Message Handler] Adding proxy_id to message");
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
        debug!("[Message Handler] SENDING: {}", message);
    }
}
