// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

use log::{debug, error, info};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
//use tokio::time::{sleep, Duration};

pub struct PacketHandler {
    name: String,
    // Hashmap key is peer, stores a tuple of time and message
    queue: Arc<Mutex<HashMap<SocketAddr, (u64, String)>>>,
    reassembly_window: u64,
}

impl PacketHandler {
    pub fn new(name: &str, reassembly_window: u64) -> PacketHandler {
        PacketHandler {
            name: name.to_string(),
            queue: Arc::new(Mutex::new(HashMap::new())),
            reassembly_window: reassembly_window,
        }
    }

    pub async fn attempt_message_reassembly(
        &self,
        new_message_string: String,
        peer: SocketAddr,
    ) -> Option<serde_json::Value> {
        // FIXME: This is a hack to get this concept working. Ideally we aren't cleaning the queue while
        // Processing a message
        self.clean_queue().await;
        // FIXME: Ideally this entire function should not lock the mutex all the time

        match serde_json::from_str::<serde_json::Value>(new_message_string.as_str()) {
            Ok(msg) => {
                self.queue.lock().await.remove(&peer);
                return Some(msg);
            }
            Err(_) => (),
        }

        let mut output_message: Option<serde_json::Value> = None;
        let mut message_for_peer = "".to_string();
        // TODO: This probably would be so much better if we walked the entire set of potential messages
        // From the peer we're testing against
        // and ONLY remove the messages that are valid for message reconstitution.
        // TODO: This method does not consider messages being received out of order
        // Basically we are only considering a single case for a peer: messages being received IN ORDER
        // And ONLY ONE FRAGMENTED MESSAGE AT A TIME
        if self.queue.lock().await.contains_key(&peer) {
            info!(
                "[UDP SERVER: {}] Message received from {} is being reassembled",
                self.name, peer
            );
            let (_, message_to_test) = self.queue.lock().await.get(&peer).unwrap().clone();
            message_for_peer = message_to_test.clone() + &new_message_string;
            match serde_json::from_str::<serde_json::Value>(message_for_peer.as_str()) {
                Ok(msg_deserialized) => {
                    info!(
                        "[UDP SERVER: {}] Reassembled a message from peer {}",
                        self.name, peer
                    );
                    // FIXME: This feels so very wrong, but if we reassemble a message it's possible that the last part of the
                    // message came in outside of the skew window, which will cause it to get rejected by the message_handler.
                    // So.....for now, we'll leave it alone but maybe we should replace the time stamp?
                    // Or perhaps we flag the message as "reassembled" and then the message_handler can decide what to do with it?

                    output_message = Some(msg_deserialized);
                }
                Err(e) => info!("{e}"),
            };
        }

        match output_message {
            Some(_) => {
                self.queue.lock().await.remove(&peer);
            }
            None => {
                if message_for_peer.len() == 0 {
                    message_for_peer = new_message_string;
                }

                let current_time = match SystemTime::now().duration_since(UNIX_EPOCH) {
                    Ok(n) => n.as_secs(),
                    Err(_) => 0,
                };

                self.queue
                    .lock()
                    .await
                    .insert(peer, (current_time, message_for_peer));
            }
        }

        output_message
    }

    pub async fn clean_queue(&self) {
        //        loop {
        //            sleep(Duration::from_millis(self.reassembly_window * 1000)).await;
        let current_time = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(n) => n.as_secs(),
            Err(_) => 0,
        };

        if current_time == 0 {
            error!("[UDP SERVER: {}] Error getting current time", self.name);
            //continue;
            return;
        }

        self.queue.lock().await.retain(|peer, old_messages| {
                let (time, _) = old_messages;
                let time_diff = current_time - *time;
                if time_diff > self.reassembly_window {
                    debug!("[UDP SERVER {}] Peer {peer} has been idle for {time_diff} seconds, removing from queue", self.name);
                    false
                } else {
                    debug!("[UDP SERVER {}] Peer {peer} has been idle for {time_diff} seconds, keeping in queue", self.name);
                    true
                }
            });
    }
    //   }
}
