// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

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
            reassembly_window,
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

        if let Ok(msg) = serde_json::from_str(&new_message_string) {
            self.queue.lock().await.remove(&peer);
            return Some(msg);
        }

        let mut output_message: Option<serde_json::Value> = None;
        let mut message_for_peer = String::new();
        let mut old_time: Option<u64> = None; // Save the time of the first message for this peer

        // TODO: Handle message reassembly for out of sequence messages
        // TODO: Handle message reassembly for a peer where the peer is sending multiple fragmented messages
        // Maybe on those two? This could get really tricky to know if the message we've reassembled is all the same message
        // Because we could end up in a position where the packet splits in the same spot and things look right but the packets belong to different messages

        if self.queue.lock().await.contains_key(&peer) {
            info!(
                "[UDP SERVER: {}] Message received from {} is being reassembled",
                self.name, peer
            );
            let (time, message_to_test) = self.queue.lock().await.get(&peer).unwrap().clone();
            old_time = Some(time); // We have a good peer, save the time
            message_for_peer = format!("{}{}", message_to_test, new_message_string);
            match serde_json::from_str::<serde_json::Value>(message_for_peer.as_str()) {
                Ok(msg_deserialized) => {
                    info!(
                        "[UDP SERVER: {}] Reassembled a message from peer {}",
                        self.name, peer
                    );

                    // The default skew_window and are the same (1 second, but it doesn't matter)
                    // So we shouldn't see any weird issues where the message is reassembled
                    // BUT the time is off and causes the message to be rejected
                    // Below we use the FIRST non-reasssembled time to base the expiration of the entire queue off of.

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
                // If the len is 0 then it's the first non-reassembled message, so we'll save the new message in to the queue
                // Otherwise message_for_peer should already have the old messages + the new one already in it.
                if message_for_peer.is_empty() {
                    message_for_peer = new_message_string;
                }

                // We want the peer's message queue to expire once the FIRST message received from the peer is older
                // than the reassembly window. Therefore we use the old_time we grabbed from the queue above, or if it's the first
                // message we get the current time.

                let message_queue_time = match old_time {
                    Some(t) => t,
                    None => match SystemTime::now().duration_since(UNIX_EPOCH) {
                        Ok(n) => n.as_secs(),
                        Err(_) => 0,
                    },
                };

                self.queue
                    .lock()
                    .await
                    .insert(peer, (message_queue_time, message_for_peer));
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
