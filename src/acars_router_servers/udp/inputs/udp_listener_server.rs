// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

// Server used to receive UDP data

use log::{debug, error, info, trace, warn};
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::str;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::UdpSocket;
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

pub struct UDPListenerServer {
    pub buf: Vec<u8>,
    pub to_send: Option<(usize, SocketAddr)>,
    pub proto_name: String,
    pub reassembly_window: u64,
}

impl UDPListenerServer {
    pub async fn run(
        self,
        listen_udp_port: String,
        channel: Sender<serde_json::Value>,
    ) -> Result<(), io::Error> {
        let UDPListenerServer {
            mut buf,
            mut to_send,
            proto_name,
            reassembly_window,
        } = self;

        let s = UdpSocket::bind(&listen_udp_port).await;

        match s {
            Ok(socket) => {
                info!(
                    "[UDP SERVER: {}]: Listening on: {}",
                    proto_name,
                    socket.local_addr()?
                );
                // Hashmap key is peer, stores a tuple of time and message
                let partial_messages: Arc<Mutex<HashMap<SocketAddr, (u64, String)>>> =
                    Arc::new(Mutex::new(HashMap::new()));

                let clean_queue = Arc::clone(&partial_messages);
                let loop_assembly_window = reassembly_window; // time is in seconds
                let loop_name = proto_name.clone();

                tokio::spawn(async move {
                    loop {
                        sleep(Duration::from_millis(loop_assembly_window * 1000)).await;
                        clean_invalid_message_queue(
                            Arc::clone(&clean_queue),
                            loop_assembly_window,
                            loop_name.as_str(),
                        )
                        .await;
                    }
                });

                loop {
                    if let Some((size, peer)) = to_send {
                        let msg_string = match str::from_utf8(buf[..size].as_ref()) {
                            Ok(s) => s,
                            Err(_) => {
                                warn!(
                                    "[UDP SERVER: {}] Invalid message received from {}",
                                    proto_name, peer
                                );
                                continue;
                            }
                        };

                        let split_messages: Vec<&str> = msg_string.split_terminator('\n').collect();

                        for msg in split_messages {
                            let partial_messages_context = Arc::clone(&partial_messages);
                            // First attempt to deserialise just the new message
                            match attempt_message_reassembly(
                                partial_messages_context,
                                msg.to_string(),
                                peer,
                                proto_name.clone(),
                            )
                            .await
                            {
                                Some(msg) => {
                                    // We have valid JSON
                                    trace!(
                                        "[UDP SERVER: {}] {}/{}: {}",
                                        proto_name,
                                        size,
                                        peer,
                                        msg
                                    );

                                    match channel.send(msg).await {
                                        Ok(_) => trace!(
                                            "[UDP SERVER: {}] Message sent to channel",
                                            proto_name
                                        ),
                                        Err(e) => warn!(
                                            "[UDP SERVER: {}] Error sending message to channel: {}",
                                            proto_name, e
                                        ),
                                    };
                                }
                                None => {
                                    // The message is invalid. It's been saved for (maybe) later
                                    trace!(
                                        "[UDP SERVER: {}] Invalid message received from {}.",
                                        proto_name,
                                        peer
                                    );
                                }
                            }
                        }
                    }
                    to_send = Some(socket.recv_from(&mut buf).await?);
                }
            }

            Err(e) => {
                error!(
                    "[UDP SERVER: {}] Error listening on port: {}",
                    proto_name, e
                );
            }
        };
        Ok(())
    }
}

async fn attempt_message_reassembly(
    messages: Arc<Mutex<HashMap<SocketAddr, (u64, String)>>>,
    new_message_string: String,
    peer: SocketAddr,
    server_type: String,
) -> Option<serde_json::Value> {
    // FIXME: Ideally this entire function should not lock the mutex all the time

    match serde_json::from_str::<serde_json::Value>(new_message_string.as_str()) {
        Ok(msg) => {
            messages.lock().await.clear();
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
    if messages.lock().await.contains_key(&peer) {
        info!(
            "[UDP SERVER: {}] Message received from {} is being reassembled",
            server_type, peer
        );
        let (_, message_to_test) = messages.lock().await.get(&peer).unwrap().clone();
        message_for_peer = message_to_test.clone() + &new_message_string;
        match serde_json::from_str::<serde_json::Value>(message_for_peer.as_str()) {
            Ok(msg_deserialized) => {
                info!(
                    "[UDP SERVER: {}] Reassembled a message from peer {}",
                    server_type, peer
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
            messages.lock().await.remove(&peer);
        }
        None => {
            if message_for_peer.len() == 0 {
                message_for_peer = new_message_string;
            }

            let current_time = match SystemTime::now().duration_since(UNIX_EPOCH) {
                Ok(n) => n.as_secs(),
                Err(_) => 0,
            };

            messages
                .lock()
                .await
                .insert(peer, (current_time, message_for_peer));
        }
    }

    output_message
}

async fn clean_invalid_message_queue(
    queue: Arc<Mutex<HashMap<SocketAddr, (u64, String)>>>,
    reassembly_window: u64,
    queue_type: &str,
) {
    let current_time = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(n) => n.as_secs(),
        Err(_) => 0,
    };

    if current_time == 0 {
        return;
    }

    queue.lock().await.retain(|peer, old_messages| {
        let (time, _) = old_messages;
        let time_diff = current_time - *time;
        if time_diff > reassembly_window {
            debug!("[UDP SERVER {queue_type}] Peer {peer} has been idle for {time_diff} seconds, removing from queue");
            false
        } else {
            debug!("[UDP SERVER {queue_type}] Peer {peer} has been idle for {time_diff} seconds, keeping in queue");
            true
        }
    });
}
