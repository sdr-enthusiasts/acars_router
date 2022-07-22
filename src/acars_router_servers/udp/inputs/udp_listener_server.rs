// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

// Server used to receive UDP data

use crate::helper_functions::strip_line_endings;
use log::{debug, error, info, trace, warn};
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
                // Tuple is time, peer and message
                let partial_messages: Arc<Mutex<Vec<(u64, SocketAddr, String)>>> =
                    Arc::new(Mutex::new(vec![]));

                let clean_queue = Arc::clone(&partial_messages);
                let loop_assembly_window = reassembly_window; // time is in seconds

                tokio::spawn(async move {
                    loop {
                        sleep(Duration::from_millis(loop_assembly_window * 1000)).await;
                        clean_invalid_message_queue(Arc::clone(&clean_queue), loop_assembly_window)
                            .await;
                    }
                });

                loop {
                    if let Some((size, peer)) = to_send {
                        let s: String = match str::from_utf8(buf[..size].as_ref()) {
                            Ok(s) => strip_line_endings(&s.to_string()).to_owned(),
                            Err(_) => {
                                warn!(
                                    "[UDP SERVER: {}] Invalid message received from {}",
                                    proto_name, peer
                                );
                                continue;
                            }
                        };
                        let partial_messages_context = Arc::clone(&partial_messages);
                        // First attempt to deserialise just the new message
                        match attempt_message_reassembly(
                            partial_messages_context,
                            s,
                            peer,
                            proto_name.clone(),
                        )
                        .await
                        {
                            Some(msg) => {
                                // We have valid JSON
                                trace!("[UDP SERVER: {}] {}/{}: {}", proto_name, size, peer, msg);

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
    messages: Arc<Mutex<Vec<(u64, SocketAddr, String)>>>,
    new_message_string: String,
    peer: SocketAddr,
    server_type: String,
) -> Option<serde_json::Value> {
    match serde_json::from_str::<serde_json::Value>(new_message_string.as_str()) {
        Ok(msg) => {
            messages.lock().await.clear();
            return Some(msg);
        }
        Err(_) => (),
    }
    info!("Checking message");
    let test_message: Arc<Mutex<String>> = Arc::new(Mutex::new("".to_string()));
    let mut output_message: Option<serde_json::Value> = None;
    // TODO: This probably would be so much better if we walked the entire set of potential messages
    // From the peer we're testing against
    // and ONLY remove the messages that are valid for message reconstitution.
    // TODO: This method does not consider messages being received out of order
    for (_, peer_in_loop, msg) in messages.lock().await.iter() {
        if *peer_in_loop != peer {
            continue;
        }

        let message_built_up = test_message.lock().await.clone();
        let message_to_test =
            message_built_up.clone() + *&msg.as_str().clone() + &new_message_string;
        match serde_json::from_str::<serde_json::Value>(message_to_test.as_str()) {
            Ok(msg_deserialized) => {
                debug!(
                    "[UDP SERVER: {}] Reconstituted a message {}",
                    server_type, message_to_test
                );
                output_message = Some(msg_deserialized);
                break;
            }
            Err(_) => (),
        };

        *test_message.lock().await = message_built_up.clone() + *&msg.as_str();
    }

    match output_message {
        Some(_) => {
            messages.lock().await.retain(|(_, peer, _)| peer != peer);
        }
        None => {
            let current_time = match SystemTime::now().duration_since(UNIX_EPOCH) {
                Ok(n) => n.as_secs(),
                Err(_) => 0,
            };

            messages
                .lock()
                .await
                .push((current_time, peer, new_message_string));
        }
    }

    output_message
}

async fn clean_invalid_message_queue(
    queue: Arc<Mutex<Vec<(u64, SocketAddr, String)>>>,
    reassembly_window: u64,
) {
    let current_time = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(n) => n.as_secs(),
        Err(_) => 0,
    };

    if current_time == 0 {
        return;
    }

    queue.lock().await.retain(|old_messages| {
        let (time, _, _) = old_messages;
        if time + reassembly_window < current_time {
            false
        } else {
            true
        }
    });
}
