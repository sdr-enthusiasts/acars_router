// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

// Used to send UDP data

use serde_json::Value;
use tokio::net::UdpSocket;
use tokio::sync::mpsc::Receiver;
use tokio::time::{sleep, Duration};

#[derive(Debug)]
pub struct UDPSenderServer {
    pub host: Vec<String>,
    pub proto_name: String,
    pub socket: UdpSocket,
    pub max_udp_packet_size: usize,
    pub channel: Receiver<Value>,
}

impl UDPSenderServer {
    pub async fn send_message(mut self) {
        // send the message to the socket
        // Loop through all of the sockets in the host list
        // We will send out a configured max amount bytes at a time until the buffer is exhausted

        while let Some(message) = self.channel.recv().await {
            let message_as_string: Result<String, serde_json::Error> = serde_json::to_string(&message["out_json"]);
            match message_as_string {
                Err(parse_error) => error!("Failed to parse Value to String: {}", parse_error),
                Ok(string) => {
                    let message_as_bytes = string.as_bytes();
                    let message_size: usize = message_as_bytes.len();

                    for addr in &self.host {
                        let mut keep_sending: bool = true;
                        let mut buffer_position: usize = 0;
                        let mut buffer_end: usize = match message_as_bytes.len() < self.max_udp_packet_size {
                            true => message_as_bytes.len(),
                            false => self.max_udp_packet_size,
                        };

                        while keep_sending {
                            trace!("[UDP SENDER {}] Sending {buffer_position} to {buffer_end} of {message_size} to {addr}", self.proto_name);
                            let bytes_sent = self
                                .socket
                                .send_to(&message_as_bytes[buffer_position..buffer_end], addr)
                                .await;

                            match bytes_sent {
                                Ok(bytes_sent) => {
                                    debug!(
                                "[UDP SENDER {}] sent {} bytes to {}",
                                self.proto_name, bytes_sent, addr
                            );
                                }
                                Err(e) => {
                                    warn!(
                                "[UDP SENDER {}] failed to send message to {}: {:?}",
                                self.proto_name, addr, e
                            );
                                }
                            }

                            if buffer_end == message_size {
                                keep_sending = false;
                            } else {
                                buffer_position = buffer_end;
                                buffer_end = match buffer_position + self.max_udp_packet_size < message_size
                                {
                                    true => buffer_position + self.max_udp_packet_size,
                                    false => message_size,
                                };

                                // Slow the sender down!
                                sleep(Duration::from_millis(100)).await;
                            }
                            trace!(
                        "[UDP SENDER {}] New buffer start: {}, end: {}",
                        self.proto_name,
                        buffer_position,
                        buffer_end
                    );
                        }
                    }
                }
            }
        }
    }
}
