// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

// Server used to receive UDP data

use std::io;
use acars_vdlm2_parser::AcarsVdlm2Message;
use tokio::net::UdpSocket;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::{sleep, Duration};
use acars_metrics::MessageDestination;
use crate::ServerType;


#[derive(Debug)]
pub(crate) struct UDPSenderServer {
    pub(crate) host: Vec<String>,
    pub(crate) proto_name: ServerType,
    pub(crate) logging_identifier: String,
    pub(crate) socket: UdpSocket,
    pub(crate) max_udp_packet_size: usize,
    pub(crate) channel: UnboundedReceiver<AcarsVdlm2Message>,
}

impl UDPSenderServer {
    pub(crate) fn new(
        send_udp: &[String],
        server_type: ServerType,
        socket: UdpSocket,
        max_udp_packet_size: &usize,
        rx_processed: UnboundedReceiver<AcarsVdlm2Message>,
    ) -> Self {
        let logging_identifier: String = match socket.local_addr() {
            Ok(local_addr) => format!("{}_UDP_SEND_{}", server_type, local_addr),
            Err(_) => format!("{}_UDP_SEND", server_type)
        };
        Self {
            host: send_udp.to_vec(),
            proto_name: server_type,
            logging_identifier,
            socket,
            max_udp_packet_size: *max_udp_packet_size,
            channel: rx_processed,
        }
    }

    pub(crate) async fn send_message(mut self) {
        // send the message to the socket
        // Loop through all of the sockets in the host list
        // We will send out a configured max amount bytes at a time until the buffer is exhausted

        while let Some(message) = self.channel.recv().await {
            match message.to_bytes_newline() {
                Err(bytes_error) => error!(
                    "[UDP SENDER {}] Failed to encode to bytes: {}",
                    self.logging_identifier, bytes_error
                ),
                Ok(message_as_bytes) => {
                    let message_size: usize = message_as_bytes.len();
                    for addr in &self.host {
                        let mut keep_sending: bool = true;
                        let mut buffer_position: usize = 0;
                        let mut buffer_end: usize =
                            match message_as_bytes.len() < self.max_udp_packet_size {
                                true => message_as_bytes.len(),
                                false => self.max_udp_packet_size,
                            };

                        while keep_sending {
                            trace!("[UDP SENDER {}] Sending {buffer_position} to {buffer_end} of {message_size} to {addr}", self.logging_identifier);
                            let bytes_sent: io::Result<usize> = self
                                .socket
                                .send_to(&message_as_bytes[buffer_position..buffer_end], addr)
                                .await;

                            match bytes_sent {
                                Ok(bytes_sent) => debug!(
                                    "[UDP SENDER {}] sent {} bytes to {}",
                                    self.logging_identifier, bytes_sent, addr
                                ),
                                Err(e) => warn!(
                                    "[UDP SENDER {}] failed to send message to {}: {:?}",
                                    self.logging_identifier, addr, e
                                ),
                            }

                            if buffer_end == message_size {
                                keep_sending = false;
                            } else {
                                buffer_position = buffer_end;
                                buffer_end = match buffer_position + self.max_udp_packet_size
                                    < message_size
                                {
                                    true => buffer_position + self.max_udp_packet_size,
                                    false => message_size,
                                };

                                // Slow the sender down!
                                sleep(Duration::from_millis(100)).await;
                            }
                            trace!(
                                "[UDP SENDER {}] New buffer start: {}, end: {}",
                                self.logging_identifier,
                                buffer_position,
                                buffer_end
                            );
                        }
                        self.proto_name.inc_message_destination_type_metric(MessageDestination::SendUdp);
                    }
                }
            }
        }
    }
}
