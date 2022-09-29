// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

// Server used to receive UDP data

use crate::packet_handler::{PacketHandler, ProcessAssembly};
use acars_vdlm2_parser::AcarsVdlm2Message;
use std::io;
use std::net::SocketAddr;
use tokio::net::UdpSocket;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration};

#[derive(Debug, Clone)]
pub(crate) struct UDPListenerServer {
    pub(crate) proto_name: String,
    pub(crate) reassembly_window: f64,
}

#[derive(Debug)]
pub(crate) struct UDPSenderServer {
    pub(crate) host: Vec<String>,
    pub(crate) proto_name: String,
    pub(crate) socket: UdpSocket,
    pub(crate) max_udp_packet_size: usize,
    pub(crate) channel: Receiver<AcarsVdlm2Message>,
}

impl UDPListenerServer {
    pub(crate) fn new(proto_name: &str, reassembly_window: &f64) -> Self {
        Self {
            proto_name: proto_name.to_string(),
            reassembly_window: *reassembly_window,
        }
    }

    pub(crate) async fn run(
        self,
        listen_udp_port: &str,
        channel: Sender<String>,
    ) -> Result<(), io::Error> {
        let mut buf: Vec<u8> = vec![0; 5000];
        let mut to_send: Option<(usize, SocketAddr)> = None;

        let s = UdpSocket::bind(listen_udp_port).await;

        match s {
            Err(e) => error!(
                "[UDP SERVER: {}] Error listening on port: {}",
                self.proto_name, e
            ),
            Ok(socket) => {
                info!(
                    "[UDP SERVER: {}]: Listening on: {}",
                    self.proto_name,
                    socket.local_addr()?
                );

                let packet_handler: PacketHandler =
                    PacketHandler::new(&self.proto_name, self.reassembly_window);

                loop {
                    if let Some((size, peer)) = to_send {
                        let msg_string = match std::str::from_utf8(buf[..size].as_ref()) {
                            Ok(s) => s,
                            Err(_) => {
                                warn!(
                                    "[UDP SERVER: {}] Invalid message received from {}",
                                    self.proto_name, peer
                                );
                                continue;
                            }
                        };
                        let split_messages_by_newline: Vec<&str> =
                            msg_string.split_terminator('\n').collect();

                        for msg_by_newline in split_messages_by_newline {
                            let split_messages_by_brackets: Vec<&str> =
                                msg_by_newline.split_terminator("}{").collect();
                            if split_messages_by_brackets.len().eq(&1) {
                                packet_handler
                                    .attempt_message_reassembly(
                                        split_messages_by_brackets[0].to_string(),
                                        peer,
                                    )
                                    .await
                                    .process_reassembly(&self.proto_name, &channel, "UDP")
                                    .await;
                            } else {
                                // We have a message that was split by brackets if the length is greater than one
                                for (count, msg_by_brackets) in
                                    split_messages_by_brackets.iter().enumerate()
                                {
                                    let final_message = if count == 0 {
                                        // First case is the first element, which should only ever need a single closing bracket
                                        trace!("[UDP SERVER: {}] Multiple messages received in a packet.", self.proto_name);
                                        format!("{}{}", "}", msg_by_brackets)
                                    } else if count == split_messages_by_brackets.len() - 1 {
                                        // This case is for the last element, which should only ever need a single opening bracket
                                        trace!(
                                            "[UDP SERVER: {}] End of a multiple message packet",
                                            self.proto_name
                                        );
                                        format!("{}{}", "{", msg_by_brackets)
                                    } else {
                                        // This case is for any middle elements, which need both an opening and closing bracket
                                        trace!(
                                            "[UDP SERVER: {}] Middle of a multiple message packet",
                                            self.proto_name
                                        );
                                        format!("{}{}{}", "{", msg_by_brackets, "}")
                                    };
                                    packet_handler
                                        .attempt_message_reassembly(final_message, peer)
                                        .await
                                        .process_reassembly(&self.proto_name, &channel, "UDP")
                                        .await;
                                }
                            }
                        }
                    }
                    to_send = Some(socket.recv_from(&mut buf).await?);
                }
            }
        };
        Ok(())
    }
}

impl UDPSenderServer {
    pub(crate) fn new(
        send_udp: &[String],
        server_type: &str,
        socket: UdpSocket,
        max_udp_packet_size: &usize,
        rx_processed: Receiver<AcarsVdlm2Message>,
    ) -> Self {
        Self {
            host: send_udp.to_vec(),
            proto_name: server_type.to_string(),
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
                    self.proto_name, bytes_error
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
                            trace!("[UDP SENDER {}] Sending {buffer_position} to {buffer_end} of {message_size} to {addr}", self.proto_name);
                            let bytes_sent = self
                                .socket
                                .send_to(&message_as_bytes[buffer_position..buffer_end], addr)
                                .await;

                            match bytes_sent {
                                Ok(bytes_sent) => debug!(
                                    "[UDP SENDER {}] sent {} bytes to {}",
                                    self.proto_name, bytes_sent, addr
                                ),
                                Err(e) => warn!(
                                    "[UDP SENDER {}] failed to send message to {}: {:?}",
                                    self.proto_name, addr, e
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
