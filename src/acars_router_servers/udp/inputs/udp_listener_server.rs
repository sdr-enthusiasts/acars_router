// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

// Server used to receive UDP data

use crate::packet_handler::PacketHandler;
use log::{error, info, trace, warn};
use std::io;
use std::net::SocketAddr;
use std::str;
use tokio::net::UdpSocket;
use tokio::sync::mpsc::Sender;

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

                let loop_assembly_window = reassembly_window; // time is in seconds
                let loop_name = proto_name.clone();

                let packet_handler = PacketHandler::new(&loop_name, loop_assembly_window);

                //tokio::spawn(async move { packet_handler.clean_queue().await });

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
                            // First attempt to deserialise just the new message
                            match packet_handler
                                .attempt_message_reassembly(msg.to_string(), peer)
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
