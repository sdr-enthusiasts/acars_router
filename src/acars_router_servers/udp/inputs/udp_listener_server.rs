// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

// Server used to receive UDP data

use crate::helper_functions::strip_line_endings;
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
        } = self;

        let s = UdpSocket::bind(&listen_udp_port).await;

        match s {
            Ok(socket) => {
                info!(
                    "[UDP SERVER: {}]: Listening on: {}",
                    proto_name,
                    socket.local_addr()?
                );

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

                        match serde_json::from_str::<serde_json::Value>(s.as_str()) {
                            Ok(msg) => {
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
                            Err(e) => {
                                warn!(
                                    "[UDP SERVER: {}] {}/{}: Invalid Message. {}. {}",
                                    proto_name, size, peer, e, s
                                )
                            }
                        };
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
