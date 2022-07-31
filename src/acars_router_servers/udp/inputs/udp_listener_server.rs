// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

// Server used to receive UDP data

use crate::packet_handler::{PacketHandler, ProcessAssembly};
use std::io;
use std::net::SocketAddr;
use std::str;
use tokio::net::UdpSocket;
use tokio::sync::mpsc::Sender;

pub struct UDPListenerServer {
    pub buf: Vec<u8>,
    pub to_send: Option<(usize, SocketAddr)>,
    pub proto_name: String,
    pub reassembly_window: f64,
}

impl UDPListenerServer {
    pub async fn run(self, listen_udp_port: &str, channel: Sender<String>) -> Result<(), io::Error> {

        let mut buf = self.buf;
        let mut to_send= self.to_send;

        let s = UdpSocket::bind(listen_udp_port).await;

        match s {
            Err(e) => error!("[UDP SERVER: {}] Error listening on port: {}", self.proto_name, e),
            Ok(socket) => {
                info!("[UDP SERVER: {}]: Listening on: {}", self.proto_name, socket.local_addr()?);

                let packet_handler: PacketHandler = PacketHandler::new(&self.proto_name, self.reassembly_window);

                loop {
                    if let Some((size, peer)) = to_send {
                        let msg_string = match str::from_utf8(buf[..size].as_ref()) {
                            Ok(s) => s,
                            Err(_) => {
                                warn!("[UDP SERVER: {}] Invalid message received from {}", self.proto_name, peer);
                                continue;
                            }
                        };
                        let split_messages_by_newline: Vec<&str> = msg_string.split_terminator('\n').collect();

                        for msg_by_newline in split_messages_by_newline {
                            let split_messages_by_brackets: Vec<&str> = msg_by_newline.split_terminator("}{").collect();
                            if split_messages_by_brackets.len().eq(&1) {
                                packet_handler.attempt_message_reassembly(split_messages_by_brackets[0].to_string(), peer).await
                                    .process_reassembly(&self.proto_name, &channel, "UDP").await;
                            } else {
                                // We have a message that was split by brackets if the length is greater than one
                                for (count, msg_by_brackets) in split_messages_by_brackets.iter().enumerate() {
                                    let final_message = if count == 0 {
                                        // First case is the first element, which should only ever need a single closing bracket
                                        trace!("[UDP SERVER: {}] Multiple messages received in a packet.", self.proto_name);
                                        format!("{}{}", "}", msg_by_brackets)
                                    } else if count == split_messages_by_brackets.len() - 1 {
                                        // This case is for the last element, which should only ever need a single opening bracket
                                        trace!("[UDP SERVER: {}] End of a multiple message packet", self.proto_name);
                                        format!("{}{}", "{", msg_by_brackets)
                                    } else {
                                        // This case is for any middle elements, which need both an opening and closing bracket
                                        trace!("[UDP SERVER: {}] Middle of a multiple message packet", self.proto_name);
                                        format!("{}{}{}", "{", msg_by_brackets, "}")
                                    };
                                    packet_handler.attempt_message_reassembly(final_message, peer).await
                                        .process_reassembly(&self.proto_name, &channel, "UDP").await;
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
