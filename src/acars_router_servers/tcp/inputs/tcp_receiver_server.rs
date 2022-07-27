// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

// Server used to connect out to a client over TCP to receive data

use crate::generics::reconnect_options;
use crate::packet_handler::PacketHandler;
use std::net::SocketAddr;
use stubborn_io::StubbornTcpStream;
use tokio::sync::mpsc::Sender;
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec};

pub struct TCPReceiverServer {
    pub host: String,
    pub proto_name: String,
    pub reassembly_window: u64,
}

impl TCPReceiverServer {
    pub async fn run(
        self,
        channel: Sender<serde_json::Value>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // let TCPReceiverServer {
        //     host,
        //     proto_name,
        //     reassembly_window,
        // } = self;
        trace!("[TCP Receiver Server {}] Starting", self.proto_name);
        // create a SocketAddr from host
        let addr = match self.host.parse::<SocketAddr>() {
            Ok(addr) => addr,
            Err(e) => {
                error!(
                    "[TCP Receiver Server {}] Error parsing host: {}",
                    self.proto_name, e
                );
                return Ok(());
            }
        };

        let stream = match StubbornTcpStream::connect_with_options(addr, reconnect_options()).await
        {
            Ok(stream) => stream,
            Err(e) => {
                error!(
                    "[TCP Receiver Server {}] Error connecting to {}: {}",
                    self.proto_name, self.host, e
                );
                Err(e)?
            }
        };

        // create a buffered reader and send the messages to the channel

        let reader = tokio::io::BufReader::new(stream);
        let mut lines = Framed::new(reader, LinesCodec::new());
        let packet_handler = PacketHandler::new(&self.proto_name, self.reassembly_window);

        while let Some(Ok(line)) = lines.next().await {
            // Clean up the line endings. This is probably unnecessary but it's here for safety.
            let split_messages_by_newline: Vec<&str> = line.split_terminator('\n').collect();

            for msg_by_newline in split_messages_by_newline {
                let split_messages_by_brackets: Vec<&str> =
                    msg_by_newline.split_terminator("}{").collect();

                for (count, msg_by_brackets) in split_messages_by_brackets.iter().enumerate() {
                    let final_message: String;
                    // FIXME: This feels very non-rust idomatic and is ugly

                    // Our message had no brackets, so we can just send it
                    if split_messages_by_brackets.len() == 1 {
                        final_message = msg_by_brackets.to_string();
                    }
                    // We have a message that was split by brackets if the length is greater than one
                    // First case is the first element, which should only ever need a single closing bracket
                    else if count == 0 {
                        trace!(
                            "[TCP Receiver Server {}]Multiple messages received in a packet.",
                            self.proto_name
                        );
                        final_message = format!("{}{}", "}", msg_by_brackets);
                    } else if count == split_messages_by_brackets.len() - 1 {
                        // This case is for the last element, which should only ever need a single opening bracket
                        trace!(
                            "[TCP Receiver Server {}] End of a multiple message packet",
                            self.proto_name
                        );
                        final_message = format!("{}{}", "{", msg_by_brackets);
                    } else {
                        // This case is for any middle elements, which need both an opening and closing bracket
                        trace!(
                            "[TCP Receiver Server {}] Middle of a multiple message packet",
                            self.proto_name
                        );
                        final_message = format!("{}{}{}", "{", msg_by_brackets, "}");
                    }
                    match packet_handler
                        .attempt_message_reassembly(final_message, addr)
                        .await
                    {
                        Some(msg) => {
                            trace!(
                                "[TCP Receiver Server {}] Received message: {}",
                                self.proto_name,
                                msg
                            );
                            match channel.send(msg).await {
                                Ok(_) => {
                                    trace!(
                                        "[TCP SERVER {}] Message sent to channel",
                                        self.proto_name
                                    )
                                }
                                Err(e) => error!(
                                    "[TCP Receiver Server {}]Error sending message to channel: {}",
                                    self.proto_name, e
                                ),
                            };
                        }
                        None => trace!("[TCP Receiver Server {}] Invalid Message", self.proto_name),
                    }
                }
            }
        }

        Ok(())
    }
}
