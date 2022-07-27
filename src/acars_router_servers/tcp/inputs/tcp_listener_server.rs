// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

// A receiver of data that passively listens for a TCP connection
// aka does not connect and then sends the data in to be processed internally

use crate::packet_handler::PacketHandler;
use std::error::Error;
use std::io;
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::Sender;
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec};

pub struct TCPListenerServer {
    pub proto_name: String,
    pub reassembly_window: u64,
}

impl TCPListenerServer {
    pub async fn run(
        self,
        listen_acars_udp_port: String,
        channel: Sender<serde_json::Value>,
    ) -> Result<(), io::Error> {
        let listener: TcpListener =
            TcpListener::bind(format!("0.0.0.0:{}", listen_acars_udp_port)).await?;
        info!(
            "[TCP Listener SERVER: {}]: Listening on: {}",
            self.proto_name,
            listener.local_addr()?
        );

        loop {
            trace!(
                "[TCP Listener SERVER: {}]: Waiting for connection",
                self.proto_name
            );
            // Asynchronously wait for an inbound TcpStream.
            let (stream, addr) = listener.accept().await?;
            let new_channel = channel.clone();
            let new_proto_name = format!("{}:{}", self.proto_name, addr);
            info!(
                "[TCP Listener SERVER: {}]:accepted connection from {}",
                self.proto_name, addr
            );
            // Spawn our handler to be run asynchronously.
            tokio::spawn(async move {
                match process_tcp_sockets(
                    stream,
                    &new_proto_name,
                    new_channel,
                    addr,
                    self.reassembly_window,
                )
                .await
                {
                    Ok(_) => debug!(
                        "[TCP Listener SERVER: {}] connection closed",
                        new_proto_name
                    ),
                    Err(e) => error!(
                        "[TCP Listener SERVER: {}] connection error: {}",
                        new_proto_name.clone(),
                        e
                    ),
                };
            });
        }
    }
}

async fn process_tcp_sockets(
    stream: TcpStream,
    proto_name: &str,
    channel: Sender<serde_json::Value>,
    peer: SocketAddr,
    reassembly_window: u64,
) -> Result<(), Box<dyn Error>> {
    let mut lines = Framed::new(stream, LinesCodec::new_with_max_length(8000));

    let packet_handler = PacketHandler::new(proto_name, reassembly_window);

    while let Some(Ok(line)) = lines.next().await {
        let split_messages_by_newline: Vec<&str> = line.split_terminator('\n').collect();

        for msg_by_newline in split_messages_by_newline {
            let split_messages_by_brackets: Vec<&str> =
                msg_by_newline.split_terminator("}{").collect();

            for (count, msg_by_brackets) in split_messages_by_brackets.iter().enumerate() {
                let final_message: String;

                // Our message had no brackets, so we can just send it
                if split_messages_by_brackets.len() == 1 {
                    final_message = msg_by_brackets.to_string();
                }
                // We have a message that was split by brackets if the length is greater than one
                // First case is the first element, which should only ever need a single closing bracket
                else if count == 0 {
                    trace!(
                        "[TCP Listener SERVER: {}] Multiple messages received in a packet.",
                        proto_name
                    );
                    final_message = format!("}}{}", msg_by_brackets);
                } else if count == split_messages_by_brackets.len() - 1 {
                    // This case is for the last element, which should only ever need a single opening bracket
                    trace!(
                        "[TCP Listener SERVER: {}] End of a multiple message packet",
                        proto_name
                    );
                    final_message = format!("{{{}", msg_by_brackets);
                } else {
                    // This case is for any middle elements, which need both an opening and closing bracket
                    trace!(
                        "[TCP Listener SERVER: {}] Middle of a multiple message packet",
                        proto_name
                    );
                    final_message = format!("{{{}}}", msg_by_brackets);
                }
                match packet_handler
                    .attempt_message_reassembly(final_message, peer)
                    .await
                {
                    Some(msg) => {
                        trace!(
                            "[TCP Listener SERVER: {}] Received message: {}",
                            proto_name,
                            msg
                        );
                        match channel.send(msg).await {
                            Ok(_) => debug!(
                                "[TCP Listener SERVER: {proto_name}] Message sent to channel"
                            ),
                            Err(e) => error!(
                                "[TCP Listener SERVER: {}] sending message to channel: {}",
                                proto_name, e
                            ),
                        };
                    }
                    None => trace!("[TCP Listener SERVER: {}] Invalid Message", proto_name),
                }
            }
        }
    }

    Ok(())
}
