// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

// A receiver of data that passively listens for a TCP connection
// aka does not connect and then sends the data in to be processed internally

use crate::helper_functions::strip_line_endings;
use log::{debug, error, info, trace};
use std::error::Error;
use std::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::Sender;
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec};

pub struct TCPListenerServer {
    pub proto_name: String,
}

impl TCPListenerServer {
    pub async fn run(
        self,
        listen_acars_udp_port: String,
        channel: Sender<serde_json::Value>,
    ) -> Result<(), io::Error> {
        let TCPListenerServer { proto_name } = self;

        let listener = TcpListener::bind("0.0.0.0:".to_string() + &listen_acars_udp_port).await?;
        info!(
            "[TCP SERVER: {}]: Listening on: {}",
            proto_name,
            listener.local_addr()?
        );

        loop {
            trace!("[TCP SERVER: {}]: Waiting for connection", proto_name);
            // Asynchronously wait for an inbound TcpStream.
            let (stream, addr) = listener.accept().await?;
            let new_channel = channel.clone();
            let new_proto_name = proto_name.clone() + ":" + &addr.to_string();
            info!(
                "[TCP SERVER: {}]:accepted connection from {}",
                proto_name, addr
            );
            // Spawn our handler to be run asynchronously.
            tokio::spawn(async move {
                match process_tcp_sockets(stream, &new_proto_name, new_channel).await {
                    Ok(_) => debug!("[TCP SERVER {}] connection closed", new_proto_name),
                    Err(e) => error!(
                        "[TCP SERVER {}] connection error: {}",
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
    proto_name: &String,
    channel: Sender<serde_json::Value>,
) -> Result<(), Box<dyn Error>> {
    let mut lines = Framed::new(stream, LinesCodec::new_with_max_length(8000));

    while let Some(Ok(line)) = lines.next().await {
        // Clean up the line endings. This is probably unnecessary but it's here for safety.
        let stripped = strip_line_endings(&line).to_owned();

        match serde_json::from_str::<serde_json::Value>(stripped.as_str()) {
            Ok(msg) => {
                trace!("[TCP SERVER: {}] Received message: {}", proto_name, msg);
                match channel.send(msg).await {
                    Ok(_) => debug!("[TCP SERVER {proto_name}] Message sent to channel"),
                    Err(e) => error!(
                        "[TCP SERVER {}] sending message to channel: {}",
                        proto_name, e
                    ),
                };
            }
            Err(e) => error!("[TCP SERVER {}] Invalid Message: {}", proto_name, e),
        }
    }

    Ok(())
}
