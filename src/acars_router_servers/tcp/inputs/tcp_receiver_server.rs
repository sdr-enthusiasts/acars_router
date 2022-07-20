// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

// Server used to connect out to a client over TCP to receive data

use crate::generics::reconnect_options;
use crate::helper_functions::strip_line_endings;
use log::{debug, error, trace};
use stubborn_io::StubbornTcpStream;
use tokio::sync::mpsc::Sender;
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec};

pub struct TCPReceiverServer {
    pub host: String,
    pub proto_name: String,
}

impl TCPReceiverServer {
    pub async fn run(
        self,
        channel: Sender<serde_json::Value>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let TCPReceiverServer { host, proto_name } = self;
        trace!("[TCP Receiver Server {}] Starting", proto_name);

        let stream = match StubbornTcpStream::connect_with_options(
            host.clone(),
            reconnect_options(),
        )
        .await
        {
            Ok(stream) => stream,
            Err(e) => {
                error!(
                    "[TCP Receiver Server {}] Error connecting to {}: {}",
                    proto_name, host, e
                );
                Err(e)?
            }
        };

        // create a buffered reader and send the messages to the channel

        let reader = tokio::io::BufReader::new(stream);
        let mut lines = Framed::new(reader, LinesCodec::new());

        while let Some(Ok(line)) = lines.next().await {
            // Clean up the line endings. This is probably unnecessary but it's here for safety.
            let stripped = strip_line_endings(&line).to_owned();

            match serde_json::from_str::<serde_json::Value>(stripped.as_str()) {
                Ok(msg) => {
                    trace!("[TCP SERVER: {}] Received message: {}", proto_name, msg);
                    match channel.send(msg).await {
                        Ok(_) => debug!("[TCP SERVER {proto_name}] Message sent to channel"),
                        Err(e) => error!(
                            "[TCP SERVER {}] Error sending message to channel: {}",
                            proto_name, e
                        ),
                    };
                }
                Err(e) => error!("[TCP SERVER {}] {}", proto_name, e),
            }
        }

        Ok(())
    }
}
