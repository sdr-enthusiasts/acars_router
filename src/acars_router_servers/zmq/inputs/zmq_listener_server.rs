// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

// NOTE: This is a listener. WE **SUB** to a *PUB* socket.

use futures::StreamExt;
use log::{debug, error, trace};
use tmq::{subscribe, Context, Result};
use tokio::sync::mpsc::Sender;

pub struct ZMQListnerServer {
    pub host: String,
    pub proto_name: String,
}

impl ZMQListnerServer {
    pub async fn run(self, channel: Sender<serde_json::Value>) -> Result<()> {
        debug!("[ZMQ LISTENER SERVER {}] Starting", self.proto_name);
        let address = "tcp://".to_string() + &self.host;
        let mut socket = subscribe(&Context::new())
            .connect(&address)?
            .subscribe(b"")?;

        while let Some(msg) = socket.next().await {
            let message = match msg {
                Ok(message) => message,
                Err(e) => {
                    error!("[ZMQ LISTENER SERVER {}] Error: {}", self.proto_name, e);
                    continue;
                }
            };

            let composed_message = message
                .iter()
                .map(|item| item.as_str().unwrap_or("invalid text"))
                .collect::<Vec<&str>>();

            let message_string = composed_message.join(" ");
            trace!(
                "[ZMQ LISTENER SERVER {}] Received: {}",
                self.proto_name,
                message_string
            );
            let stripped = &message_string
                .strip_suffix("\r\n")
                .or(message_string.strip_suffix("\n"))
                .unwrap_or(&message_string);

            match serde_json::from_str::<serde_json::Value>(stripped) {
                Ok(json) => match channel.send(json).await {
                    Ok(_) => trace!(
                        "[ZMQ LISTENER SERVER {}] Message sent to channel",
                        self.proto_name
                    ),
                    Err(e) => error!(
                        "[ZMQ LISTENER SERVER {}] Error sending message to channel: {}",
                        self.proto_name, e
                    ),
                },
                Err(e) => {
                    error!("{}", e);
                }
            }
        }
        Ok(())
    }
}
