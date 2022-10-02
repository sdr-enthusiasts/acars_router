// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

// NOTE: This is a listener. WE **SUB** to a *PUB* socket.

use crate::SenderServer;
use acars_vdlm2_parser::AcarsVdlm2Message;
use futures::SinkExt;
use futures::StreamExt;
use tmq::publish::Publish;
use tmq::{subscribe, Context, Result};
use tokio::sync::mpsc::{Receiver, Sender};

pub struct ZMQListnerServer {
    pub host: String,
    pub proto_name: String,
}

impl ZMQListnerServer {
    pub async fn run(self, channel: Sender<String>) -> Result<()> {
        debug!("[ZMQ LISTENER SERVER {}] Starting", self.proto_name);
        let address = format!("tcp://{}", self.host);
        let mut socket = subscribe(&Context::new())
            .connect(&address)?
            .subscribe(b"")?;

        while let Some(msg) = socket.next().await {
            let message = match msg {
                Ok(message) => message,
                Err(e) => {
                    error!("[ZMQ LISTENER SERVER {}] Error: {:?}", self.proto_name, e);
                    continue;
                }
            };

            let composed_message = message
                .iter()
                .map(|item| item.as_str().unwrap_or("invalid text"))
                .collect::<Vec<&str>>()
                .join(" ");
            trace!(
                "[ZMQ LISTENER SERVER {}] Received: {}",
                self.proto_name,
                composed_message
            );
            let stripped = composed_message
                .strip_suffix("\r\n")
                .or_else(|| composed_message.strip_suffix('\n'))
                .unwrap_or(&composed_message);

            match channel.send(stripped.to_string()).await {
                Ok(_) => trace!(
                    "[ZMQ LISTENER SERVER {}] Message sent to channel",
                    self.proto_name
                ),
                Err(e) => error!(
                    "[ZMQ LISTENER SERVER {}] Error sending message to channel: {}",
                    self.proto_name, e
                ),
            }
        }
        Ok(())
    }
}

impl SenderServer<Publish> {
    pub(crate) fn new(
        server_address: &str,
        name: &str,
        socket: Publish,
        channel: Receiver<AcarsVdlm2Message>,
    ) -> Self {
        Self {
            host: server_address.to_string(),
            proto_name: name.to_string(),
            socket,
            channel,
        }
    }
    pub async fn send_message(mut self) {
        tokio::spawn(async move {
            while let Some(message) = self.channel.recv().await {
                match message.to_string_newline() {
                    Err(decode_error) => error!(
                        "[ZMQ SENDER]: Error parsing message to string: {}",
                        decode_error
                    ),
                    // For some Subscribers it appears that a "blank" topic causes it to never receive the message
                    // This needs more investigation...
                    // Right now it seems that any Sub who listens to the blank topic gets all of the messages, even
                    // if they aren't sub'd to the topic we're broadcasting on. This should fix the issue with moronic (hello node)
                    // zmq implementations not getting the message if the topic is blank.
                    // TODO: verify this doesn't break other kinds of zmq implementations....Like perhaps acars_router itself?
                    Ok(payload) => match self.socket.send(vec!["acars", &payload]).await {
                        Ok(_) => (),
                        Err(e) => error!(
                            "[ZMQ SENDER]: Error sending message on 'acars' topic: {:?}",
                            e
                        ),
                    },
                }
            }
        });
    }
}
