// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

// NOTE: WE **SUB** to a *PUB* socket.

use crate::SenderServer;
use acars_vdlm2_parser::AcarsVdlm2Message;
use futures::SinkExt;
use futures::StreamExt;
use log::{debug, error, trace};
use tmq::publish::Publish;
use tmq::{Context, Result, subscribe};
use tokio::sync::mpsc::Sender;

/// ZMQ Receiver server. This is used to connect to a remote ZMQ server and process the messages.
/// Used for incoming ZMQ data for ACARS Router to process
pub struct ZMQReceiverServer {
    pub host: String,
    pub proto_name: String,
}

/// ZMQ Receiver server. This is used to connect to a remote ZMQ server and process the messages.
/// Used for incoming ZMQ data for ACARS Router to process
impl ZMQReceiverServer {
    pub async fn run(self, channel: Sender<String>) -> Result<()> {
        debug!("[ZMQ RECEIVER SERVER {}] Starting", self.proto_name);
        let address = format!("tcp://{}", self.host);
        let mut socket = subscribe(&Context::new())
            .connect(&address)?
            .subscribe(b"")?;

        while let Some(msg) = socket.next().await {
            let message = match msg {
                Ok(message) => message,
                Err(e) => {
                    error!("[ZMQ RECEIVER SERVER {}] Error: {:?}", self.proto_name, e);
                    continue;
                }
            };

            let composed_message = message
                .iter()
                .map(|item| item.as_str().unwrap_or("invalid text"))
                .collect::<Vec<&str>>()
                .join(" ");
            trace!(
                "[ZMQ RECEIVER SERVER {}] Received: {}",
                self.proto_name, composed_message
            );
            let stripped = composed_message
                .strip_suffix("\r\n")
                .or_else(|| composed_message.strip_suffix('\n'))
                .unwrap_or(&composed_message);

            match channel.send(stripped.to_string()).await {
                Ok(()) => trace!(
                    "[ZMQ RECEIVER SERVER {}] Message sent to channel",
                    self.proto_name
                ),
                Err(e) => error!(
                    "[ZMQ RECEIVER SERVER {}] Error sending message to channel: {}",
                    self.proto_name, e
                ),
            }
        }
        Ok(())
    }
}

/// ZMQ Listener server. This is used to listen for incoming ZMQ data for ACARS Router to process
/// Used for incoming ZMQ data for ACARS Router to process
pub struct ZMQListenerServer {
    pub(crate) proto_name: String,
}

/// ZMQ Listener server. This is used to listen for incoming ZMQ data for ACARS Router to process
/// Used for incoming ZMQ data for ACARS Router to process
impl ZMQListenerServer {
    #[must_use]
    pub fn new(proto_name: &str) -> Self {
        Self {
            proto_name: proto_name.to_string(),
        }
    }
    pub async fn run(self, listen_acars_zmq_port: String, channel: Sender<String>) -> Result<()> {
        debug!("[ZMQ LISTENER SERVER {}] Starting", self.proto_name);
        let address = format!("tcp://0.0.0.0:{listen_acars_zmq_port}");
        debug!(
            "[ZMQ LISTENER SERVER {}] Listening on {}",
            self.proto_name, address
        );
        let mut socket = subscribe(&Context::new()).bind(&address)?.subscribe(b"")?;

        while let Some(msg) = socket.next().await {
            match msg {
                Ok(message) => {
                    for item in message {
                        let item_text = item.as_str().unwrap_or("invalid text");
                        let composed_message = item_text
                            .strip_suffix("\r\n")
                            .or_else(|| item_text.strip_suffix('\n'))
                            .unwrap_or(item_text);
                        trace!(
                            "[ZMQ LISTENER SERVER {}] Received: {}",
                            self.proto_name, composed_message
                        );
                        match channel.send(composed_message.to_string()).await {
                            Ok(()) => trace!(
                                "[ZMQ LISTENER SERVER {}] Message sent to channel",
                                self.proto_name
                            ),
                            Err(e) => error!(
                                "[ZMQ LISTENER SERVER {}] Error sending message to channel: {}",
                                self.proto_name, e
                            ),
                        }
                    }
                }
                Err(e) => error!("[ZMQ LISTENER SERVER {}] Error: {:?}", self.proto_name, e),
            }
        }

        Ok(())
    }
}

/// ZMQ Sender server. This is used to send messages to a remote ZMQ server.
/// Used for outgoing ZMQ data for ACARS Router to process
impl SenderServer<Publish> {
    pub(crate) fn new(
        server_address: &str,
        name: &str,
        socket: Publish,
        channel: tokio::sync::broadcast::Receiver<AcarsVdlm2Message>,
    ) -> Self {
        Self {
            host: server_address.to_string(),
            proto_name: name.to_string(),
            socket,
            channel,
        }
    }
    pub(crate) fn send_message(mut self) {
        tokio::spawn(async move {
            loop {
                let message = match self.channel.recv().await {
                    Ok(m) => m,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        log::warn!(
                            "[ZMQ SENDER {}]: broadcast lagged; {n} message(s) dropped",
                            self.proto_name
                        );
                        continue;
                    }
                };
                match message.to_string_newline() {
                    Err(decode_error) => {
                        error!("[ZMQ SENDER]: Error parsing message to string: {decode_error}");
                    }
                    // For some Subscribers it appears that a "blank" topic causes it to never receive the message
                    // This needs more investigation...
                    // Right now it seems that any Sub who listens to the blank topic gets all of the messages, even
                    // if they aren't sub'd to the topic we're broadcasting on. This should fix the issue with moronic (hello node)
                    // zmq implementations not getting the message if the topic is blank.
                    // TODO: verify this doesn't break other kinds of zmq implementations....Like perhaps acars_router itself?
                    Ok(payload) => match self.socket.send(vec![&payload]).await {
                        Ok(()) => (),
                        Err(e) => {
                            error!("[ZMQ SENDER]: Error sending message on 'acars' topic: {e:?}");
                        }
                    },
                }
            }
        });
    }
}
