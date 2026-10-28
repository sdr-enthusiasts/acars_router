// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

//! ZMQ transport.
//!
//! NOTE: we **SUB** to a *PUB* socket. dumpvdl2 / `acarsdec` (in zmq
//! mode) publish; we subscribe. For outbound delivery we publish so
//! external subscribers can consume.
//!
//! * [`ZMQReceiverServer`] — `SUB` socket, `connect()` to a remote
//!   publisher.
//! * [`ZMQListenerServer`] — `SUB` socket, `bind()` locally so external
//!   publishers can connect inward.
//! * `impl SenderServer<Publish>` — `PUB` socket fan-out for processed
//!   messages.

use crate::{OutboundMessage, SenderServer};
use futures::SinkExt;
use futures::StreamExt;
use tmq::publish::Publish;
use tmq::{Context, Multipart, Result, subscribe};
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace, warn};

/// Extract the payload from a (possibly multipart) ZMQ message.
///
/// Returns the LAST frame as UTF-8 text with one trailing `"\r\n"` or
/// `"\n"` stripped, or `None` if there are no frames or the last frame
/// is not valid UTF-8.
///
/// Single-frame publishers (dumpvdl2, acarsdec, `acars_router`'s own
/// `serve_zmq`) are unaffected since the last frame is also the first.
/// xng publishes every message as `[topic, json]`, so taking the last
/// frame drops the topic and keeps the JSON payload.
fn payload_frame(message: &Multipart) -> Option<&str> {
    let text = message.0.back()?.as_str()?;
    Some(
        text.strip_suffix("\r\n")
            .or_else(|| text.strip_suffix('\n'))
            .unwrap_or(text),
    )
}

/// Forward the payload frame of `message` onto `channel`, dropping
/// messages with no usable (non-empty, UTF-8) payload. Empty payloads
/// are never forwarded.
async fn forward_payload(message: &Multipart, channel: &Sender<String>) {
    let Some(text) = payload_frame(message).filter(|text| !text.is_empty()) else {
        warn!("Dropping ZMQ message with no usable payload frame");
        return;
    };
    trace!("Received: {}", text);
    match channel.send(text.to_string()).await {
        Ok(()) => trace!("Message sent to channel"),
        Err(e) => error!("Error sending message to channel: {}", e),
    }
}

/// Outbound-connecting ZMQ subscriber: `connect()`s to a remote `PUB`
/// socket and pushes each message into the per-protocol mpsc input
/// channel.
pub struct ZMQReceiverServer {
    pub host: String,
    pub proto_name: String,
}

impl ZMQReceiverServer {
    #[tracing::instrument(
        name = "zmq_receiver",
        skip_all,
        fields(proto = %self.proto_name, host = %self.host),
    )]
    pub async fn run(self, channel: Sender<String>, shutdown: CancellationToken) -> Result<()> {
        debug!("Starting");
        let address = format!("tcp://{}", self.host);
        let mut socket = subscribe(&Context::new())
            .connect(&address)?
            .subscribe(b"")?;

        loop {
            let msg = tokio::select! {
                () = shutdown.cancelled() => {
                    info!("shutdown requested");
                    return Ok(());
                }
                next = socket.next() => match next {
                    Some(m) => m,
                    None => return Ok(()),
                },
            };
            let message = match msg {
                Ok(message) => message,
                Err(e) => {
                    error!("Error: {:?}", e);
                    continue;
                }
            };

            forward_payload(&message, &channel).await;
        }
    }
}

/// Inbound-binding ZMQ subscriber: `bind()`s a local port so external
/// publishers can connect inward.
pub struct ZMQListenerServer {
    pub(crate) proto_name: String,
}

impl ZMQListenerServer {
    #[must_use]
    pub fn new(proto_name: &str) -> Self {
        Self {
            proto_name: proto_name.to_string(),
        }
    }
    #[tracing::instrument(
        name = "zmq_listener",
        skip_all,
        fields(proto = %self.proto_name, port = %listen_acars_zmq_port),
    )]
    pub async fn run(
        self,
        listen_acars_zmq_port: String,
        channel: Sender<String>,
        shutdown: CancellationToken,
    ) -> Result<()> {
        debug!("Starting");
        let address = format!("tcp://0.0.0.0:{listen_acars_zmq_port}");
        debug!("Listening on {}", address);
        let mut socket = subscribe(&Context::new()).bind(&address)?.subscribe(b"")?;

        loop {
            let msg = tokio::select! {
                () = shutdown.cancelled() => {
                    info!("shutdown requested");
                    return Ok(());
                }
                next = socket.next() => match next {
                    Some(m) => m,
                    None => return Ok(()),
                },
            };
            match msg {
                Ok(message) => forward_payload(&message, &channel).await,
                Err(e) => error!("Error: {:?}", e),
            }
        }
    }
}

/// Outbound ZMQ publisher. Drains the per-protocol broadcast and
/// publishes each newline-terminated payload on the `PUB` socket. Slow
/// subscribers are dropped by ZMQ itself (high-water-mark).
impl SenderServer<Publish> {
    pub(crate) fn new(
        server_address: &str,
        name: &str,
        socket: Publish,
        channel: tokio::sync::broadcast::Receiver<OutboundMessage>,
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
                        tracing::warn!("broadcast lagged; {n} message(s) dropped");
                        continue;
                    }
                };
                // For some Subscribers it appears that a "blank" topic causes it to never receive the message
                // This needs more investigation...
                // Right now it seems that any Sub who listens to the blank topic gets all of the messages, even
                // if they aren't sub'd to the topic we're broadcasting on. This should fix the issue with moronic (hello node)
                // zmq implementations not getting the message if the topic is blank.
                // TODO: verify this doesn't break other kinds of zmq implementations....Like perhaps acars_router itself?
                if let Err(e) = self.socket.send(vec![&*message]).await {
                    error!(": Error sending message on 'acars' topic: {e:?}");
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::payload_frame;
    use tmq::Multipart;

    fn multipart(frames: Vec<Vec<u8>>) -> Multipart {
        frames.into()
    }

    #[test]
    fn single_frame_strips_trailing_newline() {
        let msg = multipart(vec![b"{\"a\":1}\n".to_vec()]);
        assert_eq!(payload_frame(&msg), Some("{\"a\":1}"));
    }

    #[test]
    fn single_frame_strips_trailing_crlf() {
        let msg = multipart(vec![b"{\"a\":1}\r\n".to_vec()]);
        assert_eq!(payload_frame(&msg), Some("{\"a\":1}"));
    }

    #[test]
    fn two_frames_topic_and_json_returns_json() {
        let msg = multipart(vec![b"acars".to_vec(), b"{\"a\":1}".to_vec()]);
        assert_eq!(payload_frame(&msg), Some("{\"a\":1}"));
    }

    #[test]
    fn three_frames_returns_last() {
        let msg = multipart(vec![
            b"first".to_vec(),
            b"second".to_vec(),
            b"{\"last\":true}\n".to_vec(),
        ]);
        assert_eq!(payload_frame(&msg), Some("{\"last\":true}"));
    }

    #[test]
    fn empty_multipart_returns_none() {
        let msg = multipart(Vec::new());
        assert_eq!(payload_frame(&msg), None);
    }

    #[test]
    fn invalid_utf8_last_frame_returns_none() {
        let msg = multipart(vec![b"acars".to_vec(), vec![0xff, 0xfe]]);
        assert_eq!(payload_frame(&msg), None);
    }
}
