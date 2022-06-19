// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

use log::{trace, warn};
use tokio::net::UdpSocket;

#[derive(Debug)]
pub struct UDPSenderServer {
    pub host: Vec<String>,
    pub proto_name: String,
    pub socket: UdpSocket,
}

impl UDPSenderServer {
    pub async fn send_message(&self, message: serde_json::Value) {
        trace!("{}: {}", self.proto_name, message.to_string());

        // send the message to the socket

        let message_out = message["out_json"].clone();
        let message_as_string = message_out.to_string() + "\n";
        let message_as_bytes = message_as_string.as_bytes();
        for addr in &self.host {
            // TODO: Verify exceptionally large messages are sent correctly
            let bytes_sent = self.socket.send_to(message_as_bytes, addr).await;
            match bytes_sent {
                Ok(bytes_sent) => {
                    trace!("{} sent {} bytes to {}", self.proto_name, bytes_sent, addr);
                }
                Err(e) => {
                    warn!(
                        "{} failed to send message to {}: {:?}",
                        self.proto_name, addr, e
                    );
                }
            }
        }
    }
}
