// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

use log::{info, trace, warn};
use std::io;
use std::net::SocketAddr;
use std::str;
use tokio::net::UdpSocket;
use tokio::sync::mpsc::Receiver;

#[derive(Debug)]
pub struct UDPSenderServer {
    pub host: Vec<String>,
    pub proto_name: String,
    pub socket: UdpSocket,
}

impl UDPSenderServer {
    //let sock = UdpSocket::bind(udp_port).await.unwrap();
    pub async fn send_message(&self, message: serde_json::Value) {
        trace!("HELLO {}", message.to_string());
        // send the message to the socket
        let message = message.to_string();
        let message = message.as_bytes();
        for addr in &self.host {
            let bytes_sent = self.socket.send_to(message, addr).await;
            match bytes_sent {
                Ok(bytes_sent) => {
                    info!("{} sent {} bytes to {}", self.proto_name, bytes_sent, addr);
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
