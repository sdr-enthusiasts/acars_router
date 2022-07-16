// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

use crate::generics::SenderServer;
use log::error;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

impl SenderServer<TcpStream> {
    pub async fn send_message(mut self) {
        tokio::spawn(async move {
            while let Some(message) = self.channel.recv().await {
                // send message to all client
                let message_out = message["out_json"].clone();
                let message_as_string = message_out.to_string() + "\n";
                let message_as_bytes = message_as_string.as_bytes();

                match self.socket.write_all(message_as_bytes).await {
                    Ok(_) => (),
                    Err(e) => error!("[TCP SENDER]: Error sending message: {}", e),
                };
            }
        });
    }
}
