// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

// Used to go connection to consumers of data, and then send data to them as it comes in

use crate::generics::SenderServer;
use log::{error, trace};
use stubborn_io::tokio::StubbornIo;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

// TODO: The error and info messages kicked out by StubbornIO are kinda...useless.
// It doesn't include things like host, port, etc.
// Can we fix that?

impl SenderServer<StubbornIo<TcpStream, String>> {
    pub async fn send_message(mut self) {
        tokio::spawn(async move {
            while let Some(message) = self.channel.recv().await {
                // send message to all client
                let message_out = message["out_json"].clone();
                let message_as_string = format!("{}\n", message_out);
                let message_as_bytes = message_as_string.as_bytes();

                match self.socket.write_all(message_as_bytes).await {
                    Ok(_) => trace!("[TCP SENDER {}]: sent message", self.proto_name),
                    Err(e) => error!(
                        "[TCP SENDER {}]: Error sending message: {}",
                        self.proto_name, e
                    ),
                };
            }
        });
    }
}
