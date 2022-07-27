// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

// Used to go connection to consumers of data, and then send data to them as it comes in

use crate::generics::SenderServer;
use stubborn_io::tokio::StubbornIo;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

// TODO: The error and info messages kicked out by StubbornIO are kinda...useless.
// It doesn't include things like host, port, etc.
// I created an issue on the crate. I'm hoping to get a better error message. If it's not
// Fixed relatively soon I'll attempt a patch of the crate.
// https://github.com/craftytrickster/stubborn-io/issues/23

impl SenderServer<StubbornIo<TcpStream, String>> {
    pub async fn send_message(mut self) {
        tokio::spawn(async move {
            while let Some(message) = self.channel.recv().await {
                // send message to all client
                let message_as_string: Result<String, serde_json::Error> =
                    serde_json::to_string(&message["out_json"]);

                match message_as_string {
                    Err(parse_error) => error!("Unable to parse Value to String: {}", parse_error),
                    Ok(value) => {
                        let final_message: String = format!("{}\n", value);
                        match self.socket.write_all(final_message.as_bytes()).await {
                            Ok(_) => trace!("[TCP SENDER {}]: sent message", self.proto_name),
                            Err(e) => error!(
                                "[TCP SENDER {}]: Error sending message: {}",
                                self.proto_name, e
                            ),
                        };
                    }
                }
            }
        });
    }
}
