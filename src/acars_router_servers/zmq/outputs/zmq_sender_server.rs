// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

// NOTE: This is a sender. WE **PUB** to a *SUB* socket.

use crate::generics::SenderServer;
use futures::SinkExt;
use log::error;
use tmq::publish::Publish;

impl SenderServer<Publish> {
    pub async fn send_message(mut self) {
        tokio::spawn(async move {
            while let Some(message) = self.channel.recv().await {
                // send message to all client
                // TODO: message is type "serde_json::Value"....does format! do the .to_string() for us?
                let message_out = format!("{}\n", message["out_json"].to_string());

                match self.socket.send(vec!["", &message_out]).await {
                    Ok(_) => (),
                    Err(e) => error!("[TCP SENDER]: Error sending message: {}", e),
                };
            }
        });
        // let mut message = message.to_string();
        // message.push_str("\n");

        // self.socket.send(vec!["", &message]);
    }
}
