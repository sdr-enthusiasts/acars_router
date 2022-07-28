// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

// NOTE: This is a sender. WE **PUB** to a *SUB* socket.

use crate::generics::SenderServer;
use futures::SinkExt;
use tmq::publish::Publish;

impl SenderServer<Publish> {
    pub async fn send_message(mut self) {
        tokio::spawn(async move {
            while let Some(message) = self.channel.recv().await {
                match self.socket.send(vec!["", &message]).await {
                    Ok(_) => (),
                    Err(e) => error!("[TCP SENDER]: Error sending message: {:?}", e),
                };
            }
        });
    }
}
