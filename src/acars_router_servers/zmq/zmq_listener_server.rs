// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

// NOTE: This is a listener. WE **SUB** to a *PUB* socket.

use futures::StreamExt;
use log::{error, info, trace, warn};
use tmq::{subscribe, Context, Result};
use tokio::sync::mpsc::Sender;

pub struct ZMQListnerServer {
    pub host: String,
    pub proto_name: String,
}

impl ZMQListnerServer {
    pub async fn run(self, channel: Sender<serde_json::Value>) -> Result<()> {
        trace!("{}", self.proto_name);
        trace!("{}", self.host);
        let address = "tcp://".to_string() + &self.host;
        let mut socket = subscribe(&Context::new())
            .connect(&address)?
            .subscribe(b"")?;

        while let Some(msg) = socket.next().await {
            let message = msg.unwrap();
            let composed_message = message
                .iter()
                .map(|item| item.as_str().unwrap_or("invalid text"))
                .collect::<Vec<&str>>();

            let message_string = composed_message.join(" ");
            info!("{}", message_string);
            let stripped = &message_string
                .strip_suffix("\r\n")
                .or(message_string.strip_suffix("\n"))
                .unwrap_or(&message_string);

            match serde_json::from_str::<serde_json::Value>(stripped) {
                Ok(json) => {
                    channel.send(json).await.unwrap();
                }
                Err(e) => {
                    error!("{}", e);
                }
            }
        }
        warn!("oh no!");
        Ok(())
    }
}
