// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

use log::debug;
use tokio::sync::mpsc::Receiver;

pub async fn watch_message_queue(mut queue: Receiver<serde_json::Value>) {
    while let Some(message) = queue.recv().await {
        debug!("GOT: {}", message);
    }
}
