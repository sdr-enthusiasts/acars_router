// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

use log::debug;
use tokio::sync::mpsc::Receiver;

pub async fn watch_message_queue(mut queue: Receiver<serde_json::Value>) {
    while let Some(mut message) = queue.recv().await {
        debug!("GOT: {}", message);

        match message["vdl2"].get("app") {
            // dumpvdl2 message
            Some(_) => {
                debug!("dumpvdl2 message");
                message["vdl2"]["app"]["proxied"] = serde_json::Value::Bool(true);
                message["vdl2"]["app"]["proxied_by"] =
                    serde_json::Value::String("acars_router".to_string());
            }
            // acarsdec or vdlm2dec message
            None => match message.get("app") {
                Some(_) => {
                    message["app"]["proxied"] = serde_json::Value::Bool(true);
                    message["app"]["proxied_by"] =
                        serde_json::Value::String("acars_router".to_string());
                }
                None => {
                    // insert app in to message
                    message["app"] = serde_json::Value::Object(serde_json::map::Map::new());
                    message["app"]["proxied"] = serde_json::Value::Bool(true);
                    message["app"]["proxied_by"] =
                        serde_json::Value::String("acars_router".to_string());
                }
            },
        }

        debug!("SENDING: {}", message);
    }
}
