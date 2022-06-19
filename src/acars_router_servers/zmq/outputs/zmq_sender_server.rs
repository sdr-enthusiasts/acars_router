// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

// NOTE: This is a sender. WE **PUB** to a *SUB* socket.

use futures::SinkExt;
use serde_json::Value;
use std::sync::{Arc, Mutex};
use tmq::publish::Publish;

pub struct ZMQSenderServer {
    pub host: String,
    pub proto_name: String,
    pub socket: Publish,
}

impl ZMQSenderServer {
    pub fn send_message(mut self, message: Value) {
        let mut message = message.to_string();
        message.push_str("\n");

        self.socket.send(vec!["", &message]);
    }
}
