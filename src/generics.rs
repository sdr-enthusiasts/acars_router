// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//
use serde_json::Value;
use tokio::sync::mpsc::Receiver;

pub struct SenderServer<T> {
    pub host: String,
    pub proto_name: String,
    pub socket: T,
    pub channel: Receiver<Value>,
}
