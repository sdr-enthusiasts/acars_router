// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//
use serde_json::Value;
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;

/// Shorthand for the transmit half of the message channel.
pub type Tx = mpsc::UnboundedSender<String>;

/// Shorthand for the receive half of the message channel.
pub type Rx = mpsc::UnboundedReceiver<String>;

pub struct SenderServer<T> {
    pub host: String,
    pub proto_name: String,
    pub socket: T,
    pub channel: Receiver<Value>,
}

pub struct Shared {
    pub peers: HashMap<SocketAddr, Tx>,
}
