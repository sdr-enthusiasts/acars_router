// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//
use derive_getters::Getters;
use serde_json::Value;
use std::collections::HashMap;
use std::net::SocketAddr;
use stubborn_io::ReconnectOptions;
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

#[derive(Getters, Clone)]
pub struct SenderServerConfig {
    pub send_udp: Vec<String>,
    pub send_tcp: Vec<String>,
    pub serve_tcp: Vec<String>,
    pub serve_zmq: Vec<String>,
}

pub struct OutputServerConfig {
    pub send_udp: Vec<String>,
    pub send_tcp: Vec<String>,
    pub serve_tcp: Vec<String>,
    pub serve_zmq: Vec<String>,
}

// create ReconnectOptions. We want the TCP stuff that goes out and connects to clients
// to attempt to reconnect
// TODO: Should we modify the reconnect intervals? Right now it increases in time to something like 30 minutes between attempts
// See: https://docs.rs/stubborn-io/latest/src/stubborn_io/config.rs.html#93

pub fn reconnect_options() -> ReconnectOptions {
    return ReconnectOptions::new().with_exit_if_first_connect_fails(false);
}
