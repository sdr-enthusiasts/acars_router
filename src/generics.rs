// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;
use stubborn_io::ReconnectOptions;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;

/// Shorthand for the transmit half of the message channel.
pub type Tx = mpsc::UnboundedSender<String>;

/// Shorthand for the receive half of the message channel.
pub type Rx = mpsc::UnboundedReceiver<String>;

pub type DurationIterator = Box<dyn Iterator<Item = Duration> + Send + Sync>;

pub struct SenderServer<T> {
    pub host: String,
    pub proto_name: String,
    pub socket: T,
    pub channel: Receiver<String>,
}

pub struct Shared {
    pub peers: HashMap<SocketAddr, Tx>,
}

#[derive(Debug, Clone)]
pub struct SenderServerConfig {
    pub send_udp: Option<Vec<String>>,
    pub send_tcp: Option<Vec<String>>,
    pub serve_tcp: Option<Vec<String>>,
    pub serve_zmq: Option<Vec<String>>,
    pub max_udp_packet_size: usize,
}

impl SenderServerConfig {
    pub fn new(
        send_udp: &Option<Vec<String>>,
        send_tcp: &Option<Vec<String>>,
        serve_tcp: &Option<Vec<String>>,
        serve_zmq: &Option<Vec<String>>,
        max_udp_packet_size: &u64,
    ) -> Self {
        Self {
            send_udp: send_udp.clone(),
            send_tcp: send_tcp.clone(),
            serve_tcp: serve_tcp.clone(),
            serve_zmq: serve_zmq.clone(),
            max_udp_packet_size: *max_udp_packet_size as usize,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct OutputServerConfig {
    pub listen_udp: Option<Vec<String>>,
    pub listen_tcp: Option<Vec<String>>,
    pub receive_tcp: Option<Vec<String>>,
    pub receive_zmq: Option<Vec<String>>,
    pub reassembly_window: u64,
}

impl OutputServerConfig {
    pub fn new(
        listen_udp: &Option<Vec<String>>,
        listen_tcp: &Option<Vec<String>>,
        receive_tcp: &Option<Vec<String>>,
        receive_zmq: &Option<Vec<String>>,
        reassembly_window: &u64,
    ) -> Self {
        Self {
            listen_udp: listen_udp.clone(),
            listen_tcp: listen_tcp.clone(),
            receive_tcp: receive_tcp.clone(),
            receive_zmq: receive_zmq.clone(),
            reassembly_window: *reassembly_window,
        }
    }
}

// create ReconnectOptions. We want the TCP stuff that goes out and connects to clients
// to attempt to reconnect
// See: https://docs.rs/stubborn-io/latest/src/stubborn_io/config.rs.html#93

pub fn reconnect_options() -> ReconnectOptions {
    ReconnectOptions::new()
        .with_exit_if_first_connect_fails(false)
        .with_retries_generator(get_our_standard_reconnect_strategy)
}

fn get_our_standard_reconnect_strategy() -> DurationIterator {
    let initial_attempts = vec![
        Duration::from_secs(5),
        Duration::from_secs(5),
        Duration::from_secs(5),
        Duration::from_secs(5),
        Duration::from_secs(5),
        Duration::from_secs(5),
        Duration::from_secs(5),
        Duration::from_secs(5),
        Duration::from_secs(5),
        Duration::from_secs(5),
        Duration::from_secs(5),
        Duration::from_secs(5),
        Duration::from_secs(5),
        Duration::from_secs(5),
        Duration::from_secs(10),
        Duration::from_secs(20),
        Duration::from_secs(30),
        Duration::from_secs(40),
        Duration::from_secs(50),
        Duration::from_secs(60),
    ];

    let repeat = std::iter::repeat(Duration::from_secs(60));

    let forever_iterator = initial_attempts.into_iter().chain(repeat);

    Box::new(forever_iterator)
}
