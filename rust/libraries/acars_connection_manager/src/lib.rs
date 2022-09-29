#[macro_use]
extern crate log;
extern crate acars_config;
extern crate acars_vdlm2_parser;
extern crate async_trait;
extern crate futures;
extern crate stubborn_io;
extern crate tmq;
pub extern crate tokio as tokio;
extern crate tokio_stream;
extern crate tokio_util;
extern crate zmq;

pub mod message_handler;
pub mod packet_handler;
pub mod service_init;
pub mod tcp_services;
pub mod udp_services;
pub mod zmq_services;

use acars_vdlm2_parser::AcarsVdlm2Message;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Formatter;
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

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct SenderServer<T> {
    pub(crate) host: String,
    pub(crate) proto_name: String,
    pub(crate) socket: T,
    pub(crate) channel: Receiver<AcarsVdlm2Message>,
}

#[derive(Debug, Default)]
pub(crate) struct Shared {
    pub(crate) peers: HashMap<SocketAddr, Tx>,
}

#[derive(Debug, Clone)]
pub(crate) struct SocketListenerServer {
    pub(crate) proto_name: String,
    pub(crate) port: u16,
    pub(crate) reassembly_window: f64,
    pub(crate) socket_type: SocketType,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) enum SocketType {
    Tcp,
    Udp,
}

#[derive(Debug, Clone)]
pub(crate) enum ServerType {
    Acars,
    Vdlm2,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SenderServerConfig {
    pub(crate) send_udp: Option<Vec<String>>,
    pub(crate) send_tcp: Option<Vec<String>>,
    pub(crate) serve_tcp: Option<Vec<u16>>,
    pub(crate) serve_zmq: Option<Vec<u16>>,
    pub(crate) max_udp_packet_size: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct OutputServerConfig {
    pub(crate) listen_udp: Option<Vec<u16>>,
    pub(crate) listen_tcp: Option<Vec<u16>>,
    pub(crate) receive_tcp: Option<Vec<String>>,
    pub(crate) receive_zmq: Option<Vec<String>>,
    pub(crate) reassembly_window: f64,
    pub(crate) output_server_type: ServerType,
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

impl fmt::Display for ServerType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ServerType::Acars => write!(f, "ACARS"),
            ServerType::Vdlm2 => write!(f, "VDLM"),
        }
    }
}

impl fmt::Display for SocketType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            SocketType::Tcp => write!(f, "TCP"),
            SocketType::Udp => write!(f, "UDP"),
        }
    }
}
