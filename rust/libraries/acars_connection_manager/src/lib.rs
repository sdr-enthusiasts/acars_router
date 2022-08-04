#[macro_use] extern crate log;
extern crate stubborn_io;
pub extern crate tokio as tokio;
extern crate tokio_stream;
extern crate tokio_util;
extern crate futures;
extern crate async_trait;
extern crate tmq;
extern crate zmq;
extern crate acars_vdlm2_parser;
extern crate acars_config;

pub mod packet_handler;
pub mod service_init;
pub mod tcp_services;
pub mod udp_services;
pub mod zmq_services;
pub mod message_handler;


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

#[derive(Debug)]
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
pub(crate) struct SocketListenerServer {
    pub(crate) proto_name: String,
    pub(crate) port: u16,
    pub(crate) reassembly_window: f64,
    pub(crate) socket_type: SocketType
}

#[derive(Debug, Clone)]
pub(crate) enum SocketType {
    Tcp,
    Udp
}

#[derive(Debug, Clone)]
pub(crate) enum ServerType {
    Acars,
    Vdlm2
}

#[derive(Debug, Clone, Default)]
pub struct SenderServerConfig {
    pub send_udp: Option<Vec<String>>,
    pub send_tcp: Option<Vec<String>>,
    pub serve_tcp: Option<Vec<u16>>,
    pub serve_zmq: Option<Vec<u16>>,
    pub max_udp_packet_size: usize,
}

#[derive(Debug, Clone, Default)]
pub struct OutputServerConfig {
    pub listen_udp: Option<Vec<u16>>,
    pub listen_tcp: Option<Vec<u16>>,
    pub receive_tcp: Option<Vec<String>>,
    pub receive_zmq: Option<Vec<String>>,
    pub reassembly_window: f64,
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
            ServerType::Vdlm2 => write!(f, "VDLM")
        }
    }
}