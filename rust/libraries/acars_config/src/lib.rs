pub extern crate clap as clap;
#[macro_use]
extern crate log;
extern crate acars_logging;

pub mod sanity_checker;

use acars_logging::SetupLogging;
use clap::Parser;

#[derive(Parser, Debug, Clone, Default)]
#[command(name = "ACARS Router", author, version, about, long_about = None)]
pub struct Input {
    // Output Options
    /// Set the log level. debug, trace, info are valid options.
    #[clap(short, long, action = clap::ArgAction::Count)]
    pub verbose: u8,
    /// Enable message deduplication
    #[clap(long, env = "AR_ENABLE_DEDUPE", value_parser)]
    pub enable_dedupe: bool,
    /// Set the number of seconds that a message will be considered as a duplicate
    /// if it is received again.
    #[clap(long, env = "AR_DEDUPE_WINDOW", value_parser, default_value = "2")]
    pub dedupe_window: u64,
    /// Reject messages with a timestamp greater than +/- this many seconds.
    #[clap(long, env = "AR_SKEW_WINDOW", value_parser, default_value = "1")]
    pub skew_window: u64,
    /// Set maximum UDP packet size, peer-to-peer.
    #[clap(
        long,
        env = "AR_MAX_UDP_PACKET_SIZE",
        value_parser,
        default_value = "60000"
    )]
    pub max_udp_packet_size: u64,
    // Message Modification
    /// Set to true to enable message modification
    /// This will add a "proxied" field to the message
    #[clap(long, env = "AR_ADD_PROXY_ID", value_parser)]
    pub add_proxy_id: bool,
    /// Override the station name in the message.
    #[clap(long, env = "AR_OVERRIDE_STATION_NAME", value_parser)]
    pub override_station_name: Option<String>,
    /// Print statistics every N minutes
    #[clap(long, env = "AR_STATS_EVERY", value_parser, default_value = "5")]
    pub stats_every: u64,
    /// Attempt message reassembly on incomplete messages within the specified number of seconds
    #[clap(
        long,
        env = "AR_REASSEMBLY_WINDOW",
        value_parser,
        default_value = "1.0"
    )]
    pub reassembly_window: f64,
    // Input Options

    // ACARS
    /// Semi-Colon separated list of arguments. ie 5550;5551;5552
    #[clap(long, value_parser, value_delimiter = ';')]
    pub listen_udp_acars: Option<Vec<u16>>,
    /// Semi-Colon separated list of arguments. ie 5550;5551;5552
    #[clap(long, value_parser, value_delimiter = ';')]
    pub listen_tcp_acars: Option<Vec<u16>>,
    /// Semi-Colon separated list of arguments. ie host:5550;host:5551;host:5552
    #[clap(long, value_parser, value_delimiter = ';')]
    pub receive_tcp_acars: Option<Vec<String>>,
    /// Semi-Colon separated list of arguments. io host:5550;host:5551;host:5552
    #[clap(long, value_parser, value_delimiter = ';')]
    pub receive_zmq_acars: Option<Vec<String>>,

    // VDLM2
    /// Semi-Colon separated list of arguments. ie 5555;5556;5557
    #[clap(long, value_parser, value_delimiter = ';')]
    pub listen_udp_vdlm2: Option<Vec<u16>>,
    /// Semi-Colon separated list of arguments. ie 5555;5556;5557
    #[clap(long, value_parser, value_delimiter = ';')]
    pub listen_tcp_vdlm2: Option<Vec<u16>>,
    /// Semi-Colon separated list of arguments. ie host:5550;host:5551;host:5552
    #[clap(long, value_parser, value_delimiter = ';')]
    pub receive_tcp_vdlm2: Option<Vec<String>>,
    /// Semi-Colon separated list of arguments. ie  host:5550;host:5551;host:5552
    #[clap(long, value_parser, value_delimiter = ';')]
    pub receive_zmq_vdlm2: Option<Vec<String>>,
    // JSON Output options
    // ACARS
    /// Semi-Colon separated list of arguments. ie host:5550;host:5551;host:5552
    #[clap(long, value_parser, value_delimiter = ';')]
    pub send_udp_acars: Option<Vec<String>>,
    /// Semi-Colon separated list of arguments. ie host:5550;host:5551;host:5552
    #[clap(long, value_parser, value_delimiter = ';')]
    pub send_tcp_acars: Option<Vec<String>>,
    /// Semi-Colon separated list of arguments. ie 5550;5551;5552
    #[clap(long, value_parser, value_delimiter = ';')]
    pub serve_tcp_acars: Option<Vec<u16>>,
    /// Semi-Colon separated list of arguments. ie 5550;5551;5552
    #[clap(long, value_parser, value_delimiter = ';')]
    pub serve_zmq_acars: Option<Vec<u16>>,
    // VDLM
    /// Semi-Colon separated list of arguments. ie host:5555;host:5556;host:5557
    #[clap(long, value_parser, value_delimiter = ';')]
    pub send_udp_vdlm2: Option<Vec<String>>,
    /// Semi-Colon separated list of arguments. ie host:5555;host:5556;host:5557
    #[clap(long, value_parser, value_delimiter = ';')]
    pub send_tcp_vdlm2: Option<Vec<String>>,
    /// Semi-Colon separated list of arguments. ie 5550;5551;5552
    #[clap(long, value_parser, value_delimiter = ';')]
    pub serve_tcp_vdlm2: Option<Vec<u16>>,
    /// Semi-Colon separated list of arguments. ie 5550;5551;5552
    #[clap(long, value_parser, value_delimiter = ';')]
    pub serve_zmq_vdlm2: Option<Vec<u16>>,
}

impl Input {
    pub fn print_values(&self) {
        debug!("The Following configuration values were loaded:");
        debug!("AR_LISTEN_UDP_ACARS: {:?}", self.listen_udp_acars);
        debug!("AR_LISTEN_TCP_ACARS: {:?}", self.listen_tcp_acars);
        debug!("AR_RECV_TCP_ACARS: {:?}", self.receive_tcp_acars);
        debug!("AR_RECV_ZMQ_ACARS: {:?}", self.receive_zmq_acars);
        debug!("AR_LISTEN_UDP_VDLM2: {:?}", self.listen_udp_vdlm2);
        debug!("AR_LISTEN_TCP_VDLM2: {:?}", self.listen_tcp_vdlm2);
        debug!("AR_RECV_TCP_VDLM2: {:?}", self.receive_tcp_vdlm2);
        debug!("AR_RECV_ZMQ_VDLM2: {:?}", self.receive_zmq_vdlm2);
        debug!("AR_SEND_UDP_ACARS: {:?}", self.send_udp_acars);
        debug!("AR_SEND_TCP_ACARS: {:?}", self.send_tcp_acars);
        debug!("AR_SERVE_TCP_ACARS: {:?}", self.serve_tcp_acars);
        debug!("AR_SERVE_ZMQ_ACARS: {:?}", self.serve_zmq_acars);
        debug!("AR_VERBOSE: {:?}", self.verbose.set_logging_level());
        debug!("AR_ADD_PROXY_ID: {:?}", self.add_proxy_id);
        debug!("AR_ENABLE_DEDUPE: {:?}", self.enable_dedupe);
        debug!("AR_DEDUPE_WINDOW: {:?}", self.dedupe_window);
        debug!("AR_SKEW_WINDOW: {:?}", self.skew_window);
        debug!("AR_STATS_EVERY: {:?}", self.stats_every);
        debug!("AR_OVERRIDE_STATION_NAME: {:?}", self.override_station_name);
        debug!("AR_SEND_UDP_VDLM2: {:?}", self.send_udp_vdlm2);
        debug!("AR_SEND_TCP_VDLM2: {:?}", self.send_tcp_vdlm2);
        debug!("AR_SERVE_TCP_VDLM2: {:?}", self.serve_tcp_vdlm2);
        debug!("AR_SERVE_ZMQ_VDLM2: {:?}", self.serve_zmq_vdlm2);
        debug!("AR_MAX_UDP_PACKET_SIZE: {:?}", self.max_udp_packet_size);
        debug!("AR_REASSEMBLY_WINDOW: {:?}", self.reassembly_window);
    }

    pub fn acars_configured(&self) -> bool {
        self.receive_tcp_acars.is_some()
            || self.listen_udp_acars.is_some()
            || self.listen_tcp_acars.is_some()
            || self.receive_zmq_acars.is_some()
            || self.send_udp_acars.is_some()
            || self.send_tcp_acars.is_some()
            || self.serve_tcp_acars.is_some()
            || self.serve_zmq_acars.is_some()
    }

    pub fn vdlm_configured(&self) -> bool {
        self.receive_tcp_vdlm2.is_some()
            || self.listen_udp_vdlm2.is_some()
            || self.listen_tcp_vdlm2.is_some()
            || self.receive_zmq_vdlm2.is_some()
            || self.send_udp_vdlm2.is_some()
            || self.send_tcp_vdlm2.is_some()
            || self.serve_tcp_vdlm2.is_some()
            || self.serve_zmq_vdlm2.is_some()
    }
}
