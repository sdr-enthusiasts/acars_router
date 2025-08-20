pub extern crate clap as clap;
#[macro_use]
extern crate log;
extern crate sdre_rust_logging;

pub mod sanity_checker;

use clap::Parser;
use sdre_rust_logging::SetupLogging;

#[derive(Parser, Debug, Clone, Default)]
#[command(name = "ACARS Router", author, version, about, long_about = None)]
pub struct Input {
    // Output Options
    /// Set the log level. debug, trace, info are valid options.
    #[clap(
        short,
        long,
        env = "AR_VERBOSITY",
        value_parser,
        default_value = "info"
    )]
    pub verbose: String,
    /// Enable message deduplication
    #[clap(long, env = "AR_ENABLE_DEDUPE", value_parser)]
    pub enable_dedupe: bool,
    /// Set the number of seconds that a message will be considered as a duplicate
    /// if it is received again.
    #[clap(long, env = "AR_DEDUPE_WINDOW", value_parser, default_value = "2")]
    pub dedupe_window: u64,
    /// Reject messages with a timestamp greater than +/- this many seconds.
    #[clap(long, env = "AR_SKEW_WINDOW", value_parser, default_value = "5")]
    pub skew_window: u64,
    /// Set maximum UDP packet size, peer-to-peer.
    #[clap(
        long,
        env = "AR_MAX_UDP_PACKET_SIZE",
        value_parser,
        default_value = "60000"
    )]
    pub max_udp_packet_size: u64,
    /// Number of seconds DNS resolve will be cached for UDP outputs
    #[clap(
        long,
        env = "AR_UDP_DNS_CACHE_SECONDS",
        value_parser,
        default_value = "15.0"
    )]
    pub udp_dns_cache_seconds: f64,
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
    /// Chatty logging of stats
    #[clap(long, env = "AR_STATS_VERBOSE", value_parser)]
    pub stats_verbose: bool,
    /// Attempt message reassembly on incomplete messages within the specified number of seconds
    #[clap(
        long,
        env = "AR_REASSEMBLY_WINDOW",
        value_parser,
        default_value = "1.0"
    )]
    pub reassembly_window: f64,
    /// Disable ACARS input
    #[clap(long, env = "AR_DISABLE_ACARS", value_parser)]
    pub disable_acars: bool,
    /// Disable VDLM2 input
    #[clap(long, env = "AR_DISABLE_VDLM2", value_parser)]
    pub disable_vdlm2: bool,
    /// Disable HFDL input
    #[clap(long, env = "AR_DISABLE_HFDL", value_parser)]
    pub disable_hfdl: bool,
    /// Disable IMSL input
    #[clap(long, env = "AR_DISABLE_IMSL", value_parser)]
    pub disable_imsl: bool,
    /// Disable IRDM input
    #[clap(long, env = "AR_DISABLE_IRDM", value_parser)]
    pub disable_irdm: bool,

    // Input Options

    // ACARS
    /// ACARS Router will listen for ACARS messages on the specified UDP ports.
    /// Semi-Colon separated list of arguments. ie 5550;5551;5552
    #[clap(long, value_parser, value_delimiter = ';')]
    pub listen_udp_acars: Option<Vec<u16>>,
    /// ACARS Router will listen for connections from a client for ACARS messages on the specified TCP ports.
    /// Semi-Colon separated list of arguments. ie 5550;5551;5552
    #[clap(long, value_parser, value_delimiter = ';')]
    pub listen_tcp_acars: Option<Vec<u16>>,
    /// ACARS Router will listen for connections from a client for ACARS messages on the specified ZMQ ports.
    /// Semi-Colon separated list of arguments. ie 5550;5551;5552
    #[clap(long, value_parser, value_delimiter = ';')]
    pub listen_zmq_acars: Option<Vec<u16>>,
    /// ACARS Router will connect to the specified hosts for ACARS messages over TCP.
    /// Semi-Colon separated list of arguments. ie host:5550;host:5551;host:5552
    #[clap(long, value_parser, value_delimiter = ';')]
    pub receive_tcp_acars: Option<Vec<String>>,
    /// ACARS Router will connect to the specified hosts for ACARS messages in ZMQ format.
    /// Semi-Colon separated list of arguments. io host:5550;host:5551;host:5552
    #[clap(long, value_parser, value_delimiter = ';')]
    pub receive_zmq_acars: Option<Vec<String>>,

    // VDLM2
    /// ACARS Router will listen for VDLM2 messages on the specified UDP ports.
    /// Semi-Colon separated list of arguments. ie 5555;5556;5557
    #[clap(long, value_parser, value_delimiter = ';')]
    pub listen_udp_vdlm2: Option<Vec<u16>>,
    /// ACARS Router will listen for connections from a client for VDLM2 messages on the specified TCP ports.
    /// Semi-Colon separated list of arguments. ie 5555;5556;5557
    #[clap(long, value_parser, value_delimiter = ';')]
    pub listen_tcp_vdlm2: Option<Vec<u16>>,
    #[clap(long, value_parser, value_delimiter = ';')]
    /// ACARS Router will listen for connections from a client for VDLM2 messages on the specified ZMQ ports.
    /// Semi-Colon separated list of arguments. ie 5555;5556;5557
    pub listen_zmq_vdlm2: Option<Vec<u16>>,
    /// ACARS Router will connect to the specified hosts for VDLM2 messages over TCP.
    /// Semi-Colon separated list of arguments. ie host:5550;host:5551;host:5552
    #[clap(long, value_parser, value_delimiter = ';')]
    pub receive_tcp_vdlm2: Option<Vec<String>>,
    /// ACARS Router will connect to the specified hosts for VDLM2 messages in ZMQ format.
    /// Semi-Colon separated list of arguments. ie  host:5550;host:5551;host:5552
    #[clap(long, value_parser, value_delimiter = ';')]
    pub receive_zmq_vdlm2: Option<Vec<String>>,

    // HFDL
    /// ACARS Router will listen for HFDL messages on the specified UDP ports.
    /// Semi-Colon separated list of arguments. ie 5555;5556;5557
    #[clap(long, value_parser, value_delimiter = ';')]
    pub listen_udp_hfdl: Option<Vec<u16>>,
    /// ACARS Router will listen for connections from a client for HFDL messages on the specified TCP ports.
    /// Semi-Colon separated list of arguments. ie 5555;5556;5557
    #[clap(long, value_parser, value_delimiter = ';')]
    pub listen_tcp_hfdl: Option<Vec<u16>>,
    /// ACARS Router will listen for connections from a client for HFDL messages on the specified ZMQ ports.
    /// Semi-Colon separated list of arguments. ie 5555;5556;5557
    #[clap(long, value_parser, value_delimiter = ';')]
    pub listen_zmq_hfdl: Option<Vec<u16>>,
    /// ACARS Router will connect to the specified hosts for HFDL messages over TCP.
    /// Semi-Colon separated list of arguments. ie host:5550;host:5551;host:5552
    #[clap(long, value_parser, value_delimiter = ';')]
    pub receive_tcp_hfdl: Option<Vec<String>>,
    /// ACARS Router will connect to the specified hosts for HFDL messages in ZMQ format.
    /// Semi-Colon separated list of arguments. ie  host:5550;host:5551;host:5552
    #[clap(long, value_parser, value_delimiter = ';')]
    pub receive_zmq_hfdl: Option<Vec<String>>,

    // IMSL
    /// ACARS Router will listen for IMSL messages on the specified UDP ports.
    /// Semi-Colon separated list of arguments. ie 5555;5556;5557
    #[clap(long, value_parser, value_delimiter = ';')]
    pub listen_udp_imsl: Option<Vec<u16>>,
    /// ACARS Router will listen for connections from a client for IMSL messages on the specified TCP ports.
    /// Semi-Colon separated list of arguments. ie 5555;5556;5557
    #[clap(long, value_parser, value_delimiter = ';')]
    pub listen_tcp_imsl: Option<Vec<u16>>,
    /// ACARS Router will listen for connections from a client for IMSL messages on the specified ZMQ ports.
    /// Semi-Colon separated list of arguments. ie 5555;5556;5557
    #[clap(long, value_parser, value_delimiter = ';')]
    pub listen_zmq_imsl: Option<Vec<u16>>,
    /// ACARS Router will connect to the specified hosts for IMSL messages over TCP.
    /// Semi-Colon separated list of arguments. ie host:5550;host:5551;host:5552
    #[clap(long, value_parser, value_delimiter = ';')]
    pub receive_tcp_imsl: Option<Vec<String>>,
    /// ACARS Router will connect to the specified hosts for IMSL messages in ZMQ format.
    /// Semi-Colon separated list of arguments. ie  host:5550;host:5551;host:5552
    #[clap(long, value_parser, value_delimiter = ';')]
    pub receive_zmq_imsl: Option<Vec<String>>,

    // IRDM
    /// ACARS Router will listen for IRDM messages on the specified UDP ports.
    /// Semi-Colon separated list of arguments. ie 5555;5556;5557
    #[clap(long, value_parser, value_delimiter = ';')]
    pub listen_udp_irdm: Option<Vec<u16>>,
    /// ACARS Router will listen for connections from a client for IRDM messages on the specified TCP ports.
    /// Semi-Colon separated list of arguments. ie 5555;5556;5557
    #[clap(long, value_parser, value_delimiter = ';')]
    pub listen_tcp_irdm: Option<Vec<u16>>,
    /// ACARS Router will listen for connections from a client for IRDM messages on the specified ZMQ ports.
    /// Semi-Colon separated list of arguments. ie 5555;5556;5557
    #[clap(long, value_parser, value_delimiter = ';')]
    pub listen_zmq_irdm: Option<Vec<u16>>,
    /// ACARS Router will connect to the specified hosts for IRDM messages over TCP.
    /// Semi-Colon separated list of arguments. ie host:5550;host:5551;host:5552
    #[clap(long, value_parser, value_delimiter = ';')]
    pub receive_tcp_irdm: Option<Vec<String>>,
    /// ACARS Router will connect to the specified hosts for IRDM messages in ZMQ format.
    /// Semi-Colon separated list of arguments. ie  host:5550;host:5551;host:5552
    #[clap(long, value_parser, value_delimiter = ';')]
    pub receive_zmq_irdm: Option<Vec<String>>,

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

    // HFDL
    /// Semi-Colon separated list of arguments. ie host:5555;host:5556;host:5557
    #[clap(long, value_parser, value_delimiter = ';')]
    pub send_udp_hfdl: Option<Vec<String>>,
    /// Semi-Colon separated list of arguments. ie host:5555;host:5556;host:5557
    #[clap(long, value_parser, value_delimiter = ';')]
    pub send_tcp_hfdl: Option<Vec<String>>,
    /// Semi-Colon separated list of arguments. ie 5550;5551;5552
    #[clap(long, value_parser, value_delimiter = ';')]
    pub serve_tcp_hfdl: Option<Vec<u16>>,
    /// Semi-Colon separated list of arguments. ie 5550;5551;5552
    #[clap(long, value_parser, value_delimiter = ';')]
    pub serve_zmq_hfdl: Option<Vec<u16>>,

    // IMSL
    /// Semi-Colon separated list of arguments. ie host:5555;host:5556;host:5557
    #[clap(long, value_parser, value_delimiter = ';')]
    pub send_udp_imsl: Option<Vec<String>>,
    /// Semi-Colon separated list of arguments. ie host:5555;host:5556;host:5557
    #[clap(long, value_parser, value_delimiter = ';')]
    pub send_tcp_imsl: Option<Vec<String>>,
    /// Semi-Colon separated list of arguments. ie 5550;5551;5552
    #[clap(long, value_parser, value_delimiter = ';')]
    pub serve_tcp_imsl: Option<Vec<u16>>,
    /// Semi-Colon separated list of arguments. ie 5550;5551;5552
    #[clap(long, value_parser, value_delimiter = ';')]
    pub serve_zmq_imsl: Option<Vec<u16>>,

    // IRDM
    /// Semi-Colon separated list of arguments. ie host:5555;host:5556;host:5557
    #[clap(long, value_parser, value_delimiter = ';')]
    pub send_udp_irdm: Option<Vec<String>>,
    /// Semi-Colon separated list of arguments. ie host:5555;host:5556;host:5557
    #[clap(long, value_parser, value_delimiter = ';')]
    pub send_tcp_irdm: Option<Vec<String>>,
    /// Semi-Colon separated list of arguments. ie 5550;5551;5552
    #[clap(long, value_parser, value_delimiter = ';')]
    pub serve_tcp_irdm: Option<Vec<u16>>,
    /// Semi-Colon separated list of arguments. ie 5550;5551;5552
    #[clap(long, value_parser, value_delimiter = ';')]
    pub serve_zmq_irdm: Option<Vec<u16>>,
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
        debug!("AR_VERBOSE: {:?}", self.verbose.clone().set_logging_level());
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
        debug!("AR_UDP_DNS_CACHE_SECONDS: {:?}", self.udp_dns_cache_seconds);
        debug!("AR_REASSEMBLY_WINDOW: {:?}", self.reassembly_window);
        debug!("AR_STATS_VERBOSE: {:?}", self.stats_verbose);
        debug!("AR_LISTEN_UDP_HFDL: {:?}", self.listen_udp_hfdl);
        debug!("AR_LISTEN_TCP_HFDL: {:?}", self.listen_tcp_hfdl);
        debug!("AR_RECV_TCP_HFDL: {:?}", self.receive_tcp_hfdl);
        debug!("AR_RECV_ZMQ_HFDL: {:?}", self.receive_zmq_hfdl);
        debug!("AR_SEND_UDP_HFDL: {:?}", self.send_udp_hfdl);
        debug!("AR_SEND_TCP_HFDL: {:?}", self.send_tcp_hfdl);
        debug!("AR_SERVE_TCP_HFDL: {:?}", self.serve_tcp_hfdl);
        debug!("AR_SERVE_ZMQ_HFDL: {:?}", self.serve_zmq_hfdl);
        debug!("AR_LISTEN_ZMQ_ACARS: {:?}", self.listen_zmq_acars);
        debug!("AR_LISTEN_ZMQ_VDLM2: {:?}", self.listen_zmq_vdlm2);
        debug!("AR_LISTEN_ZMQ_HFDL: {:?}", self.listen_zmq_hfdl);
        debug!("AR_LISTEN_UDP_IMSL: {:?}", self.listen_udp_imsl);
        debug!("AR_LISTEN_TCP_IMSL: {:?}", self.listen_tcp_imsl);
        debug!("AR_RECV_TCP_IMSL: {:?}", self.receive_tcp_imsl);
        debug!("AR_RECV_ZMQ_IMSL: {:?}", self.receive_zmq_imsl);
        debug!("AR_SEND_UDP_IMSL: {:?}", self.send_udp_imsl);
        debug!("AR_SEND_TCP_IMSL: {:?}", self.send_tcp_imsl);
        debug!("AR_SERVE_TCP_IMSL: {:?}", self.serve_tcp_imsl);
        debug!("AR_SERVE_ZMQ_IMSL: {:?}", self.serve_zmq_imsl);
        debug!("AR_LISTEN_ZMQ_IMSL: {:?}", self.listen_zmq_imsl);
        debug!("AR_LISTEN_UDP_IRDM: {:?}", self.listen_udp_irdm);
        debug!("AR_LISTEN_TCP_IRDM: {:?}", self.listen_tcp_irdm);
        debug!("AR_RECV_TCP_IRDM: {:?}", self.receive_tcp_irdm);
        debug!("AR_RECV_ZMQ_IRDM: {:?}", self.receive_zmq_irdm);
        debug!("AR_SEND_UDP_IRDM: {:?}", self.send_udp_irdm);
        debug!("AR_SEND_TCP_IRDM: {:?}", self.send_tcp_irdm);
        debug!("AR_SERVE_TCP_IRDM: {:?}", self.serve_tcp_irdm);
        debug!("AR_SERVE_ZMQ_IRDM: {:?}", self.serve_zmq_irdm);
        debug!("AR_LISTEN_ZMQ_IRDM: {:?}", self.listen_zmq_irdm);
        debug!("AR_DISABLE_ACARS: {:?}", self.disable_acars);
        debug!("AR_DISABLE_VDLM2: {:?}", self.disable_vdlm2);
        debug!("AR_DISABLE_HFDL: {:?}", self.disable_hfdl);
        debug!("AR_DISABLE_IMSL: {:?}", self.disable_imsl);
        debug!("AR_DISABLE_IRDM: {:?}", self.disable_irdm);
    }

    pub fn acars_configured(&self) -> bool {
        !self.disable_acars
            && (self.receive_tcp_acars.is_some()
                || self.listen_udp_acars.is_some()
                || self.listen_tcp_acars.is_some()
                || self.receive_zmq_acars.is_some()
                || self.send_udp_acars.is_some()
                || self.send_tcp_acars.is_some()
                || self.serve_tcp_acars.is_some()
                || self.serve_zmq_acars.is_some())
    }

    pub fn vdlm_configured(&self) -> bool {
        !self.disable_vdlm2
            && (self.receive_tcp_vdlm2.is_some()
                || self.listen_udp_vdlm2.is_some()
                || self.listen_tcp_vdlm2.is_some()
                || self.receive_zmq_vdlm2.is_some()
                || self.send_udp_vdlm2.is_some()
                || self.send_tcp_vdlm2.is_some()
                || self.serve_tcp_vdlm2.is_some()
                || self.serve_zmq_vdlm2.is_some())
    }

    pub fn hfdl_configured(&self) -> bool {
        !self.disable_hfdl
            && (self.receive_tcp_hfdl.is_some()
                || self.listen_udp_hfdl.is_some()
                || self.listen_tcp_hfdl.is_some()
                || self.receive_zmq_hfdl.is_some()
                || self.send_udp_hfdl.is_some()
                || self.send_tcp_hfdl.is_some()
                || self.serve_tcp_hfdl.is_some()
                || self.serve_zmq_hfdl.is_some())
    }

    pub fn imsl_configured(&self) -> bool {
        !self.disable_imsl
            && (self.receive_tcp_imsl.is_some()
                || self.listen_udp_imsl.is_some()
                || self.listen_tcp_imsl.is_some()
                || self.receive_zmq_imsl.is_some()
                || self.send_udp_imsl.is_some()
                || self.send_tcp_imsl.is_some()
                || self.serve_tcp_imsl.is_some()
                || self.serve_zmq_imsl.is_some())
    }

    pub fn irdm_configured(&self) -> bool {
        !self.disable_irdm
            && (self.receive_tcp_irdm.is_some()
                || self.listen_udp_irdm.is_some()
                || self.listen_tcp_irdm.is_some()
                || self.receive_zmq_irdm.is_some()
                || self.send_udp_irdm.is_some()
                || self.send_tcp_irdm.is_some()
                || self.serve_tcp_irdm.is_some()
                || self.serve_zmq_irdm.is_some())
    }
}
