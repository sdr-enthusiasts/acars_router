pub use clap;

pub mod sanity_checker;

use clap::Parser;
use log::debug;

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
        debug!("Configuration: {self:#?}");
    }

    #[must_use]
    pub const fn acars_configured(&self) -> bool {
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

    #[must_use]
    pub const fn vdlm_configured(&self) -> bool {
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

    #[must_use]
    pub const fn hfdl_configured(&self) -> bool {
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

    #[must_use]
    pub const fn imsl_configured(&self) -> bool {
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

    #[must_use]
    pub const fn irdm_configured(&self) -> bool {
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
