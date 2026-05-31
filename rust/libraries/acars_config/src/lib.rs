pub use clap;

pub mod sanity_checker;

use clap::Parser;
use log::debug;
use std::fmt;

/// Logical protocol carried by the router. The set is fixed.
///
/// Order is meaningful: `as usize` indexes into the array returned by
/// [`Input::protocols`].
#[repr(usize)]
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum Protocol {
    Acars = 0,
    Vdlm2 = 1,
    Hfdl = 2,
    Imsl = 3,
    Irdm = 4,
}

impl Protocol {
    /// All protocols, in canonical order.
    pub const ALL: [Self; 5] = [Self::Acars, Self::Vdlm2, Self::Hfdl, Self::Imsl, Self::Irdm];

    /// Short lowercase slug, suitable for metric labels and tracing fields.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Acars => "acars",
            Self::Vdlm2 => "vdlm2",
            Self::Hfdl => "hfdl",
            Self::Imsl => "imsl",
            Self::Irdm => "irdm",
        }
    }

    /// Human-facing acronym used in log lines and queue identifiers.
    ///
    /// Note: `Vdlm2` resolves to `"VDLM"` to preserve compatibility with
    /// pre-existing log output and the `MessageHandlerConfig::queue_type`
    /// value historically passed by `service_init`.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Acars => "ACARS",
            Self::Vdlm2 => "VDLM",
            Self::Hfdl => "HFDL",
            Self::Imsl => "IMSL",
            Self::Irdm => "IRDM",
        }
    }

    /// Uppercase slug suffix used in environment variable names
    /// (e.g. `AR_LISTEN_UDP_VDLM2`). Unlike [`Self::label`], this preserves
    /// the trailing digit on `Vdlm2`.
    #[must_use]
    pub const fn env_token(self) -> &'static str {
        match self {
            Self::Acars => "ACARS",
            Self::Vdlm2 => "VDLM2",
            Self::Hfdl => "HFDL",
            Self::Imsl => "IMSL",
            Self::Irdm => "IRDM",
        }
    }
}

impl fmt::Display for Protocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

/// Per-protocol I/O view derived from [`Input`].
///
/// Unlike `Input`, every field is a flat `Vec<_>` (never `Option`) — absence
/// is encoded by `is_empty()`. Construct via [`Input::protocols`] or
/// [`Input::protocol`].
#[derive(Clone, Debug, Default)]
pub struct ProtocolIo {
    pub listen_udp: Vec<u16>,
    pub listen_tcp: Vec<u16>,
    pub listen_zmq: Vec<u16>,
    pub receive_tcp: Vec<String>,
    pub receive_zmq: Vec<String>,
    pub send_udp: Vec<String>,
    pub send_tcp: Vec<String>,
    pub serve_tcp: Vec<u16>,
    pub serve_zmq: Vec<u16>,
    pub disabled: bool,
}

impl ProtocolIo {
    /// True iff the protocol is enabled and has at least one configured
    /// endpoint of any kind. Replaces the 5 copy-pasted `Input::*_configured`
    /// methods and — unlike them — correctly considers `listen_zmq_*`.
    #[must_use]
    pub fn is_configured(&self) -> bool {
        !self.disabled
            && (!self.listen_udp.is_empty()
                || !self.listen_tcp.is_empty()
                || !self.listen_zmq.is_empty()
                || !self.receive_tcp.is_empty()
                || !self.receive_zmq.is_empty()
                || !self.send_udp.is_empty()
                || !self.send_tcp.is_empty()
                || !self.serve_tcp.is_empty()
                || !self.serve_zmq.is_empty())
    }
}

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

    /// Project the parsed CLI into a per-protocol view, in canonical order
    /// (see [`Protocol::ALL`]).
    #[must_use]
    pub fn protocols(&self) -> [ProtocolIo; 5] {
        Protocol::ALL.map(|p| self.protocol(p))
    }

    /// Project the parsed CLI into the view for a single protocol.
    #[must_use]
    pub fn protocol(&self, proto: Protocol) -> ProtocolIo {
        match proto {
            Protocol::Acars => ProtocolIo {
                listen_udp: self.listen_udp_acars.clone().unwrap_or_default(),
                listen_tcp: self.listen_tcp_acars.clone().unwrap_or_default(),
                listen_zmq: self.listen_zmq_acars.clone().unwrap_or_default(),
                receive_tcp: self.receive_tcp_acars.clone().unwrap_or_default(),
                receive_zmq: self.receive_zmq_acars.clone().unwrap_or_default(),
                send_udp: self.send_udp_acars.clone().unwrap_or_default(),
                send_tcp: self.send_tcp_acars.clone().unwrap_or_default(),
                serve_tcp: self.serve_tcp_acars.clone().unwrap_or_default(),
                serve_zmq: self.serve_zmq_acars.clone().unwrap_or_default(),
                disabled: self.disable_acars,
            },
            Protocol::Vdlm2 => ProtocolIo {
                listen_udp: self.listen_udp_vdlm2.clone().unwrap_or_default(),
                listen_tcp: self.listen_tcp_vdlm2.clone().unwrap_or_default(),
                listen_zmq: self.listen_zmq_vdlm2.clone().unwrap_or_default(),
                receive_tcp: self.receive_tcp_vdlm2.clone().unwrap_or_default(),
                receive_zmq: self.receive_zmq_vdlm2.clone().unwrap_or_default(),
                send_udp: self.send_udp_vdlm2.clone().unwrap_or_default(),
                send_tcp: self.send_tcp_vdlm2.clone().unwrap_or_default(),
                serve_tcp: self.serve_tcp_vdlm2.clone().unwrap_or_default(),
                serve_zmq: self.serve_zmq_vdlm2.clone().unwrap_or_default(),
                disabled: self.disable_vdlm2,
            },
            Protocol::Hfdl => ProtocolIo {
                listen_udp: self.listen_udp_hfdl.clone().unwrap_or_default(),
                listen_tcp: self.listen_tcp_hfdl.clone().unwrap_or_default(),
                listen_zmq: self.listen_zmq_hfdl.clone().unwrap_or_default(),
                receive_tcp: self.receive_tcp_hfdl.clone().unwrap_or_default(),
                receive_zmq: self.receive_zmq_hfdl.clone().unwrap_or_default(),
                send_udp: self.send_udp_hfdl.clone().unwrap_or_default(),
                send_tcp: self.send_tcp_hfdl.clone().unwrap_or_default(),
                serve_tcp: self.serve_tcp_hfdl.clone().unwrap_or_default(),
                serve_zmq: self.serve_zmq_hfdl.clone().unwrap_or_default(),
                disabled: self.disable_hfdl,
            },
            Protocol::Imsl => ProtocolIo {
                listen_udp: self.listen_udp_imsl.clone().unwrap_or_default(),
                listen_tcp: self.listen_tcp_imsl.clone().unwrap_or_default(),
                listen_zmq: self.listen_zmq_imsl.clone().unwrap_or_default(),
                receive_tcp: self.receive_tcp_imsl.clone().unwrap_or_default(),
                receive_zmq: self.receive_zmq_imsl.clone().unwrap_or_default(),
                send_udp: self.send_udp_imsl.clone().unwrap_or_default(),
                send_tcp: self.send_tcp_imsl.clone().unwrap_or_default(),
                serve_tcp: self.serve_tcp_imsl.clone().unwrap_or_default(),
                serve_zmq: self.serve_zmq_imsl.clone().unwrap_or_default(),
                disabled: self.disable_imsl,
            },
            Protocol::Irdm => ProtocolIo {
                listen_udp: self.listen_udp_irdm.clone().unwrap_or_default(),
                listen_tcp: self.listen_tcp_irdm.clone().unwrap_or_default(),
                listen_zmq: self.listen_zmq_irdm.clone().unwrap_or_default(),
                receive_tcp: self.receive_tcp_irdm.clone().unwrap_or_default(),
                receive_zmq: self.receive_zmq_irdm.clone().unwrap_or_default(),
                send_udp: self.send_udp_irdm.clone().unwrap_or_default(),
                send_tcp: self.send_tcp_irdm.clone().unwrap_or_default(),
                serve_tcp: self.serve_tcp_irdm.clone().unwrap_or_default(),
                serve_zmq: self.serve_zmq_irdm.clone().unwrap_or_default(),
                disabled: self.disable_irdm,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression: `Input::*_configured` historically ignored `listen_zmq_*`.
    /// A protocol configured only via `--listen-zmq-<proto>` is now reported
    /// as configured.
    #[test]
    fn listen_zmq_only_is_configured() {
        for proto in Protocol::ALL {
            let mut input = Input::default();
            match proto {
                Protocol::Acars => input.listen_zmq_acars = Some(vec![5550]),
                Protocol::Vdlm2 => input.listen_zmq_vdlm2 = Some(vec![5550]),
                Protocol::Hfdl => input.listen_zmq_hfdl = Some(vec![5550]),
                Protocol::Imsl => input.listen_zmq_imsl = Some(vec![5550]),
                Protocol::Irdm => input.listen_zmq_irdm = Some(vec![5550]),
            }
            assert!(
                input.protocol(proto).is_configured(),
                "{proto}: listen_zmq alone should mark protocol configured",
            );
        }
    }

    #[test]
    fn disabled_overrides_configured() {
        let input = Input {
            disable_acars: true,
            listen_udp_acars: Some(vec![5550]),
            ..Input::default()
        };
        assert!(!input.protocol(Protocol::Acars).is_configured());
    }

    #[test]
    fn empty_input_is_not_configured() {
        let input = Input::default();
        for proto in Protocol::ALL {
            assert!(!input.protocol(proto).is_configured(), "{proto}");
        }
    }

    #[test]
    fn empty_vec_does_not_count_as_configured() {
        let input = Input {
            listen_udp_acars: Some(vec![]),
            listen_tcp_acars: Some(vec![]),
            ..Input::default()
        };
        assert!(!input.protocol(Protocol::Acars).is_configured());
    }
}
