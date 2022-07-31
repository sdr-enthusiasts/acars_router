// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

use clap::Parser;
use log::LevelFilter;

#[derive(Parser, Debug, Clone)]
#[clap(name = "ACARS Router", author, version, about, long_about = None)]
pub struct Input {
    // Output Options
    /// Set the log level. debug, trace, info are valid options.
    #[clap(short, long, action = clap::ArgAction::Count)]
    pub(crate) verbose: u8,
    /// Enable message deduplication
    #[clap(long, env = "AR_ENABLE_DEDUPE", value_parser)]
    pub(crate) enable_dedupe: bool,
    /// Set the number of seconds that a message will be considered as a duplicate
    /// if it is received again.
    #[clap(long, env = "AR_DEDUPE_WINDOW", value_parser, default_value = "2")]
    pub(crate) dedupe_window: u64,
    /// Reject messages with a timestamp greater than +/- this many seconds.
    #[clap(long, env = "AR_SKEW_WINDOW", value_parser, default_value = "1")]
    pub(crate) skew_window: u64,
    /// Set maximum UDP packet size, peer-to-peer.
    #[clap(long, env = "AR_MAX_UDP_PACKET_SIZE", value_parser, default_value = "60000")]
    pub(crate) max_udp_packet_size: u64,
    // Message Modification
    /// Set to true to enable message modification
    /// This will add a "proxied" field to the message
    #[clap(long, env = "AR_ADD_PROXY_ID", value_parser)]
    pub(crate) add_proxy_id: bool,
    /// Override the station name in the message.
    #[clap(long, env = "AR_OVERRIDE_STATION_NAME", value_parser)]
    pub(crate) override_station_name: Option<String>,
    /// Print statistics every N minutes
    #[clap(long, env = "AR_STATS_EVERY", value_parser, default_value = "5")]
    pub(crate) stats_every: u64,
    /// Attempt message reassembly on incomplete messages within the specified number of seconds
    #[clap(long, env = "AR_REASSEMBLY_WINDOW", value_parser, default_value = "1.0")]
    pub(crate) reassembly_window: f64,
    // Input Options

    // ACARS
    /// Semi-Colon separated list of arguments. ie 5550;5551;5552
    #[clap(long, env = "AR_LISTEN_UDP_ACARS", value_parser, value_delimiter = ';')]
    pub(crate) listen_udp_acars: Option<Vec<u16>>,
    /// Semi-Colon separated list of arguments. ie 5550;5551;5552
    #[clap(long, env = "AR_LISTEN_TCP_ACARS", value_parser, value_delimiter = ';')]
    pub(crate) listen_tcp_acars: Option<Vec<u16>>,
    /// Semi-Colon separated list of arguments. ie host:5550;host:5551;host:5552
    #[clap(long, env = "AR_RECV_TCP_ACARS", value_parser, value_delimiter = ';')]
    pub(crate) receive_tcp_acars: Option<Vec<String>>,
    /// Semi-Colon separated list of arguments. io host:5550;host:5551;host:5552
    #[clap(long, env = "AR_RECV_ZMQ_ACARS", value_parser, value_delimiter = ';')]
    pub(crate) receive_zmq_acars: Option<Vec<String>>,

    // VDLM2
    /// Semi-Colon separated list of arguments. ie 5555;5556;5557
    #[clap(long, env = "AR_LISTEN_UDP_VDLM2", value_parser, value_delimiter = ';')]
    pub(crate) listen_udp_vdlm2: Option<Vec<u16>>,
    /// Semi-Colon separated list of arguments. ie 5555;5556;5557
    #[clap(long, env = "AR_LISTEN_TCP_VDLM2", value_parser, value_delimiter = ';')]
    pub(crate) listen_tcp_vdlm2: Option<Vec<u16>>,
    /// Semi-Colon separated list of arguments. ie host:5550;host:5551;host:5552
    #[clap(long, env = "AR_RECV_TCP_VDLM2", value_parser, value_delimiter = ';')]
    pub(crate) receive_tcp_vdlm2: Option<Vec<String>>,
    /// Semi-Colon separated list of arguments. ie  host:5550;host:5551;host:5552
    #[clap(long, env = "AR_RECV_ZMQ_VDLM2", value_parser, value_delimiter = ';')]
    pub(crate) receive_zmq_vdlm2: Option<Vec<String>>,
    // JSON Output options
    // ACARS
    /// Semi-Colon separated list of arguments. ie host:5550;host:5551;host:5552
    #[clap(long, env = "AR_SEND_UDP_ACARS", value_parser, value_delimiter = ';')]
    pub(crate) send_udp_acars: Option<Vec<String>>,
    /// Semi-Colon separated list of arguments. ie host:5550;host:5551;host:5552
    #[clap(long, env = "AR_SEND_TCP_ACARS", value_parser, value_delimiter = ';')]
    pub(crate) send_tcp_acars: Option<Vec<String>>,
    /// Semi-Colon separated list of arguments. ie 5550;5551;5552
    #[clap(long, env = "AR_SERVE_TCP_ACARS", value_parser, value_delimiter = ';')]
    pub(crate) serve_tcp_acars: Option<Vec<u16>>,
    /// Semi-Colon separated list of arguments. ie 5550;5551;5552
    #[clap(long, env = "AR_SERVE_ZMQ_ACARS", value_parser, value_delimiter = ';')]
    pub(crate) serve_zmq_acars: Option<Vec<u16>>,
    // VDLM
    /// Semi-Colon separated list of arguments. ie host:5555;host:5556;host:5557
    #[clap(long, env = "AR_SEND_UDP_VDLM2", value_parser, value_delimiter = ';')]
    pub(crate) send_udp_vdlm2: Option<Vec<String>>,
    /// Semi-Colon separated list of arguments. ie host:5555;host:5556;host:5557
    #[clap(long, env = "AR_SEND_TCP_VDLM2", value_parser, value_delimiter = ';')]
    pub(crate) send_tcp_vdlm2: Option<Vec<String>>,
    /// Semi-Colon separated list of arguments. ie 5550;5551;5552
    #[clap(long, env = "AR_SERVE_TCP_VDLM2", value_parser, value_delimiter = ';')]
    pub(crate) serve_tcp_vdlm2: Option<Vec<u16>>,
    /// Semi-Colon separated list of arguments. ie 5550;5551;5552
    #[clap(long, env = "AR_SERVE_ZMQ_VDLM2", value_parser, value_delimiter = ';')]
    pub(crate) serve_zmq_vdlm2: Option<Vec<u16>>,
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

pub trait SetupLogging {
    fn set_logging_level(self) -> LevelFilter;
}

impl SetupLogging for u8 {
    fn set_logging_level(self) -> LevelFilter {
        match self {
            0 => LevelFilter::Info,
            1 => LevelFilter::Debug,
            2..=u8::MAX => LevelFilter::Trace,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_set_logging_level() {
        let info_level: u8 = 0;
        let debug_level: u8 = 1;
        let trace_level: u8 = 2;
        let stupid_levels: u8 = 255;
        let info_level_logging: LevelFilter = info_level.set_logging_level();
        let debug_level_logging: LevelFilter = debug_level.set_logging_level();
        let trace_level_logging: LevelFilter = trace_level.set_logging_level();
        let stupid_levels_logging: LevelFilter = stupid_levels.set_logging_level();
        assert_eq!(info_level_logging, LevelFilter::Info);
        assert_eq!(debug_level_logging, LevelFilter::Debug);
        assert_eq!(trace_level_logging, LevelFilter::Trace);
        assert_eq!(stupid_levels_logging, LevelFilter::Trace);
    }
}
