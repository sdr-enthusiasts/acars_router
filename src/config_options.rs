// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

use clap::Parser;
use derive_getters::Getters;
use log::debug;
use std::env;
use std::str;

#[derive(Parser, Debug)]
#[clap(name = "ACARS Router", author = "Mike Nye / Fred Clausen", version = "1.0", about = "ACARS Router: A Utility to ingest ACARS/VDLM2 from many sources, process, and feed out to many consumers.", long_about = None)]
struct Args {
    // Output Options
    #[clap(short = 'v', long = "verbose", default_value = "info")]
    /// Set the log level. debug, trace, info are valid options.
    verbose: String,
    #[clap(long)]
    /// Enable message deduplication
    enable_dedupe: bool,
    #[clap(long, default_value = "2")]
    /// Set the number of seconds that a message will be considered as a duplicate
    /// if it is received again.
    dedupe_window: u64,
    #[clap(long, default_value = "1")]
    /// Reject messages with a timestamp greater than +/- this many seconds.
    skew_window: u64,
    // Message Modification
    #[clap(long)]
    /// Set to true to enable message modification
    /// This will add a "proxied" field to the message
    dont_add_proxy_id: bool,

    // Input Options

    // ACARS
    /// Semi-Colon separated list of arguments. ie 5550;5551;5552
    #[clap(long, default_value = "")]
    listen_udp_acars: String,
    /// Semi-Colon separated list of arguments. ie 5550;5551;5552
    #[clap(long, default_value = "")]
    listen_tcp_acars: String,
    /// Semi-Colon separated list of arguments. ie 5550;5551;5552
    #[clap(long, default_value = "")]
    receive_tcp_acars: String,

    // VDLM2
    /// Semi-Colon separated list of arguments. ie 5555;5556;5557
    #[clap(long, default_value = "")]
    /// Semi-Colon separated list of arguments. ie 5555;5556;5557
    listen_udp_vdlm2: String,
    #[clap(long, default_value = "")]
    /// Semi-Colon separated list of arguments. ie 5555;5556;5557
    listen_tcp_vdlm2: String,
    #[clap(long, default_value = "")]
    /// Semi-Colon separated list of arguments. ie 5555;5556;1557
    receive_tcp_vdlm2: String,
    /// Semi-Colon separated list of arguments. ie  host:5550;host:5551;host:5552
    #[clap(long, default_value = "")]
    receive_zmq_vdlm2: String,
    // JSON Output options
    // ACARS
    /// Semi-Colon separated list of arguments. ie host:5550;host:5551;host:5552
    #[clap(long, default_value = "")]
    send_udp_acars: String,
    /// Semi-Colon separated list of arguments. ie host:5550;host:5551;host:5552
    #[clap(long, default_value = "")]
    send_tcp_acars: String,
    // Semi-Colon separated list of arguments. ie 5550;5551;5552
    #[clap(long, default_value = "")]
    serve_tcp_acars: String,
    // Semi-Colon separated list of arguments. ie 5550;5551;5552
    #[clap(long, default_value = "")]
    serve_zmq_acars: String,
    // VDLM
    /// Semi-Colon separated list of arguments. ie host:5555;host:5556;host:5557
    #[clap(long, default_value = "")]
    send_udp_vdlm2: String,
    /// Semi-Colon separated list of arguments. ie host:5555;host:5556;host:5557
    #[clap(long, default_value = "")]
    send_tcp_vdlm2: String,
    // Semi-Colon separated list of arguments. ie 5550;5551;5552
    #[clap(long, default_value = "")]
    serve_tcp_vdlm2: String,
    // Semi-Colon separated list of arguments. ie 5550;5551;5552
    #[clap(long, default_value = "")]
    serve_zmq_vdlm2: String,
}

#[derive(Getters, Clone)]
pub struct ACARSRouterSettings {
    pub log_level: log::LevelFilter,
    // This field is named opposite to the command line flag.
    // The presence of the flag indicates we should NOT add the proxy id
    // The field is inverted and saved
    pub add_proxy_id: bool,
    pub dedupe: bool,
    pub dedupe_window: u64,
    pub skew_window: u64,
    pub listen_udp_acars: Vec<String>,
    pub listen_tcp_acars: Vec<String>,
    pub receive_tcp_acars: Vec<String>,
    pub listen_udp_vdlm2: Vec<String>,
    pub listen_tcp_vdlm2: Vec<String>,
    pub receive_tcp_vdlm2: Vec<String>,
    pub receive_zmq_vdlm2: Vec<String>,
    pub send_udp_acars: Vec<String>,
    pub send_tcp_acars: Vec<String>,
    pub serve_tcp_acars: Vec<String>,
    pub serve_zmq_acars: Vec<String>,
    pub send_udp_vdlm2: Vec<String>,
    pub send_tcp_vdlm2: Vec<String>,
    pub serve_tcp_vdlm2: Vec<String>,
    pub serve_zmq_vdlm2: Vec<String>,
}

impl ACARSRouterSettings {
    pub fn print_values(&self) {
        debug!("The Following configuration values were loaded:");
        debug!("AR_LISTEN_UDP_ACARS: {:?}", self.listen_udp_acars);
        debug!("AR_LISTEN_TCP_ACARS: {:?}", self.listen_tcp_acars);
        debug!("AR_RECV_TCP_ACARS: {:?}", self.receive_tcp_acars);
        debug!("AR_LISTEN_UDP_VDLM2: {:?}", self.listen_udp_vdlm2);
        debug!("AR_LISTEN_TCP_VDLM2: {:?}", self.listen_tcp_vdlm2);
        debug!("AR_RECV_TCP_VDLM2: {:?}", self.receive_tcp_vdlm2);
        debug!("AR_RECV_ZMQ_VDLM2: {:?}", self.receive_zmq_vdlm2);
        debug!("AR_SEND_UDP_ACARS: {:?}", self.send_udp_acars);
        debug!("AR_SEND_TCP_ACARS: {:?}", self.send_tcp_acars);
        debug!("AR_SERVE_TCP_ACARS: {:?}", self.serve_tcp_acars);
        debug!("AR_SERVE_ZMQ_ACARS: {:?}", self.serve_zmq_acars);
        debug!("AR_VERBOSE: {:?}", self.log_level);
        debug!("AR_ADD_PROXY_ID: {:?}", self.add_proxy_id);
        debug!("AR_ENABLE_DEDUPE: {:?}", self.dedupe);
        debug!("AR_DEDUPE_WINDOW: {:?}", self.dedupe_window);
        debug!("AR_SKEW_WINDOW: {:?}", self.skew_window);
        debug!("AR_SEND_UDP_VDLM2: {:?}", self.send_udp_vdlm2);
        debug!("AR_SEND_TCP_VDLM2: {:?}", self.send_tcp_vdlm2);
        debug!("AR_SERVE_TCP_VDLM2: {:?}", self.serve_tcp_vdlm2);
        debug!("AR_SERVE_ZMQ_VDLM2: {:?}", self.serve_zmq_vdlm2);
    }

    pub fn load_values() -> ACARSRouterSettings {
        let args = Args::parse();

        return ACARSRouterSettings {
            log_level: get_log_level(&args.verbose),
            dedupe: get_value_as_bool("AR_ENABLE_DEDUPE", &args.enable_dedupe),
            dedupe_window: get_value_as_u64("AR_DEDUPE_WINDOW", &args.dedupe_window),
            skew_window: get_value_as_u64("AR_SKEW_WINDOW", &args.skew_window),
            add_proxy_id: get_value_as_bool_invert_bool(
                "AR_DONT_ADD_PROXY_ID",
                &args.dont_add_proxy_id,
            ),
            listen_udp_acars: get_value_as_vector(
                "AR_LISTEN_UDP_ACARS",
                &args.listen_udp_acars,
                "5550",
            ),
            listen_tcp_acars: get_value_as_vector(
                "AR_LISTEN_TCP_ACARS",
                &args.listen_tcp_acars,
                "5550",
            ),
            receive_tcp_acars: get_value_as_vector(
                "AR_RECV_TCP_ACARS",
                &args.receive_tcp_acars,
                "",
            ),
            listen_udp_vdlm2: get_value_as_vector(
                "AR_LISTEN_UDP_VDLM2",
                &args.listen_udp_vdlm2,
                "5555",
            ),
            listen_tcp_vdlm2: get_value_as_vector(
                "AR_LISTEN_TCP_VDLM2",
                &args.listen_tcp_vdlm2,
                "5555",
            ),
            receive_tcp_vdlm2: get_value_as_vector(
                "AR_RECV_TCP_VDLM2",
                &args.receive_tcp_vdlm2,
                "",
            ),
            receive_zmq_vdlm2: get_value_as_vector(
                "AR_RECV_ZMQ_VDLM2",
                &args.receive_zmq_vdlm2,
                "",
            ),
            send_udp_acars: get_value_as_vector("AR_SEND_UDP_ACARS", &args.send_udp_acars, ""),
            send_tcp_acars: get_value_as_vector("AR_SEND_TCP_ACARS", &args.send_tcp_acars, ""),
            serve_tcp_acars: get_value_as_vector("AR_SERVE_TCP_ACARS", &args.serve_tcp_acars, ""),
            serve_zmq_acars: get_value_as_vector("AR_SERVE_ZMQ_ACARS", &args.serve_zmq_acars, ""),
            send_udp_vdlm2: get_value_as_vector("AR_SEND_UDP_VDLM2", &args.send_udp_vdlm2, ""),
            send_tcp_vdlm2: get_value_as_vector("AR_SEND_TCP_VDLM2", &args.send_tcp_vdlm2, ""),
            serve_tcp_vdlm2: get_value_as_vector("AR_SERVE_TCP_VDLM2", &args.serve_tcp_vdlm2, ""),
            serve_zmq_vdlm2: get_value_as_vector("AR_SERVE_ZMQ_VDLM2", &args.serve_zmq_vdlm2, ""),
        };
    }
}

fn get_env_variable(name: &str) -> Option<String> {
    match env::var(name) {
        Ok(val) => Some(val),
        Err(_) => None,
    }
}

fn split_env_safely(name: &str) -> Option<Vec<String>> {
    // get the env variable from name

    let env_var = get_env_variable(name);

    // Split the env variable on ";" and return

    match env_var {
        Some(val) => split_string_on_semi_colon(&val),
        None => None,
    }
}

fn split_string_on_semi_colon(name: &String) -> Option<Vec<String>> {
    // Split the string on ";" and return
    Some(name.split(";").map(|s| s.to_string()).collect())
}

// Functions to get the value for configuring acars_router
// If the env_name of the variable is present, that value is used over the command line flag
// If the env_name of the variable is not present, the command line flag is used if present
// If the env_name of the variable is not present and the command line flag is not present, the default value is used

fn get_value_as_vector(env_name: &str, args: &str, default: &str) -> Vec<String> {
    let env = split_env_safely(env_name);

    if env.is_some() {
        return env.unwrap();
    };

    let args = split_string_on_semi_colon(&args.to_string());

    if args.is_some() {
        return args.unwrap();
    };

    return vec![default.to_string()];
}

fn get_value_as_u64(env_name: &str, args: &u64) -> u64 {
    let env = get_env_variable(env_name);

    if env.is_some() {
        return env.unwrap().parse::<u64>().unwrap();
    };

    return *args;
}

// If the env/flag is set the config option is turned on
fn get_value_as_bool(env_name: &str, args: &bool) -> bool {
    let env = get_env_variable(env_name);

    if env.is_some() {
        return true;
    };

    match args {
        true => return true,
        false => return false,
    };
}

// If the env/flag is set the config option is turned off
fn get_value_as_bool_invert_bool(env_name: &str, args: &bool) -> bool {
    let env = get_env_variable(env_name);

    if env.is_some() {
        return false;
    };

    match args {
        true => return false,
        false => return true,
    };
}

fn get_value_as_string(env_name: &str, args: &str, default: &str) -> String {
    let env = get_env_variable(env_name);

    if env.is_some() {
        return env.unwrap();
    };

    if args != "" {
        return args.to_string();
    };

    return default.to_string();
}

// Log Level ("verbose"/"AR_VERBOSITY") supports the legacy numeric 0/1/2 options
// Documentation has been updated to indicate the new method of "info"/"debug"/"trace"

fn get_log_level(args: &str) -> log::LevelFilter {
    let log_level = get_value_as_string("AR_VERBOSITY", args, "info");

    match log_level.to_lowercase().as_str() {
        "1" | "debug" => log::LevelFilter::Debug,
        "2" | "trace" => log::LevelFilter::Trace,
        "0" | "info" | _ => log::LevelFilter::Info,
    }
}
