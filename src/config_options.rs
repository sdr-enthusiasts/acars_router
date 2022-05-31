use clap::Parser;
use derive_getters::Getters;
use log::debug;
use std::env;
use std::str;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short = 'v', long = "verbose", default_value = "0")]
    verbose: String,
    /// Name of the person to greet
    #[clap(long, default_value = "5550")]
    listen_udp_acars: String,
    #[clap(long, default_value = "5550")]
    listen_tcp_acars: String,
    #[clap(long, default_value = "")]
    receive_tcp_acars: String,
    #[clap(long, default_value = "5555")]
    listen_udp_vdlm2: String,
    #[clap(long, default_value = "5555")]
    listen_tcp_vdlm2: String,
    #[clap(long, default_value = "")]
    receive_tcp_vdlm2: String,
}

#[derive(Getters, Clone)]
pub struct ACARSRouterSettings {
    pub log_level: Option<log::LevelFilter>,
    pub listen_udp_acars: Vec<String>,
    pub listen_tcp_acars: Vec<String>,
    pub receive_tcp_acars: Vec<String>,
    pub listen_udp_vdlm2: Vec<String>,
    pub listen_tcp_vdlm2: Vec<String>,
    pub receive_tcp_vdlm2: Vec<String>,
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
        debug!("AR_VERBOSE: {:?}", self.log_level.unwrap());
    }

    pub fn load_values() -> ACARSRouterSettings {
        let args = Args::parse();
        return ACARSRouterSettings {
            log_level: get_log_level(&args.verbose),
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

    // Split the env variable on ":" and return

    match env_var {
        Some(val) => split_string_on_colon(&val),
        None => None,
    }
}

fn split_string_on_colon(name: &String) -> Option<Vec<String>> {
    // Split the string on ":" and return

    Some(name.split(":").map(|s| s.to_string()).collect())
}

// Function to get the value for configuring acars_router
// If the env_name of the variable is present, that value is used over the command line flag
// If the env_name of the variable is not present, the command line flag is used if present
// If the env_name of the variable is not present and the command line flag is not present, the default value is used

fn get_value_as_vector(env_name: &str, args: &str, default: &str) -> Vec<String> {
    let env = split_env_safely(env_name);

    if env.is_some() {
        return env.unwrap();
    };

    let args = split_string_on_colon(&args.to_string());

    if args.is_some() {
        return args.unwrap();
    };

    return vec![default.to_string()];
}

fn get_value(env_name: &str, args: &str, default: &str) -> String {
    let env = get_env_variable(env_name);

    if env.is_some() {
        return env.unwrap();
    };

    if args != "" {
        return args.to_string();
    };

    return default.to_string();
}

fn get_log_level(args: &str) -> Option<log::LevelFilter> {
    let log_level = get_value("AR_VERBOSITY", args, "0");

    match log_level.as_str() {
        "1" => Some(log::LevelFilter::Debug),
        "2" => Some(log::LevelFilter::Trace),
        "0" | _ => Some(log::LevelFilter::Info),
    }
}
