// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

use chrono::Local;
use env_logger::Builder;
use log::{debug, error, info, trace};
use std::error::Error;
use std::io::Write;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::time::{sleep, Duration};

#[path = "./config_options.rs"]
mod config_options;
#[path = "./data_processing/message_handler.rs"]
mod message_handler;
#[path = "./acars_router_servers/udp/udp_listener_server.rs"]
mod udp_listener_server;
use config_options::ACARSRouterSettings;
use message_handler::watch_message_queue;
use udp_listener_server::UDPListenerServer;

fn exit_process(code: i32) {
    std::process::exit(code);
}

fn start_udp_listener_servers(
    decoder_type: &String,
    ports: &Vec<String>,
    channel: Sender<serde_json::Value>,
) {
    for udp_port in ports {
        match udp_port.chars().all(char::is_numeric) {
            true => trace!("{} UDP Port is numeric. Found: {}", decoder_type, udp_port),
            false => {
                error!(
                    "{} UDP Listen Port is not numeric. Found: {}",
                    decoder_type, udp_port
                );
                exit_process(12);
            }
        }
        let new_channel = channel.clone();
        let server_udp_port = "127.0.0.1:".to_string() + udp_port.as_str();
        let proto_name = decoder_type.to_string() + "_UDP_LISTEN_" + server_udp_port.as_str();
        let server = UDPListenerServer {
            buf: vec![0; 5000],
            to_send: None,
            proto_name: proto_name,
        };

        // // This starts the server task.
        debug!(
            "Starting {} UDP server on {}",
            decoder_type, server_udp_port
        );
        tokio::spawn(async move { server.run(server_udp_port, new_channel).await });
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config: ACARSRouterSettings = ACARSRouterSettings::load_values();

    let log_level = config.log_level().unwrap();
    Builder::new()
        .format(|buf, record| {
            writeln!(
                buf,
                "{} [{}] - {}",
                Local::now().format("%Y-%m-%dT%H:%M:%S"),
                record.level(),
                record.args()
            )
        })
        .filter(None, log_level)
        .init();

    config.print_values();
    // Print the log level out to the user
    info!("Log level: {:?}", config.log_level().unwrap());

    // Create the input channel all receivers will send their data to.
    let (tx, rx) = mpsc::channel(32);

    // Start the UDP listener servers
    start_udp_listener_servers(&"ACARS".to_string(), config.listen_udp_acars(), tx.clone());
    start_udp_listener_servers(&"VDLM".to_string(), config.listen_udp_vdlm2(), tx.clone());

    // Start the message handler task.
    watch_message_queue(rx).await;

    // TODO: Is this the best way of doing this?
    // Without sleeping and waiting the entire program exits immediately.
    // For reasons

    trace!("Starting the sleep loop");

    loop {
        sleep(Duration::from_millis(100)).await;
    }
    Ok(())
}
