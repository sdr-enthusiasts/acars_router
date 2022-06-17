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
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration};

#[path = "./config_options.rs"]
mod config_options;
#[path = "./data_processing/hasher.rs"]
mod hasher;
#[path = "./data_processing/message_handler.rs"]
mod message_handler;
#[path = "./sanity_checker.rs"]
mod sanity_checker;
#[path = "./acars_router_servers/tcp/tcp_listener_server.rs"]
mod tcp_listener_server;
#[path = "./acars_router_servers/udp/udp_listener_server.rs"]
mod udp_listener_server;
#[path = "./acars_router_servers/udp/udp_sender_server.rs"]
mod udp_sender_server;
#[path = "./acars_router_servers/zmq/zmq_listener_server.rs"]
mod zmq_listener_server;

#[path = "./listener_servers.rs"]
mod listener_servers;

#[path = "./helper_functions.rs"]
mod helper_functions;

use config_options::ACARSRouterSettings;
use helper_functions::{exit_process, should_start_service};
use listener_servers::start_listener_servers;
use message_handler::{watch_received_message_queue, MessageHandlerConfig};
use sanity_checker::check_config_option_sanity;
use udp_sender_server::UDPSenderServer;

async fn start_udp_senders_servers(
    decoder_type: &String,
    ports: &Vec<String>,
    mut channel: Receiver<serde_json::Value>,
) {
    let sock = UdpSocket::bind("0.0.0.0:0".to_string())
        .await
        // create an empty socket
        .unwrap();

    let server: UDPSenderServer = UDPSenderServer {
        proto_name: decoder_type.to_string() + "_UDP_SEND",
        host: ports.clone(),
        socket: sock,
    };
    trace!("Starting {} UDP sender server", decoder_type);
    tokio::spawn(async move {
        while let Some(message) = channel.recv().await {
            server.send_message(message.clone()).await;
        }
    });
}

async fn start_processes() {
    let config: ACARSRouterSettings = ACARSRouterSettings::load_values();
    let log_level = config.log_level();

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
        .filter(None, log_level.clone())
        .init();

    config.print_values();

    match check_config_option_sanity(&config) {
        Ok(_) => {
            info!("Configuration is valid. Starting ACARS Router");
        }
        Err(e) => {
            error!("Configuration is invalid. Exiting ACARS Router");
            error!("{}", e);
            exit_process(12);
        }
    }
    let message_handler_config_acars = MessageHandlerConfig {
        add_proxy_id: config.add_proxy_id,
        dedupe: config.dedupe,
        dedupe_window: config.dedupe_window,
        skew_window: config.skew_window,
        queue_type: "ACARS".to_string(),
    };

    let mut message_handler_config_vdlm = message_handler_config_acars.clone();
    message_handler_config_vdlm.queue_type = "VDLM".to_string();

    // Print the log level out to the user
    info!("Log level: {:?}", config.log_level());

    // ACARS Servers
    // Create the input channel all receivers will send their data to.
    // NOTE: To keep this straight in my head, the "TX" is the RECEIVER server (and needs a channel to TRANSMIT data to)
    // The "RX" is the TRANSMIT server (and needs a channel to RECEIVE data from)

    let (tx_receivers_acars, rx_receivers_acars) = mpsc::channel(32);
    // Create the input channel processed messages will be sent to
    let (tx_processed_acars, rx_processed_acars) = mpsc::channel(32);
    // VDLM
    // Create the input channel all receivers will send their data to.
    let (tx_receivers_vdlm, rx_receivers_vdlm) = mpsc::channel(32);
    // Create the input channel processed messages will be sent to
    let (tx_processed_vdlm, rx_processed_vdlm) = mpsc::channel(32);

    // start the input servers

    start_listener_servers(&config, tx_receivers_acars, tx_receivers_vdlm);

    //TODO: Move the sender service to it's own wrapper to handle all output types
    if should_start_service(config.send_udp_acars()) {
        // Start the UDP sender servers for ACARS
        start_udp_senders_servers(
            &"ACARS".to_string(),
            config.send_udp_acars(),
            rx_processed_acars,
        )
        .await;
    } else {
        trace!("No ACARS UDP ports to send on. Skipping");
    }

    if should_start_service(config.send_udp_vdlm2()) {
        // Start the UDP sender servers for VDLM
        start_udp_senders_servers(
            &"VDLM2".to_string(),
            config.send_udp_vdlm2(),
            rx_processed_vdlm,
        )
        .await;
    } else {
        trace!("No VDLM2 UDP ports to send on. Skipping");
    }

    // Start the message handler tasks.
    tokio::spawn(async move {
        watch_received_message_queue(
            rx_receivers_acars,
            tx_processed_acars,
            &message_handler_config_acars,
        )
        .await
    });

    tokio::spawn(async move {
        watch_received_message_queue(
            rx_receivers_vdlm,
            tx_processed_vdlm,
            &message_handler_config_vdlm,
        )
        .await;
    });

    // TODO: Is this the best way of doing this?
    // Without sleeping and waiting the entire program exits immediately.
    // For reasons

    trace!("Starting the sleep loop");

    loop {
        sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    start_processes().await;
    Ok(())
}
