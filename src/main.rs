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
// use tokio::time::{sleep, Duration};

#[path = "./config_options.rs"]
mod config_options;
#[path = "./data_processing/hasher.rs"]
mod hasher;
#[path = "./data_processing/message_handler.rs"]
mod message_handler;
#[path = "./sanity_checker.rs"]
mod sanity_checker;
#[path = "./acars_router_servers/udp/udp_listener_server.rs"]
mod udp_listener_server;
#[path = "./acars_router_servers/udp/udp_sender_server.rs"]
mod udp_sender_server;
use config_options::ACARSRouterSettings;
use message_handler::{watch_received_message_queue, MessageHandlerConfig};
use sanity_checker::check_config_option_sanity;
use udp_listener_server::UDPListenerServer;
use udp_sender_server::UDPSenderServer;

fn exit_process(code: i32) {
    std::process::exit(code);
}

fn should_start_service(config: &Vec<String>) -> bool {
    config.len() > 0 && config[0].len() > 0
}

fn start_udp_listener_servers(
    decoder_type: &String,
    ports: &Vec<String>,
    channel: Sender<serde_json::Value>,
) {
    for udp_port in ports {
        let new_channel = channel.clone();
        let server_udp_port = "127.0.0.1:".to_string() + udp_port.as_str();
        let proto_name = decoder_type.to_string() + "_UDP_LISTEN_" + server_udp_port.as_str();
        let server = UDPListenerServer {
            buf: vec![0; 5000],
            to_send: None,
            proto_name: proto_name,
        };

        // This starts the server task.
        debug!(
            "Starting {} UDP server on {}",
            decoder_type, server_udp_port
        );
        tokio::spawn(async move { server.run(server_udp_port, new_channel).await });
    }
}

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
    let message_handler_config = MessageHandlerConfig {
        add_proxy_id: config.add_proxy_id,
        dedupe: config.dedupe,
        dedupe_window: config.dedupe_window,
        skew_window: config.skew_window,
    };

    // Print the log level out to the user
    info!("Log level: {:?}", config.log_level());

    // ACARS Servers
    // Create the input channel all receivers will send their data to.
    let (tx_recevers_acars, rx_receivers_acars) = mpsc::channel(32);
    // Create the input channel processed messages will be sent to
    let (tx_processed_acars, rx_processed_acars) = mpsc::channel(32);
    // VDLM
    // Create the input channel all receivers will send their data to.
    let (tx_recevers_vdlm, rx_receivers_vdlm) = mpsc::channel(32);
    // Create the input channel processed messages will be sent to
    let (tx_processed_vdlm, rx_processed_vdlm) = mpsc::channel(32);

    // Start the UDP listener servers

    // Make sure we have at least one UDP port to listen on
    if should_start_service(config.listen_udp_acars()) {
        // Start the UDP listener servers for ACARS
        start_udp_listener_servers(
            &"ACARS".to_string(),
            config.listen_udp_acars(),
            tx_recevers_acars.clone(),
        );
    } else {
        trace!("No ACARS UDP ports to listen on. Skipping");
    }

    if should_start_service(config.listen_udp_vdlm2()) {
        // Start the UDP listener servers for VDLM
        start_udp_listener_servers(
            &"VDLM2".to_string(),
            config.listen_udp_vdlm2(),
            tx_recevers_vdlm.clone(),
        );
    } else {
        trace!("No VDLM2 UDP ports to listen on. Skipping");
    }

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
    watch_received_message_queue(
        rx_receivers_acars,
        tx_processed_acars,
        &message_handler_config,
    )
    .await;

    watch_received_message_queue(
        rx_receivers_vdlm,
        tx_processed_vdlm,
        &message_handler_config,
    )
    .await;
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    start_processes().await;
    Ok(())
}
