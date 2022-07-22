// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

// TODO: For UDP and probably TCP, we need to cover off over-sized messages exceeding the buffer size
// TODO: Related to the above, we probably should also track the protocol, port, and address of the sender for all protocols
// This would be pretty useful for message reconstruction as well as general logging
// Something to look at would be https://medium.com/tresorit-engineering/collecting-broadcast-udp-packets-using-async-networking-in-rust-7fd93a631eac
// Basically using the concept of UdpFramed (and maybe TCPFramed as well?) to function as the stream deserializer, and use that to
// Capture "bad" packets for potential reassembly.

#[path = "./config_options.rs"]
mod config_options;
#[path = "./data_processing/hasher.rs"]
mod hasher;
#[path = "./data_processing/message_handler.rs"]
mod message_handler;
#[path = "./sanity_checker.rs"]
mod sanity_checker;
#[path = "./acars_router_servers/tcp/inputs/tcp_listener_server.rs"]
mod tcp_listener_server;
#[path = "./acars_router_servers/udp/inputs/udp_listener_server.rs"]
mod udp_listener_server;
#[path = "./acars_router_servers/udp/outputs/udp_sender_server.rs"]
mod udp_sender_server;
#[path = "./acars_router_servers/zmq/inputs/zmq_listener_server.rs"]
mod zmq_listener_server;

#[path = "./acars_router_servers/listener_servers.rs"]
mod listener_servers;

#[path = "./acars_router_servers/sender_servers.rs"]
mod sender_servers;

#[path = "./helper_functions.rs"]
mod helper_functions;

#[path = "./acars_router_servers/zmq/outputs/zmq_sender_server.rs"]
mod zmq_sender_server;

#[path = "./acars_router_servers/tcp/inputs/tcp_receiver_server.rs"]
mod tcp_receiver_server;

#[path = "./acars_router_servers/tcp/outputs/tcp_sender_server.rs"]
mod tcp_sender_server;

#[path = "./acars_router_servers/tcp/outputs/tcp_serve_server.rs"]
mod tcp_serve_server;

#[path = "./generics.rs"]
mod generics;

#[path = "./data_processing/packet_handler.rs"]
mod packet_handler;

use chrono::Local;
use config_options::ACARSRouterSettings;
use env_logger::Builder;
use generics::{OutputServerConfig, SenderServerConfig};
use helper_functions::exit_process;
use listener_servers::start_listener_servers;
use log::{debug, error, info, trace};
use message_handler::{watch_received_message_queue, MessageHandlerConfig};
use sanity_checker::check_config_option_sanity;
use sender_servers::start_sender_servers;
use std::error::Error;
use std::io::Write;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

async fn start_processes() {
    let config: ACARSRouterSettings = ACARSRouterSettings::load_values();
    let log_level = config.log_level();
    let should_start_acars_watcher =
        config.should_start_acars_inputs() && config.should_start_acars_outputs();
    let should_start_vdlm_watcher =
        config.should_start_vdlm2_inputs() && config.should_start_vdlm2_outputs();

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
        should_override_station_name: config.should_override_station_name,
        station_name: config.override_station_name.clone(),
        stats_every: config.stats_every,
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
    debug!("Starting input servers");
    // start_listener_servers(&config, tx_receivers_acars, tx_receivers_vdlm);

    if config.should_start_acars_inputs() {
        // start the acars input servers
        info!("Starting ACARS input servers");
        let acars_input_config = OutputServerConfig {
            listen_udp: config.listen_udp_acars().clone(),
            listen_tcp: config.listen_tcp_acars().clone(),
            receive_zmq: config.receive_zmq_acars().clone(),
            receive_tcp: config.receive_tcp_acars().clone(),
            reassembly_window: config.reassembly_window().clone(),
        };

        tokio::spawn(async move {
            start_listener_servers(acars_input_config, tx_receivers_acars, "ACARS".to_string());
        });
    } else {
        info!("No valid ACARS input servers configured. Not starting ACARS Input Servers");
    }

    if config.should_start_vdlm2_inputs() {
        // start the vdlm input servers
        info!("Starting VDLM2 input servers");
        let vdlm_input_config = OutputServerConfig {
            listen_udp: config.listen_udp_vdlm2().clone(),
            listen_tcp: config.listen_tcp_vdlm2().clone(),
            receive_zmq: config.receive_zmq_vdlm2().clone(),
            receive_tcp: config.receive_tcp_vdlm2().clone(),
            reassembly_window: config.reassembly_window().clone(),
        };

        tokio::spawn(async move {
            start_listener_servers(vdlm_input_config, tx_receivers_vdlm, "VDLM".to_string());
        });
    } else {
        info!("No valid VDLM2 input servers configured. Not starting VDLM2 Input Servers");
    }

    // start the output servers
    debug!("Starting output servers");
    if config.should_start_acars_outputs() {
        info!("Starting ACARS Output Servers");
        let acars_output_config = SenderServerConfig {
            send_udp: config.send_udp_acars().clone(),
            send_tcp: config.send_tcp_acars().clone(),
            serve_zmq: config.serve_zmq_acars().clone(),
            serve_tcp: config.serve_tcp_acars().clone(),
            max_udp_packet_size: config.max_udp_packet_size().clone(),
        };

        tokio::spawn(async move {
            start_sender_servers(
                &acars_output_config,
                rx_processed_acars,
                "ACARS".to_string(),
            )
            .await;
        });
    } else {
        info!("No valid ACARS outputs configured. Not starting ACARS Output Servers");
    }

    if config.should_start_vdlm2_outputs() {
        info!("Starting VDLM Output Servers");
        let vdlm_output_config = SenderServerConfig {
            send_udp: config.send_udp_vdlm2().clone(),
            send_tcp: config.send_tcp_vdlm2().clone(),
            serve_zmq: config.serve_zmq_vdlm2().clone(),
            serve_tcp: config.serve_tcp_vdlm2().clone(),
            max_udp_packet_size: config.max_udp_packet_size().clone(),
        };

        tokio::spawn(async move {
            start_sender_servers(&vdlm_output_config, rx_processed_vdlm, "VDLM".to_string()).await;
        });
    } else {
        info!("No valid VDLM outputs configured. Not starting VDLM Output Servers");
    }

    // Start the message handler tasks.
    // Don't start the queue watcher UNLESS there is a valid input source AND output source for the message type

    debug!("Starting the message handler tasks");

    if should_start_acars_watcher {
        tokio::spawn(async move {
            watch_received_message_queue(
                rx_receivers_acars,
                tx_processed_acars,
                &message_handler_config_acars,
            )
            .await
        });
    } else {
        info!("Not starting the ACARS message handler task. No input and/or output sources specified.");
    }

    if should_start_vdlm_watcher {
        tokio::spawn(async move {
            watch_received_message_queue(
                rx_receivers_vdlm,
                tx_processed_vdlm,
                &message_handler_config_vdlm,
            )
            .await;
        });
    } else {
        info!(
            "Not starting the VDLM message handler task. No input and/or output sources specified."
        );
    }

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
