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

#[path = "./acars_router_servers/listener_servers.rs"]
mod listener_servers;

#[path = "./acars_router_servers/sender_servers.rs"]
mod sender_servers;

#[path = "./helper_functions.rs"]
mod helper_functions;

// #[path = "./acars_router_servers/zmq/zmq_sender_server.rs"]
// mod zmq_sender_server;

#[path = "./acars_router_servers/tcp/tcp_receiver_server.rs"]
mod tcp_receiver_server;

use config_options::ACARSRouterSettings;
use helper_functions::exit_process;
use listener_servers::start_listener_servers;
use message_handler::{watch_received_message_queue, MessageHandlerConfig};
use sanity_checker::check_config_option_sanity;
use sender_servers::start_sender_servers;

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
    debug!("Starting input servers");
    start_listener_servers(&config, tx_receivers_acars, tx_receivers_vdlm);
    // start the output servers
    debug!("Starting output servers");
    tokio::spawn(async move {
        start_sender_servers(&config, rx_processed_acars, rx_processed_vdlm).await
    });

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
