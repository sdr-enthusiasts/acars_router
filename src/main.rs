// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

#[macro_use] extern crate log;
extern crate tokio;
extern crate tokio_stream;
extern crate tokio_util;
extern crate futures;
extern crate clap;
extern crate chrono;
extern crate serde;
extern crate serde_json;
extern crate zmq;
extern crate tmq;
extern crate failure;
extern crate stubborn_io;
extern crate env_logger;
extern crate derive_getters;

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
use env_logger::Builder;
use std::error::Error;
use std::io::Write;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use clap::Parser;
use crate::config_options::{Input, SetupLogging};
use crate::generics::{OutputServerConfig, SenderServerConfig};
use crate::listener_servers::start_listener_servers;
use crate::message_handler::{watch_received_message_queue, MessageHandlerConfig};
use crate::sender_servers::start_sender_servers;

async fn start_processes(args: Input) {
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
        .filter(None, args.verbose.set_logging_level())
        .init();

    args.print_values();

    let message_handler_config_acars = MessageHandlerConfig::new(&args, "ACARS");
    let message_handler_config_vdlm = MessageHandlerConfig::new(&args, "VDLM");

    // Print the log level out to the user
    // info!("Log level: {:?}", config.log_level());

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
    info!("Starting ACARS input servers");
    let acars_input_config = OutputServerConfig::new(&args.listen_udp_acars,
                                                     &args.listen_tcp_acars,
                                                     &args.receive_tcp_acars,
                                                     &args.receive_zmq_acars,
                                                     &args.reassembly_window);
    tokio::spawn(async move {
        start_listener_servers(acars_input_config, tx_receivers_acars, "ACARS");
    });

    let vdlm_input_config = OutputServerConfig::new(&args.listen_udp_vdlm2,
                                                    &args.listen_tcp_vdlm2,
                                                    &args.receive_zmq_vdlm2,
                                                    &args.receive_tcp_vdlm2,
                                                    &args.reassembly_window);
    tokio::spawn(async move {
        start_listener_servers(vdlm_input_config, tx_receivers_vdlm, "VDLM");
    });

    // start the output servers
    debug!("Starting output servers");

    info!("Starting ACARS Output Servers");
    let acars_output_config: SenderServerConfig = SenderServerConfig::new(&args.send_udp_acars,
                                                      &args.send_tcp_acars,
                                                      &args.serve_zmq_acars,
                                                      &args.serve_tcp_acars,
                                                      &args.max_udp_packet_size);

    tokio::spawn(async move {
        start_sender_servers(
            acars_output_config,
            rx_processed_acars,
            "ACARS",
        ).await;
    });

    info!("Starting VDLM Output Servers");
    let vdlm_output_config = SenderServerConfig::new(&args.send_udp_vdlm2,
                                                     &args.send_tcp_vdlm2,
                                                     &args.serve_zmq_vdlm2,
                                                     &args.serve_tcp_vdlm2,
                                                     &args.max_udp_packet_size);

    tokio::spawn(async move {
        start_sender_servers(vdlm_output_config, rx_processed_vdlm, "VDLM").await;
    });

    // Start the message handler tasks.
    // Don't start the queue watcher UNLESS there is a valid input source AND output source for the message type

    debug!("Starting the message handler tasks");

    if args.acars_configured() {
        tokio::spawn(async move {
            watch_received_message_queue(
                rx_receivers_acars,
                tx_processed_acars,
                message_handler_config_acars,
            ).await
        });
    } else {
        info!("Not starting the ACARS message handler task. No input and/or output sources specified.");
    }

    if args.vdlm_configured() {
        tokio::spawn(async move {
            watch_received_message_queue(
                rx_receivers_vdlm,
                tx_processed_vdlm,
                message_handler_config_vdlm,
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
    let args: Input = Input::parse();
    start_processes(args).await;
    Ok(())
}
