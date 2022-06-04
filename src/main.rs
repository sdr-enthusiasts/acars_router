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
use std::net::ToSocketAddrs;
use tokio::net::unix::SocketAddr;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
// use tokio::time::{sleep, Duration};

#[path = "./config_options.rs"]
mod config_options;
#[path = "./data_processing/message_handler.rs"]
mod message_handler;
#[path = "./acars_router_servers/udp/udp_listener_server.rs"]
mod udp_listener_server;
#[path = "./acars_router_servers/udp/udp_sender_server.rs"]
mod udp_sender_server;
use config_options::ACARSRouterSettings;
use message_handler::{watch_received_message_queue, MessageHandlerConfig};
use udp_listener_server::UDPListenerServer;
use udp_sender_server::UDPSenderServer;

fn exit_process(code: i32) {
    std::process::exit(code);
}

fn start_udp_listener_servers(
    decoder_type: &String,
    ports: &Vec<String>,
    channel: Sender<serde_json::Value>,
) {
    for udp_port in ports {
        // TODO: Move santiy checker of input values to another place...one before we start the servers
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
    let mut udp_outputs = vec![];
    // Veryify the input is valid
    for udp_port in ports {
        // split the udp port into host and port and grab the second field
        let udp_port_split: Vec<&str> = udp_port.split(":").collect();
        // verify port is numeric
        match udp_port_split[1].chars().all(char::is_numeric) {
            true => trace!(
                "{} UDP Port is numeric. Found: {}",
                decoder_type,
                udp_port_split[1]
            ),
            false => {
                error!(
                    "{} UDP Send Port is not numeric. Found: {}",
                    decoder_type, udp_port
                );
                exit_process(12);
            }
        }
    }

    let internal_addr = "0.0.0.0:65000".to_string();
    trace!("{}", internal_addr);
    let sock = UdpSocket::bind(internal_addr)
        .await
        // create an empty socket
        .unwrap();

    // connect the socket using processed_socket

    //sock.connect(udp_port).await.unwrap();

    let server: UDPSenderServer = UDPSenderServer {
        proto_name: decoder_type.to_string() + "_UDP_SEND_ACARS",
        host: ports.clone(),
        socket: sock,
    };

    udp_outputs.push(server);

    tokio::spawn(async move {
        while let Some(message) = channel.recv().await {
            for udp_output in &udp_outputs {
                trace!("Sending to output: {}", udp_output.proto_name);
                udp_output.send_message(message.clone()).await;
            }
        }
    });
}

async fn start_processes() {
    let config: ACARSRouterSettings = ACARSRouterSettings::load_values();
    let message_handler_config = MessageHandlerConfig {
        add_proxy_id: config.add_proxy_id,
        dedupe: config.dedupe,
        dedupe_window: config.dedupe_window,
        skew_window: config.skew_window,
    };

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
    let (tx_recevers, rx_receivers) = mpsc::channel(32);
    // Create the input channel processed messages will be sent to
    let (tx_processed, rx_processed) = mpsc::channel(32);

    // Start the UDP listener servers
    start_udp_listener_servers(
        &"ACARS".to_string(),
        config.listen_udp_acars(),
        tx_recevers.clone(),
    );
    start_udp_listener_servers(
        &"VDLM".to_string(),
        config.listen_udp_vdlm2(),
        tx_recevers.clone(),
    );

    // Start the UDP sender servers
    start_udp_senders_servers(&"ACARS".to_string(), config.send_udp_acars(), rx_processed).await;

    // Start the message handler task.
    watch_received_message_queue(rx_receivers, tx_processed, &message_handler_config).await;

    // TODO: Is this the best way of doing this?
    // Without sleeping and waiting the entire program exits immediately.
    // For reasons
    // TODO: Part Duex....apparently when I moved this to a function outside of
    // main it now doesn't require a sleep loop. Weird.

    // trace!("Starting the sleep loop");

    // loop {
    //     sleep(Duration::from_millis(100)).await;
    // }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    start_processes().await;
    Ok(())
}
