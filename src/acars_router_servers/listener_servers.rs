// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

// NOTE: This is a sender. WE **PUB** to a *SUB* socket.

use crate::generics::OutputServerConfig;
use crate::helper_functions::should_start_service;
use crate::tcp_listener_server::TCPListenerServer;
use crate::tcp_receiver_server::TCPReceiverServer;
use crate::udp_listener_server::UDPListenerServer;
use crate::zmq_listener_server::ZMQListnerServer;
use log::{debug, error, info};
use tokio::sync::mpsc::Sender;

pub fn start_listener_servers(
    config: OutputServerConfig,
    tx_receivers: Sender<serde_json::Value>,
    server_type: String,
) {
    // Start the UDP listener servers

    // Make sure we have at least one UDP port to listen on
    if should_start_service(config.listen_udp()) {
        // Start the UDP listener servers for server_type
        info!("Starting UDP listener servers for {server_type}");
        start_udp_listener_servers(
            &server_type,
            config.listen_udp(),
            tx_receivers.clone(),
            config.reassembly_window().clone(),
        );
    }

    // Start the TCP listeners

    if should_start_service(config.listen_tcp()) {
        // Start the TCP listener servers for server_type
        info!("Starting TCP listener servers for {server_type}");
        start_tcp_listener_servers(&server_type, config.listen_tcp(), tx_receivers.clone());
    }

    // Start the ZMQ listeners

    if should_start_service(config.receive_zmq()) {
        // Start the ZMQ listener servers for {server_type}
        info!("Starting ZMQ Receiver servers for {server_type}");
        start_zmq_listener_servers(&server_type, config.receive_zmq(), tx_receivers.clone());
    }

    if should_start_service(config.receive_tcp()) {
        info!("Starting TCP Receiver servers for {server_type}");
        start_tcp_receiver_servers(
            &server_type,
            config.receive_tcp(),
            tx_receivers.clone(),
            config.reassembly_window().clone(),
        );
    }
}

fn start_zmq_listener_servers(
    decoder_type: &String,
    hosts: &Vec<String>,
    channel: Sender<serde_json::Value>,
) {
    for host in hosts {
        let new_channel = channel.clone();
        let proto_name = decoder_type.to_string() + "_ZMQ_RECEIVER_" + host;
        let server_host = host.clone();

        tokio::spawn(async move {
            let zmq_listener_server = ZMQListnerServer {
                host: server_host.to_string(),
                proto_name: proto_name.to_string(),
            };
            match zmq_listener_server.run(new_channel).await {
                Ok(_) => debug!("{} connection closed", proto_name),
                Err(e) => error!("{} connection error: {}", proto_name.clone(), e),
            };
        });
    }
}

fn start_tcp_listener_servers(
    decoder_type: &String,
    ports: &Vec<String>,
    channel: Sender<serde_json::Value>,
) {
    for port in ports {
        let new_channel = channel.clone();
        let server_tcp_port = port.clone();
        let proto_name = decoder_type.to_string() + "_TCP_LISTEN_" + &server_tcp_port;
        let server = TCPListenerServer {
            proto_name: proto_name,
        };

        debug!(
            "Starting {} TCP server on {}",
            decoder_type, server_tcp_port
        );
        tokio::spawn(async move { server.run(server_tcp_port, new_channel).await });
    }
}

fn start_udp_listener_servers(
    decoder_type: &String,
    ports: &Vec<String>,
    channel: Sender<serde_json::Value>,
    reassembly_window: u64,
) {
    for udp_port in ports {
        let new_channel = channel.clone();
        let server_udp_port = "0.0.0.0:".to_string() + udp_port.as_str();
        let proto_name = decoder_type.to_string() + "_UDP_LISTEN_" + server_udp_port.as_str();
        let server = UDPListenerServer {
            buf: vec![0; 5000],
            to_send: None,
            proto_name: proto_name,
            reassembly_window: reassembly_window,
        };

        // This starts the server task.
        debug!(
            "Starting {} UDP server on {}",
            decoder_type, server_udp_port
        );
        tokio::spawn(async move { server.run(server_udp_port, new_channel).await });
    }
}

fn start_tcp_receiver_servers(
    decoder_type: &str,
    hosts: &Vec<String>,
    channel: Sender<serde_json::Value>,
    reassembly_window: u64,
) {
    for host in hosts {
        let new_channel = channel.clone();
        let proto_name = decoder_type.to_string() + "_TCP_RECEIVER_" + host;
        let server_host = host.clone();

        tokio::spawn(async move {
            let tcp_receiver_server = TCPReceiverServer {
                host: server_host.to_string(),
                proto_name: proto_name.to_string(),
                reassembly_window: reassembly_window,
            };
            match tcp_receiver_server.run(new_channel).await {
                Ok(_) => debug!("{} connection closed", proto_name),
                Err(e) => error!("{} connection error: {}", proto_name.clone(), e),
            }
        });
    }
}
