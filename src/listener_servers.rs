// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

// NOTE: This is a sender. WE **PUB** to a *SUB* socket.

use crate::config_options::ACARSRouterSettings;
use crate::helper_functions::should_start_service;
use crate::tcp_listener_server::TCPListenerServer;
use crate::udp_listener_server::UDPListenerServer;
use crate::zmq_listener_server::ZMQListnerServer;
use log::{debug, error, info, trace};
use tokio::sync::mpsc::Sender;

pub fn start_listener_servers(
    config: &ACARSRouterSettings,
    tx_receivers_acars: Sender<serde_json::Value>,
    tx_receivers_vdlm: Sender<serde_json::Value>,
) {
    // Start the UDP listener servers

    // Make sure we have at least one UDP port to listen on
    if should_start_service(config.listen_udp_acars()) {
        // Start the UDP listener servers for ACARS
        start_udp_listener_servers(
            &"ACARS".to_string(),
            config.listen_udp_acars(),
            tx_receivers_acars.clone(),
        );
    } else {
        trace!("No ACARS UDP ports to listen on. Skipping");
    }

    if should_start_service(config.listen_udp_vdlm2()) {
        // Start the UDP listener servers for VDLM
        start_udp_listener_servers(
            &"VDLM2".to_string(),
            config.listen_udp_vdlm2(),
            tx_receivers_vdlm.clone(),
        );
    } else {
        trace!("No VDLM2 UDP ports to listen on. Skipping");
    }

    // Start the TCP listeners

    if should_start_service(config.listen_tcp_acars()) {
        // Start the TCP listener servers for ACARS
        start_tcp_listener_servers(
            &"ACARS".to_string(),
            config.listen_tcp_acars(),
            tx_receivers_acars.clone(),
        );
    } else {
        trace!("No ACARS TCP ports to listen on. Skipping");
    }

    if should_start_service(config.listen_tcp_vdlm2()) {
        // Start the TCP listener servers for VDLM
        start_tcp_listener_servers(
            &"VDLM2".to_string(),
            config.listen_tcp_vdlm2(),
            tx_receivers_vdlm.clone(),
        );
    } else {
        trace!("No VDLM2 TCP ports to listen on. Skipping");
    }

    // Start the ZMQ listeners

    if should_start_service(config.receive_zmq_vdlm2()) {
        // Start the ZMQ listener servers for ACARS
        start_zmq_listener_servers(
            &"VDLM".to_string(),
            config.receive_zmq_vdlm2(),
            tx_receivers_vdlm.clone(),
        );
    } else {
        trace!("No VDLM ZMQ ports to listen on. Skipping");
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
) {
    for udp_port in ports {
        let new_channel = channel.clone();
        let server_udp_port = "0.0.0.0:".to_string() + udp_port.as_str();
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
