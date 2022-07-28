// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

// NOTE: This is a sender. WE **PUB** to a *SUB* socket.

use crate::generics::OutputServerConfig;
use crate::tcp_listener_server::TCPListenerServer;
use crate::tcp_receiver_server::TCPReceiverServer;
use crate::udp_listener_server::UDPListenerServer;
use crate::zmq_listener_server::ZMQListnerServer;
use serde_json::Value;
use tokio::sync::mpsc::Sender;

pub fn start_listener_servers(
    config: OutputServerConfig,
    tx_receivers: Sender<Value>,
    server_type: &str,
) {
    // Start the UDP listener servers

    // Make sure we have at least one UDP port to listen on
    if let Some(listen_udp) = config.listen_udp {
        // Start the UDP listener servers for server_type
        info!("Starting UDP listener servers for {server_type}");
        start_udp_listener_servers(
            server_type,
            &listen_udp,
            tx_receivers.clone(),
            &config.reassembly_window,
        );
    }

    // Start the TCP listeners

    if let Some(listen_tcp) = config.listen_tcp {
        // Start the TCP listener servers for server_type
        info!("Starting TCP listener servers for {server_type}");
        start_tcp_listener_servers(
            server_type,
            &listen_tcp,
            tx_receivers.clone(),
            &config.reassembly_window,
        );
    }

    // Start the ZMQ listeners

    if let Some(receive_zmq) = config.receive_zmq {
        // Start the ZMQ listener servers for {server_type}
        info!("Starting ZMQ Receiver servers for {server_type}");
        start_zmq_listener_servers(server_type, &receive_zmq, tx_receivers.clone());
    }

    if let Some(receive_tcp) = config.receive_tcp {
        info!("Starting TCP Receiver servers for {server_type}");
        start_tcp_receiver_servers(
            server_type,
            &receive_tcp,
            tx_receivers,
            &config.reassembly_window,
        );
    }
}

fn start_zmq_listener_servers(decoder_type: &str, hosts: &[String], channel: Sender<Value>) {
    let hosts: Vec<String> = hosts.to_vec();
    for host in hosts {
        let new_channel: Sender<Value> = channel.clone();
        let proto_name: String = format!("{}_ZMQ_RECEIVER_{}", decoder_type, host);

        tokio::spawn(async move {
            let zmq_listener_server = ZMQListnerServer {
                host: host.to_string(),
                proto_name: proto_name.to_string(),
            };
            match zmq_listener_server.run(new_channel).await {
                Ok(_) => debug!("{} connection closed", proto_name),
                Err(e) => error!("{} connection error: {:?}", proto_name.clone(), e),
            };
        });
    }
}

fn start_tcp_listener_servers(
    decoder_type: &str,
    ports: &[u16],
    channel: Sender<Value>,
    reassembly_window: &u64,
) {
    for port in ports {
        let new_channel: Sender<Value> = channel.clone();
        let server_tcp_port: String = port.to_string();
        let proto_name: String = format!("{}_TCP_LISTEN_{}", decoder_type, &server_tcp_port);
        let server = TCPListenerServer {
            proto_name,
            reassembly_window: *reassembly_window,
        };

        debug!("Starting {decoder_type} TCP server on {server_tcp_port}");
        tokio::spawn(async move { server.run(server_tcp_port, new_channel).await });
    }
}

fn start_udp_listener_servers(
    decoder_type: &str,
    ports: &[u16],
    channel: Sender<Value>,
    reassembly_window: &u64,
) {
    for udp_port in ports {
        let new_channel: Sender<Value> = channel.clone();
        let server_udp_port: String = format!("0.0.0.0:{}", udp_port);
        let proto_name: String = format!("{}_UDP_LISTEN_{}", decoder_type, &server_udp_port);
        let server = UDPListenerServer {
            buf: vec![0; 5000],
            to_send: None,
            proto_name,
            reassembly_window: *reassembly_window,
        };

        // This starts the server task.
        debug!("Starting {decoder_type} UDP server on {server_udp_port}");
        tokio::spawn(async move { server.run(&server_udp_port, new_channel).await });
    }
}

fn start_tcp_receiver_servers(
    decoder_type: &str,
    hosts: &[String],
    channel: Sender<Value>,
    reassembly_window: &u64,
) {
    for host in hosts {
        let new_channel: Sender<Value> = channel.clone();
        let proto_name: String = format!("{}_TCP_RECEIVER_{}", decoder_type, host);
        let server_host: String = host.to_string();
        let reassembly_window: u64 = *reassembly_window;
        tokio::spawn(async move {
            let tcp_receiver_server = TCPReceiverServer {
                host: server_host.to_string(),
                proto_name: proto_name.to_string(),
                reassembly_window,
            };
            match tcp_receiver_server.run(new_channel).await {
                Ok(_) => debug!("{} connection closed", proto_name),
                Err(e) => error!("{} connection error: {}", proto_name.clone(), e),
            }
        });
    }
}
