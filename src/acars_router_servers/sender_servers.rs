// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

use crate::config_options::ACARSRouterSettings;
use crate::helper_functions::should_start_service;
use crate::tcp_sender_server::TCPSenderServer;
use crate::udp_sender_server::UDPSenderServer;
use crate::zmq_sender_server::ZMQSenderServer;
use log::{error, trace};
use serde_json::Value;
//use std::sync::{Arc, Mutex};
use tmq::{publish, Context};

use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt;
use std::sync::Arc;
use stubborn_io::tokio::StubbornIo;
use stubborn_io::StubbornTcpStream;
use tokio::net::TcpStream;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};

pub async fn start_sender_servers(
    config: &ACARSRouterSettings,
    mut rx_processed_acars: Receiver<Value>,
    mut rx_processed_vdlm: Receiver<Value>,
) {
    // Optional variables to store the output sockets.
    // Using optionals because the queue of messages can only have one "owner" at a time.
    // So we need to separate out the sockets from watching the queue.
    // Flow is check and see if there are any configured outputs for the queue
    // If so, start it up and save it to the appropriate server variables.
    // Then start watchers for the ACARS and VDLM queue, and once a message comes in that needs to be transmitted
    // Check if each type of server exists, and if so, send the message to it.

    let mut acars_udp_server: Option<UDPSenderServer> = None;
    let mut vdlm_udp_server: Option<UDPSenderServer> = None;
    let mut acars_tcp_sender_servers: Vec<Sender<Value>> = Vec::new();
    let mut vdlm_tcp_sender_servers: Vec<Sender<Value>> = Vec::new();
    let mut acars_zmq_publish_server: Vec<ZMQSenderServer> = Vec::new();
    let mut vdlm_zmq_publish_server: Vec<ZMQSenderServer> = Vec::new();

    if should_start_service(config.send_udp_acars()) {
        // Start the UDP sender servers for ACARS
        acars_udp_server =
            start_udp_senders_servers(&"ACARS".to_string(), config.send_udp_acars()).await;
    } else {
        trace!("No ACARS UDP ports to send on. Skipping");
    }

    if should_start_service(config.send_udp_vdlm2()) {
        // Start the UDP sender servers for VDLM
        vdlm_udp_server =
            start_udp_senders_servers(&"VDLM2".to_string(), config.send_udp_vdlm2()).await;
    } else {
        trace!("No VDLM2 UDP ports to send on. Skipping");
    }

    if should_start_service(config.send_tcp_acars()) {
        // Start the TCP sender servers for ACARS
        for host in config.send_tcp_acars() {
            let socket = StubbornTcpStream::connect(host.clone()).await;
            match socket {
                Ok(socket) => {
                    let (tx_processed_acars, rx_processed_acars) = mpsc::channel(32);
                    let tcp_sender_server = TCPSenderServer {
                        host: host.clone(),
                        proto_name: "ACARS".to_string(),
                        socket: socket,
                        channel: rx_processed_acars,
                    };
                    acars_tcp_sender_servers.push(tx_processed_acars);
                    tokio::spawn(async move {
                        tcp_sender_server.send_message().await;
                    });
                }
                Err(e) => {
                    error!("[TCP SENDER ACARS]: Error connecting to {}: {}", host, e);
                }
            }
        }
    }

    if should_start_service(config.send_tcp_vdlm2()) {
        // Start the TCP sender servers for VDLM
        for host in config.send_tcp_vdlm2() {
            let socket = StubbornTcpStream::connect(host.clone()).await;
            match socket {
                Ok(socket) => {
                    let (tx_processed_vdlm, rx_processed_vdlm) = mpsc::channel(32);
                    let tcp_sender_server = TCPSenderServer {
                        host: host.clone(),
                        proto_name: "VDLM2".to_string(),
                        socket: socket,
                        channel: rx_processed_vdlm,
                    };
                    vdlm_tcp_sender_servers.push(tx_processed_vdlm);
                    tokio::spawn(async move {
                        tcp_sender_server.send_message().await;
                    });
                }
                Err(e) => {
                    error!("[TCP SENDER VDLM2]: Error connecting to {}: {}", host, e);
                }
            }
        }
    }

    if should_start_service(config.serve_zmq_acars()) {
        // Start the ZMQ sender servers for ACARS
        for port in config.serve_zmq_acars() {
            let server_address = "tcp://127.0.0.1:".to_string() + &port;
            let name = "ZMQ_SENDER_SERVER_ACARS_".to_string() + &port;
            let socket = publish(&Context::new()).bind(&server_address);
            match socket {
                Ok(socket) => {
                    acars_zmq_publish_server.push(ZMQSenderServer {
                        socket: socket,
                        proto_name: name,
                        host: port.to_string(),
                    });
                }
                Err(e) => {
                    error!("Error starting ZMQ ACARS server on port {}: {}", port, e);
                }
            }
        }
    } else {
        trace!("No ACARS ZMQ ports to send on. Skipping");
    }

    trace!("Starting the ACARS Output Queue");

    tokio::spawn(async move {
        while let Some(message) = rx_processed_acars.recv().await {
            match acars_udp_server {
                Some(ref acars_udp_server) => {
                    acars_udp_server.send_message(message.clone()).await;
                }
                None => (),
            }

            for tcp_sender_server in acars_tcp_sender_servers.iter() {
                tcp_sender_server.send(message.clone()).await;
            }

            // for server in acars_zmq_publish_server.iter() {
            //     // lock the server and call send_message
            //     let mut server = server.send_message(message.clone());
            // }
        }
    });

    trace!("Starting the VDLM Output Queue");

    tokio::spawn(async move {
        while let Some(message) = rx_processed_vdlm.recv().await {
            match vdlm_udp_server {
                Some(ref vdlm_udp_server) => {
                    vdlm_udp_server.send_message(message.clone()).await;
                }
                None => (),
            }

            for tcp_sender_server in vdlm_tcp_sender_servers.iter() {
                tcp_sender_server.send(message.clone()).await;
            }

            // for server in vdlm_zmq_publish_server.iter() {
            //     // lock the server and call send_message
            //     let mut server = server.send_message(message.clone());
            // }
        }
    });
}

async fn start_udp_senders_servers(
    decoder_type: &String,
    ports: &Vec<String>,
) -> Option<UDPSenderServer> {
    // Create an ephermeal socket for the UDP sender server
    let socket = UdpSocket::bind("0.0.0.0:0".to_string()).await;

    // Verify the socket was bound correctly

    match &socket {
        Ok(_) => (), // valid socket, move on
        Err(e) => {
            // socket did not bind, return None. We don't want the program to think it has a socket to work with
            error!("{} failed to create socket: {:?}", decoder_type, e);
            return None;
        }
    }

    // We have a valid socket, return it

    return Some(UDPSenderServer {
        proto_name: decoder_type.to_string() + "_UDP_SEND",
        host: ports.clone(),
        socket: socket.unwrap(),
    });
}
