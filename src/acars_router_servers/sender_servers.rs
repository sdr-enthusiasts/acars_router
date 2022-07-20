// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

use crate::generics::{reconnect_options, SenderServer, SenderServerConfig, Shared};
use crate::helper_functions::should_start_service;
use crate::tcp_serve_server::TCPServeServer;
use crate::udp_sender_server::UDPSenderServer;
use log::{debug, error};
use serde_json::Value;
use std::sync::Arc;
use stubborn_io::StubbornTcpStream;
use tmq::{publish, Context};
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, Mutex};

pub async fn start_sender_servers(
    config: &SenderServerConfig,
    rx_processed: Receiver<Value>,
    server_type: String,
) {
    // Optional variables to store the output sockets.
    // Using optionals because the queue of messages can only have one "owner" at a time.
    // So we need to separate out the sockets from watching the queue.
    // Flow is check and see if there are any configured outputs for the queue
    // If so, start it up and save it to the appropriate server variables.
    // Then start watchers for the input queue

    // TODO: This seems like a stupid way of doing the UDP output. Why did I do it this way?
    // Why can't it be done in the same way as the other outputs?

    let mut udp_server: Option<UDPSenderServer> = None;
    let sender_servers: Arc<Mutex<Vec<Sender<Value>>>> = Arc::new(Mutex::new(Vec::new()));

    if should_start_service(config.send_udp()) {
        // Start the UDP sender servers for {server_type}
        udp_server = start_udp_senders_servers(server_type.clone(), config.send_udp()).await;
    }

    if should_start_service(config.send_tcp()) {
        for host in config.send_tcp() {
            let new_state = Arc::clone(&sender_servers);
            let hostname = host.clone();
            let s_type = server_type.clone();
            tokio::spawn(async move { start_tcp(hostname, s_type, new_state).await });
        }
    }

    if should_start_service(config.serve_tcp()) {
        // Start the TCP servers for {server_type}

        for host in config.serve_tcp() {
            let hostname = "0.0.0.0:".to_string() + host.as_str();
            let socket = TcpListener::bind(hostname.clone()).await;

            match socket {
                Ok(socket) => {
                    let (tx_processed_acars, rx_processed_acars) = mpsc::channel(32);
                    let tcp_sender_server = TCPServeServer {
                        socket: socket,
                        proto_name: server_type.clone() + " " + hostname.as_str(),
                    };
                    let new_state = Arc::clone(&sender_servers);
                    new_state.lock().await.push(tx_processed_acars.clone());
                    let state = Arc::new(Mutex::new(Shared::new()));
                    tokio::spawn(async move {
                        tcp_sender_server
                            .watch_for_connections(rx_processed_acars, state)
                            .await;
                    });
                }
                Err(e) => {
                    error!(
                        "[TCP SERVE {server_type}]: Error connecting to {}: {}",
                        host, e
                    );
                }
            }
        }
    }

    if should_start_service(config.serve_zmq()) {
        // Start the ZMQ sender servers for {server_type}
        for port in config.serve_zmq() {
            let server_address = "tcp://127.0.0.1:".to_string() + &port;
            let name = "ZMQ_SENDER_SERVER_{server_type}_".to_string() + &port;
            let socket = publish(&Context::new()).bind(&server_address);
            let new_state = Arc::clone(&sender_servers);
            let (tx_processed_acars, rx_processed_acars) = mpsc::channel(32);
            match socket {
                Ok(socket) => {
                    let zmq_sender_server = SenderServer {
                        host: server_address.clone(),
                        proto_name: name.clone(),
                        socket: socket,
                        channel: rx_processed_acars,
                    };
                    new_state.lock().await.push(tx_processed_acars);
                    tokio::spawn(async move {
                        zmq_sender_server.send_message().await;
                    });
                }
                Err(e) => {
                    error!(
                        "Error starting ZMQ {server_type} server on port {}: {}",
                        port, e
                    );
                }
            }
        }
    }

    let monitor_state = Arc::clone(&sender_servers);

    monitor_queue(rx_processed, udp_server, monitor_state, server_type).await;
}

async fn monitor_queue(
    mut rx_processed: mpsc::Receiver<Value>,
    udp_server: Option<UDPSenderServer>,
    sender_servers: Arc<Mutex<Vec<Sender<Value>>>>,
    name: String,
) {
    debug!("Starting the {name} Output Queue");
    while let Some(message) = rx_processed.recv().await {
        debug!("[CHANNEL SENDER {name}] Message received in the output queue. Sending to {name} clients");
        match udp_server {
            Some(ref vdlm_udp_server) => {
                vdlm_udp_server.send_message(message.clone()).await;
            }
            None => (),
        }

        for sender_server in sender_servers.lock().await.iter() {
            match sender_server.send(message.clone()).await {
                Ok(_) => debug!("[CHANNEL SENDER {name}] Successfully sent the {name} message"),
                Err(e) => {
                    error!("[CHANNEL SENDER {name}]: Error sending message: {}", e);
                }
            }
        }
    }
}

async fn start_udp_senders_servers(
    decoder_type: String,
    ports: &Vec<String>,
) -> Option<UDPSenderServer> {
    // Create an ephermeal socket for the UDP sender server
    let socket = UdpSocket::bind("0.0.0.0:0".to_string()).await;

    // Verify the socket was bound correctly
    match socket {
        Ok(s) => {
            return Some(UDPSenderServer {
                proto_name: decoder_type.to_string() + "_UDP_SEND",
                host: ports.clone(),
                socket: s,
            });
        } // valid socket, move on
        Err(e) => {
            // socket did not bind, return None. We don't want the program to think it has a socket to work with
            error!("{} failed to create socket: {:?}", decoder_type, e);
            return None;
        }
    }
}

async fn start_tcp(
    host: String,
    socket_type: String,
    sender_server: Arc<Mutex<Vec<Sender<Value>>>>,
) {
    // Start a TCP sender server for {server_type}
    let socket = StubbornTcpStream::connect_with_options(host.clone(), reconnect_options()).await;
    match socket {
        Ok(socket) => {
            let (tx_processed_acars, rx_processed_acars) = mpsc::channel(32);
            let tcp_sender_server = SenderServer {
                host: host.clone(),
                proto_name: socket_type.clone(),
                socket: socket,
                channel: rx_processed_acars,
            };
            sender_server.lock().await.push(tx_processed_acars);
            tokio::spawn(async move {
                tcp_sender_server.send_message().await;
            });
        }
        Err(e) => {
            error!(
                "[TCP SENDER {socket_type}]: Error connecting to {}: {}",
                host, e
            );
        }
    }
}
