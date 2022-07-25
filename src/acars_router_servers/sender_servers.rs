// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

use crate::generics::{reconnect_options, SenderServer, SenderServerConfig, Shared};
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
    config: SenderServerConfig,
    rx_processed: Receiver<Value>,
    server_type: &str,
) {
    // Flow is check and see if there are any configured outputs for the queue
    // If so, start it up and save the transmit channel to the list of sender servers.
    // Then start watchers for the input queue

    let sender_servers: Arc<Mutex<Vec<Sender<Value>>>> = Arc::new(Mutex::new(Vec::new()));

    if let Some(send_udp) = config.send_udp {
        // Start the UDP sender servers for {server_type}

        match UdpSocket::bind("0.0.0.0:0".to_string()).await {
            Ok(socket) => {
                let (tx_processed, rx_processed) = mpsc::channel(32);
                let udp_sender_server = UDPSenderServer {
                    host: send_udp,
                    proto_name: server_type.to_string(),
                    socket,
                    max_udp_packet_size: config.max_udp_packet_size,
                    channel: rx_processed,
                };

                let new_state = Arc::clone(&sender_servers);
                new_state.lock().await.push(tx_processed.clone());

                tokio::spawn(async move {
                    udp_sender_server.send_message().await;
                });
            }
            Err(e) => {
                error!("[{}] Failed to start UDP sender server: {}", server_type, e);
            }
        };
    }

    if let Some(send_tcp) = config.send_tcp {
        for host in send_tcp {
            let new_state = Arc::clone(&sender_servers);
            let s_type = server_type.to_string();
            tokio::spawn(async move { start_tcp(host.clone(), s_type, new_state).await });
        }
    }

    if let Some(serve_tcp) = config.serve_tcp {
        // Start the TCP servers for {server_type}
        for host in serve_tcp {
            let hostname = format!("0.0.0.0:{}", host);
            let socket = TcpListener::bind(&hostname).await;
            match socket {
                Err(e) => error!("[TCP SERVE {server_type}]: Error connecting to {host}: {e}"),
                Ok(socket) => {
                    let (tx_processed, rx_processed) = mpsc::channel(32);
                    let tcp_sender_server = TCPServeServer {
                        socket,
                        proto_name: format!("{} {}", server_type, hostname),
                    };
                    let new_state = Arc::clone(&sender_servers);
                    new_state.lock().await.push(tx_processed.clone());
                    let state = Arc::new(Mutex::new(Shared::new()));
                    tokio::spawn(async move {
                        tcp_sender_server
                            .watch_for_connections(rx_processed, state)
                            .await;
                    });
                }
            }
        }
    }

    if let Some(serve_zmq) = config.serve_zmq {
        // Start the ZMQ sender servers for {server_type}
        for port in serve_zmq {
            let server_address = format!("tcp://127.0.0.1:{}", &port);
            let name = format!("ZMQ_SENDER_SERVER_{}_{}", server_type, &port);
            let socket = publish(&Context::new()).bind(&server_address);
            let new_state = Arc::clone(&sender_servers);
            let (tx_processed, rx_processed) = mpsc::channel(32);
            match socket {
                Err(e) => error!("Error starting ZMQ {server_type} server on port {port}: {:?}", e),
                Ok(socket) => {
                    let zmq_sender_server = SenderServer {
                        host: server_address,
                        proto_name: name,
                        socket,
                        channel: rx_processed,
                    };
                    new_state.lock().await.push(tx_processed);
                    tokio::spawn(async move {
                        zmq_sender_server.send_message().await;
                    });
                }
            }
        }
    }

    let monitor_state = Arc::clone(&sender_servers);

    monitor_queue(rx_processed, monitor_state, server_type).await;
}

async fn monitor_queue(
    mut rx_processed: mpsc::Receiver<Value>,
    sender_servers: Arc<Mutex<Vec<Sender<Value>>>>,
    name: &str,
) {
    debug!("Starting the {name} Output Queue");
    while let Some(message) = rx_processed.recv().await {
        debug!("[CHANNEL SENDER {name}] Message received in the output queue. Sending to {name} clients");
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

async fn start_tcp(
    host: String,
    socket_type: String,
    sender_server: Arc<Mutex<Vec<Sender<Value>>>>,
) {
    // Start a TCP sender server for {server_type}
    let socket = StubbornTcpStream::connect_with_options(host.clone(), reconnect_options()).await;
    match socket {
        Err(e) => error!("[TCP SENDER {socket_type}]: Error connecting to {host}: {e}"),
        Ok(socket) => {
            let (tx_processed, rx_processed) = mpsc::channel(32);
            let tcp_sender_server = SenderServer {
                host: host.clone(),
                proto_name: socket_type.clone(),
                socket,
                channel: rx_processed,
            };
            sender_server.lock().await.push(tx_processed);
            tokio::spawn(async move {
                tcp_sender_server.send_message().await;
            });
        }
    }
}
