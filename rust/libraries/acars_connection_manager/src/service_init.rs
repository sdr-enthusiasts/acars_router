// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

use std::sync::Arc;
use stubborn_io::StubbornTcpStream;
use tmq::{publish, Context};
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, Mutex};
use tokio::time::{sleep, Duration};
use acars_config::Input;
use async_trait::async_trait;
use crate::tcp_services::{TCPListenerServer, TCPServeServer, TCPReceiverServer};
use crate::udp_services::{UDPListenerServer, UDPSenderServer};
use crate::zmq_services::ZMQListnerServer;
use crate::{reconnect_options, SenderServer, SenderServerConfig, Shared, OutputServerConfig, SocketListenerServer, SocketType};
use crate::message_handler::MessageHandlerConfig;

pub async fn start_processes(args: Input) {
    
    args.print_values();
    
    let message_handler_config_acars: MessageHandlerConfig = MessageHandlerConfig::new(&args, "ACARS");
    let message_handler_config_vdlm: MessageHandlerConfig = MessageHandlerConfig::new(&args, "VDLM");
    
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
    let acars_input_config = OutputServerConfig::new(
        &args.listen_udp_acars,
        &args.listen_tcp_acars,
        &args.receive_tcp_acars,
        &args.receive_zmq_acars,
        &args.reassembly_window,
    );
    tokio::spawn(async move {
        acars_input_config.start_listeners(tx_receivers_acars, "ACARS");
    });
    
    let vdlm_input_config = OutputServerConfig::new(
        &args.listen_udp_vdlm2,
        &args.listen_tcp_vdlm2,
        &args.receive_tcp_vdlm2,
        &args.receive_zmq_vdlm2,
        &args.reassembly_window,
    );
    tokio::spawn(async move {
        vdlm_input_config.start_listeners(tx_receivers_vdlm, "VDLM");
    });
    
    // start the output servers
    debug!("Starting output servers");
    
    info!("Starting ACARS Output Servers");
    let acars_output_config: SenderServerConfig = SenderServerConfig::new(
        &args.send_udp_acars,
        &args.send_tcp_acars,
        &args.serve_tcp_acars,
        &args.serve_zmq_acars,
        &args.max_udp_packet_size,
    );
    
    tokio::spawn(async move {
        acars_output_config.start_senders(rx_processed_acars, "ACARS").await;
    });
    
    info!("Starting VDLM Output Servers");
    let vdlm_output_config = SenderServerConfig::new(
        &args.send_udp_vdlm2,
        &args.send_tcp_vdlm2,
        &args.serve_tcp_vdlm2,
        &args.serve_zmq_vdlm2,
        &args.max_udp_packet_size,
    );
    
    tokio::spawn(async move {
        vdlm_output_config.start_senders(rx_processed_vdlm, "VDLM").await;
    });
    
    // Start the message handler tasks.
    // Don't start the queue watcher UNLESS there is a valid input source AND output source for the message type
    
    debug!("Starting the message handler tasks");
    
    if args.acars_configured() {
        tokio::spawn(async move {
            message_handler_config_acars.watch_message_queue(
                rx_receivers_acars,
                tx_processed_acars).await
        });
    } else {
        info!("Not starting the ACARS message handler task. No input and/or output sources specified.");
    }
    
    if args.vdlm_configured() {
        tokio::spawn(async move {
                message_handler_config_vdlm.watch_message_queue(
                rx_receivers_vdlm,
                tx_processed_vdlm).await;
        });
    } else {
        info!(
            "Not starting the VDLM message handler task. No input and/or output sources specified."
        );
    }
    
    trace!("Starting the sleep loop");
    
    loop {
        sleep(Duration::from_millis(100)).await;
    }
}

impl OutputServerConfig {
    fn new(
        listen_udp: &Option<Vec<u16>>,
        listen_tcp: &Option<Vec<u16>>,
        receive_tcp: &Option<Vec<String>>,
        receive_zmq: &Option<Vec<String>>,
        reassembly_window: &f64,
    ) -> Self {
        Self {
            listen_udp: listen_udp.clone(),
            listen_tcp: listen_tcp.clone(),
            receive_tcp: receive_tcp.clone(),
            receive_zmq: receive_zmq.clone(),
            reassembly_window: *reassembly_window,
        }
    }
    
    fn start_listeners(self, tx_receivers: Sender<String>, server_type: &str) {
        // Start the UDP listener servers
    
        // Make sure we have at least one UDP port to listen on
        if let Some(listen_udp) = self.listen_udp {
            // Start the UDP listener servers for server_type
            info!("Starting UDP listener servers for {server_type}");
            listen_udp.udp_port_listener(server_type, tx_receivers.clone(), &self.reassembly_window);
        }
    
        // Start the TCP listeners
    
        if let Some(listen_tcp) = self.listen_tcp {
            // Start the TCP listener servers for server_type
            info!("Starting TCP listener servers for {server_type}");
            listen_tcp.tcp_port_listener(server_type, tx_receivers.clone(), &self.reassembly_window);
        }
    
        // Start the ZMQ listeners
    
        if let Some(receive_zmq) = self.receive_zmq {
            // Start the ZMQ listener servers for {server_type}
            info!("Starting ZMQ Receiver servers for {server_type}");
            receive_zmq.start_zmq(server_type, tx_receivers.clone());
        }
    
        if let Some(receive_tcp) = self.receive_tcp {
            info!("Starting TCP Receiver servers for {server_type}");
            receive_tcp.start_tcp_receivers(server_type, tx_receivers, &self.reassembly_window);
        }
    }
}

trait StartHostListeners {
    fn start_zmq(self, decoder_type: &str, channel: Sender<String>);
    fn start_tcp_receivers(self, decoder_type: &str, channel: Sender<String>, reassembly_window: &f64);
}

trait StartPortListener {
    fn tcp_port_listener(self, decoder_type: &str, channel: Sender<String>, reassembly_window: &f64);
    fn udp_port_listener(self, decoder_type: &str, channel: Sender<String>, reassembly_window: &f64);
}

impl StartHostListeners for Vec<String> {
    fn start_zmq(self, decoder_type: &str, channel: Sender<String>) {
        for host in self {
            let new_channel: Sender<String> = channel.clone();
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
    
    fn start_tcp_receivers(self, decoder_type: &str, channel: Sender<String>, reassembly_window: &f64) {
        for host in self {
            let new_channel: Sender<String> = channel.clone();
            let proto_name: String = format!("{}_TCP_RECEIVER_{}", decoder_type, host);
            let server_host: String = host.to_string();
            let reassembly_window: f64 = *reassembly_window;
            tokio::spawn(async move {
                let tcp_receiver_server = TCPReceiverServer::new(&server_host, &proto_name, reassembly_window);
                match tcp_receiver_server.run(new_channel).await {
                    Ok(_) => debug!("{} connection closed", proto_name),
                    Err(e) => error!("{} connection error: {}", proto_name, e),
                }
            });
        }
    }
}

impl StartPortListener for Vec<u16> {
    fn tcp_port_listener(self, decoder_type: &str, channel: Sender<String>, reassembly_window: &f64) {
        for port in self {
            let new_channel: Sender<String> = channel.clone();
            let server_tcp_port: String = port.to_string();
            let proto_name: String = format!("{}_TCP_LISTEN_{}", decoder_type, &server_tcp_port);
            let server: TCPListenerServer = TCPListenerServer::new(&proto_name, reassembly_window);
            debug!("Starting {decoder_type} TCP server on {server_tcp_port}");
            tokio::spawn(async move { server.run(server_tcp_port, new_channel).await });
        }
    }
    
    fn udp_port_listener(self, decoder_type: &str, channel: Sender<String>, reassembly_window: &f64) {
        for udp_port in self {
            let new_channel: Sender<String> = channel.clone();
            let server_udp_port: String = format!("0.0.0.0:{}", udp_port);
            let proto_name: String = format!("{}_UDP_LISTEN_{}", decoder_type, &server_udp_port);
            let server: UDPListenerServer = UDPListenerServer::new(&proto_name, reassembly_window);
            debug!("Starting {decoder_type} UDP server on {server_udp_port}");
            tokio::spawn(async move { server.run(&server_udp_port, new_channel).await });
        }
    }
}

impl SenderServerConfig {
    fn new(
        send_udp: &Option<Vec<String>>,
        send_tcp: &Option<Vec<String>>,
        serve_tcp: &Option<Vec<u16>>,
        serve_zmq: &Option<Vec<u16>>,
        max_udp_packet_size: &u64,
    ) -> Self {
        Self {
            send_udp: send_udp.clone(),
            send_tcp: send_tcp.clone(),
            serve_tcp: serve_tcp.clone(),
            serve_zmq: serve_zmq.clone(),
            max_udp_packet_size: *max_udp_packet_size as usize,
        }
    }
    
    async fn start_senders(self, rx_processed: Receiver<String>, server_type: &str) {
        // Flow is check and see if there are any configured outputs for the queue
        // If so, start it up and save the transmit channel to the list of sender servers.
        // Then start watchers for the input queue
        
        let sender_servers: Arc<Mutex<Vec<Sender<String>>>> = Arc::new(Mutex::new(Vec::new()));
        
        if let Some(send_udp) = self.send_udp {
            // Start the UDP sender servers for {server_type}
            
            match UdpSocket::bind("0.0.0.0:0".to_string()).await {
                Err(e) => error!("[{}] Failed to start UDP sender server: {}", server_type, e),
                Ok(socket) => {
                    let (tx_processed, rx_processed) = mpsc::channel(32);
                    let udp_sender_server: UDPSenderServer = UDPSenderServer::new(&send_udp, server_type, socket, &self.max_udp_packet_size, rx_processed);
                    
                    let new_state = Arc::clone(&sender_servers);
                    new_state.lock().await.push(tx_processed.clone());
                    
                    tokio::spawn(async move {
                        udp_sender_server.send_message().await;
                    });
                }
            };
        }
        
        if let Some(send_tcp) = self.send_tcp {
            for host in send_tcp {
                let new_state = Arc::clone(&sender_servers);
                let s_type = server_type.to_string();
                tokio::spawn(async move { new_state.start_tcp(&s_type, &host).await });
            }
        }
        
        if let Some(serve_tcp) = self.serve_tcp {
            // Start the TCP servers for {server_type}
            for host in serve_tcp {
                let hostname = format!("0.0.0.0:{}", host);
                let socket = TcpListener::bind(&hostname).await;
                match socket {
                    Err(e) => error!("[TCP SERVE {server_type}]: Error binding to {host}: {e}"),
                    Ok(socket) => {
                        let (tx_processed, rx_processed) = mpsc::channel(32);
                        
                        let tcp_sender_server = TCPServeServer::new(socket, format!("{} {}", server_type, hostname).as_str());
                        let new_state = Arc::clone(&sender_servers);
                        new_state.lock().await.push(tx_processed.clone());
                        let state = Arc::new(Mutex::new(Shared::new()));
                        tokio::spawn(async move {
                            tcp_sender_server.watch_for_connections(rx_processed, &state).await;
                        });
                    }
                }
            }
        }
        
        if let Some(serve_zmq) = self.serve_zmq {
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
                        let zmq_sender_server = SenderServer::new(&server_address, &name, socket, rx_processed);
                        new_state.lock().await.push(tx_processed);
                        tokio::spawn(async move {
                            zmq_sender_server.send_message().await;
                        });
                    }
                }
            }
        }
        
        let monitor_state = Arc::clone(&sender_servers);
        
        monitor_state.monitor(rx_processed, server_type).await;
    }
}

#[async_trait]
trait SenderServers {
    async fn monitor(self, rx_processed: Receiver<String>, name: &str);
    async fn start_tcp(self, socket_type: &str, host: &str);
}

#[async_trait]
impl SenderServers for Arc<Mutex<Vec<Sender<String>>>> {
    async fn monitor(self, mut rx_processed: Receiver<String>, name: &str) {
        debug!("Starting the {name} Output Queue");
        while let Some(message) = rx_processed.recv().await {
            debug!("[CHANNEL SENDER {name}] Message received in the output queue. Sending to {name} clients");
            for sender_server in self.lock().await.iter() {
                match sender_server.send(message.clone()).await {
                    Ok(_) => debug!("[CHANNEL SENDER {name}] Successfully sent the {name} message"),
                    Err(e) => error!("[CHANNEL SENDER {name}]: Error sending message: {}", e)
                }
            }
        }
    }
    
    async fn start_tcp(self, socket_type: &str, host: &str) {
        // Start a TCP sender server for {server_type}
        let socket = StubbornTcpStream::connect_with_options(host.to_string(), reconnect_options()).await;
        match socket {
            Err(e) => error!("[TCP SENDER {socket_type}]: Error connecting to {host}: {e}"),
            Ok(socket) => {
                let (tx_processed, rx_processed) = mpsc::channel(32);
                let tcp_sender_server = SenderServer {
                    host: host.to_string(),
                    proto_name: socket_type.to_string(),
                    socket,
                    channel: rx_processed,
                };
                self.lock().await.push(tx_processed);
                tokio::spawn(async move {
                    tcp_sender_server.send_message().await;
                });
            }
        }
    }
}

impl SocketListenerServer {
    pub(crate) fn new(proto_name: &str, port: &u16, reassembly_window: &f64, socket_type: SocketType) -> Self {
        Self {
            proto_name: proto_name.to_string(),
            port: *port,
            reassembly_window: *reassembly_window,
            socket_type
        }
    }
    
    
}