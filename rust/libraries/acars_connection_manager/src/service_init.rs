// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

use crate::message_handler::{MessageHandlerConfig, ProcessSocketListenerMessages};
use crate::packet_handler::PacketHandler;
use crate::tcp_services::{process_tcp_sockets, TCPReceiverServer, TCPServeServer};
use crate::udp_services::UDPSenderServer;
use crate::zmq_services::ZMQListnerServer;
use crate::{reconnect_options, OutputServerConfig, SenderServer, SenderServerConfig, ServerType, Shared, SocketListenerServer, SocketType, ReceiverType};
use acars_config::Input;
use acars_vdlm2_parser::AcarsVdlm2Message;
use async_trait::async_trait;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use stubborn_io::tokio::StubbornIo;
use stubborn_io::StubbornTcpStream;
use tmq::publish::Publish;
use tmq::{publish, Context};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::mpsc::{UnboundedSender, UnboundedReceiver};
use tokio::sync::{mpsc, Mutex};
use tokio::time::{sleep, Duration};

pub async fn start_processes(args: Input) {
    args.print_values();
    
    // NOTE: To keep this straight in my head, the "TX" is the RECEIVER server (and needs a channel to TRANSMIT data to)
    // The "RX" is the TRANSMIT server (and needs a channel to RECEIVE data from)

    // START ACARS
    
    match args.acars_configured() {
        false => info!("Not starting the ACARS message handler task as there are no input and/or output sources specified."),
        true => {
            info!("ACARS configuration found, prepping to handle.");
            debug!("Setting up ACARS message handler.");
            debug!("Building configuration.");
            let message_handler_config_acars: MessageHandlerConfig =
                MessageHandlerConfig::new(&args, ServerType::Acars);
            debug!("Creating ACARS message queues.");
            // Ingress queue
            let (tx_receivers_acars, rx_receivers_acars) = mpsc::unbounded_channel();
            // Egress queue
            let (tx_processed_acars, rx_processed_acars) = mpsc::unbounded_channel();
            
            debug!("ACARS message handler configuration and queues done.");
            
            debug!("Building ACARS ingress configuration.");
            let acars_input_config: OutputServerConfig = OutputServerConfig::configure_message_receiver(&args, ServerType::Acars);
    
            debug!("Building ACARS egress configuration.");
            let acars_output_config: SenderServerConfig = SenderServerConfig::configure_message_sender(&args, ServerType::Acars);
    
            info!("Starting ACARS input servers.");
    
            tokio::spawn(async move {
                acars_input_config.start_listeners(tx_receivers_acars);
            });
    
            info!("Starting ACARS Output Servers.");
    
    
            tokio::spawn(async move {
                acars_output_config
                    .start_senders(rx_processed_acars, ServerType::Acars)
                    .await;
            });
    
            tokio::spawn(async move {
                message_handler_config_acars
                    .watch_message_queue(rx_receivers_acars, tx_processed_acars)
                    .await
            });
        }
    }
    
    // END ACARS
    
    // START VDLM
    
    match args.vdlm_configured() {
        false => info!("Not starting the VDLM message handler task as there are no input and/or output sources specified."),
        true => {
            info!("VDLM configuration found, prepping to handle.");
            debug!("Setting up VDLM message handler.");
            debug!("Building configuration.");
            let message_handler_config_vdlm: MessageHandlerConfig =
                MessageHandlerConfig::new(&args, ServerType::Vdlm2);
            debug!("Creating VDLM message queues.");
            // Ingress Queue
            let (tx_receivers_vdlm, rx_receivers_vdlm) = mpsc::unbounded_channel();
            // Egress Queue
            let (tx_processed_vdlm, rx_processed_vdlm) = mpsc::unbounded_channel();
    
            debug!("VDLM message handler configuration and queues done.");
    
            debug!("Building VDLM ingress configuration.");
            let vdlm_input_config: OutputServerConfig = OutputServerConfig::configure_message_receiver(&args, ServerType::Vdlm2);
    
            debug!("Building VDLM egress configuration.");
            let vdlm_output_config: SenderServerConfig = SenderServerConfig::configure_message_sender(&args, ServerType::Vdlm2);
    
            info!("Starting VDLM Input Servers.");
    
            tokio::spawn(async move {
                vdlm_input_config.start_listeners(tx_receivers_vdlm);
            });
    
            info!("Starting VDLM Output Servers.");
    
            tokio::spawn(async move {
                vdlm_output_config
                    .start_senders(rx_processed_vdlm, ServerType::Vdlm2)
                    .await;
            });
    
            tokio::spawn(async move {
                message_handler_config_vdlm
                    .watch_message_queue(rx_receivers_vdlm, tx_processed_vdlm)
                    .await;
            });
        }
    }
    

    // END VDLM
    
    

    
    trace!("Starting the sleep loop");

    loop {
        sleep(Duration::from_millis(100)).await;
    }
}

impl OutputServerConfig {
    
    fn configure_message_receiver(args: &Input, server_type: ServerType) -> Self {
        let mut output_config: Self = Self {
            reassembly_window: args.reassembly_window,
            output_server_type: server_type,
            ..Default::default()
        };
        match server_type {
            ServerType::Acars => {
                output_config.listen_udp = args.listen_udp_acars.clone();
                output_config.listen_tcp = args.listen_tcp_acars.clone();
                output_config.receive_tcp = args.receive_tcp_acars.clone();
                output_config.receive_zmq = args.receive_zmq_acars.clone();
                
            }
            ServerType::Vdlm2 => {
                output_config.listen_udp = args.listen_udp_vdlm2.clone();
                output_config.listen_tcp = args.listen_tcp_vdlm2.clone();
                output_config.receive_tcp = args.receive_tcp_vdlm2.clone();
                output_config.receive_zmq = args.receive_zmq_vdlm2.clone();
            }
        };
        output_config
    }

    fn start_listeners(self, tx_receivers: UnboundedSender<String>) {
        // Start the UDP listener servers

        // Make sure we have at least one UDP port to listen on
        
        trace!("Starting {} UDP listeners.", self.output_server_type);
        // OutputServerConfig::listen(self.listen_udp, self.output_server_type, tx_receivers.clone(), self.reassembly_window, SocketType::Udp);
        self.listen_to_ports(tx_receivers.clone(), SocketType::Udp);

        // Start the TCP listeners

        trace!("Starting {} TCP listeners.", self.output_server_type);
    
        // OutputServerConfig::listen(self.listen_tcp, self.output_server_type, tx_receivers.clone(), self.reassembly_window, SocketType::Tcp);
        self.listen_to_ports(tx_receivers.clone(), SocketType::Tcp);

        // Start the ZMQ listeners
        
        self.receive_on_ports(tx_receivers.clone(), ReceiverType::Zmq);

        // if let Some(receive_zmq) = self.receive_zmq {
        //     // Start the ZMQ listener servers for {server_type}
        //     info!(
        //         "Starting ZMQ Receiver servers for {}",
        //         &self.output_server_type
        //     );
        //     receive_zmq.start_zmq(self.output_server_type, tx_receivers.clone());
        // }
        
        self.receive_on_ports(tx_receivers.clone(), ReceiverType::Tcp);
    }
    
    fn listen_to_ports(&self, channel: UnboundedSender<String>, socket_type: SocketType) {
        let Some(ports) = (match socket_type {
            SocketType::Tcp => &self.listen_tcp,
            SocketType::Udp => &self.listen_udp
        }) else {
            trace!("Nothing to do for {} {}.", self.output_server_type, socket_type);
            return;
        };
        info!("Starting {} listener server(s) for {}", socket_type, self.output_server_type);
        for port in ports {
            let new_channel: UnboundedSender<String> = channel.clone();
            let server: SocketListenerServer = SocketListenerServer::new(self.output_server_type, *port, self.reassembly_window, socket_type);
            debug!("Starting {} {} server on {}", self.output_server_type, socket_type, port);
            tokio::spawn( async move { server.run(new_channel).await });
        }
    }
    
    fn receive_on_ports(&self, channel: UnboundedSender<String>, receiver_type: ReceiverType) {
        let Some(hosts) = (match receiver_type {
            ReceiverType::Tcp => &self.receive_tcp,
            ReceiverType::Zmq => &self.receive_zmq
        }) else {
            trace!("Nothing to do for {} {}", self.output_server_type, receiver_type);
            return;
        };
        info!("Starting {} receiver server(s) for {}", receiver_type, self.output_server_type);
        for host in hosts {
            let receiver_config: ReceiverConfig = match receiver_type {
                ReceiverType::Tcp => ReceiverConfig::Tcp(TCPReceiverServer::new(host, self.output_server_type, self.reassembly_window)),
                ReceiverType::Zmq => ReceiverConfig::Zmq(ZMQListnerServer::new(host, self.output_server_type))
            };
            receiver_config.run(channel.clone());
        }
    }
}

#[derive(Debug, Clone)]
enum ReceiverConfig {
    Tcp(TCPReceiverServer),
    Zmq(ZMQListnerServer)
}

impl ReceiverConfig {
    fn run(self, channel: UnboundedSender<String>) {
        tokio::spawn( async move {
            let logging_identifier: String = self.get_logging_identifier();
            match self {
                ReceiverConfig::Tcp(tcp_config) => {
                    let Err(tcp_error) = tcp_config.run(channel).await else {
                        debug!("{} connection closed", logging_identifier);
                        return;
                    };
                    error!("{} connection error: {}", logging_identifier, tcp_error);
                }
                ReceiverConfig::Zmq(zmq_config) => {
                    let Err(zmq_error) = zmq_config.run(channel).await else {
                        debug!("{} connection closed", logging_identifier);
                        return;
                    };
                    error!("{} connection error: {:?}", logging_identifier, zmq_error);
                }
            };
        });
    }
    
    fn get_logging_identifier(&self) -> String {
        match self {
            ReceiverConfig::Tcp(config) => config.logging_identifier.to_string(),
            ReceiverConfig::Zmq(config) => config.logging_identifier.to_string(),
        }
    }
}

trait StartHostListeners {
    fn start_zmq(self, decoder_type: ServerType, channel: UnboundedSender<String>);
}

impl StartHostListeners for Vec<String> {
    fn start_zmq(self, decoder_type: ServerType, channel: UnboundedSender<String>) {
        for host in self {
            let new_channel: UnboundedSender<String> = channel.clone();
            let proto_name: String = format!("{}_ZMQ_RECEIVER_{}", decoder_type, host);

            tokio::spawn(async move {
                let zmq_listener_server = ZMQListnerServer {
                    host: host.to_string(),
                    proto_name: decoder_type,
                    logging_identifier: proto_name.to_string()
                };
                match zmq_listener_server.run(new_channel).await {
                    Ok(_) => debug!("{} connection closed", proto_name),
                    Err(e) => error!("{} connection error: {:?}", proto_name, e),
                };
            });
        }
    }
}



impl SenderServerConfig {
    
    fn configure_message_sender(args: &Input, server_type: ServerType) -> Self {
        let mut sender_config: Self = Self {
            max_udp_packet_size: args.max_udp_packet_size as usize,
            ..Default::default()
        };
        match server_type {
            ServerType::Acars => {
                sender_config.send_udp = args.send_udp_acars.clone();
                sender_config.send_tcp = args.send_tcp_acars.clone();
                sender_config.serve_tcp = args.serve_tcp_acars.clone();
                sender_config.serve_zmq = args.serve_zmq_acars.clone();
            }
            ServerType::Vdlm2 => {
                sender_config.send_udp = args.send_udp_vdlm2.clone();
                sender_config.send_tcp = args.send_tcp_vdlm2.clone();
                sender_config.serve_tcp = args.serve_tcp_vdlm2.clone();
                sender_config.serve_zmq = args.serve_zmq_vdlm2.clone();
            }
        }
        sender_config
    }

    async fn start_senders(self, rx_processed: UnboundedReceiver<AcarsVdlm2Message>, server_type: ServerType) {
        // Flow is check and see if there are any configured outputs for the queue
        // If so, start it up and save the transmit channel to the list of sender servers.
        // Then start watchers for the input queue

        let sender_servers: Arc<Mutex<Vec<UnboundedSender<AcarsVdlm2Message>>>> =
            Arc::new(Mutex::new(Vec::new()));

        if let Some(send_udp) = self.send_udp {
            // Start the UDP sender servers for {server_type}
            info!("Starting {} UDP Sender", server_type);
            match UdpSocket::bind("0.0.0.0:0".to_string()).await {
                Err(e) => error!("[{}] Failed to start UDP sender server: {}", server_type, e),
                Ok(socket) => {
                    let (tx_processed, rx_processed) = mpsc::unbounded_channel();
                    let udp_sender_server: UDPSenderServer = UDPSenderServer::new(
                        &send_udp,
                        server_type,
                        socket,
                        &self.max_udp_packet_size,
                        rx_processed,
                    );

                    let new_state: Arc<Mutex<Vec<UnboundedSender<AcarsVdlm2Message>>>> =
                        Arc::clone(&sender_servers);
                    new_state.lock().await.push(tx_processed.clone());

                    tokio::spawn(async move {
                        udp_sender_server.send_message().await;
                    });
                }
            };
        }

        if let Some(send_tcp) = self.send_tcp {
            for host in send_tcp {
                let new_state: Arc<Mutex<Vec<UnboundedSender<AcarsVdlm2Message>>>> =
                    Arc::clone(&sender_servers);
                info!("Starting {} TCP Sender {} ", server_type, host);
                tokio::spawn(async move { new_state.start_tcp(server_type, &host).await });
            }
        }

        if let Some(serve_tcp) = self.serve_tcp {
            // Start the TCP servers for {server_type}
            for host in serve_tcp {
                let hostname: String = format!("0.0.0.0:{}", host);
                let socket: Result<TcpListener, io::Error> = TcpListener::bind(&hostname).await;
                info!("Starting {} TCP Server {} ", server_type, hostname);
                match socket {
                    Err(e) => error!("[TCP SERVE {server_type}]: Error binding to {host}: {e}"),
                    Ok(socket) => {
                        let (tx_processed, rx_processed) = mpsc::unbounded_channel();
                        let tcp_sender_server: TCPServeServer = TCPServeServer::new(
                            socket,
                            format!("{} {}", server_type, hostname).as_str(),
                            server_type
                        );
                        let new_state: Arc<Mutex<Vec<UnboundedSender<AcarsVdlm2Message>>>> =
                            Arc::clone(&sender_servers);
                        new_state.lock().await.push(tx_processed.clone());
                        let state: Arc<Mutex<Shared>> = Arc::new(Mutex::new(Shared::new()));
                        tokio::spawn(async move {
                            tcp_sender_server
                                .watch_for_connections(rx_processed, &state)
                                .await;
                        });
                    }
                }
            }
        }

        if let Some(serve_zmq) = self.serve_zmq {
            // Start the ZMQ sender servers for {server_type}
            for port in serve_zmq {
                let server_address: String = format!("tcp://0.0.0.0:{}", &port);
                let name: String = format!("ZMQ_SENDER_SERVER_{}_{}", server_type, &port);
                let new_state: Arc<Mutex<Vec<UnboundedSender<AcarsVdlm2Message>>>> =
                    Arc::clone(&sender_servers);
                let (tx_processed, rx_processed) = mpsc::unbounded_channel();
                info!("Starting {}", name);
                match publish(&Context::new()).bind(&server_address) {
                    Err(e) => error!(
                        "Error starting ZMQ {server_type} server on port {port}: {:?}",
                        e
                    ),
                    Ok(socket) => {
                        let zmq_sender_server: SenderServer<Publish> =
                            SenderServer::new(&server_address, server_type, &name, socket, rx_processed);
                        new_state.lock().await.push(tx_processed);
                        tokio::spawn(async move {
                            zmq_sender_server.send_message().await;
                        });
                    }
                }
            }
        }

        let monitor_state: Arc<Mutex<Vec<UnboundedSender<AcarsVdlm2Message>>>> = Arc::clone(&sender_servers);

        monitor_state.monitor(rx_processed, server_type).await;
    }
}

#[async_trait]
trait SenderServers {
    async fn monitor(self, rx_processed: UnboundedReceiver<AcarsVdlm2Message>, name: ServerType);
    async fn start_tcp(self, server_type: ServerType, host: &str);
}

#[async_trait]
impl SenderServers for Arc<Mutex<Vec<UnboundedSender<AcarsVdlm2Message>>>> {
    async fn monitor(self, mut rx_processed: UnboundedReceiver<AcarsVdlm2Message>, name: ServerType) {
        debug!("Starting the {} Output Queue", name);
        while let Some(message) = rx_processed.recv().await {
            debug!("[CHANNEL SENDER {name}] Message received in the output queue. Sending to {} clients", name);
            for sender_server in self.lock().await.iter() {
                match sender_server.send(message.clone()) {
                    Ok(_) => debug!("[CHANNEL SENDER {name}] Successfully sent the {name} message"),
                    Err(e) => error!("[CHANNEL SENDER {name}]: Error sending message: {}", e),
                }
            }
        }
    }

    async fn start_tcp(self, server_type: ServerType, host: &str) {
        // Start a TCP sender server for {server_type}
        let socket: Result<StubbornIo<TcpStream, String>, io::Error> =
            StubbornTcpStream::connect_with_options(host.to_string(), reconnect_options()).await;
        match socket {
            Err(e) => error!("[TCP SENDER {server_type}]: Error connecting to {host}: {e}"),
            Ok(socket) => {
                let (tx_processed, rx_processed) = mpsc::unbounded_channel();
                let tcp_sender_server = SenderServer {
                    host: host.to_string(),
                    proto_name: server_type,
                    logging_identifier: server_type.to_string(),
                    socket,
                    channel: rx_processed,
                };
                self.lock().await.push(tx_processed);
                tcp_sender_server.send_message().await;
            }
        }
    }
}

// All this below is brand new.
// It's a first pass at unifying the listener logic, and can very likely be made into smaller functions.
// Doing this will help DRY it up more and make it more testable.
impl SocketListenerServer {
    pub(crate) fn new(
        proto_name: ServerType,
        port: u16,
        reassembly_window: f64,
        socket_type: SocketType,
    ) -> Self {
        Self {
            proto_name,
            port,
            reassembly_window,
            socket_type,
        }
    }

    pub(crate) async fn run(
        self,
        channel: UnboundedSender<String>,
    ) {
        
        let Some(socket_address) = build_ip_address(self.port, self.socket_type) else {
            return;
        };
    
        
        match self.socket_type {
            SocketType::Tcp => self.start_tcp(socket_address, &channel).await,
            SocketType::Udp => self.start_udp(socket_address, &channel).await
        }
    }
    
    async fn start_tcp(self, socket_address: SocketAddr, channel: &UnboundedSender<String>) {
        let logging_identifier: String = format!("{}_{}", self.proto_name, socket_address.to_string());
        match TcpListener::bind(socket_address).await {
            Err(tcp_error) => error!("[{} SERVER: {}] Error listening on port: {}",
                                self.socket_type, logging_identifier, tcp_error),
            Ok(listener) => {
                info!("[{} Listener SERVER: {}]: Listening on: {}", self.socket_type, self.proto_name, socket_address);
                loop {
                    trace!("[{} Listener SERVER: {}]: Waiting for connection", self.socket_type, self.proto_name);
                    // Asynchronously wait for an inbound TcpStream.
                    match listener.accept().await {
                        Err(accept_error) => error!("[{} Listener SERVER: {}]: Error accepting connection: {}", self.socket_type, self.proto_name, accept_error),
                        Ok((stream, addr)) => {
                            let new_channel: UnboundedSender<String> = channel.clone();
                            let new_proto_name: String = format!("{}:{}", self.proto_name, addr);
                            info!("[{} Listener SERVER: {}]:accepted connection from {}",
                                                self.socket_type, self.proto_name, addr);
                            // Spawn our handler to be run asynchronously.
                            tokio::spawn(async move {
                                match process_tcp_sockets(
                                    stream, self.proto_name, &new_proto_name,
                                    new_channel, addr, self.reassembly_window,
                                ).await {
                                    Ok(_) => debug!("[{} Listener SERVER: {}] connection closed",
                                                        self.socket_type, new_proto_name),
                                    Err(e) => error!("[{} Listener SERVER: {}] connection error: {}",
                                                        self.socket_type, new_proto_name.clone(), e)
                                }
                            });
                        }
                    }
                }
            }
        }
    }
    
    async fn start_udp(self, socket_address: SocketAddr, channel: &UnboundedSender<String>) {
        let logging_identifier: String = format!("{}_{}", self.proto_name, socket_address.to_string());
        let mut buf: Vec<u8> = vec![0; 5000];
        let mut to_send: Option<(usize, SocketAddr)> = None;
    
        match UdpSocket::bind(socket_address).await {
            Err(socketing_binding_error) => error!("[{} SERVER: {}] Error listening on port: {}",
                                self.socket_type, logging_identifier, socketing_binding_error),
            Ok(socket) => {
                info!("[{} SERVER: {}]: Listening on: {}",
                                    self.socket_type, logging_identifier, socket_address);
                let packet_handler: PacketHandler =
                    PacketHandler::new(&logging_identifier, self.reassembly_window);
            
                loop {
                    if let Some((size, peer)) = to_send {
                        let msg_string: &str = match std::str::from_utf8(
                            buf[..size].as_ref(),
                        ) {
                            Ok(s) => s,
                            Err(_) => {
                                warn!("[{} SERVER: {}] Invalid message received from {}",
                                                    self.socket_type, logging_identifier, peer);
                                continue;
                            }
                        };
                    
                        msg_string.split_terminator('\n')
                                  .collect::<Vec<&str>>()
                                  .process_messages(&packet_handler, &peer, &channel, &logging_identifier,
                                                    self.socket_type, self.proto_name).await;
                    }
                    to_send = match socket.recv_from(&mut buf).await {
                        Ok(data) => Some(data),
                        Err(receive_error) => {
                            error!("[{} SERVER: {}] Error listening on port: {}",
                                                self.socket_type, logging_identifier, receive_error);
                            None
                        }
                    };
                }
            }
        };
    }
}

fn build_ip_address(port: u16, socket_type: SocketType) -> Option<SocketAddr> {
    match IpAddr::from_str("0.0.0.0") {
        Ok(ip_address) => Some(SocketAddr::new(ip_address, port)),
        Err(ip_parse_error) => {
            error!("[{} Receiver Server] Error building socket address: {}",
                    socket_type, ip_parse_error);
            None
        }
        
    }
}