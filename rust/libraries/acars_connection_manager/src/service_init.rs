// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

use crate::message_handler::MessageHandlerConfig;
use crate::packet_handler::{PacketHandler, ProcessAssembly};
use crate::tcp_services::{TCPListenerServer, TCPReceiverServer, TCPServeServer};
use crate::udp_services::{UDPListenerServer, UDPSenderServer};
use crate::zmq_services::ZMQListnerServer;
use crate::{
    reconnect_options, OutputServerConfig, SenderServer, SenderServerConfig, ServerType, Shared,
    SocketListenerServer, SocketType,
};
use acars_config::Input;
use acars_vdlm2_parser::AcarsVdlm2Message;
use async_trait::async_trait;
use std::io;
use std::net::{AddrParseError, IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use stubborn_io::tokio::StubbornIo;
use stubborn_io::StubbornTcpStream;
use tmq::publish::Publish;
use tmq::{publish, Context, TmqError};
use tokio::io::BufReader;
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, Mutex};
use tokio::time::{sleep, Duration};
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec};

pub async fn start_processes(args: Input) {
    args.print_values();

    let message_handler_config_acars: MessageHandlerConfig =
        MessageHandlerConfig::new(&args, "ACARS");
    let message_handler_config_vdlm: MessageHandlerConfig =
        MessageHandlerConfig::new(&args, "VDLM");

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
    let acars_input_config: OutputServerConfig = OutputServerConfig::new(
        &args.listen_udp_acars,
        &args.listen_tcp_acars,
        &args.receive_tcp_acars,
        &args.receive_zmq_acars,
        &args.reassembly_window,
        ServerType::Acars,
    );
    tokio::spawn(async move {
        acars_input_config.start_listeners(tx_receivers_acars);
    });

    let vdlm_input_config: OutputServerConfig = OutputServerConfig::new(
        &args.listen_udp_vdlm2,
        &args.listen_tcp_vdlm2,
        &args.receive_tcp_vdlm2,
        &args.receive_zmq_vdlm2,
        &args.reassembly_window,
        ServerType::Vdlm2,
    );
    tokio::spawn(async move {
        vdlm_input_config.start_listeners(tx_receivers_vdlm);
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
        acars_output_config
            .start_senders(rx_processed_acars, "ACARS")
            .await;
    });

    info!("Starting VDLM Output Servers");
    let vdlm_output_config: SenderServerConfig = SenderServerConfig::new(
        &args.send_udp_vdlm2,
        &args.send_tcp_vdlm2,
        &args.serve_tcp_vdlm2,
        &args.serve_zmq_vdlm2,
        &args.max_udp_packet_size,
    );

    tokio::spawn(async move {
        vdlm_output_config
            .start_senders(rx_processed_vdlm, "VDLM")
            .await;
    });

    // Start the message handler tasks.
    // Don't start the queue watcher UNLESS there is a valid input source AND output source for the message type

    debug!("Starting the message handler tasks");

    if args.acars_configured() {
        tokio::spawn(async move {
            message_handler_config_acars
                .watch_message_queue(rx_receivers_acars, tx_processed_acars)
                .await
        });
    } else {
        info!("Not starting the ACARS message handler task. No input and/or output sources specified.");
    }

    if args.vdlm_configured() {
        tokio::spawn(async move {
            message_handler_config_vdlm
                .watch_message_queue(rx_receivers_vdlm, tx_processed_vdlm)
                .await;
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
        output_server_type: ServerType,
    ) -> Self {
        Self {
            listen_udp: listen_udp.clone(),
            listen_tcp: listen_tcp.clone(),
            receive_tcp: receive_tcp.clone(),
            receive_zmq: receive_zmq.clone(),
            reassembly_window: *reassembly_window,
            output_server_type,
        }
    }

    fn start_listeners(self, tx_receivers: Sender<String>) {
        // Start the UDP listener servers

        // Make sure we have at least one UDP port to listen on
        if let Some(listen_udp) = self.listen_udp {
            // Start the UDP listener servers for server_type
            info!(
                "Starting UDP listener servers for {}",
                &self.output_server_type.to_string()
            );
            listen_udp.udp_port_listener(
                &self.output_server_type.to_string(),
                tx_receivers.clone(),
                &self.reassembly_window,
            );
        }

        // Start the TCP listeners

        if let Some(listen_tcp) = self.listen_tcp {
            // Start the TCP listener servers for server_type
            info!(
                "Starting TCP listener servers for {}",
                &self.output_server_type.to_string()
            );
            listen_tcp.tcp_port_listener(
                &self.output_server_type.to_string(),
                tx_receivers.clone(),
                &self.reassembly_window,
            );
        }

        // Start the ZMQ listeners

        if let Some(receive_zmq) = self.receive_zmq {
            // Start the ZMQ listener servers for {server_type}
            info!(
                "Starting ZMQ Receiver servers for {}",
                &self.output_server_type.to_string()
            );
            receive_zmq.start_zmq(&self.output_server_type.to_string(), tx_receivers.clone());
        }

        if let Some(receive_tcp) = self.receive_tcp {
            info!(
                "Starting TCP Receiver servers for {}",
                &self.output_server_type.to_string()
            );
            receive_tcp.start_tcp_receivers(
                &self.output_server_type.to_string(),
                tx_receivers,
                &self.reassembly_window,
            );
        }
    }
}

trait StartHostListeners {
    fn start_zmq(self, decoder_type: &str, channel: Sender<String>);
    fn start_tcp_receivers(
        self,
        decoder_type: &str,
        channel: Sender<String>,
        reassembly_window: &f64,
    );
}

trait StartPortListener {
    fn tcp_port_listener(
        self,
        decoder_type: &str,
        channel: Sender<String>,
        reassembly_window: &f64,
    );
    fn udp_port_listener(
        self,
        decoder_type: &str,
        channel: Sender<String>,
        reassembly_window: &f64,
    );
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

    fn start_tcp_receivers(
        self,
        decoder_type: &str,
        channel: Sender<String>,
        reassembly_window: &f64,
    ) {
        for host in self {
            let new_channel: Sender<String> = channel.clone();
            let proto_name: String = format!("{}_TCP_RECEIVER_{}", decoder_type, host);
            let server_host: String = host.to_string();
            let reassembly_window: f64 = *reassembly_window;
            tokio::spawn(async move {
                let tcp_receiver_server: TCPReceiverServer =
                    TCPReceiverServer::new(&server_host, &proto_name, reassembly_window);
                match tcp_receiver_server.run(new_channel).await {
                    Ok(_) => debug!("{} connection closed", proto_name),
                    Err(e) => error!("{} connection error: {}", proto_name, e),
                }
            });
        }
    }
}

impl StartPortListener for Vec<u16> {
    fn tcp_port_listener(
        self,
        decoder_type: &str,
        channel: Sender<String>,
        reassembly_window: &f64,
    ) {
        for port in self {
            let new_channel: Sender<String> = channel.clone();
            let server_tcp_port: String = port.to_string();
            let proto_name: String = format!("{}_TCP_LISTEN_{}", decoder_type, &server_tcp_port);
            let server: TCPListenerServer = TCPListenerServer::new(&proto_name, reassembly_window);
            debug!("Starting {decoder_type} TCP server on {server_tcp_port}");
            tokio::spawn(async move { server.run(server_tcp_port, new_channel).await });
        }
    }

    fn udp_port_listener(
        self,
        decoder_type: &str,
        channel: Sender<String>,
        reassembly_window: &f64,
    ) {
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

    async fn start_senders(self, rx_processed: Receiver<AcarsVdlm2Message>, server_type: &str) {
        // Flow is check and see if there are any configured outputs for the queue
        // If so, start it up and save the transmit channel to the list of sender servers.
        // Then start watchers for the input queue

        let sender_servers: Arc<Mutex<Vec<Sender<AcarsVdlm2Message>>>> =
            Arc::new(Mutex::new(Vec::new()));

        if let Some(send_udp) = self.send_udp {
            // Start the UDP sender servers for {server_type}
            info!("Starting {} UDP Sender", server_type);
            match UdpSocket::bind("0.0.0.0:0".to_string()).await {
                Err(e) => error!("[{}] Failed to start UDP sender server: {}", server_type, e),
                Ok(socket) => {
                    let (tx_processed, rx_processed) = mpsc::channel(32);
                    let udp_sender_server: UDPSenderServer = UDPSenderServer::new(
                        &send_udp,
                        server_type,
                        socket,
                        &self.max_udp_packet_size,
                        rx_processed,
                    );

                    let new_state: Arc<Mutex<Vec<Sender<AcarsVdlm2Message>>>> =
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
                let new_state: Arc<Mutex<Vec<Sender<AcarsVdlm2Message>>>> =
                    Arc::clone(&sender_servers);
                let s_type: String = server_type.to_string();
                info!("Starting {} TCP Sender {} ", server_type, host);
                tokio::spawn(async move { new_state.start_tcp(&s_type, &host).await });
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
                        let (tx_processed, rx_processed) = mpsc::channel(32);

                        let tcp_sender_server: TCPServeServer = TCPServeServer::new(
                            socket,
                            format!("{} {}", server_type, hostname).as_str(),
                        );
                        let new_state: Arc<Mutex<Vec<Sender<AcarsVdlm2Message>>>> =
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
                let socket: Result<Publish, TmqError> =
                    publish(&Context::new()).bind(&server_address);
                let new_state: Arc<Mutex<Vec<Sender<AcarsVdlm2Message>>>> =
                    Arc::clone(&sender_servers);
                let (tx_processed, rx_processed) = mpsc::channel(32);
                info!("Starting {}", name);
                match socket {
                    Err(e) => error!(
                        "Error starting ZMQ {server_type} server on port {port}: {:?}",
                        e
                    ),
                    Ok(socket) => {
                        let zmq_sender_server: SenderServer<Publish> =
                            SenderServer::new(&server_address, &name, socket, rx_processed);
                        new_state.lock().await.push(tx_processed);
                        tokio::spawn(async move {
                            zmq_sender_server.send_message().await;
                        });
                    }
                }
            }
        }

        let monitor_state: Arc<Mutex<Vec<Sender<AcarsVdlm2Message>>>> = Arc::clone(&sender_servers);

        monitor_state.monitor(rx_processed, server_type).await;
    }
}

#[async_trait]
trait SenderServers {
    async fn monitor(self, rx_processed: Receiver<AcarsVdlm2Message>, name: &str);
    async fn start_tcp(self, socket_type: &str, host: &str);
}

#[async_trait]
impl SenderServers for Arc<Mutex<Vec<Sender<AcarsVdlm2Message>>>> {
    async fn monitor(self, mut rx_processed: Receiver<AcarsVdlm2Message>, name: &str) {
        debug!("Starting the {} Output Queue", name);
        while let Some(message) = rx_processed.recv().await {
            debug!("[CHANNEL SENDER {name}] Message received in the output queue. Sending to {} clients", name);
            for sender_server in self.lock().await.iter() {
                match sender_server.send(message.clone()).await {
                    Ok(_) => debug!("[CHANNEL SENDER {name}] Successfully sent the {name} message"),
                    Err(e) => error!("[CHANNEL SENDER {name}]: Error sending message: {}", e),
                }
            }
        }
    }

    async fn start_tcp(self, socket_type: &str, host: &str) {
        // Start a TCP sender server for {server_type}
        let socket: Result<StubbornIo<TcpStream, String>, io::Error> =
            StubbornTcpStream::connect_with_options(host.to_string(), reconnect_options()).await;
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

// All this below is brand new, not wired in.
// Needs to be logic checked etc, then wired in.
// It's a first pass at unifying the listener logic, and can very likely be made into smaller functions.
// Doing this will help DRY it up more.
#[allow(dead_code)]
impl SocketListenerServer {
    pub(crate) fn new(
        proto_name: &str,
        port: &u16,
        reassembly_window: &f64,
        socket_type: SocketType,
    ) -> Self {
        Self {
            proto_name: proto_name.to_string(),
            port: *port,
            reassembly_window: *reassembly_window,
            socket_type,
        }
    }

    pub(crate) async fn run(
        self,
        channel: Sender<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let build_ip_address: Result<IpAddr, AddrParseError> = IpAddr::from_str("0.0.0.0");
        let build_socket_address: Result<SocketAddr, AddrParseError> = match build_ip_address {
            Ok(ip_address) => Ok(SocketAddr::new(ip_address, self.port)),
            Err(ip_parse_error) => Err(ip_parse_error),
        };
        match build_socket_address {
            Err(address_error) => Err(address_error.into()),
            Ok(socket_address) => {
                match self.socket_type {
                    SocketType::Tcp => {
                        trace!(
                            "[{} Receiver Server {}] Starting",
                            self.socket_type.to_string(),
                            self.proto_name
                        );
                        let open_stream: io::Result<StubbornIo<TcpStream, SocketAddr>> =
                            StubbornTcpStream::connect_with_options(
                                socket_address,
                                reconnect_options(),
                            )
                            .await;
                        match open_stream {
                            Err(stream_error) => {
                                error!(
                                    "[{} Receiver Server {}] Error connecting to {}: {}",
                                    self.socket_type.to_string(),
                                    self.proto_name,
                                    socket_address.to_string(),
                                    &stream_error
                                );
                                Err(stream_error.into())
                            }
                            Ok(stream) => {
                                let reader: BufReader<StubbornIo<TcpStream, SocketAddr>> =
                                    BufReader::new(stream);
                                let mut lines: Framed<
                                    BufReader<StubbornIo<TcpStream, SocketAddr>>,
                                    LinesCodec,
                                > = Framed::new(reader, LinesCodec::new());
                                let packet_handler: PacketHandler =
                                    PacketHandler::new(&self.proto_name, self.reassembly_window);
                                while let Some(Ok(line)) = lines.next().await {
                                    let split_messages_by_newline: Vec<&str> =
                                        line.split_terminator('\n').collect();
                                    for msg_by_newline in split_messages_by_newline {
                                        let split_messages_by_brackets: Vec<&str> =
                                            msg_by_newline.split_terminator("}{").collect();
                                        if split_messages_by_brackets.len().eq(&1) {
                                            let final_message: String =
                                                split_messages_by_brackets[0].to_string();
                                            packet_handler
                                                .attempt_message_reassembly(
                                                    final_message,
                                                    socket_address,
                                                )
                                                .await
                                                .process_reassembly(
                                                    &self.proto_name,
                                                    &channel,
                                                    &self.socket_type.to_string(),
                                                )
                                                .await;
                                        } else {
                                            for (count, msg_by_brackets) in
                                                split_messages_by_brackets.iter().enumerate()
                                            {
                                                let final_message: String = if count == 0 {
                                                    // First case is the first element, which should only ever need a single closing bracket
                                                    trace!("[{} Receiver Server {}]Multiple messages received in a packet.", self.socket_type.to_string(), self.proto_name);
                                                    format!("{}}}", msg_by_brackets)
                                                } else if count
                                                    == split_messages_by_brackets.len() - 1
                                                {
                                                    // This case is for the last element, which should only ever need a single opening bracket
                                                    trace!("[{} Receiver Server {}] End of a multiple message packet", self.socket_type.to_string(), self.proto_name);
                                                    format!("{{{}", msg_by_brackets)
                                                } else {
                                                    // This case is for any middle elements, which need both an opening and closing bracket
                                                    trace!("[{} Receiver Server {}] Middle of a multiple message packet", self.socket_type.to_string(), self.proto_name);
                                                    format!("{{{}}}", msg_by_brackets)
                                                };
                                                packet_handler
                                                    .attempt_message_reassembly(
                                                        final_message,
                                                        socket_address,
                                                    )
                                                    .await
                                                    .process_reassembly(
                                                        &self.proto_name,
                                                        &channel,
                                                        &self.socket_type.to_string(),
                                                    )
                                                    .await
                                            }
                                        };
                                    }
                                }
                                Ok(())
                            }
                        }
                    }
                    SocketType::Udp => {
                        let mut buf: Vec<u8> = vec![0; 5000];
                        let mut to_send: Option<(usize, SocketAddr)> = None;
                        let build_ip_address: Result<IpAddr, AddrParseError> =
                            IpAddr::from_str("0.0.0.0");
                        let build_socket_address: Result<SocketAddr, AddrParseError> =
                            match build_ip_address {
                                Ok(ip_address) => Ok(SocketAddr::new(ip_address, self.port)),
                                Err(ip_parse_error) => Err(ip_parse_error),
                            };

                        let open_socket: Result<UdpSocket, Box<dyn std::error::Error>> =
                            match build_socket_address {
                                Err(socket_error) => {
                                    error!(
                                        "[{} SERVER: {} ] Error creating socket address: {}",
                                        self.socket_type.to_string(),
                                        self.proto_name,
                                        socket_error
                                    );
                                    Err(socket_error.into())
                                }
                                Ok(socket_address) => {
                                    let init_socket: Result<UdpSocket, io::Error> =
                                        UdpSocket::bind(socket_address).await;
                                    match init_socket {
                                        Err(socket_open_error) => Err(socket_open_error.into()),
                                        Ok(socket) => Ok(socket),
                                    }
                                }
                            };

                        match open_socket {
                            Err(e) => error!(
                                "[{} SERVER: {}] Error listening on port: {}",
                                self.socket_type.to_string(),
                                self.proto_name,
                                e
                            ),
                            Ok(socket) => {
                                info!(
                                    "[{} SERVER: {}]: Listening on: {}",
                                    self.socket_type.to_string(),
                                    self.proto_name,
                                    socket.local_addr()?
                                );
                                let packet_handler: PacketHandler =
                                    PacketHandler::new(&self.proto_name, self.reassembly_window);
                                loop {
                                    if let Some((size, peer)) = to_send {
                                        let msg_string = match std::str::from_utf8(
                                            buf[..size].as_ref(),
                                        ) {
                                            Ok(s) => s,
                                            Err(_) => {
                                                warn!("[{} SERVER: {}] Invalid message received from {}", self.socket_type.to_string(), self.proto_name, peer);
                                                continue;
                                            }
                                        };
                                        let split_messages_by_newline: Vec<&str> =
                                            msg_string.split_terminator('\n').collect();

                                        for msg_by_newline in split_messages_by_newline {
                                            let split_messages_by_brackets: Vec<&str> =
                                                msg_by_newline.split_terminator("}{").collect();
                                            if split_messages_by_brackets.len().eq(&1) {
                                                packet_handler
                                                    .attempt_message_reassembly(
                                                        split_messages_by_brackets[0].to_string(),
                                                        peer,
                                                    )
                                                    .await
                                                    .process_reassembly(
                                                        &self.proto_name,
                                                        &channel,
                                                        &self.socket_type.to_string(),
                                                    )
                                                    .await;
                                            } else {
                                                // We have a message that was split by brackets if the length is greater than one
                                                for (count, msg_by_brackets) in
                                                    split_messages_by_brackets.iter().enumerate()
                                                {
                                                    let final_message: String = if count == 0 {
                                                        // First case is the first element, which should only ever need a single closing bracket
                                                        trace!("[{} SERVER: {}] Multiple messages received in a packet.", self.socket_type.to_string(), self.proto_name);
                                                        format!("{}}}", msg_by_brackets)
                                                    } else if count
                                                        == split_messages_by_brackets.len() - 1
                                                    {
                                                        // This case is for the last element, which should only ever need a single opening bracket
                                                        trace!("[{} SERVER: {}] End of a multiple message packet", self.socket_type.to_string(), self.proto_name);
                                                        format!("{{{}", msg_by_brackets)
                                                    } else {
                                                        // This case is for any middle elements, which need both an opening and closing bracket
                                                        trace!("[{} SERVER: {}] Middle of a multiple message packet", self.socket_type.to_string(), self.proto_name);
                                                        format!("{{{}}}", msg_by_brackets)
                                                    };
                                                    packet_handler
                                                        .attempt_message_reassembly(
                                                            final_message,
                                                            peer,
                                                        )
                                                        .await
                                                        .process_reassembly(
                                                            &self.proto_name,
                                                            &channel,
                                                            &self.socket_type.to_string(),
                                                        )
                                                        .await;
                                                }
                                            }
                                        }
                                    }
                                    to_send = Some(socket.recv_from(&mut buf).await?);
                                }
                            }
                        };
                        Ok(())
                    }
                }
            }
        }
    }
}
