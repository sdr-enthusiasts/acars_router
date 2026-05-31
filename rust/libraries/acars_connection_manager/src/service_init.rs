// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

use crate::message_handler::MessageHandlerConfig;
use crate::tcp_services::{TCPListenerServer, TCPReceiverServer, TCPServeServer};
use crate::udp_services::{UDPListenerServer, UDPSenderServer};
use crate::zmq_services::{ZMQListenerServer, ZMQReceiverServer};
use crate::{OutputServerConfig, SenderServer, SenderServerConfig, Shared, dns, reconnect_options};
use acars_config::{Input, Protocol, ProtocolIo};
use acars_vdlm2_parser::AcarsVdlm2Message;
use async_trait::async_trait;
use log::{debug, error, info, trace};
use sdre_stubborn_io::StubbornTcpStream;
use sdre_stubborn_io::tokio::StubbornIo;
use std::io;
use std::sync::Arc;
use tmq::publish::Publish;
use tmq::{Context, TmqError, publish};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{Mutex, mpsc};
use tokio::time::{Duration, sleep};

/// Nomenclature used in the code.
///
/// Input:
///     Receiver: ACARS router will connect out to a remote host and receive data from it. (TCP/ZMQ)
///     Listener: ACARS router will listen on a port for incoming data or incoming connection
///               based on socket type (TCP/UDP/ZMQ)
/// Output:
///     Sender: ACARS router will connect out to a remote host and send data to it. (TCP/ZMQ)
///     Server: ACARS router will send data to a remote host (UDP) or listen for incoming connection (TCP/ZMQ)
///             and send data to it.
pub async fn start_processes(args: Input) {
    args.print_values();

    // Create the input channel all receivers will send their data to.
    // NOTE: To keep this straight in my head, the "TX" is the RECEIVER server (and needs a channel to TRANSMIT data to)
    // The "RX" is the TRANSMIT server (and needs a channel to RECEIVE data from)

    debug!("Starting the message handler tasks");

    // Build the shared async DNS resolver. Cloning this `Arc` is cheap and we
    // hand a clone to every receiver/sender that performs hostname lookups.
    let resolver = dns::new_shared_resolver();

    for proto in Protocol::ALL {
        let io = args.protocol(proto);
        if !io.is_configured() {
            info!(
                "Not starting the {proto} message handler task. No input and/or output sources specified.",
            );
            continue;
        }
        spawn_protocol_pipeline(proto, &io, &args, Arc::clone(&resolver));
    }

    trace!("Starting the sleep loop");

    loop {
        sleep(Duration::from_millis(100)).await;
    }
}

/// Wire one protocol's listener/handler/sender trio onto the runtime.
fn spawn_protocol_pipeline(
    proto: Protocol,
    io: &ProtocolIo,
    args: &Input,
    resolver: Arc<dns::Resolver>,
) {
    let message_handler_config = MessageHandlerConfig::new(args, proto.label());

    // Receivers TX into rx_receivers; the message handler drains it.
    let (tx_receivers, rx_receivers) = mpsc::channel(32);
    // The message handler TX'es processed messages into rx_processed; senders drain it.
    let (tx_processed, rx_processed) = mpsc::channel(32);

    info!("Starting {proto} input servers");
    let input_config =
        OutputServerConfig::from_io(io, args.reassembly_window, proto, Arc::clone(&resolver));
    tokio::spawn(async move {
        input_config.start_listeners(tx_receivers);
    });

    info!("Starting {proto} Output Servers");
    let output_config = SenderServerConfig::from_io(
        io,
        args.max_udp_packet_size,
        args.udp_dns_cache_seconds,
        resolver,
    );
    let proto_label = proto.label();
    tokio::spawn(async move {
        output_config.start_senders(rx_processed, proto_label).await;
    });
    tokio::spawn(async move {
        Box::pin(message_handler_config.watch_message_queue(rx_receivers, tx_processed)).await;
    });
}

impl OutputServerConfig {
    fn from_io(
        io: &ProtocolIo,
        reassembly_window: f64,
        proto: Protocol,
        resolver: Arc<dns::Resolver>,
    ) -> Self {
        Self {
            listen_udp: nonempty(io.listen_udp.clone()),
            listen_tcp: nonempty(io.listen_tcp.clone()),
            listen_zmq: nonempty(io.listen_zmq.clone()),
            receive_tcp: nonempty(io.receive_tcp.clone()),
            receive_zmq: nonempty(io.receive_zmq.clone()),
            reassembly_window,
            output_server_type: proto,
            resolver: Some(resolver),
        }
    }

    fn start_listeners(self, tx_receivers: Sender<String>) {
        let proto_label = self.output_server_type.label();

        // Start the UDP listener servers.
        if let Some(listen_udp) = self.listen_udp {
            info!("Starting UDP listener servers for {proto_label}");
            listen_udp.udp_port_listener(proto_label, tx_receivers.clone(), self.reassembly_window);
        }

        // Start the TCP listeners.
        if let Some(listen_tcp) = self.listen_tcp {
            info!("Starting TCP listener servers for {proto_label}");
            listen_tcp.tcp_port_listener(proto_label, tx_receivers.clone(), self.reassembly_window);
        }

        if let Some(listen_zmq) = self.listen_zmq {
            info!("Starting ZMQ listener servers for {proto_label}");
            listen_zmq.zmq_port_listener(proto_label, tx_receivers.clone());
        }

        if let Some(receive_zmq) = self.receive_zmq {
            info!("Starting ZMQ Receiver servers for {proto_label}");
            receive_zmq.start_zmq(proto_label, tx_receivers.clone());
        }

        if let Some(receive_tcp) = self.receive_tcp {
            info!("Starting TCP Receiver servers for {proto_label}");
            let resolver = self
                .resolver
                .expect("TCP receivers require a shared DNS resolver");
            receive_tcp.start_tcp_receivers(
                proto_label,
                tx_receivers,
                self.reassembly_window,
                resolver,
            );
        }
    }
}

/// `Some(v)` if `v` is non-empty, otherwise `None`. Mirrors the historical
/// `Option<Vec<_>>` semantics of `OutputServerConfig`/`SenderServerConfig`
/// where "no endpoints configured" was modelled as `None`, not `Some(vec![])`.
fn nonempty<T>(v: Vec<T>) -> Option<Vec<T>> {
    (!v.is_empty()).then_some(v)
}

trait StartHostListeners {
    fn start_zmq(self, decoder_type: &str, channel: Sender<String>);
    fn start_tcp_receivers(
        self,
        decoder_type: &str,
        channel: Sender<String>,
        reassembly_window: f64,
        resolver: Arc<dns::Resolver>,
    );
}

trait StartPortListener {
    fn tcp_port_listener(self, decoder_type: &str, channel: Sender<String>, reassembly_window: f64);
    fn udp_port_listener(self, decoder_type: &str, channel: Sender<String>, reassembly_window: f64);
    fn zmq_port_listener(self, decoder_type: &str, channel: Sender<String>);
}

impl StartHostListeners for Vec<String> {
    fn start_zmq(self, decoder_type: &str, channel: Sender<String>) {
        for host in self {
            let new_channel: Sender<String> = channel.clone();
            let proto_name: String = format!("{decoder_type}_ZMQ_RECEIVER_{host}");

            tokio::spawn(async move {
                let zmq_listener_server = ZMQReceiverServer {
                    host: host.clone(),
                    proto_name: proto_name.clone(),
                };
                match zmq_listener_server.run(new_channel).await {
                    Ok(()) => debug!("{proto_name} connection closed"),
                    Err(e) => error!("{} connection error: {:?}", proto_name.clone(), e),
                }
            });
        }
    }

    fn start_tcp_receivers(
        self,
        decoder_type: &str,
        channel: Sender<String>,
        reassembly_window: f64,
        resolver: Arc<dns::Resolver>,
    ) {
        for host in self {
            let new_channel: Sender<String> = channel.clone();
            let proto_name: String = format!("{decoder_type}_TCP_RECEIVER_{host}");
            let server_host: String = host.clone();
            let resolver = Arc::clone(&resolver);
            tokio::spawn(async move {
                let tcp_receiver_server: TCPReceiverServer =
                    TCPReceiverServer::new(&server_host, &proto_name, reassembly_window, resolver);
                match Box::pin(tcp_receiver_server.run(new_channel)).await {
                    Ok(()) => debug!("{proto_name} connection closed"),
                    Err(e) => error!("{proto_name} connection error: {e}"),
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
        reassembly_window: f64,
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
        reassembly_window: f64,
    ) {
        for udp_port in self {
            let new_channel: Sender<String> = channel.clone();
            let server_udp_port: String = format!("0.0.0.0:{udp_port}");
            let proto_name: String = format!("{}_UDP_LISTEN_{}", decoder_type, &server_udp_port);
            let server: UDPListenerServer = UDPListenerServer::new(&proto_name, reassembly_window);
            debug!("Starting {decoder_type} UDP server on {server_udp_port}");
            tokio::spawn(async move { Box::pin(server.run(&server_udp_port, new_channel)).await });
        }
    }

    fn zmq_port_listener(self, decoder_type: &str, channel: Sender<String>) {
        for zmq_port in self {
            let new_channel: Sender<String> = channel.clone();
            let server_zmq_port: String = zmq_port.to_string();
            let proto_name: String = format!("{}_ZMQ_LISTEN_{}", decoder_type, &server_zmq_port);
            let server: ZMQListenerServer = ZMQListenerServer::new(&proto_name);
            debug!("Starting {decoder_type} ZMQ server on {server_zmq_port}");
            tokio::spawn(async move { server.run(server_zmq_port, new_channel).await });
        }
    }
}

impl SenderServerConfig {
    fn from_io(
        io: &ProtocolIo,
        max_udp_packet_size: u64,
        udp_dns_cache_seconds: f64,
        resolver: Arc<dns::Resolver>,
    ) -> Self {
        Self {
            send_udp: nonempty(io.send_udp.clone()),
            send_tcp: nonempty(io.send_tcp.clone()),
            serve_tcp: nonempty(io.serve_tcp.clone()),
            serve_zmq: nonempty(io.serve_zmq.clone()),
            max_udp_packet_size: usize::try_from(max_udp_packet_size).unwrap_or(usize::MAX),
            udp_dns_cache_seconds,
            resolver: Some(resolver),
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
            info!("Starting {server_type} UDP Sender");
            let resolver = self
                .resolver
                .clone()
                .expect("UDP sender requires a shared DNS resolver");
            match UdpSocket::bind("0.0.0.0:0".to_string()).await {
                Err(e) => error!("[{server_type}] Failed to start UDP sender server: {e}"),
                Ok(socket) => {
                    let (tx_processed, rx_processed) = mpsc::channel(32);
                    let udp_sender_server: UDPSenderServer = UDPSenderServer::new(
                        &send_udp,
                        server_type,
                        socket,
                        self.max_udp_packet_size,
                        self.udp_dns_cache_seconds,
                        rx_processed,
                        resolver,
                    );

                    let new_state: Arc<Mutex<Vec<Sender<AcarsVdlm2Message>>>> =
                        Arc::clone(&sender_servers);
                    new_state.lock().await.push(tx_processed.clone());

                    tokio::spawn(async move {
                        udp_sender_server.send_message().await;
                    });
                }
            }
        }

        if let Some(send_tcp) = self.send_tcp {
            for host in send_tcp {
                let new_state: Arc<Mutex<Vec<Sender<AcarsVdlm2Message>>>> =
                    Arc::clone(&sender_servers);
                let s_type: String = server_type.to_string();
                info!("Starting {server_type} TCP Sender {host} ");
                tokio::spawn(async move { new_state.start_tcp(&s_type, &host).await });
            }
        }

        if let Some(serve_tcp) = self.serve_tcp {
            // Start the TCP servers for {server_type}
            for host in serve_tcp {
                let hostname: String = format!("0.0.0.0:{host}");
                let socket: Result<TcpListener, io::Error> = TcpListener::bind(&hostname).await;
                info!("Starting {server_type} TCP Server {hostname} ");
                match socket {
                    Err(e) => error!("[TCP SERVE {server_type}]: Error binding to {host}: {e}"),
                    Ok(socket) => {
                        let (tx_processed, rx_processed) = mpsc::channel(32);

                        let tcp_sender_server: TCPServeServer = TCPServeServer::new(
                            socket,
                            format!("{server_type} {hostname}").as_str(),
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
                info!("Starting {name}");
                match socket {
                    Err(e) => {
                        error!("Error starting ZMQ {server_type} server on port {port}: {e:?}");
                    }
                    Ok(socket) => {
                        let zmq_sender_server: SenderServer<Publish> =
                            SenderServer::new(&server_address, &name, socket, rx_processed);
                        new_state.lock().await.push(tx_processed);
                        tokio::spawn(async move {
                            zmq_sender_server.send_message();
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
        debug!("Starting the {name} Output Queue");
        while let Some(message) = rx_processed.recv().await {
            debug!(
                "[CHANNEL SENDER {name}] Message received in the output queue. Sending to {name} clients"
            );
            for sender_server in self.lock().await.iter() {
                match sender_server.send(message.clone()).await {
                    Ok(()) => {
                        debug!("[CHANNEL SENDER {name}] Successfully sent the {name} message");
                    }
                    Err(e) => error!("[CHANNEL SENDER {name}]: Error sending message: {e}"),
                }
            }
        }
    }

    async fn start_tcp(self, socket_type: &str, host: &str) {
        // Start a TCP sender server for {server_type}
        let socket: Result<StubbornIo<TcpStream, String>, io::Error> =
            StubbornTcpStream::connect_with_options(host.to_string(), reconnect_options(host))
                .await;
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
                    tcp_sender_server.send_message();
                });
            }
        }
    }
}
