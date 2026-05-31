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
use crate::{
    OutputServerConfig, SenderServer, SenderServerConfig, ServerType, Shared, reconnect_options,
};
use acars_config::Input;
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

    if args.acars_configured() {
        let message_handler_config_acars: MessageHandlerConfig =
            MessageHandlerConfig::new(&args, "ACARS");

        let (tx_receivers_acars, rx_receivers_acars) = mpsc::channel(32);
        // Create the input channel processed messages will be sent to
        let (tx_processed_acars, rx_processed_acars) = mpsc::channel(32);

        // start the input servers
        debug!("Starting input servers");
        // start_listener_servers(&config, tx_receivers_acars, tx_receivers_vdlm);
        info!("Starting ACARS input servers");
        let acars_input_config: OutputServerConfig = OutputServerConfig::new(
            args.listen_udp_acars.as_ref(),
            args.listen_tcp_acars.as_ref(),
            args.listen_zmq_acars.as_ref(),
            args.receive_tcp_acars.as_ref(),
            args.receive_zmq_acars.as_ref(),
            args.reassembly_window,
            ServerType::Acars,
        );
        tokio::spawn(async move {
            acars_input_config.start_listeners(tx_receivers_acars);
        });

        info!("Starting ACARS Output Servers");
        let acars_output_config: SenderServerConfig = SenderServerConfig::new(
            args.send_udp_acars.as_ref(),
            args.send_tcp_acars.as_ref(),
            args.serve_tcp_acars.as_ref(),
            args.serve_zmq_acars.as_ref(),
            args.max_udp_packet_size,
            args.udp_dns_cache_seconds,
        );

        tokio::spawn(async move {
            acars_output_config
                .start_senders(rx_processed_acars, "ACARS")
                .await;
        });
        tokio::spawn(async move {
            Box::pin(
                message_handler_config_acars
                    .watch_message_queue(rx_receivers_acars, tx_processed_acars),
            )
            .await;
        });
    } else {
        info!(
            "Not starting the ACARS message handler task. No input and/or output sources specified."
        );
    }

    if args.vdlm_configured() {
        let message_handler_config_vdlm: MessageHandlerConfig =
            MessageHandlerConfig::new(&args, "VDLM");

        // VDLM
        // Create the input channel all receivers will send their data to.
        let (tx_receivers_vdlm, rx_receivers_vdlm) = mpsc::channel(32);
        // Create the input channel processed messages will be sent to
        let (tx_processed_vdlm, rx_processed_vdlm) = mpsc::channel(32);

        let vdlm_input_config: OutputServerConfig = OutputServerConfig::new(
            args.listen_udp_vdlm2.as_ref(),
            args.listen_tcp_vdlm2.as_ref(),
            args.listen_zmq_vdlm2.as_ref(),
            args.receive_tcp_vdlm2.as_ref(),
            args.receive_zmq_vdlm2.as_ref(),
            args.reassembly_window,
            ServerType::Vdlm2,
        );
        tokio::spawn(async move {
            vdlm_input_config.start_listeners(tx_receivers_vdlm);
        });

        info!("Starting VDLM Output Servers");
        let vdlm_output_config: SenderServerConfig = SenderServerConfig::new(
            args.send_udp_vdlm2.as_ref(),
            args.send_tcp_vdlm2.as_ref(),
            args.serve_tcp_vdlm2.as_ref(),
            args.serve_zmq_vdlm2.as_ref(),
            args.max_udp_packet_size,
            args.udp_dns_cache_seconds,
        );

        tokio::spawn(async move {
            vdlm_output_config
                .start_senders(rx_processed_vdlm, "VDLM")
                .await;
        });

        tokio::spawn(async move {
            Box::pin(
                message_handler_config_vdlm
                    .watch_message_queue(rx_receivers_vdlm, tx_processed_vdlm),
            )
            .await;
        });
    } else {
        info!(
            "Not starting the VDLM message handler task. No input and/or output sources specified."
        );
    }

    if args.hfdl_configured() {
        let message_handler_config_hfdl: MessageHandlerConfig =
            MessageHandlerConfig::new(&args, "HFDL");
        // HFDL
        // Create the input channel all receivers will send their data to.
        let (tx_receivers_hfdl, rx_receivers_hfdl) = mpsc::channel(32);
        // Create the input channel processed messages will be sent to
        let (tx_processed_hfdl, rx_processed_hfdl) = mpsc::channel(32);

        let hfdl_input_config: OutputServerConfig = OutputServerConfig::new(
            args.listen_udp_hfdl.as_ref(),
            args.listen_tcp_hfdl.as_ref(),
            args.listen_zmq_hfdl.as_ref(),
            args.receive_tcp_hfdl.as_ref(),
            args.receive_zmq_hfdl.as_ref(),
            args.reassembly_window,
            ServerType::Hfdl,
        );

        tokio::spawn(async move {
            hfdl_input_config.start_listeners(tx_receivers_hfdl);
        });

        info!("Starting HFDL Output Servers");
        let hfdl_output_config: SenderServerConfig = SenderServerConfig::new(
            args.send_udp_hfdl.as_ref(),
            args.send_tcp_hfdl.as_ref(),
            args.serve_tcp_hfdl.as_ref(),
            args.serve_zmq_hfdl.as_ref(),
            args.max_udp_packet_size,
            args.udp_dns_cache_seconds,
        );

        tokio::spawn(async move {
            hfdl_output_config
                .start_senders(rx_processed_hfdl, "HFDL")
                .await;
        });

        tokio::spawn(async move {
            Box::pin(
                message_handler_config_hfdl
                    .watch_message_queue(rx_receivers_hfdl, tx_processed_hfdl),
            )
            .await;
        });
    } else {
        info!(
            "Not starting the HFDL message handler task. No input and/or output sources specified."
        );
    }

    if args.imsl_configured() {
        let message_handler_config_imsl: MessageHandlerConfig =
            MessageHandlerConfig::new(&args, "IMSL");
        // IMSL
        // Create the input channel all receivers will send their data to.
        let (tx_receivers_imsl, rx_receivers_imsl) = mpsc::channel(32);
        // Create the input channel processed messages will be sent to
        let (tx_processed_imsl, rx_processed_imsl) = mpsc::channel(32);

        let imsl_input_config: OutputServerConfig = OutputServerConfig::new(
            args.listen_udp_imsl.as_ref(),
            args.listen_tcp_imsl.as_ref(),
            args.listen_zmq_imsl.as_ref(),
            args.receive_tcp_imsl.as_ref(),
            args.receive_zmq_imsl.as_ref(),
            args.reassembly_window,
            ServerType::Imsl,
        );

        tokio::spawn(async move {
            imsl_input_config.start_listeners(tx_receivers_imsl);
        });

        info!("Starting IMSL Output Servers");
        let imsl_output_config: SenderServerConfig = SenderServerConfig::new(
            args.send_udp_imsl.as_ref(),
            args.send_tcp_imsl.as_ref(),
            args.serve_tcp_imsl.as_ref(),
            args.serve_zmq_imsl.as_ref(),
            args.max_udp_packet_size,
            args.udp_dns_cache_seconds,
        );

        tokio::spawn(async move {
            imsl_output_config
                .start_senders(rx_processed_imsl, "IMSL")
                .await;
        });

        tokio::spawn(async move {
            Box::pin(
                message_handler_config_imsl
                    .watch_message_queue(rx_receivers_imsl, tx_processed_imsl),
            )
            .await;
        });
    } else {
        info!(
            "Not starting the IMSL message handler task. No input and/or output sources specified."
        );
    }

    if args.irdm_configured() {
        let message_handler_config_irdm: MessageHandlerConfig =
            MessageHandlerConfig::new(&args, "IRDM");
        // IRDM
        // Create the input channel all receivers will send their data to.
        let (tx_receivers_irdm, rx_receivers_irdm) = mpsc::channel(32);
        // Create the input channel processed messages will be sent to
        let (tx_processed_irdm, rx_processed_irdm) = mpsc::channel(32);

        let irdm_input_config: OutputServerConfig = OutputServerConfig::new(
            args.listen_udp_irdm.as_ref(),
            args.listen_tcp_irdm.as_ref(),
            args.listen_zmq_irdm.as_ref(),
            args.receive_tcp_irdm.as_ref(),
            args.receive_zmq_irdm.as_ref(),
            args.reassembly_window,
            ServerType::Irdm,
        );

        tokio::spawn(async move {
            irdm_input_config.start_listeners(tx_receivers_irdm);
        });

        info!("Starting IRDM Output Servers");
        let irdm_output_config: SenderServerConfig = SenderServerConfig::new(
            args.send_udp_irdm.as_ref(),
            args.send_tcp_irdm.as_ref(),
            args.serve_tcp_irdm.as_ref(),
            args.serve_zmq_irdm.as_ref(),
            args.max_udp_packet_size,
            args.udp_dns_cache_seconds,
        );

        tokio::spawn(async move {
            irdm_output_config
                .start_senders(rx_processed_irdm, "IRDM")
                .await;
        });

        tokio::spawn(async move {
            Box::pin(
                message_handler_config_irdm
                    .watch_message_queue(rx_receivers_irdm, tx_processed_irdm),
            )
            .await;
        });
    } else {
        info!(
            "Not starting the IRDM message handler task. No input and/or output sources specified."
        );
    }

    trace!("Starting the sleep loop");

    loop {
        sleep(Duration::from_millis(100)).await;
    }
}

impl OutputServerConfig {
    fn new(
        listen_udp: Option<&Vec<u16>>,
        listen_tcp: Option<&Vec<u16>>,
        listen_zmq: Option<&Vec<u16>>,
        receive_tcp: Option<&Vec<String>>,
        receive_zmq: Option<&Vec<String>>,
        reassembly_window: f64,
        output_server_type: ServerType,
    ) -> Self {
        Self {
            listen_udp: listen_udp.cloned(),
            listen_tcp: listen_tcp.cloned(),
            listen_zmq: listen_zmq.cloned(),
            receive_tcp: receive_tcp.cloned(),
            receive_zmq: receive_zmq.cloned(),
            reassembly_window,
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
                self.reassembly_window,
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
                self.reassembly_window,
            );
        }

        if let Some(listen_zmq) = self.listen_zmq {
            // Start the ZMQ listener servers for server_type
            info!(
                "Starting ZMQ listener servers for {}",
                &self.output_server_type.to_string()
            );

            listen_zmq
                .zmq_port_listener(&self.output_server_type.to_string(), tx_receivers.clone());
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
                self.reassembly_window,
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
        reassembly_window: f64,
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
    ) {
        for host in self {
            let new_channel: Sender<String> = channel.clone();
            let proto_name: String = format!("{decoder_type}_TCP_RECEIVER_{host}");
            let server_host: String = host.clone();
            tokio::spawn(async move {
                let tcp_receiver_server: TCPReceiverServer =
                    TCPReceiverServer::new(&server_host, &proto_name, reassembly_window);
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
    fn new(
        send_udp: Option<&Vec<String>>,
        send_tcp: Option<&Vec<String>>,
        serve_tcp: Option<&Vec<u16>>,
        serve_zmq: Option<&Vec<u16>>,
        max_udp_packet_size: u64,
        udp_dns_cache_seconds: f64,
    ) -> Self {
        Self {
            send_udp: send_udp.cloned(),
            send_tcp: send_tcp.cloned(),
            serve_tcp: serve_tcp.cloned(),
            serve_zmq: serve_zmq.cloned(),
            max_udp_packet_size: usize::try_from(max_udp_packet_size).unwrap_or(usize::MAX),
            udp_dns_cache_seconds,
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
                        debug!("[CHANNEL SENDER {name}] Successfully sent the {name} message")
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
