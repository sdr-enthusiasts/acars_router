// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

use crate::cached_dns_tcp::{CachedDnsTcp, ConnectTarget};
use crate::message_handler::MessageHandlerConfig;
use crate::tcp_services::{TCPListenerServer, TCPReceiverServer, TCPServeServer};
use crate::udp_services::{UDPListenerServer, UDPSenderServer};
use crate::zmq_services::{ZMQListenerServer, ZMQReceiverServer};
use crate::{
    BROADCAST_CAPACITY, OutputServerConfig, SenderServer, SenderServerConfig, dns,
    reconnect_options,
};
use acars_config::{Input, Protocol, ProtocolIo};
use acars_vdlm2_parser::AcarsVdlm2Message;
use sdre_stubborn_io::tokio::StubbornIo;
use std::io;
use std::sync::Arc;
use tmq::publish::Publish;
use tmq::{Context, TmqError, publish};
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::broadcast;
use tokio::sync::mpsc::{self, Sender};
use tokio::task::JoinSet;
use tokio::time::{Duration, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// How long we wait after cancellation for top-level pipeline tasks to drain
/// before we drop them and let the runtime shut down.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

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
pub async fn start_processes(args: Input, shutdown: CancellationToken) {
    args.print_values();

    debug!("Starting the message handler tasks");

    // Build the shared async DNS resolver. Cloning this `Arc` is cheap and we
    // hand a clone to every receiver/sender that performs hostname lookups.
    let resolver = dns::new_shared_resolver();

    // Track the per-protocol pipeline tasks so we can drain them on shutdown.
    // Per-host / per-port / per-peer child spawns inherit cancellation via the
    // CancellationToken; we do not track each one individually.
    let mut pipelines: JoinSet<()> = JoinSet::new();

    for proto in Protocol::ALL {
        let io = args.protocol(proto);
        if !io.is_configured() {
            info!(
                "Not starting the {proto} message handler task. No input and/or output sources specified.",
            );
            continue;
        }
        spawn_protocol_pipeline(
            &mut pipelines,
            proto,
            &io,
            &args,
            Arc::clone(&resolver),
            shutdown.clone(),
        );
    }

    shutdown.cancelled().await;
    info!("Draining {} pipeline task(s)", pipelines.len());

    let drained = timeout(SHUTDOWN_GRACE, async {
        while pipelines.join_next().await.is_some() {}
    })
    .await;
    if drained.is_ok() {
        info!("All pipeline tasks drained cleanly");
    } else {
        warn!(
            "Pipeline tasks did not drain within {SHUTDOWN_GRACE:?}; aborting remaining {} task(s)",
            pipelines.len()
        );
    }
    pipelines.shutdown().await;
}

/// Wire one protocol's listener/handler/sender trio onto the runtime.
fn spawn_protocol_pipeline(
    pipelines: &mut JoinSet<()>,
    proto: Protocol,
    io: &ProtocolIo,
    args: &Input,
    resolver: Arc<dns::Resolver>,
    shutdown: CancellationToken,
) {
    let message_handler_config = MessageHandlerConfig::new(args, proto.label());

    // Receivers TX into rx_receivers; the message handler drains it.
    let (tx_receivers, rx_receivers) = mpsc::channel(32);
    // Per-protocol fan-out from message handler to every configured sender.
    // Each sender calls `.subscribe()` on this in `start_senders`.
    let (tx_processed, _initial_rx) = broadcast::channel(BROADCAST_CAPACITY);

    info!("Starting {proto} input servers");
    let input_config =
        OutputServerConfig::from_io(io, args.reassembly_window, proto, Arc::clone(&resolver));
    let listeners_shutdown = shutdown.clone();
    pipelines.spawn(async move {
        input_config.start_listeners(tx_receivers, listeners_shutdown);
    });

    info!("Starting {proto} Output Servers");
    let output_config = SenderServerConfig::from_io(
        io,
        args.max_udp_packet_size,
        args.udp_dns_cache_seconds,
        resolver,
    );
    let proto_label = proto.label();
    let tx_processed_senders = tx_processed.clone();
    let senders_shutdown = shutdown.clone();
    pipelines.spawn(async move {
        output_config
            .start_senders(tx_processed_senders, proto_label, senders_shutdown)
            .await;
    });
    let handler_shutdown = shutdown;
    pipelines.spawn(async move {
        Box::pin(message_handler_config.watch_message_queue(
            rx_receivers,
            tx_processed,
            handler_shutdown,
        ))
        .await;
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

    fn start_listeners(self, tx_receivers: Sender<String>, shutdown: CancellationToken) {
        let proto_label = self.output_server_type.label();

        // Start the UDP listener servers.
        if let Some(listen_udp) = self.listen_udp {
            info!("Starting UDP listener servers for {proto_label}");
            listen_udp.udp_port_listener(
                proto_label,
                tx_receivers.clone(),
                self.reassembly_window,
                shutdown.clone(),
            );
        }

        // Start the TCP listeners.
        if let Some(listen_tcp) = self.listen_tcp {
            info!("Starting TCP listener servers for {proto_label}");
            listen_tcp.tcp_port_listener(
                proto_label,
                tx_receivers.clone(),
                self.reassembly_window,
                shutdown.clone(),
            );
        }

        if let Some(listen_zmq) = self.listen_zmq {
            info!("Starting ZMQ listener servers for {proto_label}");
            listen_zmq.zmq_port_listener(proto_label, tx_receivers.clone(), shutdown.clone());
        }

        if let Some(receive_zmq) = self.receive_zmq {
            info!("Starting ZMQ Receiver servers for {proto_label}");
            receive_zmq.start_zmq(proto_label, tx_receivers.clone(), shutdown.clone());
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
                shutdown,
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
    fn start_zmq(self, decoder_type: &str, channel: Sender<String>, shutdown: CancellationToken);
    fn start_tcp_receivers(
        self,
        decoder_type: &str,
        channel: Sender<String>,
        reassembly_window: f64,
        resolver: Arc<dns::Resolver>,
        shutdown: CancellationToken,
    );
}

trait StartPortListener {
    fn tcp_port_listener(
        self,
        decoder_type: &str,
        channel: Sender<String>,
        reassembly_window: f64,
        shutdown: CancellationToken,
    );
    fn udp_port_listener(
        self,
        decoder_type: &str,
        channel: Sender<String>,
        reassembly_window: f64,
        shutdown: CancellationToken,
    );
    fn zmq_port_listener(
        self,
        decoder_type: &str,
        channel: Sender<String>,
        shutdown: CancellationToken,
    );
}

impl StartHostListeners for Vec<String> {
    fn start_zmq(self, decoder_type: &str, channel: Sender<String>, shutdown: CancellationToken) {
        for host in self {
            let new_channel: Sender<String> = channel.clone();
            let proto_name: String = format!("{decoder_type}_ZMQ_RECEIVER_{host}");
            let shutdown = shutdown.clone();

            tokio::spawn(async move {
                let zmq_listener_server = ZMQReceiverServer {
                    host: host.clone(),
                    proto_name: proto_name.clone(),
                };
                match zmq_listener_server.run(new_channel, shutdown).await {
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
        shutdown: CancellationToken,
    ) {
        for host in self {
            let new_channel: Sender<String> = channel.clone();
            let proto_name: String = format!("{decoder_type}_TCP_RECEIVER_{host}");
            let server_host: String = host.clone();
            let resolver = Arc::clone(&resolver);
            let shutdown = shutdown.clone();
            tokio::spawn(async move {
                let tcp_receiver_server: TCPReceiverServer =
                    TCPReceiverServer::new(&server_host, &proto_name, reassembly_window, resolver);
                match Box::pin(tcp_receiver_server.run(new_channel, shutdown)).await {
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
        shutdown: CancellationToken,
    ) {
        for port in self {
            let new_channel: Sender<String> = channel.clone();
            let server_tcp_port: String = port.to_string();
            let proto_name: String = format!("{}_TCP_LISTEN_{}", decoder_type, &server_tcp_port);
            let server: TCPListenerServer = TCPListenerServer::new(&proto_name, reassembly_window);
            let shutdown = shutdown.clone();
            debug!("Starting {decoder_type} TCP server on {server_tcp_port}");
            tokio::spawn(async move { server.run(server_tcp_port, new_channel, shutdown).await });
        }
    }

    fn udp_port_listener(
        self,
        decoder_type: &str,
        channel: Sender<String>,
        reassembly_window: f64,
        shutdown: CancellationToken,
    ) {
        for udp_port in self {
            let new_channel: Sender<String> = channel.clone();
            let server_udp_port: String = format!("0.0.0.0:{udp_port}");
            let proto_name: String = format!("{}_UDP_LISTEN_{}", decoder_type, &server_udp_port);
            let server: UDPListenerServer = UDPListenerServer::new(&proto_name, reassembly_window);
            let shutdown = shutdown.clone();
            debug!("Starting {decoder_type} UDP server on {server_udp_port}");
            tokio::spawn(async move {
                Box::pin(server.run(&server_udp_port, new_channel, shutdown)).await
            });
        }
    }

    fn zmq_port_listener(
        self,
        decoder_type: &str,
        channel: Sender<String>,
        shutdown: CancellationToken,
    ) {
        for zmq_port in self {
            let new_channel: Sender<String> = channel.clone();
            let server_zmq_port: String = zmq_port.to_string();
            let proto_name: String = format!("{}_ZMQ_LISTEN_{}", decoder_type, &server_zmq_port);
            let server: ZMQListenerServer = ZMQListenerServer::new(&proto_name);
            let shutdown = shutdown.clone();
            debug!("Starting {decoder_type} ZMQ server on {server_zmq_port}");
            tokio::spawn(async move { server.run(server_zmq_port, new_channel, shutdown).await });
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

    async fn start_senders(
        self,
        tx_processed: broadcast::Sender<AcarsVdlm2Message>,
        server_type: &str,
        shutdown: CancellationToken,
    ) {
        // Each sender owns its own broadcast::Receiver via `subscribe()`.
        // No shared Vec<Sender>, no Mutex held across `.await`.
        if let Some(send_udp) = self.send_udp {
            info!("Starting {server_type} UDP Sender");
            let resolver = self
                .resolver
                .clone()
                .expect("UDP sender requires a shared DNS resolver");
            match UdpSocket::bind("0.0.0.0:0".to_string()).await {
                Err(e) => error!("[{server_type}] Failed to start UDP sender server: {e}"),
                Ok(socket) => {
                    let udp_sender_server: UDPSenderServer = UDPSenderServer::new(
                        &send_udp,
                        server_type,
                        socket,
                        self.max_udp_packet_size,
                        self.udp_dns_cache_seconds,
                        tx_processed.subscribe(),
                        resolver,
                    );
                    tokio::spawn(async move {
                        udp_sender_server.send_message().await;
                    });
                }
            }
        }

        if let Some(send_tcp) = self.send_tcp {
            let resolver = self
                .resolver
                .clone()
                .expect("TCP sender requires a shared DNS resolver");
            for host in send_tcp {
                let s_type: String = server_type.to_string();
                let resolver = Arc::clone(&resolver);
                let rx = tx_processed.subscribe();
                info!("Starting {server_type} TCP Sender {host} ");
                tokio::spawn(async move { start_tcp_sender(&s_type, &host, resolver, rx).await });
            }
        }

        if let Some(serve_tcp) = self.serve_tcp {
            for host in serve_tcp {
                let hostname: String = format!("0.0.0.0:{host}");
                let socket: Result<TcpListener, io::Error> = TcpListener::bind(&hostname).await;
                info!("Starting {server_type} TCP Server {hostname} ");
                match socket {
                    Err(e) => error!("[TCP SERVE {server_type}]: Error binding to {host}: {e}"),
                    Ok(socket) => {
                        let tcp_sender_server: TCPServeServer = TCPServeServer::new(
                            socket,
                            format!("{server_type} {hostname}").as_str(),
                        );
                        let tx = tx_processed.clone();
                        let serve_shutdown = shutdown.clone();
                        tokio::spawn(async move {
                            tcp_sender_server
                                .watch_for_connections(tx, serve_shutdown)
                                .await;
                        });
                    }
                }
            }
        }

        if let Some(serve_zmq) = self.serve_zmq {
            for port in serve_zmq {
                let server_address: String = format!("tcp://0.0.0.0:{}", &port);
                let name: String = format!("ZMQ_SENDER_SERVER_{}_{}", server_type, &port);
                let socket: Result<Publish, TmqError> =
                    publish(&Context::new()).bind(&server_address);
                info!("Starting {name}");
                match socket {
                    Err(e) => {
                        error!("Error starting ZMQ {server_type} server on port {port}: {e:?}");
                    }
                    Ok(socket) => {
                        let zmq_sender_server: SenderServer<Publish> = SenderServer::new(
                            &server_address,
                            &name,
                            socket,
                            tx_processed.subscribe(),
                        );
                        tokio::spawn(async move {
                            zmq_sender_server.send_message();
                        });
                    }
                }
            }
        }

        // The broadcast channel stays alive as long as `tx_processed` is held
        // by the message handler (kept on the heap by its spawned task), so
        // returning here is fine; subscribers continue to drain in their own
        // tasks.
        let _ = tx_processed;
    }
}

async fn start_tcp_sender(
    socket_type: &str,
    host: &str,
    resolver: Arc<dns::Resolver>,
    rx: broadcast::Receiver<AcarsVdlm2Message>,
) {
    let target = ConnectTarget {
        host: Arc::from(host),
        resolver,
    };
    let socket: Result<StubbornIo<CachedDnsTcp>, io::Error> =
        StubbornIo::<CachedDnsTcp>::connect_with_options(target, reconnect_options(host)).await;
    match socket {
        Err(e) => error!("[TCP SENDER {socket_type}]: Error connecting to {host}: {e}"),
        Ok(socket) => {
            let tcp_sender_server = SenderServer {
                host: host.to_string(),
                proto_name: socket_type.to_string(),
                socket,
                channel: rx,
            };
            tcp_sender_server.send_message();
        }
    }
}
