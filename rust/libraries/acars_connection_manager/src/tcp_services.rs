// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

//! TCP transport: inbound listeners, outbound receiver clients, and the
//! fan-out serve-on-TCP path.
//!
//! Three server shapes live here:
//!
//! * [`TCPListenerServer`] — binds a port, accepts inbound TCP clients
//!   (typically `acarsdec` / `dumpvdl2`), and feeds parsed messages into
//!   the per-protocol mpsc input channel.
//! * [`TCPReceiverServer`] — *client* role: connects out to a remote
//!   feeder via [`StubbornIo`] + [`CachedDnsTcp`] (reconnect + cached
//!   DNS), then funnels messages into the same input channel.
//! * [`TCPServeServer`] — accepts inbound subscribers and forwards every
//!   processed broadcast message to them; slow peers are dropped (see
//!   [`forward_to_peer`]).
//!
//! All loops cooperatively shut down on a [`CancellationToken`]. The
//! fragment reassembly logic itself lives in [`crate::packet_handler`].

use sdre_stubborn_io::tokio::StubbornIo;
use std::error::Error;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::sync::mpsc::Sender;
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace};

use crate::cached_dns_tcp::{CachedDnsTcp, ConnectTarget};
use crate::packet_handler::{PacketHandler, ProcessAssembly, split_concatenated_json};
use crate::{OutboundMessage, SenderServer, dns, reconnect_options};

/// Accepts inbound TCP clients on a bound port and forwards every parsed
/// message into a per-protocol mpsc input channel. Each accepted peer
/// gets its own [`PacketHandler`] reassembly state via
/// [`process_tcp_sockets`].
pub(crate) struct TCPListenerServer {
    pub(crate) proto_name: String,
    pub(crate) reassembly_window: f64,
}

impl TCPListenerServer {
    pub(crate) fn new(proto_name: &str, reassembly_window: f64) -> Self {
        Self {
            proto_name: proto_name.to_string(),
            reassembly_window,
        }
    }

    #[tracing::instrument(
        name = "tcp_listener",
        skip_all,
        fields(proto = %self.proto_name, port = %listen_acars_udp_port),
    )]
    pub(crate) async fn run(
        self,
        listen_acars_udp_port: String,
        channel: Sender<String>,
        shutdown: CancellationToken,
    ) -> Result<(), io::Error> {
        let listener: TcpListener =
            TcpListener::bind(format!("0.0.0.0:{listen_acars_udp_port}")).await?;
        info!("Listening on: {}", listener.local_addr()?);

        loop {
            trace!("Waiting for connection");
            // Asynchronously wait for an inbound TcpStream, or abort on shutdown.
            let (stream, addr) = tokio::select! {
                () = shutdown.cancelled() => {
                    info!(
                        "shutdown requested; closing accept loop"
                    );
                    return Ok(());
                }
                res = listener.accept() => res?,
            };
            let new_channel = channel.clone();
            let new_proto_name = format!("{}:{}", self.proto_name, addr);
            let peer_shutdown = shutdown.clone();
            info!("accepted connection from {}", addr);
            // Spawn our handler to be run asynchronously.
            tokio::spawn(async move {
                let processed = Box::pin(process_tcp_sockets(
                    stream,
                    &new_proto_name,
                    new_channel,
                    addr,
                    self.reassembly_window,
                    peer_shutdown,
                ))
                .await;
                match processed {
                    Ok(()) => debug!("connection closed"),
                    Err(e) => error!("connection error: {}", e),
                }
            });
        }
    }
}

/// Per-peer reassembly loop. Splits the incoming line on `\n` and `}{`
/// boundaries (some feeders concatenate JSON objects without separators)
/// and pushes each candidate through [`PacketHandler::attempt_message_reassembly`].
async fn process_tcp_sockets(
    stream: TcpStream,
    proto_name: &str,
    channel: Sender<String>,
    peer: SocketAddr,
    reassembly_window: f64,
    shutdown: CancellationToken,
) -> Result<(), Box<dyn Error>> {
    let mut lines = Framed::new(stream, LinesCodec::new_with_max_length(8000));

    let packet_handler = PacketHandler::new(proto_name, "TCP", reassembly_window);

    loop {
        let line = tokio::select! {
            () = shutdown.cancelled() => return Ok(()),
            next = lines.next() => match next {
                Some(Ok(line)) => line,
                Some(Err(_)) | None => return Ok(()),
            },
        };
        for final_message in split_concatenated_json(&line) {
            packet_handler
                .attempt_message_reassembly(final_message, peer)
                .await
                .process_reassembly(proto_name, &channel, "TCP")
                .await;
        }
    }
}

/// Outbound TCP client: connects to a remote feeder.
///
/// Uses [`StubbornIo`] + [`CachedDnsTcp`] so the connection survives
/// feeder restarts and DNS churn. Resolved address is computed once
/// up-front to give the [`PacketHandler`] a stable peer key; the
/// reconnect path re-resolves independently.
pub struct TCPReceiverServer {
    pub host: String,
    pub proto_name: String,
    pub reassembly_window: f64,
    pub resolver: Arc<dns::Resolver>,
}

impl TCPReceiverServer {
    pub(crate) fn new(
        server_host: &str,
        proto_name: &str,
        reassembly_window: f64,
        resolver: Arc<dns::Resolver>,
    ) -> Self {
        Self {
            host: server_host.to_string(),
            proto_name: proto_name.to_string(),
            reassembly_window,
            resolver,
        }
    }

    #[tracing::instrument(
        name = "tcp_receiver",
        skip_all,
        fields(proto = %self.proto_name, host = %self.host),
    )]
    pub async fn run(
        self,
        channel: Sender<String>,
        shutdown: CancellationToken,
    ) -> Result<(), Box<dyn Error>> {
        trace!("Starting");
        // Resolve once up-front so we can attribute reassembly to a stable
        // peer address; `CachedDnsTcp` will re-resolve on every reconnect.
        let addr = match dns::resolve_host_port(&self.resolver, &self.host).await {
            Ok(addr) => addr,
            Err(e) => {
                error!("Error resolving host `{}`: {}", self.host, e);
                return Ok(());
            }
        };

        let target = ConnectTarget {
            host: Arc::from(self.host.as_str()),
            resolver: Arc::clone(&self.resolver),
        };
        let stream = match StubbornIo::<CachedDnsTcp>::connect_with_options(
            target,
            reconnect_options(&self.proto_name),
        )
        .await
        {
            Ok(stream) => stream,
            Err(e) => {
                error!("Error connecting to {}: {}", self.host, e);
                Err(e)?
            }
        };

        // TCP keepalive is applied inside `CachedDnsTcp::establish`, so it
        // survives reconnects (configuration applied here via `Deref` would be
        // silently lost on reconnect).

        // create a buffered reader and send the messages to the channel

        let reader = tokio::io::BufReader::new(stream);
        let mut lines = Framed::new(reader, LinesCodec::new());
        let packet_handler = PacketHandler::new(&self.proto_name, "TCP", self.reassembly_window);

        loop {
            let line = tokio::select! {
                () = shutdown.cancelled() => break,
                next = lines.next() => match next {
                    Some(Ok(line)) => line,
                    Some(Err(_)) | None => break,
                },
            };
            for final_message in split_concatenated_json(&line) {
                if let Some(text) = packet_handler
                    .attempt_message_reassembly(final_message, addr)
                    .await
                {
                    trace!("Received message: {}", text);
                    match channel.send(text).await {
                        Ok(()) => trace!("Message sent to channel"),
                        Err(e) => error!("Error sending message to channel: {}", e),
                    }
                } else {
                    trace!("Incomplete JSON object");
                }
            }
        }

        Ok(())
    }
}

/// Outbound TCP sender. Drains the per-protocol [`broadcast::Receiver`]
/// and writes each newline-terminated payload to the upstream peer.
/// Lagged broadcast slots are logged and skipped rather than crashing
/// the task.
impl SenderServer<StubbornIo<CachedDnsTcp>> {
    pub(crate) fn send_message(mut self) {
        tokio::spawn(async move {
            loop {
                let message = match self.channel.recv().await {
                    Ok(m) => m,
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("broadcast lagged; {n} message(s) dropped");
                        continue;
                    }
                };
                match self.socket.write_all(message.as_bytes()).await {
                    Ok(()) => trace!("sent message"),
                    Err(e) => error!("Error sending message: {}", e),
                }
            }
        });
    }
}

/// TCP Serve Server. Listens for incoming TCP client connections and, for
/// each accepted client, forwards every broadcast message until the client
/// disconnects.
pub struct TCPServeServer {
    pub socket: TcpListener,
    pub proto_name: String,
}

impl TCPServeServer {
    pub(crate) fn new(socket: TcpListener, proto_name: &str) -> Self {
        Self {
            socket,
            proto_name: proto_name.to_string(),
        }
    }

    /// Accept loop: every incoming client spawns its own forwarding task with
    /// its own [`broadcast::Receiver`]. Eliminates the previous
    /// `Mutex<HashMap<peer, Sender>>` + per-peer `mpsc::unbounded_channel`
    /// fan-out (and the `broadcast()` method that walked it inside a lock).
    #[tracing::instrument(name = "tcp_serve", skip_all, fields(proto = %self.proto_name))]
    pub(crate) async fn watch_for_connections(
        self,
        tx_processed: broadcast::Sender<OutboundMessage>,
        shutdown: CancellationToken,
    ) {
        loop {
            let accepted = tokio::select! {
                () = shutdown.cancelled() => {
                    info!("shutdown requested; closing accept loop");
                    return;
                }
                res = self.socket.accept() => res,
            };
            match accepted {
                Ok((stream, addr)) => {
                    let mut rx = tx_processed.subscribe();
                    let peer_shutdown = shutdown.clone();
                    tokio::spawn(async move {
                        info!("accepted connection from {addr}");
                        if let Err(e) = forward_to_peer(stream, addr, &mut rx, peer_shutdown).await
                        {
                            info!("peer {addr} disconnected; error = {e:?}");
                        } else {
                            info!("peer {addr} disconnected");
                        }
                    });
                }
                Err(e) => {
                    error!(": Error accepting connection: {e}");
                }
            }
        }
    }
}

/// Per-peer forwarding loop. Returns `Ok(())` on clean disconnect, `Err` on
/// I/O failure. A lagged broadcast receiver drops the slow client (the only
/// sensible behaviour for a fan-out TCP server: holding back-pressure on the
/// shared channel would penalise every other peer).
async fn forward_to_peer(
    stream: TcpStream,
    addr: SocketAddr,
    rx: &mut broadcast::Receiver<OutboundMessage>,
    shutdown: CancellationToken,
) -> Result<(), Box<dyn Error>> {
    let mut stream = stream;
    loop {
        let recv = tokio::select! {
            () = shutdown.cancelled() => return Ok(()),
            r = rx.recv() => r,
        };
        match recv {
            Ok(message) => {
                if let Err(e) = stream.write_all(message.as_bytes()).await {
                    return Err(e.into());
                }
                debug!("sent message to {addr}");
            }
            Err(broadcast::error::RecvError::Closed) => return Ok(()),
            Err(broadcast::error::RecvError::Lagged(n)) => {
                error!(
                    "peer {addr} lagged; dropping {n} message(s) and \
                     disconnecting (slow consumer)"
                );
                return Ok(());
            }
        }
    }
}
