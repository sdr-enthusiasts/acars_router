// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

use acars_vdlm2_parser::AcarsVdlm2Message;
use log::{debug, error, info, trace};
use sdre_stubborn_io::tokio::StubbornIo;
use std::error::Error;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::sync::mpsc::Sender;
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec};

use crate::cached_dns_tcp::{CachedDnsTcp, ConnectTarget};
use crate::packet_handler::{PacketHandler, ProcessAssembly};
use crate::{SenderServer, dns, reconnect_options};

/// TCP Listener server. This is used to listen for incoming TCP connections and process them.
/// Used for incoming TCP data for ACARS Router to process
pub(crate) struct TCPListenerServer {
    pub(crate) proto_name: String,
    pub(crate) reassembly_window: f64,
}

/// TCP Listener server. This is used to listen for incoming TCP connections and process them.
/// Used for incoming TCP data for ACARS Router to process
impl TCPListenerServer {
    pub(crate) fn new(proto_name: &str, reassembly_window: f64) -> Self {
        Self {
            proto_name: proto_name.to_string(),
            reassembly_window,
        }
    }

    pub(crate) async fn run(
        self,
        listen_acars_udp_port: String,
        channel: Sender<String>,
    ) -> Result<(), io::Error> {
        let listener: TcpListener =
            TcpListener::bind(format!("0.0.0.0:{listen_acars_udp_port}")).await?;
        info!(
            "[TCP Listener SERVER: {}]: Listening on: {}",
            self.proto_name,
            listener.local_addr()?
        );

        loop {
            trace!(
                "[TCP Listener SERVER: {}]: Waiting for connection",
                self.proto_name
            );
            // Asynchronously wait for an inbound TcpStream.
            let (stream, addr) = listener.accept().await?;
            let new_channel = channel.clone();
            let new_proto_name = format!("{}:{}", self.proto_name, addr);
            info!(
                "[TCP Listener SERVER: {}]:accepted connection from {}",
                self.proto_name, addr
            );
            // Spawn our handler to be run asynchronously.
            tokio::spawn(async move {
                match Box::pin(process_tcp_sockets(
                    stream,
                    &new_proto_name,
                    new_channel,
                    addr,
                    self.reassembly_window,
                ))
                .await
                {
                    Ok(()) => debug!("[TCP Listener SERVER: {new_proto_name}] connection closed"),
                    Err(e) => error!(
                        "[TCP Listener SERVER: {}] connection error: {}",
                        new_proto_name.clone(),
                        e
                    ),
                }
            });
        }
    }
}

/// This function is used to process the TCP socket. It will read the socket and send the messages to the channel.
/// Used for incoming TCP data for ACARS Router to process
async fn process_tcp_sockets(
    stream: TcpStream,
    proto_name: &str,
    channel: Sender<String>,
    peer: SocketAddr,
    reassembly_window: f64,
) -> Result<(), Box<dyn Error>> {
    let mut lines = Framed::new(stream, LinesCodec::new_with_max_length(8000));

    let packet_handler = PacketHandler::new(proto_name, "TCP", reassembly_window);

    while let Some(Ok(line)) = lines.next().await {
        let split_messages_by_newline: Vec<&str> = line.split_terminator('\n').collect();

        for msg_by_newline in split_messages_by_newline {
            let split_messages_by_brackets: Vec<&str> =
                msg_by_newline.split_terminator("}{").collect();
            if split_messages_by_brackets.len().eq(&1) {
                packet_handler
                    .attempt_message_reassembly(split_messages_by_brackets[0].to_string(), peer)
                    .await
                    .process_reassembly(proto_name, &channel, "TCP")
                    .await;
            } else {
                // We have a message that was split by brackets if the length is greater than one
                for (count, msg_by_brackets) in split_messages_by_brackets.iter().enumerate() {
                    let final_message = if count == 0 {
                        // First case is the first element, which should only ever need a single closing bracket
                        trace!(
                            "[TCP Listener SERVER: {proto_name}] Multiple messages received in a packet."
                        );
                        format!("{msg_by_brackets}}}")
                    } else if count == split_messages_by_brackets.len() - 1 {
                        // This case is for the last element, which should only ever need a single opening bracket
                        trace!(
                            "[TCP Listener SERVER: {proto_name}] End of a multiple message packet"
                        );
                        format!("{{{msg_by_brackets}")
                    } else {
                        // This case is for any middle elements, which need both an opening and closing bracket
                        trace!(
                            "[TCP Listener SERVER: {proto_name}] Middle of a multiple message packet"
                        );
                        format!("{{{msg_by_brackets}}}")
                    };
                    packet_handler
                        .attempt_message_reassembly(final_message, peer)
                        .await
                        .process_reassembly(proto_name, &channel, "TCP")
                        .await;
                }
            }
        }
    }

    Ok(())
}

/// TCP Receiver server. This is used to connect to a remote TCP server and process the messages.
/// Used for incoming TCP data for ACARS Router to process
pub struct TCPReceiverServer {
    pub host: String,
    pub proto_name: String,
    pub reassembly_window: f64,
    pub resolver: Arc<dns::Resolver>,
}

/// TCP Receiver server. This is used to connect to a remote TCP server and process the messages.
/// Used for incoming TCP data for ACARS Router to process
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

    pub async fn run(self, channel: Sender<String>) -> Result<(), Box<dyn Error>> {
        trace!("[TCP Receiver Server {}] Starting", self.proto_name);
        // Resolve once up-front so we can attribute reassembly to a stable
        // peer address; `CachedDnsTcp` will re-resolve on every reconnect.
        let addr = match dns::resolve_host_port(&self.resolver, &self.host).await {
            Ok(addr) => addr,
            Err(e) => {
                error!(
                    "[TCP Receiver Server {}] Error resolving host `{}`: {}",
                    self.proto_name, self.host, e
                );
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
                error!(
                    "[TCP Receiver Server {}] Error connecting to {}: {}",
                    self.proto_name, self.host, e
                );
                Err(e)?
            }
        };

        // Set TCP KeepAlive. Two derefs: StubbornIo -> CachedDnsTcp -> TcpStream.
        let sock_ref = socket2::SockRef::from(&**stream);

        let mut ka = socket2::TcpKeepalive::new();
        ka = ka.with_time(Duration::from_secs(5));
        ka = ka.with_interval(Duration::from_secs(5));

        sock_ref.set_tcp_keepalive(&ka)?;

        // create a buffered reader and send the messages to the channel

        let reader = tokio::io::BufReader::new(stream);
        let mut lines = Framed::new(reader, LinesCodec::new());
        let packet_handler = PacketHandler::new(&self.proto_name, "TCP", self.reassembly_window);

        while let Some(Ok(line)) = lines.next().await {
            // Clean up the line endings. This is probably unnecessary but it's here for safety.
            let split_messages_by_newline: Vec<&str> = line.split_terminator('\n').collect();

            for msg_by_newline in split_messages_by_newline {
                let split_messages_by_brackets: Vec<&str> =
                    msg_by_newline.split_terminator("}{").collect();

                for (count, msg_by_brackets) in split_messages_by_brackets.iter().enumerate() {
                    let final_message: String;
                    // FIXME: This feels very non-rust idomatic and is ugly

                    // Our message had no brackets, so we can just send it
                    if split_messages_by_brackets.len() == 1 {
                        final_message = msg_by_brackets.to_string();
                    }
                    // We have a message that was split by brackets if the length is greater than one
                    // First case is the first element, which should only ever need a single closing bracket
                    else if count == 0 {
                        trace!(
                            "[TCP Receiver Server {}]Multiple messages received in a packet.",
                            self.proto_name
                        );
                        final_message = format!("{}{}", "}", msg_by_brackets);
                    } else if count == split_messages_by_brackets.len() - 1 {
                        // This case is for the last element, which should only ever need a single opening bracket
                        trace!(
                            "[TCP Receiver Server {}] End of a multiple message packet",
                            self.proto_name
                        );
                        final_message = format!("{}{}", "{", msg_by_brackets);
                    } else {
                        // This case is for any middle elements, which need both an opening and closing bracket
                        trace!(
                            "[TCP Receiver Server {}] Middle of a multiple message packet",
                            self.proto_name
                        );
                        final_message = format!("{}{}{}", "{", msg_by_brackets, "}");
                    }
                    match packet_handler
                        .attempt_message_reassembly(final_message, addr)
                        .await
                    {
                        Some(encoded_msg) => {
                            let parse_msg = encoded_msg.to_string();
                            match parse_msg {
                                Err(parse_error) => error!("{parse_error}"),
                                Ok(msg) => {
                                    trace!(
                                        "[TCP Receiver Server {}] Received message: {}",
                                        self.proto_name, msg
                                    );
                                    match channel.send(msg).await {
                                        Ok(()) => trace!(
                                            "[TCP SERVER {}] Message sent to channel",
                                            self.proto_name
                                        ),
                                        Err(e) => error!(
                                            "[TCP Receiver Server {}]Error sending message to channel: {}",
                                            self.proto_name, e
                                        ),
                                    }
                                }
                            }
                        }
                        None => trace!("[TCP Receiver Server {}] Invalid Message", self.proto_name),
                    }
                }
            }
        }

        Ok(())
    }
}

/// TCP Sender server. This is used to connect to a remote TCP server and send the messages.
/// Used for outgoing TCP data for ACARS Router to a client
impl SenderServer<StubbornIo<CachedDnsTcp>> {
    pub(crate) fn send_message(mut self) {
        tokio::spawn(async move {
            loop {
                let message = match self.channel.recv().await {
                    Ok(m) => m,
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        log::warn!(
                            "[TCP SENDER {}]: broadcast lagged; {n} message(s) dropped",
                            self.proto_name
                        );
                        continue;
                    }
                };
                match message.to_bytes_newline() {
                    Err(encode_error) => error!(
                        "[TCP SENDER {}]: Error converting message: {}",
                        self.proto_name, encode_error
                    ),
                    Ok(encoded_message) => match self.socket.write_all(&encoded_message).await {
                        Ok(()) => trace!("[TCP SENDER {}]: sent message", self.proto_name),
                        Err(e) => error!(
                            "[TCP SENDER {}]: Error sending message: {}",
                            self.proto_name, e
                        ),
                    },
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
    pub(crate) async fn watch_for_connections(
        self,
        tx_processed: broadcast::Sender<AcarsVdlm2Message>,
    ) {
        loop {
            let proto = self.proto_name.clone();
            match self.socket.accept().await {
                Ok((stream, addr)) => {
                    let mut rx = tx_processed.subscribe();
                    tokio::spawn(async move {
                        info!("[TCP SERVER {proto}] accepted connection from {addr}");
                        if let Err(e) = forward_to_peer(stream, addr, &proto, &mut rx).await {
                            info!("[TCP SERVER {proto}] peer {addr} disconnected; error = {e:?}");
                        } else {
                            info!("[TCP SERVER {proto}] peer {addr} disconnected");
                        }
                    });
                }
                Err(e) => {
                    error!("[TCP SERVER {proto}]: Error accepting connection: {e}");
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
    proto: &str,
    rx: &mut broadcast::Receiver<AcarsVdlm2Message>,
) -> Result<(), Box<dyn Error>> {
    let mut stream = stream;
    loop {
        match rx.recv().await {
            Ok(message) => match message.to_string_newline() {
                Ok(payload) => {
                    if let Err(e) = stream.write_all(payload.as_bytes()).await {
                        return Err(e.into());
                    }
                    debug!("[TCP SERVER {proto}] sent message to {addr}");
                }
                Err(e) => {
                    error!("[TCP SERVER {proto}] Failed to encode message for {addr}: {e}");
                }
            },
            Err(broadcast::error::RecvError::Closed) => return Ok(()),
            Err(broadcast::error::RecvError::Lagged(n)) => {
                error!(
                    "[TCP SERVER {proto}] peer {addr} lagged; dropping {n} message(s) and \
                     disconnecting (slow consumer)"
                );
                return Ok(());
            }
        }
    }
}
