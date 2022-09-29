// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

use acars_vdlm2_parser::AcarsVdlm2Message;
use futures::SinkExt;
use std::collections::HashMap;
use std::error::Error;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use stubborn_io::tokio::StubbornIo;
use stubborn_io::StubbornTcpStream;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, Mutex, MutexGuard};
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec};

use crate::packet_handler::{PacketHandler, ProcessAssembly};
use crate::{reconnect_options, Rx, SenderServer, Shared};

pub(crate) struct TCPListenerServer {
    pub(crate) proto_name: String,
    pub(crate) reassembly_window: f64,
}

impl TCPListenerServer {
    pub(crate) fn new(proto_name: &str, reassembly_window: &f64) -> Self {
        Self {
            proto_name: proto_name.to_string(),
            reassembly_window: *reassembly_window,
        }
    }

    pub(crate) async fn run(
        self,
        listen_acars_udp_port: String,
        channel: Sender<String>,
    ) -> Result<(), io::Error> {
        let listener: TcpListener =
            TcpListener::bind(format!("0.0.0.0:{}", listen_acars_udp_port)).await?;
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
                match process_tcp_sockets(
                    stream,
                    &new_proto_name,
                    new_channel,
                    addr,
                    self.reassembly_window,
                )
                .await
                {
                    Ok(_) => debug!(
                        "[TCP Listener SERVER: {}] connection closed",
                        new_proto_name
                    ),
                    Err(e) => error!(
                        "[TCP Listener SERVER: {}] connection error: {}",
                        new_proto_name.clone(),
                        e
                    ),
                };
            });
        }
    }
}

async fn process_tcp_sockets(
    stream: TcpStream,
    proto_name: &str,
    channel: Sender<String>,
    peer: SocketAddr,
    reassembly_window: f64,
) -> Result<(), Box<dyn Error>> {
    let mut lines = Framed::new(stream, LinesCodec::new_with_max_length(8000));

    let packet_handler = PacketHandler::new(proto_name, reassembly_window);

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
                            "[TCP Listener SERVER: {}] Multiple messages received in a packet.",
                            proto_name
                        );
                        format!("{}}}", msg_by_brackets)
                    } else if count == split_messages_by_brackets.len() - 1 {
                        // This case is for the last element, which should only ever need a single opening bracket
                        trace!(
                            "[TCP Listener SERVER: {}] End of a multiple message packet",
                            proto_name
                        );
                        format!("{{{}", msg_by_brackets)
                    } else {
                        // This case is for any middle elements, which need both an opening and closing bracket
                        trace!(
                            "[TCP Listener SERVER: {}] Middle of a multiple message packet",
                            proto_name
                        );
                        format!("{{{}}}", msg_by_brackets)
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

pub struct TCPReceiverServer {
    pub host: String,
    pub proto_name: String,
    pub reassembly_window: f64,
}

impl TCPReceiverServer {
    pub(crate) fn new(server_host: &str, proto_name: &str, reassembly_window: f64) -> Self {
        Self {
            host: server_host.to_string(),
            proto_name: proto_name.to_string(),
            reassembly_window,
        }
    }

    pub async fn run(self, channel: Sender<String>) -> Result<(), Box<dyn Error>> {
        trace!("[TCP Receiver Server {}] Starting", self.proto_name);
        // create a SocketAddr from host
        let addr = match self.host.parse::<SocketAddr>() {
            Ok(addr) => addr,
            Err(e) => {
                error!(
                    "[TCP Receiver Server {}] Error parsing host: {}",
                    self.proto_name, e
                );
                return Ok(());
            }
        };

        let stream = match StubbornTcpStream::connect_with_options(addr, reconnect_options()).await
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

        // create a buffered reader and send the messages to the channel

        let reader = tokio::io::BufReader::new(stream);
        let mut lines = Framed::new(reader, LinesCodec::new());
        let packet_handler = PacketHandler::new(&self.proto_name, self.reassembly_window);

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
                                Err(parse_error) => error!("{}", parse_error),
                                Ok(msg) => {
                                    trace!(
                                        "[TCP Receiver Server {}] Received message: {}",
                                        self.proto_name,
                                        msg
                                    );
                                    match channel.send(msg).await {
                                        Ok(_) => trace!("[TCP SERVER {}] Message sent to channel", self.proto_name),
                                        Err(e) => error!("[TCP Receiver Server {}]Error sending message to channel: {}", self.proto_name, e)
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

impl SenderServer<StubbornIo<TcpStream, String>> {
    pub async fn send_message(mut self) {
        tokio::spawn(async move {
            while let Some(message) = self.channel.recv().await {
                match message.to_bytes_newline() {
                    Err(encode_error) => error!(
                        "[TCP SENDER {}]: Error converting message: {}",
                        self.proto_name, encode_error
                    ),
                    Ok(encoded_message) => match self.socket.write_all(&encoded_message).await {
                        Ok(_) => trace!("[TCP SENDER {}]: sent message", self.proto_name),
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

pub struct TCPServeServer {
    pub socket: TcpListener,
    pub proto_name: String,
}

/// The state for each connected client.
struct Peer {
    /// The TCP socket wrapped with the `Lines` codec, defined below.
    ///
    /// This handles sending and receiving data on the socket. When using
    /// `Lines`, we can work at the line level instead of having to manage the
    /// raw byte operations.
    lines: Framed<TcpStream, LinesCodec>,

    /// Receive half of the message channel.
    ///
    /// This is used to receive messages from peers. When a message is received
    /// off of this `Rx`, it will be written to the socket.
    rx: Rx,
}

impl Shared {
    /// Create a new, empty, instance of `Shared`.
    pub(crate) fn new() -> Self {
        Shared {
            peers: HashMap::new(),
        }
    }

    /// Send a `LineCodec` encoded message to every peer, except
    /// for the sender.
    async fn broadcast(&mut self, message: &str) {
        for peer in self.peers.iter_mut() {
            let _ = peer.1.send(message.into());
        }
    }
}

impl Peer {
    /// Create a new instance of `Peer`.
    async fn new(
        state: Arc<Mutex<Shared>>,
        lines: Framed<TcpStream, LinesCodec>,
    ) -> io::Result<Peer> {
        // Get the client socket address
        let addr: SocketAddr = lines.get_ref().peer_addr()?;

        // Create a channel for this peer
        let (tx, rx) = mpsc::unbounded_channel();

        // Add an entry for this `Peer` in the shared state map.
        state.lock().await.peers.insert(addr, tx);

        Ok(Peer { lines, rx })
    }
}

impl TCPServeServer {
    pub(crate) fn new(socket: TcpListener, proto_name: &str) -> Self {
        Self {
            socket,
            proto_name: proto_name.to_string(),
        }
    }
    pub(crate) async fn watch_for_connections(
        self,
        channel: Receiver<AcarsVdlm2Message>,
        state: &Arc<Mutex<Shared>>,
    ) {
        let new_state: Arc<Mutex<Shared>> = Arc::clone(state);
        tokio::spawn(async move { handle_message(new_state, channel).await });
        loop {
            let new_proto: String = self.proto_name.to_string();
            match self.socket.accept().await {
                Ok((stream, addr)) => {
                    // Clone a handle to the `Shared` state for the new connection.
                    let state: Arc<Mutex<Shared>> = Arc::clone(state);

                    // Spawn our handler to be run asynchronously.
                    tokio::spawn(async move {
                        info!("[TCP SERVER {new_proto}] accepted connection");
                        if let Err(e) = process(&state, stream, addr).await {
                            info!(
                                "[TCP SERVER {new_proto}] an error occurred; error = {:?}",
                                e
                            );
                        }
                    });
                }
                Err(e) => {
                    error!("[TCP SERVER {new_proto}]: Error accepting connection: {e}");
                    continue;
                }
            };
        }
    }
}

async fn handle_message(state: Arc<Mutex<Shared>>, mut channel: Receiver<AcarsVdlm2Message>) {
    loop {
        if let Some(received_message) = channel.recv().await {
            // state.lock().await.broadcast(&format!("{}\n",received_message)).await;
            match received_message.to_string_newline() {
                Err(message_parse_error) => {
                    error!("Failed to parse message to string: {}", message_parse_error)
                }
                Ok(message) => state.lock().await.broadcast(&message).await,
            }
        }
    }
}

async fn process(
    state: &Arc<Mutex<Shared>>,
    stream: TcpStream,
    addr: SocketAddr,
) -> Result<(), Box<dyn Error>> {
    // If this section is reached it means that the client was disconnected!
    // Let's let everyone still connected know about it.
    let lines: Framed<TcpStream, LinesCodec> = Framed::new(stream, LinesCodec::new());
    let mut peer: Peer = match Peer::new(state.clone(), lines).await {
        Ok(peer) => peer,
        Err(e) => {
            error!("[TCP SERVER {addr}]: Error creating peer: {}", e);
            return Ok(());
        }
    };
    loop {
        tokio::select! {
            // A message was received from a peer. Send it to the current user.
            Some(msg) = peer.rx.recv() => {
                match peer.lines.send(&msg).await {
                    Ok(_) => {
                        debug!("[TCP SERVER {addr}]: Sent message");
                    }
                    Err(e) => {
                        error!("[TCP SERVER {addr}]: Error sending message: {}", e);
                    }
                };
            }
            result = peer.lines.next() => match result {
                // We received a message on this socket. Why? Dunno. Do nothing.
                Some(Ok(_)) => (),
                // An error occurred.
                Some(Err(e)) => {
                    error!(
                        "[TCP SERVER {addr}]: [YOU SHOULD NEVER SEE THIS!] an error occurred while processing messages; error = {:?}", e
                    );
                }
                // The stream has been exhausted.
                None => break,
            },
        }
    }
    {
        info!("[TCP SERVER {addr}]: Client disconnected");
        let mut state: MutexGuard<Shared> = state.lock().await;
        state.peers.remove(&addr);
        Ok(())
    }
}
