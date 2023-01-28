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
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, Mutex, MutexGuard};
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec};
use acars_metrics::MessageDestination;

use crate::packet_handler::PacketHandler;
use crate::{reconnect_options, Rx, SenderServer, ServerType, Shared, SocketType};
use crate::message_handler::ProcessSocketListenerMessages;

pub(crate) async fn process_tcp_sockets(
    stream: TcpStream,
    proto_name: ServerType,
    logging_entity: &str,
    channel: Sender<String>,
    peer: SocketAddr,
    reassembly_window: f64,
) -> Result<(), Box<dyn Error>> {
    let mut lines: Framed<TcpStream, LinesCodec> = Framed::new(stream, LinesCodec::new_with_max_length(8000));

    let packet_handler: PacketHandler = PacketHandler::new(logging_entity, reassembly_window);

    while let Some(Ok(line)) = lines.next().await {
        
        line.split_terminator('\n')
            .collect::<Vec<&str>>()
            .process_messages(&packet_handler, &peer, &channel,
                              &logging_entity, SocketType::Tcp, proto_name)
            .await;
        
    }

    Ok(())
}

#[derive(Debug, Clone)]
pub struct TCPReceiverServer {
    pub host: String,
    pub logging_identifier: String,
    pub proto_name: ServerType,
    pub reassembly_window: f64,
}

impl TCPReceiverServer {
    pub(crate) fn new(server_host: &str, proto_name: ServerType, reassembly_window: f64) -> Self {
        Self {
            host: server_host.to_string(),
            logging_identifier: format!("{}_TCP_RECEIVER_{}", proto_name, server_host),
            proto_name,
            reassembly_window,
        }
    }

    pub async fn run(self, channel: Sender<String>) -> Result<(), Box<dyn Error>> {
        trace!("[TCP Receiver Server {}] Starting", self.logging_identifier);
        // create a SocketAddr from host
        let addr: SocketAddr = match self.host.parse::<SocketAddr>() {
            Ok(addr) => addr,
            Err(e) => {
                error!("[TCP Receiver Server {}] Error parsing host: {}",
                    self.logging_identifier, e);
                return Ok(());
            }
        };

        let stream: StubbornIo<TcpStream, SocketAddr> = match StubbornTcpStream::connect_with_options(addr, reconnect_options()).await
        {
            Ok(stream) => stream,
            Err(e) => {
                error!(
                    "[TCP Receiver Server {}] Error connecting to {}: {}",
                    self.logging_identifier, self.host, e
                );
                Err(e)?
            }
        };

        // create a buffered reader and send the messages to the channel

        let reader: BufReader<StubbornIo<TcpStream, SocketAddr>> = BufReader::new(stream);
        let mut lines: Framed<BufReader<StubbornIo<TcpStream, SocketAddr>>, LinesCodec> = Framed::new(reader, LinesCodec::new());
        let packet_handler: PacketHandler = PacketHandler::new(&self.logging_identifier, self.reassembly_window);

        while let Some(Ok(line)) = lines.next().await {
            // Clean up the line endings. This is probably unnecessary but it's here for safety.
            
            line.split_terminator('\n')
                .collect::<Vec<&str>>()
                .process_messages(&packet_handler, &addr, &channel, &self.logging_identifier,
                                  SocketType::Tcp, self.proto_name).await;
        }

        Ok(())
    }
}

impl SenderServer<StubbornIo<TcpStream, String>> {
    pub async fn send_message(mut self) {
        tokio::spawn(async move {
            while let Some(message) = self.channel.recv().await {
                match message.to_bytes_newline() {
                    Err(encode_error) => error!("[TCP SENDER {}]: Error converting message: {}", self.logging_identifier, encode_error),
                    Ok(encoded_message) => {
                        let Err(send_error) = self.socket.write_all(&encoded_message).await else {
                            trace!("[TCP SENDER {}]: sent message", self.logging_identifier);
                            self.proto_name.inc_message_destination_type_metric(MessageDestination::ServeTcp);
                            return;
                        };
                        error!("[TCP SENDER {}]: Error sending message: {}", self.logging_identifier, send_error);
                    }
                }
            }
        });
    }
}

#[derive(Debug)]
pub struct TCPServeServer {
    pub socket: TcpListener,
    pub logging_identifier: String,
    pub proto_name: ServerType,
}

/// The state for each connected client.
#[derive(Debug)]
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
    async fn broadcast(&mut self, proto_name: ServerType, message: &str) {
        for peer in self.peers.iter_mut() {
            let _ = peer.1.send(message.into());
            proto_name.inc_message_destination_type_metric(MessageDestination::ServeTcp);
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
    pub(crate) fn new(socket: TcpListener, logging_identifier: &str, proto_name: ServerType) -> Self {
        Self {
            socket,
            logging_identifier: logging_identifier.to_string(),
            proto_name,
        }
    }
    pub(crate) async fn watch_for_connections(
        self,
        channel: Receiver<AcarsVdlm2Message>,
        state: &Arc<Mutex<Shared>>,
    ) {
        let new_state: Arc<Mutex<Shared>> = Arc::clone(state);
        tokio::spawn(async move { handle_message(new_state, channel, self.proto_name).await });
        loop {
            let new_proto: String = self.logging_identifier.to_string();
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

async fn handle_message(state: Arc<Mutex<Shared>>, mut channel: Receiver<AcarsVdlm2Message>, proto_name: ServerType) {
    loop {
        if let Some(received_message) = channel.recv().await {
            match received_message.to_string_newline() {
                Err(message_parse_error) => {
                    error!("Failed to parse message to string: {}", message_parse_error)
                }
                Ok(message) => state.lock().await.broadcast(proto_name, &message).await,
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
