// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

// Funny name for a file, but it's named after the command line flag that will use it
// TCP server used to send data out. Will monitor for new TCP connections and send data to them.
// AKA....just because I am thick and will forget in a day and a half, this socket passively
// listens for new connections. It does not actively connect to new clients.

use futures::SinkExt;
use std::collections::HashMap;
use std::error::Error;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::Receiver;
use tokio::sync::{mpsc, Mutex};
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec};

use crate::generics::{Rx, Shared};

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
    pub fn new() -> Self {
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
        let addr = lines.get_ref().peer_addr()?;

        // Create a channel for this peer
        let (tx, rx) = mpsc::unbounded_channel();

        // Add an entry for this `Peer` in the shared state map.
        state.lock().await.peers.insert(addr, tx);

        Ok(Peer { lines, rx })
    }
}

impl TCPServeServer {
    pub async fn watch_for_connections(
        self,
        channel: Receiver<String>,
        state: &Arc<Mutex<Shared>>,
    ) {
        let new_state = Arc::clone(state);
        tokio::spawn(async move { handle_message(new_state, channel).await });
        loop {
            let new_proto: String = self.proto_name.to_string();
            match self.socket.accept().await {
                Ok((stream, addr)) => {
                    // Clone a handle to the `Shared` state for the new connection.
                    let state = Arc::clone(state);

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

async fn handle_message(state: Arc<Mutex<Shared>>, mut channel: Receiver<String>) {
    loop {
        if let Some(received_message) = channel.recv().await {
            state.lock().await.broadcast(&received_message).await;
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
    let lines = Framed::new(stream, LinesCodec::new());
    let mut peer = match Peer::new(state.clone(), lines).await {
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
        let mut state = state.lock().await;
        state.peers.remove(&addr);
        Ok(())
    }
}
