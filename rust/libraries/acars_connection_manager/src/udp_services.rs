// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

//! UDP transport: inbound listener and outbound sender.
//!
//! * [`UDPListenerServer`] binds a port, demuxes each datagram on `\n`
//!   and `}{` boundaries, and feeds the per-protocol mpsc input channel
//!   via [`PacketHandler`] reassembly.
//! * [`UDPSenderServer`] consumes the processed broadcast and sprays
//!   each message at every configured `host:port` destination, slicing
//!   payloads at `max_udp_packet_size` boundaries with a 100ms pacing
//!   sleep between fragments. DNS resolution is cached per-destination
//!   for up to `dns_cache_duration` (default 30 min, see
//!   `--udp-dns-cache-seconds`) and **invalidated immediately on any
//!   `sendto()` failure** so the next message re-resolves. See
//!   [`crate::dns`] for the shared hickory-backed resolver.

use crate::dns;
use crate::packet_handler::{PacketHandler, ProcessAssembly};
use acars_vdlm2_parser::AcarsVdlm2Message;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::broadcast;
use tokio::sync::mpsc::Sender;
use tokio::time::{Duration, Instant, sleep};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace, warn};

/// Inbound UDP listener: binds a port and feeds the per-protocol mpsc
/// input channel through [`PacketHandler`] reassembly.
#[derive(Debug, Clone)]
pub(crate) struct UDPListenerServer {
    pub(crate) proto_name: String,
    pub(crate) reassembly_window: f64,
}

/// Per-destination DNS cache entry used by [`UDPSenderServer`].
#[derive(Debug)]
struct ResolvedAddr {
    addr: String,
    resopt: Option<SocketAddr>,
    last_success: Instant,
}

/// Outbound UDP fan-out: consumes the processed broadcast and sprays
/// each message at every configured destination, fragmenting on
/// `max_udp_packet_size` boundaries.
#[derive(Debug)]
pub(crate) struct UDPSenderServer {
    pub(crate) proto_name: String,
    pub(crate) socket: UdpSocket,
    pub(crate) max_udp_packet_size: usize,
    pub(crate) channel: broadcast::Receiver<AcarsVdlm2Message>,
    resolved_addrs: Vec<ResolvedAddr>,
    dns_cache_duration: Duration,
    resolver: Arc<dns::Resolver>,
}

impl UDPListenerServer {
    pub(crate) fn new(proto_name: &str, reassembly_window: f64) -> Self {
        Self {
            proto_name: proto_name.to_string(),
            reassembly_window,
        }
    }

    #[tracing::instrument(
        name = "udp_listener",
        skip_all,
        fields(proto = %self.proto_name, port = %listen_udp_port),
    )]
    pub(crate) async fn run(
        self,
        listen_udp_port: &str,
        channel: Sender<String>,
        shutdown: CancellationToken,
    ) -> Result<(), io::Error> {
        let mut buf: Vec<u8> = vec![0; 5000];
        let mut to_send: Option<(usize, SocketAddr)> = None;

        let s = UdpSocket::bind(listen_udp_port).await;

        match s {
            Err(e) => error!("Error listening on port: {}", e),
            Ok(socket) => {
                info!("Listening on: {}", socket.local_addr()?);

                let packet_handler: PacketHandler =
                    PacketHandler::new(&self.proto_name, "UDP", self.reassembly_window);

                loop {
                    if let Some((size, peer)) = to_send {
                        let Ok(msg_string) = std::str::from_utf8(buf[..size].as_ref()) else {
                            warn!("Invalid message received from {}", peer);
                            continue;
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
                                    .process_reassembly(&self.proto_name, &channel, "UDP")
                                    .await;
                            } else {
                                // We have a message that was split by brackets if the length is greater than one
                                for (count, msg_by_brackets) in
                                    split_messages_by_brackets.iter().enumerate()
                                {
                                    let final_message = if count == 0 {
                                        // First case is the first element, which should only ever need a single closing bracket
                                        trace!("Multiple messages received in a packet.");
                                        format!("{}{}", "}", msg_by_brackets)
                                    } else if count == split_messages_by_brackets.len() - 1 {
                                        // This case is for the last element, which should only ever need a single opening bracket
                                        trace!("End of a multiple message packet");
                                        format!("{}{}", "{", msg_by_brackets)
                                    } else {
                                        // This case is for any middle elements, which need both an opening and closing bracket
                                        trace!("Middle of a multiple message packet");
                                        format!("{}{}{}", "{", msg_by_brackets, "}")
                                    };
                                    packet_handler
                                        .attempt_message_reassembly(final_message, peer)
                                        .await
                                        .process_reassembly(&self.proto_name, &channel, "UDP")
                                        .await;
                                }
                            }
                        }
                    }
                    to_send = Some(tokio::select! {
                        () = shutdown.cancelled() => {
                            info!(
                                "shutdown requested; closing recv loop"
                            );
                            return Ok(());
                        }
                        res = socket.recv_from(&mut buf) => res?,
                    });
                }
            }
        }
        Ok(())
    }
}

impl UDPSenderServer {
    pub(crate) fn new(
        send_udp: &[String],
        server_type: &str,
        socket: UdpSocket,
        max_udp_packet_size: usize,
        dns_cache_seconds: f64,
        rx_processed: broadcast::Receiver<AcarsVdlm2Message>,
        resolver: Arc<dns::Resolver>,
    ) -> Self {
        let mut resolved_addrs: Vec<ResolvedAddr> = Vec::new();
        for addr in send_udp {
            resolved_addrs.push(ResolvedAddr {
                addr: addr.clone(),
                resopt: None,
                last_success: Instant::now(),
            });
        }
        Self {
            proto_name: server_type.to_string(),
            socket,
            max_udp_packet_size,
            channel: rx_processed,
            resolved_addrs,
            dns_cache_duration: Duration::from_secs_f64(dns_cache_seconds),
            resolver,
        }
    }

    #[tracing::instrument(name = "udp_sender", skip_all, fields(proto = %self.proto_name))]
    pub(crate) async fn send_message(mut self) {
        // send the message to the socket
        // Loop through all of the sockets in the host list
        // We will send out a configured max amount bytes at a time until the buffer is exhausted

        loop {
            let message = match self.channel.recv().await {
                Ok(m) => m,
                Err(broadcast::error::RecvError::Closed) => break,
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!("broadcast lagged; {n} message(s) dropped");
                    continue;
                }
            };
            match message.to_bytes_newline() {
                Err(bytes_error) => error!("Failed to encode to bytes: {}", bytes_error),
                Ok(message_as_bytes) => {
                    self.send_bytes(&message_as_bytes[..]).await;
                }
            }
        }
    }

    async fn send_bytes(&mut self, message_as_bytes: &[u8]) {
        let message_size: usize = message_as_bytes.len();
        // Collect (index-into-resolved_addrs, SocketAddr) so we can
        // invalidate the cache by index after the send loop on any
        // sendto() failure without juggling overlapping borrows.
        let mut use_addrs: Vec<(usize, SocketAddr)> = Vec::new();
        for (idx, ra) in self.resolved_addrs.iter_mut().enumerate() {
            if ra.resopt.is_none() || ra.last_success.elapsed() > self.dns_cache_duration {
                // Use the shared async resolver. The previous implementation
                // invoked `std::net::ToSocketAddrs::to_socket_addrs()`, which
                // performs a blocking libc DNS lookup on the tokio runtime.
                // The per-target TTL (`last_success` / `dns_cache_duration`)
                // is independent of hickory's own cache and is preserved
                // verbatim. A failed `sendto()` further down invalidates
                // `resopt` so the *next* message re-resolves regardless of
                // how recent `last_success` is.
                match dns::resolve_host_port(&self.resolver, &ra.addr).await {
                    Ok(resolved) if resolved.is_ipv4() => {
                        debug!("Resolved: {} --> {}", ra.addr, resolved);
                        ra.resopt = Some(resolved);
                        ra.last_success = Instant::now();
                    }
                    Ok(_) => {
                        warn!("{} could not be resolved to an IPv4 address", ra.addr);
                    }
                    Err(e) => {
                        warn!("failed to resolve {}: {:?}", ra.addr, e);
                    }
                }
            }

            if let Some(resolved) = ra.resopt {
                use_addrs.push((idx, resolved));
            }
        }

        // Per-destination failure flags, written by the send loop and
        // applied to `resolved_addrs` in a separate pass so we never
        // hold a mutable borrow across an `.await`.
        let mut failed: Vec<usize> = Vec::new();

        for (idx, resolved) in &use_addrs {
            let addr = &self.resolved_addrs[*idx].addr;
            let mut keep_sending: bool = true;
            let mut buffer_position: usize = 0;
            let mut buffer_end: usize = if message_as_bytes.len() < self.max_udp_packet_size {
                message_as_bytes.len()
            } else {
                self.max_udp_packet_size
            };
            let mut had_error = false;

            while keep_sending {
                trace!(
                    "Sending {buffer_position} to {buffer_end} of {message_size} to {addr} ({resolved})"
                );

                let bytes_sent = self
                    .socket
                    .send_to(&message_as_bytes[buffer_position..buffer_end], resolved)
                    .await;

                match bytes_sent {
                    Ok(bytes_sent) => {
                        debug!("sent {} bytes to {} ({})", bytes_sent, addr, resolved)
                    }
                    Err(e) => {
                        warn!(
                            "failed to send message to {} ({}): {:?}; \
                             invalidating cached resolution",
                            addr, resolved, e
                        );
                        had_error = true;
                        // Stop fragmenting this destination: subsequent
                        // slices to a dead address are wasted work and
                        // would only repeat the warning.
                        keep_sending = false;
                        continue;
                    }
                }

                if buffer_end == message_size {
                    keep_sending = false;
                } else {
                    buffer_position = buffer_end;
                    buffer_end = if buffer_position + self.max_udp_packet_size < message_size {
                        buffer_position + self.max_udp_packet_size
                    } else {
                        message_size
                    };

                    // Slow the sender down!
                    sleep(Duration::from_millis(100)).await;
                }
                trace!("New buffer start: {}, end: {}", buffer_position, buffer_end);
            }

            if had_error {
                failed.push(*idx);
            }
        }

        // Apply failure-driven cache invalidation. Next message to the
        // same destination will re-resolve before sending, regardless
        // of where we are in the TTL window.
        for idx in failed {
            self.resolved_addrs[idx].resopt = None;
        }
    }
}
