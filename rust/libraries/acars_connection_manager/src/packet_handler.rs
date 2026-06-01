// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

//! Per-peer message reassembly.
//!
//! Inbound TCP/UDP fragments may split a single ACARS/VDLM2 JSON object
//! across multiple packets. `PacketHandler` buffers the most recent
//! partial fragment per peer and tries to decode it concatenated with each
//! new fragment. Successfully-decoded fragments drop the buffer entry;
//! buffers older than `reassembly_window` seconds are pruned lazily on the
//! next call from any peer.
//!
//! Locking discipline: every public entry point acquires the single
//! `tokio::sync::Mutex` over the queue at most once and never re-locks
//! while still holding a guard. The pre-PR7 implementation re-locked up
//! to four times per call and had a TOCTOU `.unwrap()` panic between
//! `contains_key` and `get`.

use acars_vdlm2_parser::{AcarsVdlm2Message, DecodeMessage, MessageResult};
use log::{debug, error, info, trace};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::net::SocketAddr;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tokio::sync::mpsc::Sender;

pub struct PacketHandler {
    name: String,
    listener_type: String,
    /// Per-peer (first-seen-unix-seconds, partial-fragment) buffer.
    queue: Mutex<HashMap<SocketAddr, (f64, String)>>,
    reassembly_window: f64,
}

pub trait ProcessAssembly {
    fn process_reassembly(
        &self,
        proto_name: &str,
        channel: &Sender<String>,
        listener_type: &str,
    ) -> impl std::future::Future<Output = ()> + Send;
}

impl ProcessAssembly for Option<AcarsVdlm2Message> {
    async fn process_reassembly(
        &self,
        proto_name: &str,
        channel: &Sender<String>,
        listener_type: &str,
    ) {
        let Some(reassembled_msg) = self else {
            trace!("[{listener_type} Listener SERVER: {proto_name}] Invalid Message");
            return;
        };
        let parsed_msg: MessageResult<String> = reassembled_msg.to_string_newline();
        match parsed_msg {
            Err(parse_error) => {
                error!("[{listener_type} Listener Server: {proto_name}] {parse_error}");
            }
            Ok(msg) => {
                trace!("[{listener_type} Listener SERVER: {proto_name}] Received message: {msg:?}");
                match channel.send(msg).await {
                    Ok(()) => debug!(
                        "[{listener_type} Listener SERVER: {proto_name}] Message sent to channel"
                    ),
                    Err(e) => error!(
                        "[{listener_type} Listener SERVER: {proto_name}] sending message to channel: {e}"
                    ),
                }
            }
        }
    }
}

impl PacketHandler {
    #[must_use]
    pub fn new(name: &str, listener_type: &str, reassembly_window: f64) -> Self {
        Self {
            name: name.to_string(),
            listener_type: listener_type.to_string(),
            queue: Mutex::new(HashMap::new()),
            reassembly_window,
        }
    }

    /// Try to decode `new_message_string` either standalone or concatenated
    /// with whatever partial fragment we already hold for `peer`.
    ///
    /// Returns `Some(msg)` on a successful decode (the peer's buffer, if
    /// any, is cleared). Returns `None` when the new fragment is not yet a
    /// complete message; the buffer is then updated (or seeded) and the
    /// caller is expected to feed the next fragment.
    ///
    /// Holds the queue lock exactly once. Cannot panic on missing entries.
    //
    // significant_drop_tightening: clippy doesn't recognise the borrow
    // into prune_expired as a second usage of the guard, so it suggests
    // collapsing each call site to a one-shot `.lock().await.op()` chain.
    // That would split the critical section in half and reintroduce the
    // very race this rewrite removes.
    #[allow(clippy::significant_drop_tightening)]
    pub async fn attempt_message_reassembly(
        &self,
        new_message_string: String,
        peer: SocketAddr,
    ) -> Option<AcarsVdlm2Message> {
        let now = unix_time_secs();

        // Fast path: the fragment is itself a complete message. Take the
        // lock briefly to evict any stale partial buffer for this peer and
        // to opportunistically prune expired entries.
        if let Ok(msg) = new_message_string.decode_message() {
            let mut queue = self.queue.lock().await;
            queue.remove(&peer);
            self.prune_expired(&mut queue, now);
            return Some(msg);
        }

        // Slow path: maybe a fragment. Single lock acquisition for the
        // remainder of the function; no `.await` between operations.
        let mut queue = self.queue.lock().await;
        self.prune_expired(&mut queue, now);

        match queue.entry(peer) {
            Entry::Occupied(mut occ) => {
                let (first_seen, existing) = occ.get();
                let first_seen = *first_seen;
                let combined = format!("{existing}{new_message_string}");
                if let Ok(msg) = combined.decode_message() {
                    info!(
                        "[{} SERVER: {}] Reassembled a message from peer {}",
                        self.listener_type, self.name, peer
                    );
                    occ.remove();
                    Some(msg)
                } else {
                    // Still incomplete: replace the stored buffer but
                    // preserve the original `first_seen` so the
                    // reassembly window measures wall-clock age from
                    // the *first* fragment we ever saw.
                    debug!(
                        "[{} SERVER: {}] Buffering fragment from peer {} ({} bytes total)",
                        self.listener_type,
                        self.name,
                        peer,
                        combined.len()
                    );
                    occ.insert((first_seen, combined));
                    None
                }
            }
            Entry::Vacant(vac) => {
                debug!(
                    "[{} SERVER: {}] New fragment buffered from peer {}",
                    self.listener_type, self.name, peer
                );
                vac.insert((now, new_message_string));
                None
            }
        }
    }

    /// Drop buffers older than `reassembly_window` seconds. Called while
    /// the queue lock is already held; not exposed publicly because there
    /// is no longer a use case for forcing a sweep from outside.
    fn prune_expired(&self, queue: &mut HashMap<SocketAddr, (f64, String)>, now: f64) {
        if now == 0.0 {
            error!(
                "[{} SERVER: {}] Error getting current time",
                self.listener_type, self.name
            );
            return;
        }
        queue.retain(|peer, (first_seen, _)| {
            let age = now - *first_seen;
            if age > self.reassembly_window {
                debug!(
                    "[{} SERVER {}] Peer {peer} has been idle for {age} seconds, removing from queue",
                    self.listener_type, self.name
                );
                false
            } else {
                true
            }
        });
    }
}

fn unix_time_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0.0, |n| n.as_secs_f64())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn peer(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    /// A minimal, syntactically valid ACARS JSON object the parser will
    /// accept. We deliberately split it in half to exercise the fragment
    /// reassembly path.
    const COMPLETE_MSG: &str = r#"{"timestamp":1700000000.0,"freq":131.55,"channel":0,"level":-20.0,"error":0,"mode":"2","label":"H1","block_id":"1","ack":"!","tail":"N12345","flight":"AB1234","msgno":"M01A","text":"HELLO"}"#;

    #[tokio::test]
    async fn standalone_message_returns_decoded() {
        let h = PacketHandler::new("test", "TCP", 5.0);
        let out = h
            .attempt_message_reassembly(COMPLETE_MSG.to_string(), peer(1))
            .await;
        assert!(out.is_some());
        assert!(h.queue.lock().await.is_empty());
    }

    #[tokio::test]
    async fn two_fragments_reassemble() {
        let h = PacketHandler::new("test", "TCP", 5.0);
        let (a, b) = COMPLETE_MSG.split_at(COMPLETE_MSG.len() / 2);

        let first = h.attempt_message_reassembly(a.to_string(), peer(2)).await;
        assert!(first.is_none(), "first fragment must not decode");
        assert_eq!(h.queue.lock().await.len(), 1);

        let second = h.attempt_message_reassembly(b.to_string(), peer(2)).await;
        assert!(second.is_some(), "concatenated fragments must decode");
        assert!(
            h.queue.lock().await.is_empty(),
            "successful reassembly must drop the buffer"
        );
    }

    #[tokio::test]
    async fn expired_buffer_is_pruned() {
        let h = PacketHandler::new("test", "TCP", 0.0);
        let (a, _) = COMPLETE_MSG.split_at(COMPLETE_MSG.len() / 2);

        let _ = h.attempt_message_reassembly(a.to_string(), peer(3)).await;
        assert_eq!(h.queue.lock().await.len(), 1);

        // Any subsequent call prunes entries whose age exceeds the
        // zero-second window before processing the new fragment.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let _ = h
            .attempt_message_reassembly("garbage".to_string(), peer(4))
            .await;
        // peer(3) pruned; peer(4) seeded.
        let (had_3, had_4) = {
            let q = h.queue.lock().await;
            (q.contains_key(&peer(3)), q.contains_key(&peer(4)))
        };
        assert!(!had_3);
        assert!(had_4);
    }

    #[tokio::test]
    async fn standalone_decode_clears_existing_buffer() {
        let h = PacketHandler::new("test", "TCP", 60.0);
        let (a, _) = COMPLETE_MSG.split_at(COMPLETE_MSG.len() / 2);

        let _ = h.attempt_message_reassembly(a.to_string(), peer(5)).await;
        assert_eq!(h.queue.lock().await.len(), 1);

        // A fresh, fully-formed message from the same peer must purge the
        // stale partial fragment — otherwise we'd attempt to glue old
        // garbage onto the next fragment we receive.
        let out = h
            .attempt_message_reassembly(COMPLETE_MSG.to_string(), peer(5))
            .await;
        assert!(out.is_some());
        assert!(h.queue.lock().await.is_empty());
    }
}
