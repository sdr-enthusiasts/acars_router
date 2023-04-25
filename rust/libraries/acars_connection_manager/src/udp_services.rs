// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

// Server used to receive UDP data

use std::cmp::Ordering;
// use std::io;
use acars_vdlm2_parser::AcarsVdlm2Message;
use tokio::net::UdpSocket;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::{sleep, Duration, timeout};
use acars_metrics::MessageDestination;
use crate::ServerType;
use tokio_stream::StreamExt;


#[derive(Debug)]
pub(crate) struct UDPSenderServer {
    pub(crate) host: Vec<String>,
    pub(crate) proto_name: ServerType,
    pub(crate) logging_identifier: String,
    pub(crate) socket: UdpSocket,
    pub(crate) max_udp_packet_size: usize,
    pub(crate) channel: UnboundedReceiver<AcarsVdlm2Message>,
}

impl UDPSenderServer {
    pub(crate) fn new(
        send_udp: &[String],
        server_type: ServerType,
        socket: UdpSocket,
        max_udp_packet_size: &usize,
        rx_processed: UnboundedReceiver<AcarsVdlm2Message>,
    ) -> Self {
        let logging_identifier: String = match socket.local_addr() {
            Ok(local_addr) => format!("{}_UDP_SEND_{}", server_type, local_addr),
            Err(_) => format!("{}_UDP_SEND", server_type)
        };
        Self {
            host: send_udp.to_vec(),
            proto_name: server_type,
            logging_identifier,
            socket,
            max_udp_packet_size: *max_udp_packet_size,
            channel: rx_processed,
        }
    }

    pub(crate) async fn send_message(mut self) {
        // send the message to the socket
        // Loop through all of the sockets in the host list
        // We will send out a configured max amount bytes at a time until the buffer is exhausted

        while let Some(message) = self.channel.recv().await {
            match message.to_bytes_newline() {
                Err(bytes_error) => error!(
                    "[UDP SENDER {}] Failed to encode to bytes: {}",
                    self.logging_identifier, bytes_error
                ),
                Ok(message_as_bytes) => {
                    let message_size: usize = message_as_bytes.len();
                    let mut hosts = tokio_stream::iter(&self.host);
                    while let Some(addr) = hosts.next().await {
                        match message_size.gt(&self.max_udp_packet_size) {
                            true => {
                                trace!("[UDP SENDER {}] Message size exceeds configured packet size, splitting it up and sending it in parts.",
                                    self.logging_identifier);
                                self.split_message_alt(addr, &message_as_bytes).await;
                            }
                            false => {
                                trace!("[UDP SENDER {}] Message fits into a single packet, sending.",
                                    self.logging_identifier);
                                self.single_message(addr, &message_as_bytes).await;
                            }
                        }
                    }
                    // for addr in &self.host {
                        // let mut keep_sending: bool = true;
                        // let mut buffer_position: usize = 0;
                        // let mut buffer_end: usize =
                        //     match message_as_bytes.len() < self.max_udp_packet_size {
                        //         true => message_as_bytes.len(),
                        //         false => self.max_udp_packet_size,
                        //     };
                        //
                        // while keep_sending {
                        //     trace!("[UDP SENDER {}] Sending {buffer_position} to {buffer_end} of {message_size} to {addr}", self.logging_identifier);
                        //     let bytes_sent: io::Result<usize> = self
                        //         .socket
                        //         .send_to(&message_as_bytes[buffer_position..buffer_end], addr)
                        //         .await;
                        //
                        //     match bytes_sent {
                        //         Ok(bytes_sent) => debug!(
                        //             "[UDP SENDER {}] sent {} bytes to {}",
                        //             self.logging_identifier, bytes_sent, addr
                        //         ),
                        //         Err(e) => warn!(
                        //             "[UDP SENDER {}] failed to send message to {}: {:?}",
                        //             self.logging_identifier, addr, e
                        //         ),
                        //     }
                        //
                        //     if buffer_end == message_size {
                        //         keep_sending = false;
                        //     } else {
                        //         buffer_position = buffer_end;
                        //         buffer_end = match buffer_position + self.max_udp_packet_size
                        //             < message_size
                        //         {
                        //             true => buffer_position + self.max_udp_packet_size,
                        //             false => message_size,
                        //         };
                        //
                        //         // Slow the sender down!
                        //         sleep(Duration::from_millis(100)).await;
                        //     }
                        //     trace!(
                        //         "[UDP SENDER {}] New buffer start: {}, end: {}",
                        //         self.logging_identifier,
                        //         buffer_position,
                        //         buffer_end
                        //     );
                        // }
                        // self.proto_name.inc_message_destination_type_metric(MessageDestination::SendUdp);
                    // }
                }
            }
        }
    }
    
    async fn single_message(&self, addr: &str, message: &[u8]) {
        trace!("[UDP SENDER {}] Sending all {} bytes of the message to {addr} in a single message.", self.logging_identifier, message.len());
        let Ok(send_message) = timeout(Duration::from_secs(1), self.socket.send_to(message, addr)).await else {
            error!("[UDP SENDER {}] Failed to send message to {addr} after 1 second.", self.logging_identifier);
            self.proto_name.inc_message_destination_type_metric(MessageDestination::SendUdp, false);
            return;
        };
        match send_message {
            Err(message_error) => {
                error!("[UDP SENDER {}] Failed to send message to {addr}: {}", self.logging_identifier, message_error);
                self.proto_name.inc_message_destination_type_metric(MessageDestination::SendUdp, false);
            }
            Ok(bytes_sent) => {
                self.udp_send_complete_buffer_check(addr, message.len(), bytes_sent);
                self.proto_name.inc_message_destination_type_metric(MessageDestination::SendUdp, true);
            }
        }
    }
    
    // Writing this two different ways, one to preserve the current logic, the other to fail early.
    // Solution 1: Current logic...
    #[allow(dead_code)]
    async fn split_message(&self, addr: &str, message: &[u8]) {
        let message_size: usize = message.len();
        let mut keep_sending: bool = true;
        let mut buffer_position: usize = 0;
        let mut buffer_end: usize = self.max_udp_packet_size;
        let buffer_division: f64 = message_size as f64 / self.max_udp_packet_size as f64;
        let expected_packets: u16 = buffer_division.ceil() as u16;
        let mut processing_packet: u16 = 0;
        trace!("[UDP SENDER {}] Message size of {message_size} goes into {} {buffer_division} times. We'll have to send {expected_packets} total packets.",
            self.logging_identifier, self.max_udp_packet_size);
        
        while keep_sending {
            trace!("[UDP SENDER {}] Sending {buffer_position} to {buffer_end} of {message_size} to {addr}", self.logging_identifier);
            processing_packet += 1;
            let send_message_part = timeout(
                Duration::from_secs(1),
                self.socket.send_to(&message[buffer_position..buffer_end], addr)
            ).await;
            match send_message_part {
                Err(_) => warn!("[UDP SENDER {}] Failed to send message chunk {processing_packet}/{expected_packets} to {addr} due to timeout elapsing.", self.logging_identifier),
                Ok(send_not_killed) => {
                    match send_not_killed {
                        Ok(bytes_sent) =>
                            debug!("[UDP SENDER {}] Sent {} bytes to {}",
                                self.logging_identifier, bytes_sent, addr),
                        Err(send_error) =>
                            warn!("[UDP SENDER {}] Failed to send message to {}: {:?}",
                                self.logging_identifier, addr, send_error),
                    }
                    match buffer_end.eq(&message_size) {
                        true => {
                            trace!("[UDP SENDER {}] Buffer exhausted sending to {}, is it now job done?",
                            self.logging_identifier, addr);
                            match expected_packets.eq(&processing_packet) {
                                true => trace!("[UDP SENDER {}] Booyah, math'd good and guessed that we'd send {expected_packets} packets to {addr}", self.logging_identifier),
                                false => trace!("[UDP SENDER {}] ...Missed it, guessed we'd send {expected_packets} packets to {addr} and we sent {processing_packet} instead, laaame!", self.logging_identifier)
                            }
                            keep_sending = false;
                        }
                        false => {
                            buffer_position = buffer_end;
                            let next_max_buffer: usize = buffer_position + self.max_udp_packet_size;
                            buffer_end = match next_max_buffer.lt(&message_size) {
                                true => next_max_buffer,
                                false => message_size,
                            };
                            // Slow the sender down!
                            sleep(Duration::from_millis(100)).await;
                            trace!("[UDP SENDER {}] New buffer start: {}, end: {}",
                                self.logging_identifier, buffer_position, buffer_end);
                        }
                    }
                }
            }
        }
        self.proto_name.inc_message_destination_type_metric(MessageDestination::SendUdp, true);
    }
    
    //Solution 2: Fail faster...
    #[allow(dead_code)]
    async fn split_message_alt(&self, addr: &str, message: &[u8]) {
        let message_size: usize = message.len();
        let mut keep_sending: bool = true;
        let mut buffer_position: usize = 0;
        let mut buffer_end: usize = self.max_udp_packet_size;
        let buffer_division: f64 = message_size as f64 / self.max_udp_packet_size as f64;
        let expected_packets: u16 = buffer_division.ceil() as u16;
        let mut processing_packet: u16 = 0;
        
        trace!("[UDP SENDER {}] Message size of {message_size} goes into {} {buffer_division} times. We'll have to send {expected_packets} total packets.",
            self.logging_identifier, self.max_udp_packet_size);
        
        while keep_sending {
            trace!("[UDP SENDER {}] Sending {buffer_position} to {buffer_end} of {message_size} to {addr}", self.logging_identifier);
            processing_packet += 1;
            
            let Ok(send_not_killed) = timeout(
                Duration::from_secs(1),
                self.socket.send_to(&message[buffer_position..buffer_end], addr)
            ).await else {
                error!("[UDP SENDER {}] Failed to send message chunk {processing_packet}/{expected_packets} to {addr} due to timeout elapsing.", self.logging_identifier);
                self.proto_name.inc_message_destination_type_metric(MessageDestination::SendUdp, false);
                return;
            };
            
            match send_not_killed {
                Err(e) => {
                    error!("[UDP SENDER {}] Failed to send message to {}: {:?}",
                        self.logging_identifier, addr, e);
                    self.proto_name.inc_message_destination_type_metric(MessageDestination::SendUdp, false);
                    return;
                }
                Ok(bytes_sent) => {
                    debug!("[UDP SENDER {}] Sent {bytes_sent} bytes to {addr}",
                                    self.logging_identifier);
                    match buffer_end.eq(&message_size) {
                        true => {
                            trace!("[UDP SENDER {}] Buffer exhausted sending to {}, is it now job done?",
                            self.logging_identifier, addr);
                            match expected_packets.eq(&processing_packet) {
                                true => trace!("[UDP SENDER {}] Booyah, math'd good and guessed that we'd send {expected_packets} packets to {addr}", self.logging_identifier),
                                false => trace!("[UDP SENDER {}] ...Missed it, guessed we'd send {expected_packets} packets to {addr} and we sent {processing_packet} instead, laaame!", self.logging_identifier)
                            }
                            keep_sending = false;
                        }
                        false => {
                            buffer_position = buffer_end;
                            let next_max_buffer: usize = buffer_position + self.max_udp_packet_size;
                            buffer_end = match next_max_buffer.lt(&message_size) {
                                true => next_max_buffer,
                                false => message_size,
                            };
                            // Slow the sender down!
                            sleep(Duration::from_millis(100)).await;
                            trace!("[UDP SENDER {}] New buffer start: {}, end: {}",
                                self.logging_identifier, buffer_position, buffer_end);
                        }
                    }
                }
            }
        }
        
        self.proto_name.inc_message_destination_type_metric(MessageDestination::SendUdp, true);
    }
    
    fn udp_send_complete_buffer_check(&self, addr: &str, msg_len: usize, total_sent_bytes: usize) {
        match msg_len.cmp(&total_sent_bytes) {
            Ordering::Less =>
                warn!("[UDP SENDER {}] Send to {addr} completed, but sent ({total_sent_bytes}) > message ({msg_len}).", self.logging_identifier),
            Ordering::Greater =>
                warn!("[UDP SENDER {}] Send to {addr} completed, but sent ({total_sent_bytes}) < message ({msg_len}).", self.logging_identifier),
            Ordering::Equal =>
                debug!("[UDP SENDER {}] Send to {addr} completed, sent ({total_sent_bytes}) == message ({msg_len}).", self.logging_identifier),
        }
    }
}