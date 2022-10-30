// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

use acars_config::Input;
use acars_vdlm2_parser::{AcarsVdlm2Message, DecodeMessage, MessageResult};
use std::collections::hash_map::DefaultHasher;
use std::collections::vec_deque::VecDeque;
use std::env;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use async_trait::async_trait;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};
use acars_metrics::{MessageRejectionReasons, MessageSource};
use crate::packet_handler::{PacketHandler, ProcessAssembly};
use crate::{ServerType, SocketType};

#[derive(Clone, Debug, Default)]
pub struct MessageHandlerConfig {
    pub add_proxy_id: bool,
    pub dedupe: bool,
    pub dedupe_window: u64,
    pub skew_window: u64,
    pub queue_type: ServerType,
    pub should_override_station_name: bool,
    pub station_name: String,
    pub stats_every: u64,
}

impl MessageHandlerConfig {
    pub(crate) fn new(args: &Input, queue_type: ServerType) -> Self {
        if let Some(station_name) = &args.override_station_name {
            Self {
                add_proxy_id: args.add_proxy_id,
                dedupe: args.enable_dedupe,
                dedupe_window: args.dedupe_window,
                skew_window: args.skew_window,
                queue_type,
                should_override_station_name: args.override_station_name.is_some(),
                station_name: station_name.to_string(),
                stats_every: args.stats_every,
            }
        } else {
            Self {
                add_proxy_id: args.add_proxy_id,
                dedupe: args.enable_dedupe,
                dedupe_window: args.dedupe_window,
                skew_window: args.skew_window,
                queue_type,
                should_override_station_name: false,
                station_name: Default::default(),
                stats_every: args.stats_every,
            }
        }
    }

    pub(crate) async fn watch_message_queue(
        self,
        mut input_queue: Receiver<String>,
        output_queue: Sender<AcarsVdlm2Message>,
    ) {
        let dedupe_queue: Arc<Mutex<VecDeque<(u64, u64)>>> =
            Arc::new(Mutex::new(VecDeque::with_capacity(100)));
        let total_messages_processed: Arc<Mutex<i32>> = Arc::new(Mutex::new(0));
        let total_messages_since_last: Arc<Mutex<i32>> = Arc::new(Mutex::new(0));
        let stats_every: u64 = self.stats_every * 60; // Value has to be in seconds. Input is in minutes.
        let version: &str = env!("CARGO_PKG_VERSION");

        // Generate an async loop that sleeps for the requested stats print duration and then logs
        // Give it the context for the counters
        // The stats values to the console.

        let stats_total_messages_context: Arc<Mutex<i32>> = Arc::clone(&total_messages_processed);
        let stats_total_messages_since_last_context: Arc<Mutex<i32>> =
            Arc::clone(&total_messages_since_last);

        tokio::spawn(async move {
            print_stats(
                stats_total_messages_context,
                stats_total_messages_since_last_context,
                stats_every,
                self.queue_type,
            )
            .await;
        });

        // Generate an async loop that sleeps for the requested dedupe window and then cleans the queue
        // Give it the context for the dedupe queue
        // The dedupe queue to be cleaned.
        if self.dedupe {
            let dedupe_queue_context: Arc<Mutex<VecDeque<(u64, u64)>>> = Arc::clone(&dedupe_queue);
            let dedupe_window: u64 = self.dedupe_window;

            tokio::spawn(async move {
                clean_up_dedupe_queue(
                    dedupe_queue_context,
                    dedupe_window,
                    self.queue_type,
                )
                .await;
            });
        }

        while let Some(message_content) = input_queue.recv().await {
            // Grab the mutexes for the stats counter and increment the total messages processed by the message handler.
            let parse_message: MessageResult<AcarsVdlm2Message> = message_content.decode_message();
            let stats_total_loop_context: Arc<Mutex<i32>> = Arc::clone(&total_messages_processed);
            let stats_total_loop_since_last_context: Arc<Mutex<i32>> =
                Arc::clone(&total_messages_since_last);
            let dedupe_queue_loop: Arc<Mutex<VecDeque<(u64, u64)>>> = Arc::clone(&dedupe_queue);
            *stats_total_loop_since_last_context.lock().await += 1;
            *stats_total_loop_context.lock().await += 1;
            self.queue_type.inc_messages_received_metric();

            trace!(
                "[Message Handler {}] GOT: {:?}",
                self.queue_type, parse_message);

            match parse_message {
                Err(parse_error) => {
                    error!("[Message Handler {}] Failed to parse received message: {}\nReceived: {}",
                    self.queue_type, parse_error, message_content);
                    self.queue_type.inc_messages_validity_metric(false);
                },
                Ok(mut message) => {
                    self.queue_type.inc_messages_validity_metric(true);
                    let current_time: f64 = match SystemTime::now().duration_since(UNIX_EPOCH) {
                        Ok(n) => n.as_secs_f64(),
                        Err(_) => f64::default(),
                    };

                    match message.get_time() {
                        None => {
                            error!("[Message Handler {}] Message has no timestamp field. Skipping message.", self.queue_type);
                            self.queue_type.inc_rejected_messages(MessageRejectionReasons::NoTimestamp);
                            continue;
                        }
                        Some(mut message_time) => {
                            if message_time > current_time {
                                if (message_time - current_time) > self.skew_window as f64 {
                                    error!("[Message Handler {}] Message is from the future. Skipping. Current time {}, Message time {}. Difference {}", self.queue_type, current_time, message_time, message_time - current_time);
                                    self.queue_type.inc_rejected_messages(MessageRejectionReasons::TimestampInFuture);
                                    continue;
                                }
                                trace!("[Message Handler {}] Message is from the future. Current time {}, Message time {}. Difference {}", self.queue_type, current_time, message_time, message_time - current_time);
                                message_time = current_time;
                            }
                            // If the message is older than the skew window, reject it
                            else if (current_time - message_time) > self.skew_window as f64 {
                                error!("[Message Handler {}] Message is {} seconds old. Time in message {}. Skipping message. {}", self.queue_type, current_time - message_time, message_time, self.skew_window);
                                self.queue_type.inc_rejected_messages(MessageRejectionReasons::MessageTooOld);
                                continue;
                            }

                            // Time to hash the message
                            // let hash_message: MessageResult<u64> = hash_message(message.clone());

                            match message.clone().hash_message() {
                                Err(hash_parsing_error) => {
                                    error!("[Message Handler {}] Failed to create hash of message: {}",
                                    self.queue_type, hash_parsing_error);
                                    self.queue_type.inc_rejected_messages(MessageRejectionReasons::HashingFailed);
                                },
                                Ok(hashed_value) => {
                                    if self.dedupe {
                                        let mut rejected: bool = false;
                                        for (time, hashed_value_saved) in
                                            dedupe_queue_loop.lock().await.iter()
                                        {
                                            let f64_time: f64 = *time as f64;
                                            if *hashed_value_saved == hashed_value
                                                && current_time - f64_time
                                                    < self.dedupe_window as f64
                                            // Both the time and hash have to be equal to reject the message
                                            {
                                                info!("[Message Handler {}] Message is a duplicate. Skipping message.", self.queue_type);
                                                self.queue_type.inc_rejected_messages(MessageRejectionReasons::DuplicateMessage);
                                                rejected = true;
                                                break;
                                            }
                                        }

                                        if rejected {
                                            continue;
                                        } else {
                                            dedupe_queue_loop
                                                .lock()
                                                .await
                                                .push_back((message_time as u64, hashed_value));
                                        }
                                    }

                                    if self.should_override_station_name {
                                        trace!("[Message Handler {}] Overriding station name to {}",
                                            self.queue_type, self.station_name);
                                        message.set_station_name(&self.station_name);
                                    }

                                    if self.add_proxy_id {
                                        trace!("[Message Handler {}] Adding proxy_id to message",
                                            self.queue_type
                                        );
                                        message.set_proxy_details("acars_router", version);
                                    }

                                    debug!("[Message Handler {}] SENDING: {:?}",
                                        self.queue_type, message);
                                    trace!("[Message Handler {}] Hashed value: {}",
                                        self.queue_type, hashed_value);
                                    trace!("[Message Handler {}] Final message: {:?}",
                                        self.queue_type, message);

                                    match output_queue.send(message).await {
                                        Ok(_) => debug!("[Message Handler {}] Message sent to output queue", self.queue_type),
                                        Err(e) => error!("[Message Handler {}] Error sending message to output queue: {}", self.queue_type, e)
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

pub async fn print_stats(
    total_all_time: Arc<Mutex<i32>>,
    total_since_last: Arc<Mutex<i32>>,
    stats_every: u64,
    queue_type: ServerType,
) {
    let stats_minutes: u64 = stats_every / 60;
    loop {
        sleep(Duration::from_secs(stats_every)).await;
        info!("{} in the last {} minute(s):\nTotal messages processed: {}\nTotal messages processed since last update: {}",
            queue_type, stats_minutes, queue_type.get_messages_received_metric(), total_since_last.lock().await);
        info!("{} in the last {} minute(s):\nTotal messages processed: {}\nTotal messages processed since last update: {}",
            queue_type, stats_minutes, total_all_time.lock().await, total_since_last.lock().await);
        *total_since_last.lock().await = 0;
    }
}

pub async fn clean_up_dedupe_queue(
    dedupe_queue: Arc<Mutex<VecDeque<(u64, u64)>>>,
    dedupe_window: u64,
    queue_type: ServerType,
) {
    loop {
        sleep(Duration::from_secs(dedupe_window)).await;
        // Remove old messages from the dedupe_queue
        if !dedupe_queue.lock().await.is_empty() {
            // iterate through dedupe_que and remove old messages
            let current_time: u64 = match SystemTime::now().duration_since(UNIX_EPOCH) {
                Ok(n) => n.as_secs(),
                Err(_) => 0,
            };

            dedupe_queue.lock().await.retain(|message: &(u64, u64) | {
                let (timestamp, _) = message;
                let diff: u64 = current_time - timestamp;
                diff <= dedupe_window
            });

            debug!(
                "[Message Handler {}] dedupe queue size after pruning {}",
                queue_type,
                dedupe_queue.lock().await.len()
            );
        }
    }
}

pub(crate) trait HashReceivedMessage {
    fn hash_message(self) -> MessageResult<u64>;
}

impl HashReceivedMessage for AcarsVdlm2Message {
    fn hash_message(mut self) -> MessageResult<u64> {
        let mut hasher: DefaultHasher = DefaultHasher::new();
        self.clear_proxy_details(); // Clears out "app"
        self.clear_error(); // Clears out "error"
        self.clear_level(); // Clears out "level"
        self.clear_station_name(); // Clears out "vdl2.station" or "station_id"
        self.clear_time(); // Clears out "timestamp" or "vdl2.t"
        self.clear_channel(); // Clears out "channel"
        self.clear_freq_skew(); // Clears out "vdl2.freq_skew"
        self.clear_hdr_bits_fixed(); // Clears out "vdl2.hdr_bits_fixed"
        self.clear_noise_level(); // Clears out "vdl2.noise_level"
        self.clear_octets_corrected_by_fec(); // Clears out "vdl2.octets_corrected_by_fec"
        self.clear_sig_level(); // Clears out "vdl2.sig_level"
        match self.to_string() {
            Err(parse_error) => Err(parse_error),
            Ok(msg) => {
                trace!("[Hasher] Message to be hashed: {}", msg);
                msg.hash(&mut hasher);
                let hashed_value: u64 = hasher.finish();
                Ok(hashed_value)
            }
        }
    }
}

#[async_trait]
pub(crate) trait ProcessSocketListenerMessages {
    async fn process_messages(self, packet_handler: &PacketHandler, peer: &SocketAddr, channel: &Sender<String>, logging_identifier: &str, socket_type: SocketType, proto_name: ServerType);
}

#[async_trait]
impl ProcessSocketListenerMessages for Vec<&str> {
    async fn process_messages(self,
                              packet_handler: &PacketHandler,
                              peer: &SocketAddr,
                              channel: &Sender<String>,
                              logging_identifier: &str,
                              socket_type: SocketType,
                              proto_name: ServerType) {
        for msg_by_newline in self {
            let split_messages_by_brackets: Vec<&str> = msg_by_newline.split_terminator("}{").collect();
            if split_messages_by_brackets.len().eq(&1) {
                packet_handler
                    .attempt_message_reassembly(split_messages_by_brackets[0].to_string(), peer)
                    .await
                    .process_reassembly(logging_identifier, channel, socket_type)
                    .await;
                proto_name.inc_message_source_type_metric(MessageSource::ListenUdp);
            } else {
                // We have a message that was split by brackets if the length is greater than one
                for (count, msg_by_brackets) in
                split_messages_by_brackets.iter().enumerate() {
                    let final_message: String = if count == 0 {
                        // First case is the first element, which should only ever need a single closing bracket
                        trace!("[{} SERVER: {}] Multiple messages received in a packet.",
                                                            socket_type, logging_identifier);
                        format!("{}}}", msg_by_brackets)
                    } else if count == split_messages_by_brackets.len() - 1 {
                        // This case is for the last element, which should only ever need a single opening bracket
                        trace!("[{} SERVER: {}] End of a multiple message packet",
                                                            socket_type, logging_identifier);
                        format!("{{{}", msg_by_brackets)
                    } else {
                        // This case is for any middle elements, which need both an opening and closing bracket
                        trace!("[{} SERVER: {}] Middle of a multiple message packet",
                                                            socket_type, logging_identifier);
                        format!("{{{}}}", msg_by_brackets)
                    };
                    packet_handler
                        .attempt_message_reassembly(
                            final_message,
                            peer,
                        ).await
                        .process_reassembly(
                            &logging_identifier,
                            &channel,
                            socket_type,
                        ).await;
                    proto_name.inc_message_source_type_metric(MessageSource::ListenUdp);
                }
            }
        }
    }
}