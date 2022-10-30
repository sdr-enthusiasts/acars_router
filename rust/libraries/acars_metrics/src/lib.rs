#![allow(unused_imports)]
#[macro_use] extern crate lazy_static;
#[macro_use] pub extern crate prometheus as pm;
#[macro_use] extern crate log;
extern crate prometheus_exporter;
extern crate acars_logging;
#[macro_use] extern crate prometheus_static_metric;
extern crate core;

use core::fmt;
use std::fmt::Formatter;
use prometheus::{IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Histogram, HistogramVec};
use std::net::SocketAddr;
use prometheus_exporter::{Error, Exporter};

pub trait SetupMetrics {
    fn enable_metrics(self);
}

impl SetupMetrics for Result<SocketAddr, std::net::AddrParseError> {
    fn enable_metrics(self) {
        match self {
            Err::<_, std::net::AddrParseError>(address_error) => error!("Failed to parse socket address: {}", &address_error),
            Ok::<SocketAddr, _>(binding_address) => {
                if let Err::<_, Error>(metrics_error) = prometheus_exporter::start(binding_address) {
                    error!("Failed to start metrics exporter on port 9090: {:?}", &metrics_error);
                }
            }
        }
    }
}

make_static_metric! {
    pub struct MessageSourceCounter: IntCounter {
        "message_type" => {
            vdlm2,
            acars
        },
        "source" => {
            listen_udp,
            listen_tcp,
            receive_tcp,
            receive_zmq
        }
    }
    
    pub struct MessageDestinationCounter: IntCounter {
        "message_type" => {
            vdlm2,
            acars
        },
        "destination" => {
            send_udp,
            send_tcp,
            serve_tcp,
            serve_zmq
        }
    }
    
    pub struct MessageProcessing: IntCounter {
        "message_type" => {
            vdlm2,
            acars
        },
        "status" => {
            valid,
            invalid
        }
    }
    
    pub struct ReceivedMessages: IntCounter {
        "message_type" => {
            vdlm2,
            acars
        }
    }
    
    pub struct RejectedMessages: IntCounter {
        "message_type" => {
            vdlm2,
            acars
        },
        "rejection_reason" => {
            no_timestamp,
            timestamp_in_future,
            message_too_old,
            hashing_failed,
            duplicate_message
        }
    }
}

lazy_static! {
    pub static ref MESSAGE_PROCESSING_RESULT_BY_TYPE: IntCounterVec = register_int_counter_vec!(
        "ar_message_processing_result",
        "Count of valid/invalid messagess",
        &["message_type", "status"]
    ).expect("Failed to define metric MESSAGE_PROCESSING");
    pub static ref MESSAGE_PROCESSING: MessageProcessing = MessageProcessing::from(&MESSAGE_PROCESSING_RESULT_BY_TYPE);
    pub static ref TOTAL_PROCESSED_MESSAGES_BY_TYPE: IntCounterVec = register_int_counter_vec!(
        "ar_total_processed_messages_by_type",
        "Count of total messages processed by acars_router by message type",
        &["message_type"]
    ).expect("Failed to define metric TOTAL_PROCESSED_MESSAGES");
    pub static ref TOTAL_MESSAGES_BY_TYPE: ReceivedMessages = ReceivedMessages::from(&TOTAL_PROCESSED_MESSAGES_BY_TYPE);
    pub static ref MESSAGES_BY_SOURCE_TYPE: IntCounterVec = register_int_counter_vec!(
        "ar_message_in_type",
        "Count of messages in and out by source, destination and message type",
        &["message_type", "source"]
    ).expect("Failed to define metric MESSAGES_BY_SOURCE_TYPE");
    pub static ref MESSAGES_IN: MessageSourceCounter = MessageSourceCounter::from(&MESSAGES_BY_SOURCE_TYPE);
    pub static ref MESSAGES_BY_DESTINATION_TYPE: IntCounterVec = register_int_counter_vec!(
        "ar_message_out_type",
        "Count of messages in and out by source, destination and message type",
        &["message_type", "destination"]
    ).expect("Failed to define metric MESSAGES_BY_DESTINATION_TYPE");
    pub static ref MESSAGES_OUT: MessageDestinationCounter = MessageDestinationCounter::from(&MESSAGES_BY_DESTINATION_TYPE);
    pub static ref REJECTED_MESSAGES_BY_REASON_TYPE: IntCounterVec = register_int_counter_vec!(
        "ar_rejected_messages",
        "Count of rejected messages by reason and message type",
        &["message_type", "rejection_reason"]
    ).expect("Failed to define metric REJECTED_MESSAGES_BY_REASON_TYPE");
    pub static ref REJECTED_MESSAGES: RejectedMessages = RejectedMessages::from(&REJECTED_MESSAGES_BY_REASON_TYPE);
}

#[derive(Debug, Clone, Copy, Default)]
pub enum MessageSource {
    #[default]
    ListenUdp,
    ListenTcp,
    ReceiveTcp,
    ReceiveZmq
}

impl fmt::Display for MessageSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            MessageSource::ListenUdp => write!(f, "listen_udp"),
            MessageSource::ListenTcp => write!(f, "listen_tcp"),
            MessageSource::ReceiveTcp => write!(f, "receive_tcp"),
            MessageSource::ReceiveZmq => write!(f, "receive_zmq"),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub enum MessageDestination {
    #[default]
    SendUdp,
    SendTcp,
    ServeTcp,
    ServeZmq
}

impl fmt::Display for MessageDestination {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            MessageDestination::SendUdp => write!(f, "send_udp"),
            MessageDestination::SendTcp => write!(f, "send_tcp"),
            MessageDestination::ServeTcp => write!(f, "serve_tcp"),
            MessageDestination::ServeZmq => write!(f, "serve_zmq")
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub enum MessageRejectionReasons {
    NoTimestamp,
    TimestampInFuture,
    #[default]
    MessageTooOld,
    HashingFailed,
    DuplicateMessage
}

impl fmt::Display for MessageRejectionReasons {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            MessageRejectionReasons::NoTimestamp => write!(f, "No Timestamp"),
            MessageRejectionReasons::TimestampInFuture => write!(f, "Timestamp In Future"),
            MessageRejectionReasons::MessageTooOld => write!(f, "Message Too Old"),
            MessageRejectionReasons::HashingFailed => write!(f, "Hashing Failed"),
            MessageRejectionReasons::DuplicateMessage => write!(f, "Duplicate Message"),
        }
    }
}