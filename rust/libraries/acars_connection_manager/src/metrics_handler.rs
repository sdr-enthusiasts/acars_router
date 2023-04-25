#![allow(dead_code)]
use acars_metrics::{MESSAGE_PROCESSING, MESSAGES_OUT, MessageDestination, MESSAGES_IN, MessageSource, TOTAL_MESSAGES_BY_TYPE, REJECTED_MESSAGES, MessageRejectionReasons};
use crate::ServerType;

impl ServerType {
    pub(crate) fn inc_messages_received_metric(self) {
        match self {
            ServerType::Vdlm2 => TOTAL_MESSAGES_BY_TYPE.vdlm2.inc(),
            ServerType::Acars => TOTAL_MESSAGES_BY_TYPE.acars.inc()
        }
    }
    
    pub(crate) fn get_messages_received_metric(self) -> u64 {
        match self {
            ServerType::Vdlm2 => TOTAL_MESSAGES_BY_TYPE.vdlm2.get(),
            ServerType::Acars => TOTAL_MESSAGES_BY_TYPE.acars.get()
        }
    }
    
    pub(crate) fn inc_messages_validity_metric(self, is_message_valid: bool) {
        match (self, is_message_valid) {
            (ServerType::Vdlm2, true) => MESSAGE_PROCESSING.vdlm2.valid.inc(),
            (ServerType::Vdlm2, false) => MESSAGE_PROCESSING.vdlm2.invalid.inc(),
            (ServerType::Acars, true) => MESSAGE_PROCESSING.acars.valid.inc(),
            (ServerType::Acars, false) => MESSAGE_PROCESSING.acars.invalid.inc()
        }
    }
    
    pub(crate) fn get_messages_validity_metric(self, is_message_valid: bool) -> u64 {
        match (self, is_message_valid) {
            (ServerType::Vdlm2, true) => MESSAGE_PROCESSING.vdlm2.valid.get(),
            (ServerType::Vdlm2, false) => MESSAGE_PROCESSING.vdlm2.invalid.get(),
            (ServerType::Acars, true) => MESSAGE_PROCESSING.acars.valid.get(),
            (ServerType::Acars, false) => MESSAGE_PROCESSING.acars.invalid.get()
        }
    }
    
    pub(crate) fn inc_message_source_type_metric(self, message_source: MessageSource) {
        match (self, message_source) {
            (ServerType::Vdlm2, MessageSource::ListenUdp) => MESSAGES_IN.vdlm2.listen_udp.inc(),
            (ServerType::Vdlm2, MessageSource::ListenTcp) => MESSAGES_IN.vdlm2.listen_tcp.inc(),
            (ServerType::Vdlm2, MessageSource::ReceiveTcp) => MESSAGES_IN.vdlm2.receive_tcp.inc(),
            (ServerType::Vdlm2, MessageSource::ReceiveZmq) => MESSAGES_IN.vdlm2.receive_zmq.inc(),
            (ServerType::Acars, MessageSource::ListenUdp) => MESSAGES_IN.acars.listen_udp.inc(),
            (ServerType::Acars, MessageSource::ListenTcp) => MESSAGES_IN.acars.listen_tcp.inc(),
            (ServerType::Acars, MessageSource::ReceiveTcp) => MESSAGES_IN.acars.receive_tcp.inc(),
            (ServerType::Acars, MessageSource::ReceiveZmq) => MESSAGES_IN.acars.receive_zmq.inc()
        }
    }
    
    pub(crate) fn get_message_source_type_metric(self, message_source: MessageSource) -> u64 {
        match (self, message_source) {
            (ServerType::Vdlm2, MessageSource::ListenUdp) => MESSAGES_IN.vdlm2.listen_udp.get(),
            (ServerType::Vdlm2, MessageSource::ListenTcp) => MESSAGES_IN.vdlm2.listen_tcp.get(),
            (ServerType::Vdlm2, MessageSource::ReceiveTcp) => MESSAGES_IN.vdlm2.receive_tcp.get(),
            (ServerType::Vdlm2, MessageSource::ReceiveZmq) => MESSAGES_IN.vdlm2.receive_zmq.get(),
            (ServerType::Acars, MessageSource::ListenUdp) => MESSAGES_IN.acars.listen_udp.get(),
            (ServerType::Acars, MessageSource::ListenTcp) => MESSAGES_IN.acars.listen_tcp.get(),
            (ServerType::Acars, MessageSource::ReceiveTcp) => MESSAGES_IN.acars.receive_tcp.get(),
            (ServerType::Acars, MessageSource::ReceiveZmq) => MESSAGES_IN.acars.receive_zmq.get()
        }
    }
    
    pub(crate) fn inc_message_destination_type_metric(self, message_destination: MessageDestination, send_successful: bool) {
        match (self, message_destination, send_successful) {
            (ServerType::Vdlm2, MessageDestination::SendUdp, true) => MESSAGES_OUT.vdlm2.send_udp.sent.inc(),
            (ServerType::Vdlm2, MessageDestination::SendTcp, true) => MESSAGES_OUT.vdlm2.send_tcp.sent.inc(),
            (ServerType::Vdlm2, MessageDestination::ServeTcp, true) => MESSAGES_OUT.vdlm2.serve_tcp.sent.inc(),
            (ServerType::Vdlm2, MessageDestination::ServeZmq, true) => MESSAGES_OUT.vdlm2.serve_zmq.sent.inc(),
            (ServerType::Vdlm2, MessageDestination::SendUdp, false) => MESSAGES_OUT.vdlm2.send_udp.failed.inc(),
            (ServerType::Vdlm2, MessageDestination::SendTcp, false) => MESSAGES_OUT.vdlm2.send_tcp.failed.inc(),
            (ServerType::Vdlm2, MessageDestination::ServeTcp, false) => MESSAGES_OUT.vdlm2.serve_tcp.failed.inc(),
            (ServerType::Vdlm2, MessageDestination::ServeZmq, false) => MESSAGES_OUT.vdlm2.serve_zmq.failed.inc(),
            (ServerType::Acars, MessageDestination::SendUdp, true) => MESSAGES_OUT.acars.send_udp.sent.inc(),
            (ServerType::Acars, MessageDestination::SendTcp, true) => MESSAGES_OUT.acars.send_tcp.sent.inc(),
            (ServerType::Acars, MessageDestination::ServeTcp, true) => MESSAGES_OUT.acars.serve_tcp.sent.inc(),
            (ServerType::Acars, MessageDestination::ServeZmq, true) => MESSAGES_OUT.acars.serve_zmq.sent.inc(),
            (ServerType::Acars, MessageDestination::SendUdp, false) => MESSAGES_OUT.acars.send_udp.failed.inc(),
            (ServerType::Acars, MessageDestination::SendTcp, false) => MESSAGES_OUT.acars.send_tcp.failed.inc(),
            (ServerType::Acars, MessageDestination::ServeTcp, false) => MESSAGES_OUT.acars.serve_tcp.failed.inc(),
            (ServerType::Acars, MessageDestination::ServeZmq, false) => MESSAGES_OUT.acars.serve_zmq.failed.inc(),
        }
    }
    
    pub(crate) fn get_message_destination_metric(self, message_destination: MessageDestination, send_successful: bool) -> u64 {
        match (self, message_destination, send_successful) {
            (ServerType::Vdlm2, MessageDestination::SendUdp, true) => MESSAGES_OUT.vdlm2.send_udp.sent.get(),
            (ServerType::Vdlm2, MessageDestination::SendTcp, true) => MESSAGES_OUT.vdlm2.send_tcp.sent.get(),
            (ServerType::Vdlm2, MessageDestination::ServeTcp, true) => MESSAGES_OUT.vdlm2.serve_tcp.sent.get(),
            (ServerType::Vdlm2, MessageDestination::ServeZmq, true) => MESSAGES_OUT.vdlm2.serve_zmq.sent.get(),
            (ServerType::Vdlm2, MessageDestination::SendUdp, false) => MESSAGES_OUT.vdlm2.send_udp.failed.get(),
            (ServerType::Vdlm2, MessageDestination::SendTcp, false) => MESSAGES_OUT.vdlm2.send_tcp.failed.get(),
            (ServerType::Vdlm2, MessageDestination::ServeTcp, false) => MESSAGES_OUT.vdlm2.serve_tcp.failed.get(),
            (ServerType::Vdlm2, MessageDestination::ServeZmq, false) => MESSAGES_OUT.vdlm2.serve_zmq.failed.get(),
            (ServerType::Acars, MessageDestination::SendUdp, true) => MESSAGES_OUT.acars.send_udp.sent.get(),
            (ServerType::Acars, MessageDestination::SendTcp, true) => MESSAGES_OUT.acars.send_tcp.sent.get(),
            (ServerType::Acars, MessageDestination::ServeTcp, true) => MESSAGES_OUT.acars.serve_tcp.sent.get(),
            (ServerType::Acars, MessageDestination::ServeZmq, true) => MESSAGES_OUT.acars.serve_zmq.sent.get(),
            (ServerType::Acars, MessageDestination::SendUdp, false) => MESSAGES_OUT.acars.send_udp.failed.get(),
            (ServerType::Acars, MessageDestination::SendTcp, false) => MESSAGES_OUT.acars.send_tcp.failed.get(),
            (ServerType::Acars, MessageDestination::ServeTcp, false) => MESSAGES_OUT.acars.serve_tcp.failed.get(),
            (ServerType::Acars, MessageDestination::ServeZmq, false) => MESSAGES_OUT.acars.serve_zmq.failed.get(),
        }
    }
    
    pub(crate) fn inc_rejected_messages(self, rejection_type: MessageRejectionReasons) {
        match (self, rejection_type) {
            (ServerType::Acars, MessageRejectionReasons::NoTimestamp) => REJECTED_MESSAGES.acars.no_timestamp.inc(),
            (ServerType::Acars, MessageRejectionReasons::TimestampInFuture) => REJECTED_MESSAGES.acars.timestamp_in_future.inc(),
            (ServerType::Acars, MessageRejectionReasons::MessageTooOld) => REJECTED_MESSAGES.acars.message_too_old.inc(),
            (ServerType::Acars, MessageRejectionReasons::HashingFailed) => REJECTED_MESSAGES.acars.hashing_failed.inc(),
            (ServerType::Acars, MessageRejectionReasons::DuplicateMessage) => REJECTED_MESSAGES.acars.duplicate_message.inc(),
            (ServerType::Vdlm2, MessageRejectionReasons::NoTimestamp) => REJECTED_MESSAGES.vdlm2.no_timestamp.inc(),
            (ServerType::Vdlm2, MessageRejectionReasons::TimestampInFuture) => REJECTED_MESSAGES.vdlm2.timestamp_in_future.inc(),
            (ServerType::Vdlm2, MessageRejectionReasons::MessageTooOld) => REJECTED_MESSAGES.vdlm2.message_too_old.inc(),
            (ServerType::Vdlm2, MessageRejectionReasons::HashingFailed) => REJECTED_MESSAGES.vdlm2.hashing_failed.inc(),
            (ServerType::Vdlm2, MessageRejectionReasons::DuplicateMessage) => REJECTED_MESSAGES.vdlm2.duplicate_message.inc()
        }
    }
    
    pub(crate) fn get_rejected_messages(self, rejection_type: MessageRejectionReasons) -> u64 {
        match (self, rejection_type) {
            (ServerType::Acars, MessageRejectionReasons::NoTimestamp) => REJECTED_MESSAGES.acars.no_timestamp.get(),
            (ServerType::Acars, MessageRejectionReasons::TimestampInFuture) => REJECTED_MESSAGES.acars.timestamp_in_future.get(),
            (ServerType::Acars, MessageRejectionReasons::MessageTooOld) => REJECTED_MESSAGES.acars.message_too_old.get(),
            (ServerType::Acars, MessageRejectionReasons::HashingFailed) => REJECTED_MESSAGES.acars.hashing_failed.get(),
            (ServerType::Acars, MessageRejectionReasons::DuplicateMessage) => REJECTED_MESSAGES.acars.duplicate_message.get(),
            (ServerType::Vdlm2, MessageRejectionReasons::NoTimestamp) => REJECTED_MESSAGES.vdlm2.no_timestamp.get(),
            (ServerType::Vdlm2, MessageRejectionReasons::TimestampInFuture) => REJECTED_MESSAGES.vdlm2.timestamp_in_future.get(),
            (ServerType::Vdlm2, MessageRejectionReasons::MessageTooOld) => REJECTED_MESSAGES.vdlm2.message_too_old.get(),
            (ServerType::Vdlm2, MessageRejectionReasons::HashingFailed) => REJECTED_MESSAGES.vdlm2.hashing_failed.get(),
            (ServerType::Vdlm2, MessageRejectionReasons::DuplicateMessage) => REJECTED_MESSAGES.vdlm2.duplicate_message.get()
        }
    }
}

