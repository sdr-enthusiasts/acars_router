#!/usr/bin/env python3

import os
import argparse
import socket
import logging
from twisted.internet.protocol import DatagramProtocol
from twisted.internet import reactor
from twisted.internet import task

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger('acars_router')

# Globals: Counters
COUNTER_ACARS_MSGS_RX_TOTAL = 0
COUNTER_VDLM2_MSGS_RX_TOTAL = 0
COUNTER_ACARS_MSGS_RX_LAST = 0
COUNTER_VDLM2_MSGS_RX_LAST = 0

def log_counters(trigger_in: int):
    """
    Logs the status of the global counters, then calls itself 5 minutes in the future.

    trigger_in: int
        Number of seconds into the future to trigger this function again
    """
    global COUNTER_ACARS_MSGS_RX_LAST
    global COUNTER_ACARS_MSGS_RX_TOTAL
    global COUNTER_VDLM2_MSGS_RX_LAST
    global COUNTER_VDLM2_MSGS_RX_TOTAL

    log = log.getChild('statistics')

    log.info("ACARS messages received (last {n} mins / lifetime): {last5m}/{life}".format(
        n = int(trigger_in/60),
        last5m = COUNTER_ACARS_MSGS_RX_LAST,
        life = COUNTER_ACARS_MSGS_RX_TOTAL,
    ))
    COUNTER_ACARS_MSGS_RX_LAST = 0
    log.info("VDLM2 messages received (last {n} mins / lifetime): {last5m}/{life}".format(
        n = int(trigger_in/60),
        last5m = COUNTER_VDLM2_MSGS_RX_LAST,
        life = COUNTER_VDLM2_MSGS_RX_TOTAL,
    ))
    COUNTER_VDLM2_MSGS_RX_LAST = 0
    task.deferLater(reactor, trigger_in, log_counters, trigger_in)
    return None

class MessageBuffer:
    """
    A buffer of ACARS/VDLM2 messages.
    Messages are appended to the buffer, and are sent to all clients.

    Attributes
    ----------
    clients : list
        send(data) called on each object in the list when a message is appended.

    Methods
    -------
    add_udp_client(host: str, port: int)
        Adds a UDP client. Messages will be sent in JSON via UDP to this client
    append(data: bytes)
        Appends a message to the buffer.
    """
    def __init__(self):
        self.clients = list()

    def add_udp_client(
        self,
        host: str,
        port: int,
    ):
        self.clients.append(UDPSender(host, port))

    def append(
        self,
        data: bytes,
    ):
        for client in self.clients:
            client.send(data)

class UDPSender():
    """
    Handles sending datagrams via UDP.

    Attributes
    ----------
    host : str
        hostname or IPv4 address datagrams will be sent to
    port : int
        UDP port datagrams will be sent to

    Methods
    -------
    send(data: bytes)
        Sends `bytes` to `host:port`
    """
    def __init__(
        self,
        host: str,
        port: int,
    ):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.host = host
        self.port = port

    def send(
        self,
        data: bytes,
    ):
        # TODO: implement a back-off. If sending fails, don't retry for X seconds
        try:
            self.sock.sendto(data, (self.host, self.port))
        except:
            log.error("Error sending to {host}:{port}/udp".format(
                host=self.host,
                port=self.port,
            ))

class ACARS(DatagramProtocol):
    """
    ACARS (receiving) Protocol

    Attributes
    ----------
    message_buffer : MessageBuffer
        Pointer to a MessageBuffer where messages are delivered
    
    Methods
    -------
    datagramReceived(data, addr)
        Called by twisted reactor upon receive of UDP datagram
    """
    def __init__(
        self,
        message_buffer: MessageBuffer,
    ):
        self.message_buffer = message_buffer

    def datagramReceived(self, data, addr):
        global COUNTER_ACARS_MSGS_RX_LAST
        global COUNTER_ACARS_MSGS_RX_TOTAL
        self.message_buffer.append(data)
        COUNTER_ACARS_MSGS_RX_TOTAL += 1
        COUNTER_ACARS_MSGS_RX_LAST += 1

class VDLM2(DatagramProtocol):
    """
    VDLM2 (receiving) Protocol

    Attributes
    ----------
    message_buffer : MessageBuffer
        Pointer to a MessageBuffer where messages are delivered
    
    Methods
    -------
    datagramReceived(data, addr)
        Called by twisted reactor upon receive of UDP datagram
    """
    def __init__(
        self,
        message_buffer: MessageBuffer,
    ):
        self.message_buffer = message_buffer

    def datagramReceived(self, data, addr):
        global COUNTER_VDLM2_MSGS_RX_LAST
        global COUNTER_VDLM2_MSGS_RX_TOTAL
        self.message_buffer.append(data)
        COUNTER_VDLM2_MSGS_RX_TOTAL += 1
        COUNTER_VDLM2_MSGS_RX_LAST += 1
        
def split_env_safely(
    env: str,
    sep: str=';',
):
    """
    Splits the contents of environment variable `env` with separator `sep`.
    If an exception occurs, return an empty list.
    """
    try:
        return os.getenv(env).split(sep)
    except:
        return list()
    return list()


if __name__ == "__main__":

    # Command line / OS Env
    parser = argparse.ArgumentParser(
        description='Route ACARS/VDLM2 messages'
    )
    parser.add_argument(
        '-lau', '--listen-udp-acars',
        help='UDP port to listen for ACARS messages. Can be specified multiple times to listen on multiple ports. (default: 5550)',
        type=str,
        nargs='*',
        default=os.getenv('AR_LISTEN_UDP_ACARS', "5550").split(';'),
    )
    parser.add_argument(
        '-lvu', '--listen-udp-vdlm2',
        help='UDP port to listen for VDLM2 messages. Can be specified multiple times to listen on multiple ports. (default: 5555)',
        type=str,
        nargs='*',
        default=os.getenv('AR_LISTEN_UDP_VDLM2', "5555").split(';'),
    )
    parser.add_argument(
        '-sua', '--send-udp-acars',
        help='Client to send ACARS messages in host:port format. Can be specified multiple times to send to multiple clients.',
        type=str,
        nargs='*',
        default=split_env_safely('AR_SEND_UDP_ACARS'),
    )
    parser.add_argument(
        '-suv', '--send-udp-vdlm2',
        help='Client to send VDLM2 messages in host:port format. Can be specified multiple times to send to multiple clients.',
        type=str,
        nargs='*',
        default=split_env_safely('AR_SEND_UDP_VDLM2'),
    )
    parser.add_argument(
        '--stats-every',
        help='Print statistics every N minutes (default: 5)',
        type=int,
        nargs='?',
        default=5,
    )
    args = parser.parse_args()

    # Set up message buffers
    acars_mb = MessageBuffer()
    vdlm2_mb = MessageBuffer()

    # Handle `--send-udp-acars` (set up ACARS UDP senders)
    for c in args.send_udp_acars:
        log.info("Forwarding ACARS UDP datagrams to: {client}".format(
            client=c,
        ))
        acars_mb.add_udp_client(c.split(':')[0], int(c.split(':')[1]))

    # Handle `--send-udp-vdlm2` (set up VDLM2 UDP senders)
    for c in args.send_udp_acars:
        log.info("Forwarding VDLM2 UDP datagrams to: {client}".format(
            client=c,
        ))
        vdlm2_mb.add_udp_client(c.split(':')[0], int(c.split(':')[1]))

    # Handle `--listen-udp-acars` (set up ACARS listeners)
    for s in args.listen_udp_acars:
        log.info("Listening for ACARS UDP datagrams on port: {port}".format(
            port=s,
        ))
        reactor.listenUDP(int(s), ACARS(acars_mb))

    # Handle `--listen-udp-vdlm2` (set up VDLM2 listeners)
    for s in args.listen_udp_vdlm2:
        log.info("Listening for VDLM2 UDP datagrams on port: {port}".format(
            port=s,
        ))
        reactor.listenUDP(int(s), VDLM2(vdlm2_mb))

    # Set up stats logging
    task.deferLater(reactor, args.stats_every * 60, log_counters, args.stats_every * 60)

    # Run!
    reactor.run()
