#!/usr/bin/env python3

import socket
from twisted.logger import Logger
from twisted.internet.protocol import DatagramProtocol
from twisted.internet import reactor

log = Logger()

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
        print("sending to {host}:{port}".format(
            host=self.host,
            port=self.port,
        ))
        # TODO: implement a back-off. If sending fails, don't retry for X seconds
        try:
            self.sock.sendto(data, (self.host, self.port))
        except:
            log.failure("Failed sending to {host}:{port}/udp".format(
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
        print("ACARS: received {!r} from {}".format(data, addr))
        self.message_buffer.append(data)

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
        print("VDLM2: received {!r} from {}".format(data, addr))
        self.transport.write(data, addr)
        self.message_buffer.append(data)

acars_mb = MessageBuffer()
acars_mb.add_udp_client("192.168.69.35",5550)

vdlm2_mb = MessageBuffer()
vdlm2_mb.add_udp_client("192.168.69.35",5555)

reactor.listenUDP(25550, ACARS(acars_mb))
reactor.listenUDP(25555, VDLM2(vdlm2_mb))
reactor.run()
