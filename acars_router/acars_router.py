#!/usr/bin/env python3

import argparse
import collections
import copy
import json
import logging
import os
import queue
import socket
import socketserver
import sys
import threading
import time
import uuid
import zmq


# HELPER CLASSES #


class ARQueue(queue.Queue):
    """
    A subclass of queue.Queue, allowing us to name the queue.
    """
    def __init__(self, name: str, maxsize: int = 0):
        self.name = name
        super().__init__(maxsize=maxsize)


class ARCounters():
    """
    A class to be used for counters
    """
    def __init__(self):

        # ACARS messages received via udp
        self.listen_udp_acars = 0

        # ACARS messages received via TCP (where we are the server)
        self.listen_tcp_acars = 0

        # ACARS messages received via TCP (where we are the client)
        self.receive_tcp_acars = 0

        # VDLM2 messages received via udp
        self.listen_udp_vdlm2 = 0

        # VDLM2 messages received via TCP (where we are the server)
        self.listen_tcp_vdlm2 = 0

        # VDLM2 messages received via TCP (where we are the client)
        self.receive_tcp_vdlm2 = 0

        # VDLM2 messages received via ZMQ (where we are the client)
        self.receive_zmq_vdlm2 = 0

        # Invalid message counts
        self.invalid_json_acars = 0
        self.invalid_json_vdlm2 = 0

        # duplicate messages dropped
        self.duplicate_acars = 0
        self.duplicate_vdlm2 = 0

        self.queue_lists = list()
        self.standalone_queues = list()
        self.standalone_deque = list()

    def log(self, logger: logging.Logger, level: int):
        if self.listen_udp_acars > 0:
            logger.log(level, f"ACARS messages via UDP listen: {self.listen_udp_acars}")
        if self.listen_tcp_acars > 0:
            logger.log(level, f"ACARS messages via TCP listen: {self.listen_tcp_acars}")
        if self.receive_tcp_acars > 0:
            logger.log(level, f"ACARS messages via TCP receive: {self.receive_tcp_acars}")
        if self.listen_udp_vdlm2 > 0:
            logger.log(level, f"VDLM2 messages via UDP listen: {self.listen_udp_vdlm2}")
        if self.listen_tcp_vdlm2 > 0:
            logger.log(level, f"VDLM2 messages via TCP listen: {self.listen_tcp_vdlm2}")
        if self.receive_tcp_vdlm2 > 0:
            logger.log(level, f"VDLM2 messages via TCP receive: {self.receive_tcp_vdlm2}")
        if self.receive_zmq_vdlm2 > 0:
            logger.log(level, f"VDLM2 messages via ZMQ receive: {self.receive_zmq_vdlm2}")
        if self.invalid_json_acars > 0:
            logger.log(level, f"Invalid ACARS JSON messages: {self.invalid_json_acars}")
        if self.invalid_json_vdlm2 > 0:
            logger.log(level, f"Invalid VDLM2 JSON messages: {self.invalid_json_vdlm2}")
        if self.duplicate_acars > 0:
            logger.log(level, f"Duplicate ACARS messages dropped: {self.duplicate_acars}")
        if self.duplicate_vdlm2 > 0:
            logger.log(level, f"Duplicate VDLM2 messages dropped: {self.duplicate_vdlm2}")

        # Log queue depths (TODO: should probably be debug level)
        for q in self.standalone_queues:
            # qs = q.qsize()
            # if qs > 0:
            logger.log(logging.DEBUG, f"Queue depth of {q.name}: {q.qsize()}")
        for queue_list in self.queue_lists:
            for q in queue_list:
                # qs = q.qsize()
                # if qs > 0:
                logger.log(logging.DEBUG, f"Queue depth of {q.name}: {q.qsize()}")
        for dq in self.standalone_deque:
            dqlen = len(dq[1])
            # if dqlen > 0:
            logger.log(logging.DEBUG, f"Queue depth of {dq[0]}: {dqlen}")

    def register_queue_list(self, qlist: list):
        self.queue_lists.append(qlist)

    def register_queue(self, q: ARQueue):
        self.standalone_queues.append(q)

    def register_deque(self, name: str, dq: collections.deque):
        self.standalone_deque.append((name, dq,))

    def increment(self, counter):
        """
        Increment a counter.
        eg: COUNTER.increment('receive_tcp_acars')
        """
        setattr(self, counter, getattr(self, counter) + 1)
        return None


# RECEIVING MESSAGES #


class ThreadedUDPServer(socketserver.ThreadingMixIn, socketserver.UDPServer):
    """ Mix-in for multi-threaded UDP server """
    def __init__(self, server_address, RequestHandlerClass, bind_and_activate=True, inbound_message_queue=None, protoname=None):
        self.inbound_message_queue = inbound_message_queue
        self.protoname = protoname.lower()
        socketserver.UDPServer.__init__(self, server_address, RequestHandlerClass, bind_and_activate=bind_and_activate)


class InboundUDPMessageHandler(socketserver.BaseRequestHandler):
    """ Multi-threaded UDP server to receive ACARS/VDLM2 messages """
    def __init__(self, request, client_address, server):

        # prepare logging
        self.logger = baselogger.getChild(f'input.udp.{server.protoname}')
        # self.logger.debug("spawned")

        # store variables in server object, so they can be accessed in handle() when message arrives
        self.inbound_message_queue = server.inbound_message_queue
        self.protoname = server.protoname

        # perform init of super class (socketserver.BaseRequestHandler)
        socketserver.BaseRequestHandler.__init__(self, request, client_address, server)

    def handle(self):

        # break request/socket data out into separate variables
        data = self.request[0]
        # socket = self.request[1]  # commented out for flake8 whinging about unused variables
        host = self.client_address[0]
        port = self.client_address[1]

        # prepare logging
        self.logger = baselogger.getChild(f'input.udp.{self.protoname}.{host}:{port}')

        # prepare data to be enqueued into input queue
        incoming_data = {
            'raw_json': copy.deepcopy(data),            # raw json data
            'src_host': copy.deepcopy(host),            # src host
            'src_port': copy.deepcopy(port),            # src port
            'src_name': f'input.udp.{self.protoname}',  # function originating the data
            'msg_uuid': uuid.uuid1(),                   # unique identifier for this message
        }

        # trace logging
        self.logger.log(logging_TRACE, f"in: {host}:{port}/udp; out: {self.inbound_message_queue.name}; data: {incoming_data}")

        # enqueue the data
        self.inbound_message_queue.put(incoming_data)

        # increment counters
        COUNTERS.increment(f'listen_udp_{self.protoname}')


class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    """ Mix-in for multi-threaded UDP server """
    def __init__(self, server_address, RequestHandlerClass, bind_and_activate=True, inbound_message_queue=None, protoname=None):
        self.inbound_message_queue = inbound_message_queue
        self.protoname = protoname.lower()
        socketserver.UDPServer.__init__(self, server_address, RequestHandlerClass, bind_and_activate=bind_and_activate)


class InboundTCPMessageHandler(socketserver.BaseRequestHandler):
    """ Multi-threaded TCP server to receive ACARS/VDLM2 messages """
    def __init__(self, request, client_address, server):
        self.logger = baselogger.getChild(f'input.tcpserver.{server.protoname}')
        self.logger.debug("spawned")
        self.inbound_message_queue = server.inbound_message_queue
        self.protoname = server.protoname
        socketserver.BaseRequestHandler.__init__(self, request, client_address, server)

    def handle(self):

        # break socket data down into separate variables
        host = self.client_address[0]
        port = self.client_address[1]

        # prepare logging
        self.logger = baselogger.getChild(f'input.tcpserver.{self.protoname}.{host}:{port}')
        self.logger.info("connection established")

        # loop until the session is disconnected (we receive no data)
        while True:

            # read data from socket
            data = self.request.recv(16384)

            # if nothing returned from the socket, then "disconnect"
            if not data:
                break

            # prepare data to be enqueued into input queue
            incoming_data = {
                'raw_json': copy.deepcopy(data),                                # raw json data
                'src_host': copy.deepcopy(host),                                # src host
                'src_port': copy.deepcopy(port),                                # src port
                'src_name': f'input.tcpserver.{self.protoname}.{host}:{port}',  # function originating the data
                'msg_uuid': uuid.uuid1(),                                       # unique identifier for this message
            }

            # trace logging
            self.logger.log(logging_TRACE, f"in: {host}:{port}/tcp; out: {self.inbound_message_queue.name}; data: {incoming_data}")

            # enqueue the data
            self.inbound_message_queue.put(incoming_data)

            # increment counters
            COUNTERS.increment(f'listen_tcp_{self.protoname}')

        # if broken out of the loop, then "connection lost"
        self.logger.info("connection lost")


def TCPReceiver(host: str, port: int, inbound_message_queue: ARQueue, protoname: str):
    """
    Process to receive ACARS/VDLM2 messages from a TCP server.
    Intended to be run in a thread.
    """
    protoname = protoname.lower()

    # prepare logging
    logger = baselogger.getChild(f'input.tcpclient.{protoname}.{host}:{port}')
    logger.debug("spawned")

    # loop until the session is disconnected
    while True:

        # prepare socket
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # set up socket
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            # attempt connection
            logger.debug("attempting to connect")

            # set socket connect timeout
            sock.settimeout(1)

            # attempt to connect
            try:
                sock.connect((host, port))
            except Exception as e:
                logger.error(f"connection error: {e}")
                time.sleep(10)

            # if connected with no exception
            else:
                logger.info("connection established")
                sock.settimeout(1)

                # while connected
                while True:

                    # try to receive data
                    try:
                        data = sock.recv(16384)

                    except TimeoutError:
                        pass

                    except Exception as e:
                        logger.error(f"error receiving data: {e}")

                    # if data received with no exception
                    else:

                        # if we received something
                        if data:

                            # prepare data to be enqueued into input queue
                            incoming_data = {
                                'raw_json': copy.deepcopy(data),                           # raw json data
                                'src_host': copy.deepcopy(host),                           # src host
                                'src_port': copy.deepcopy(port),                           # src port
                                'src_name': f'input.tcpclient.{protoname}.{host}:{port}',  # function originating the data
                                'msg_uuid': uuid.uuid1(),                                  # unique identifier for this message
                            }

                            # trace logging
                            logger.log(logging_TRACE, f"in: {host}:{port}/tcp; out: {inbound_message_queue.name}; data: {incoming_data}")

                            # enqueue the data
                            inbound_message_queue.put(incoming_data)

                            # increment counters
                            COUNTERS.increment(f'receive_tcp_{protoname}')

                        # if we received nothing, then "connection lost"
                        else:
                            logger.info("connection lost")

                            # close the socket
                            sock.close()

                            # sleep for a second (slow down reconnection attempts)
                            time.sleep(1)

                            # break out of inner loop, so reconnection can happen
                            break


# TODO: add test for this input in github action
def ZMQReceiver(host: str, port: int, inbound_message_queue: ARQueue, protoname: str):
    """
    Process to receive ACARS/VDLM2 messages from a TCP server.
    Intended to be run in a thread.
    """
    protoname = protoname.lower()

    # prepare logging
    logger = baselogger.getChild(f'input.zmqclient.{protoname}.{host}:{port}')
    logger.debug("spawned")

    # Prepare zmq context and sockets
    context = zmq.Context()

    # This program is a SUBscripber. dumpvdl2 is a PUBlisher.
    subscriber = context.socket(zmq.SUB)

    # Connect to dumpvdl2 zmq server
    subscriber.connect(f"tcp://{host}:{port}")

    # Subscribe for everything
    subscriber.setsockopt(zmq.SUBSCRIBE, b'')

    # Receive messages forever
    while True:

        # Receive all parts of a zmq message
        message = subscriber.recv_multipart()

        # For each json message inside the zmq message
        for data in message:

            # prepare data to be enqueued into input queue
            incoming_data = {
                'raw_json': copy.deepcopy(data),                           # raw json data
                'src_host': copy.deepcopy(host),                           # src host
                'src_port': copy.deepcopy(port),                           # src port
                'src_name': f'input.zmqclient.{protoname}.{host}:{port}',  # function originating the data
                'msg_uuid': uuid.uuid1(),                                  # unique identifier for this message
            }

            # trace logging
            logger.log(logging_TRACE, f"in: {host}:{port}/zmq; out: {inbound_message_queue.name}; data: {incoming_data}")

            # enqueue the data
            inbound_message_queue.put(incoming_data)

            # increment counters
            COUNTERS.increment(f'receive_zmq_{protoname}')


# PROCESSING MESSAGES #


def json_validator(in_queue: ARQueue, out_queue: ARQueue, protoname: str):
    """
    Deserialise JSON.

    Reads JSON objects from in_queue.
    Attempts to deserialise. If successful, puts deserialised object into out_queue.

    Intended to be run in a thread.
    """
    protoname = protoname.lower()

    # prepare logging
    logger = baselogger.getChild(f'json_validator.{protoname}')
    logger.debug("spawned")

    # pop items off queue forever
    while True:

        # pop data from queue (blocks until an item can be popped)
        data = in_queue.get()
        in_queue.task_done()

        # attempt to deserialise
        try:
            data['json'] = json.loads(data['raw_json'])

        # if an exception, log and continue
        except Exception as e:

            logger.error(f"invalid JSON received via {data['src_name']}")
            logger.debug(f"invalid JSON received: {data}, exception: {e}")

            COUNTERS.increment(f'invalid_json_{protoname}')
            continue

        # if no exception, put deserialised data onto out_queue
        else:

            # ensure json.loads resulted in a dict
            if type(data['json']) != dict:
                logger.error(f"invalid JSON received via {data['src_name']}")
                logger.debug(f"invalid JSON received: json.loads on raw_json returned non-dict object: {data}")
                COUNTERS.increment(f'invalid_json_{protoname}')
                continue

            # trace logging
            logger.log(logging_TRACE, f"in: {in_queue.name}; out: {out_queue.name}; data: {data}")

            # enqueue the data
            out_queue.put(data)


def within_acceptable_skew(timestamp: int, skew_window_secs: int):
    """
    Helper fuction that determines whether timestamp (in nanoseconds) is within +/- skew_window_secs
    """
    min_timestamp = time.time_ns() - (skew_window_secs * 1e9)
    max_timestamp = time.time_ns() + (skew_window_secs * 1e9)
    if min_timestamp <= timestamp <= max_timestamp:
        return True
    else:
        return False


def acars_hasher(
    in_queue: ARQueue,
    out_queue: ARQueue,
    protoname: str,
    skew_window_secs: int,
):
    protoname = protoname.lower()

    # prepare logging
    logger = baselogger.getChild('acars_hasher')
    logger.debug("spawned")

    # hash items "forever"
    while True:

        # pop data from queue
        data = in_queue.get()
        in_queue.task_done()

        # create timestamp (in nanoseconds) from message timestamp
        data['msgtime_ns'] = int(float(data['json']['timestamp']) * 1e9)

        # drop messages with timestamp outside of max skew range
        if not within_acceptable_skew(data['msgtime_ns'], skew_window_secs):
            logger.warning(f"message timestamp outside acceptable skew window: {data['json']['timestamp']} (now: {time.time()})")
            logger.debug(f"message timestamp outside acceptable skew window: {data}")
            continue

        # copy object so we don't molest original data
        data_to_hash = copy.deepcopy(data['json'])

        # remove feeder-specific data so we only hash data unique to the message
        del(data_to_hash['error'])
        del(data_to_hash['level'])
        del(data_to_hash['station_id'])
        del(data_to_hash['timestamp'])
        del(data_to_hash['channel'])

        # store hashed data in message object
        data['hashed_data'] = json.dumps(
            data_to_hash,
            separators=(',', ':'),
            sort_keys=True,
        )

        # store hash
        data['hash'] = hash(data['hashed_data'])

        # trace logging
        logger.log(logging_TRACE, f"in: {in_queue.name}; out: {out_queue.name}; data: {data}")

        # enqueue the data
        out_queue.put(data)


def vdlm2_hasher(
    in_queue: ARQueue,
    out_queue: ARQueue,
    protoname: str,
    skew_window_secs: int,
):
    protoname = protoname.lower()

    # prepare logging
    logger = baselogger.getChild('vdlm2_hasher')
    logger.debug("spawned")

    # hash items "forever"
    while True:

        # pop data from queue
        data = in_queue.get()
        in_queue.task_done()

        # create timestamp from t.sec & t.usec
        data['msgtime_ns'] = (int(data['json']['vdl2']['t']['sec']) * 1e9) + (int(data['json']['vdl2']['t']['usec']) * 1000)

        # drop messages with timestamp outside of max skew range
        if not within_acceptable_skew(data['msgtime_ns'], skew_window_secs):
            logger.warning(f"message timestamp outside acceptable skew window: {data['json']['vdl2']['t']['sec'].data['json']['vdl2']['t']['usec']} (now: {time.time()})")
            logger.debug(f"message timestamp outside acceptable skew window: {data}")
            continue

        # copy object so we don't molest original data
        data_to_hash = copy.deepcopy(data['json'])

        # remove feeder-specific data so we only hash data unique to the message
        del(data_to_hash['vdl2']['app'])
        del(data_to_hash['vdl2']['freq_skew'])
        del(data_to_hash['vdl2']['hdr_bits_fixed'])
        del(data_to_hash['vdl2']['noise_level'])
        del(data_to_hash['vdl2']['octets_corrected_by_fec'])
        del(data_to_hash['vdl2']['sig_level'])
        del(data_to_hash['vdl2']['station'])
        del(data_to_hash['vdl2']['t'])

        # store hashed data in message object
        data['hashed_data'] = json.dumps(
            data_to_hash,
            separators=(',', ':'),
            sort_keys=True,
        )

        # store hash
        data['hash'] = hash(data['hashed_data'])

        # trace logging
        logger.log(logging_TRACE, f"in: {in_queue.name}; out: {out_queue.name}; data: {data}")

        # enqueue the data
        out_queue.put(data)


def deduper(
    in_queue: ARQueue,
    out_queue: ARQueue,
    recent_message_queue: collections.deque,
    protoname: str,
):
    protoname = protoname.lower()

    # prepare logging
    logger = baselogger.getChild(f'deduper.{protoname}')
    logger.debug("spawned")

    # dedupe items "forever"
    while True:

        # pop data from queue
        data = in_queue.get()
        in_queue.task_done()

        # check for (and drop) dupe messages, if enabled
        dropmsg = False
        lck = lock.acquire()
        if lck:
            for recent_message in recent_message_queue:

                # if the hash matches...
                if data['hash'] == recent_message['hash']:

                    # and if the data matches...
                    if data['hashed_data'] == recent_message['hashed_data']:

                        # trace logging
                        logger.log(logging_TRACE, f"in: {in_queue.name}; out: DROP; data: {data}")

                        # increment counters
                        COUNTERS.increment(f"duplicate_{protoname}")

                        # tell following steps to drop the message & break out of for loop
                        dropmsg = True
                        break

            # if the message is unique (not dupe):
            if not dropmsg:

                # put a copy of the message into the recent msg queue
                recent_message_queue.append(copy.deepcopy(data))

            # release the lock
            lock.release()

        else:
            logger.error("could not acquire lock!")

        if not dropmsg:

            data['unique'] = True

            # trace logging
            logger.log(logging_TRACE, f"in: {in_queue.name}; out: {out_queue.name}; data: {data}")

            # enqueue the data
            out_queue.put(data)


# OUTPUTTING MESSAGES #


def output_queue_populator(in_queue: ARQueue, out_queues: list, protoname: str, station_name_override: str = None):
    """
    Puts incoming ACARS/VDLM2 messages into each output queue.
    Intended to be run in a thread.
    """
    protoname = protoname.lower()

    # prepare logging
    logger = baselogger.getChild(f'output_queue_populator.{protoname}')
    logger.debug("spawned")

    # loop forever
    while True:

        # pop message from input queue
        data = in_queue.get()
        in_queue.task_done()

        # override station ID if needed
        if station_name_override:

            # acars
            if 'station_id' in data['json']:
                data['json']['station_id'] = station_name_override

            # vdlm2
            if 'vdl2' in data['json']:
                if 'station' in data['json']['vdl2']:
                    data['json']['vdl2']['station'] = station_name_override

        # serialise json
        data['out_json'] = bytes(
            json.dumps(
                data['json'],
                separators=(',', ':'),
                sort_keys=True,
            ), 'utf-8')

        # put copy of message into each output queue
        for output_queue in out_queues:

            # trace logging
            logger.log(logging_TRACE, f"in: {in_queue.name}; out: {output_queue.name}; data: {data}")

            # enqueue a copy of the data
            output_queue.put(copy.deepcopy(data))


def UDPSender(host, port, output_queues: list, protoname: str):
    """
    Threaded function to send ACARS / VDLM2 messages via UDP
    Intended to be run in a thread.
    """
    protoname = protoname.lower()

    # prepare logging
    logger = baselogger.getChild(f'output.udp.{protoname}.{host}:{port}')
    logger.debug("spawned")

    # Create an output queue for this instance of the function & add to output queue
    qname = f'output.udp.{protoname}.{host}:{port}'
    logger.debug(f"registering output queue: {qname}")
    q = ARQueue(qname, 100)
    output_queues.append(q)

    # Set up socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.settimeout(1)

    # Loop to send messages from output queue
    while True:

        # Pop a message from the output queue
        data = q.get()
        q.task_done()

        # try to send the message to the remote host
        try:
            sock.sendto(f"{data['out_json']}\n", (host, port))

            # trace
            logger.log(logging_TRACE, f"in: {qname}; out: {host}:{port}/udp; data: {data}")

        except Exception as e:
            logger.error(f"error sending to {host}:{port}: {e}")
            break

    # clean up
    # remove this instance's queue from the output queue
    logger.debug(f"deregistering output queue: {qname}")
    output_queues.remove(q)
    # delete our queue
    del(q)


def TCPServerAcceptor(port: int, output_queues: list, protoname: str):
    """
    Accepts incoming TCP connections to serve ACARS / VDLM2 messages, and spawns a TCPServer for each connection.
    Intended to be run in a thread.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("0.0.0.0", port))
        s.listen()

        while True:
            conn, addr = s.accept()
            # create new TCPServer for this connection, and pass queue
            threading.Thread(
                target=TCPServer,
                daemon=True,
                args=(conn, addr, output_queues, protoname),
            ).start()


def TCPServer(conn: socket.socket, addr: tuple, output_queues: list, protoname: str):
    """
    Process to send ACARS/VDLM2 messages to TCP clients.
    Intended to be run in a thread.
    """
    host = addr[0]
    port = addr[1]
    protoname = protoname.lower()

    # prepare logging
    logger = baselogger.getChild(f'output.tcpserver.{protoname}.{host}:{port}')
    logger.info("connection established")

    # Create an output queue for this instance of the function & add to output queue
    qname = f'output.tcpserver.{protoname}.{host}:{port}'
    logger.debug(f"registering output queue: {qname}")
    q = ARQueue(qname, 100)
    output_queues.append(q)

    # Set up socket
    conn.settimeout(1)

    # Loop to send messages from output queue
    while True:

        # Pop a message from the output queue
        data = q.get()
        q.task_done()

        # try to send the message to the remote host
        try:
            conn.sendall(f"{data['out_json']}\n")

            # trace
            logger.log(logging_TRACE, f"in: {qname}; out: {host}:{port}/udp; data: {data}")

        except Exception as e:
            logger.error(f"error sending to {host}:{port}: {e}")
            break

    # clean up
    # remove this instance's queue from the output queue
    logger.debug(f"deregistering output queue: {qname}")
    output_queues.remove(q)
    # delete our queue
    del(q)
    # close socket
    conn.close()
    # finally, let the user know client has disconnected
    logger.info("connection lost")


def TCPSender(host: str, port: int, output_queues: list, protoname: str):
    """
    Process to send ACARS/VDLM2 messages to a TCP server.
    Intended to be run in a thread.
    """
    protoname = protoname.lower()

    # prepare logging
    logger = baselogger.getChild(f'output.tcpclient.{protoname}.{host}:{port}')
    logger.debug("spawned")

    # Loop to send messages from output queue
    while True:

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            # set socket timeout to 5 seconds
            sock.settimeout(5)

            # attempt connection
            try:
                sock.connect((host, port))

            except ConnectionRefusedError:
                logger.error("connection refused")
                time.sleep(10)

            except Exception as e:
                logger.error(f"connection error: {e}")
                time.sleep(10)

            else:
                logger.info("connection established")

                # Create an output queue for this instance of the function & add to output queue
                qname = f'output.tcpclient.{protoname}.{host}:{port}'
                q = ARQueue(qname, 100)
                output_queues.append(q)

                # put socket in blocking mode
                sock.settimeout(None)

                while True:

                    # Pop a message from the output queue
                    data = q.get()
                    q.task_done()

                    # try to send the message to the remote host
                    try:
                        sock.sendall(f"{data['out_json']}\n")

                    except Exception as e:
                        logger.error(f"error sending to {host}:{port}: {e}")
                        break

                # clean up
                # remove this instance's queue from the output queue
                logger.debug(f"deregistering output queue: {qname}")
                output_queues.remove(q)
                # delete our queue
                del(q)
                # close socket
                sock.close()
                # finally, let the user know client has disconnected
                logger.info("connection lost")


# HELPER FUNCTIONS #


def display_stats(
    mins: int = 5,
    loglevel: int = logging.INFO,
):
    """
    Displays the status of the global counters.
    Intended to be run in a thread, as this function will run forever.

    Arguments:
    mins -- the number of minutes between logging stats
    loglevel -- the log level to use when logging statistics
    """
    logger = baselogger.getChild('statistics')
    logger.debug("spawned")
    while True:
        time.sleep(mins * 60)
        COUNTERS.log(logger, loglevel)


def recent_message_queue_evictor(recent_message_queue: collections.deque, protoname: str, dedupe_window_secs: int):
    protoname = protoname.lower()
    logger = baselogger.getChild(f'recent_message_queue_evictor.{protoname}')
    logger.debug("spawned")
    while True:
        if len(recent_message_queue) > 0:
            # evict items older than 2 seconds
            if recent_message_queue[0]['msgtime_ns'] <= (time.time_ns() - (dedupe_window_secs * 1e9)):
                evictedmsg = recent_message_queue.popleft()
                logger.log(logging_TRACE, f"evicted: {evictedmsg}")
                continue
        time.sleep(0.250)


def split_env_safely(
    env: str,
    sep: str = ';',
):
    """
    Splits the contents of environment variable `env` with separator `sep`.
    If an exception occurs, return an empty list.
    """
    try:
        return os.getenv(env).split(sep)
    except AttributeError:
        return list()
    return list()


def env_true_false(
    env: str,
):
    """
    Returns True or False objects
    """
    if str(os.getenv(env, False)).lower() == "true":
        return True
    else:
        return False


def log_on_first_message(out_queues: list, protoname: str):
    """
    Logs when the first message is received.
    Intended to be run in a thread.
    """
    logger = baselogger.getChild(f'first_message.{protoname}')
    logger.debug("spawned")
    # Create an output queue for this instance of the function & add to output queue
    q = ARQueue(f'first_message.{protoname}', 100)
    out_queues.append(q)
    q.get()
    q.task_done()
    logger.info(f"Receiving {protoname} messages!")
    out_queues.remove(q)
    del(q)


def valid_tcp_udp_port(num: int):
    """
    Returns True if num is a valid TCP/UDP port number, else return False.
    """
    if type(num) == int:
        if 1 >= int(num) <= 65535:
            return True
    return False


def valid_args(args):
    """
    Returns True if command line arguments are valid, else return False.
    """

    # TODO: This function has a lot of repetition, and doesn't really conform with python's DRY principle. Fix.

    logger = baselogger.getChild('sanity_check')

    # Check listen_udp_acars, should be list of valid port numbers
    for i in args.listen_udp_acars:
        try:
            if not valid_tcp_udp_port(int(i)):
                raise ValueError
        except ValueError:
            logger.critical(f"listen_udp_acars: invalid port: {i}")
            return False

    # Check listen_tcp_acars, should be list of valid port numbers
    for i in args.listen_tcp_acars:
        try:
            if not valid_tcp_udp_port(int(i)):
                raise ValueError
        except ValueError:
            logger.critical(f"listen_tcp_acars: invalid port: {i}")
            return False

    # Check receive_tcp_acars, should be a list of host:port
    for i in args.listen_tcp_acars:
        # try to split host:port
        try:
            host = i.split(':')[0]
            port = i.split(':')[1]
        except IndexError:
            logger.critical(f"listen_tcp_acars: host:port expected, got: {i}")
            return False
        # ensure port valid
        try:
            if not valid_tcp_udp_port(port):
                raise ValueError
        except ValueError:
            logger.critical(f"listen_tcp_acars: invalid port: {port}")
            return False
        # warn if host appears wrong
        try:
            socket.gethostbyname(host)
        except socket.gaierror:
            logger.warning(f"listen_tcp_acars: host appears invalid or unresolvable: {host}")

    # Check listen_udp_vdlm2, should be list of valid port numbers
    for i in args.listen_udp_vdlm2:
        try:
            if not valid_tcp_udp_port(int(i)):
                raise ValueError
        except ValueError:
            logger.critical(f"listen_udp_vdlm2: invalid port: {i}")
            return False

    # Check listen_tcp_vdlm2, should be list of valid port numbers
    for i in args.listen_tcp_vdlm2:
        try:
            if not valid_tcp_udp_port(int(i)):
                raise ValueError
        except ValueError:
            logger.critical(f"listen_tcp_vdlm2: invalid port: {i}")
            return False

    # Check receive_tcp_vdlm2, should be a list of host:port
    for i in args.receive_tcp_vdlm2:
        # try to split host:port
        try:
            host = i.split(':')[0]
            port = i.split(':')[1]
        except IndexError:
            logger.critical(f"receive_tcp_vdlm2: host:port expected, got: {i}")
            return False
        # ensure port valid
        try:
            if not valid_tcp_udp_port(port):
                raise ValueError
        except ValueError:
            logger.critical(f"receive_tcp_vdlm2: invalid port: {port}")
            return False
        # warn if host appears wrong
        try:
            socket.gethostbyname(host)
        except socket.gaierror:
            logger.warning(f"receive_tcp_vdlm2: host appears invalid or unresolvable: {host}")

    # Check receive_tcp_vdlm2, should be a list of host:port
    for i in args.receive_zmq_vdlm2:
        # try to split host:port
        try:
            host = i.split(':')[0]
            port = i.split(':')[1]
        except IndexError:
            logger.critical(f"receive_zmq_vdlm2: host:port expected, got: {i}")
            return False
        # ensure port valid
        try:
            if not valid_tcp_udp_port(port):
                raise ValueError
        except ValueError:
            logger.critical(f"receive_zmq_vdlm2: invalid port: {port}")
            return False
        # warn if host appears wrong
        try:
            socket.gethostbyname(host)
        except socket.gaierror:
            logger.warning(f"receive_zmq_vdlm2: host appears invalid or unresolvable: {host}")

    # Check send_udp_acars, should be a list of host:port
    for i in args.send_udp_acars:
        # try to split host:port
        try:
            host = i.split(':')[0]
            port = i.split(':')[1]
        except IndexError:
            logger.critical(f"send_udp_acars: host:port expected, got: {i}")
            return False
        # ensure port valid
        try:
            if not valid_tcp_udp_port(port):
                raise ValueError
        except ValueError:
            logger.critical(f"send_udp_acars: invalid port: {port}")
            return False
        # warn if host appears wrong
        try:
            socket.gethostbyname(host)
        except socket.gaierror:
            logger.warning(f"send_udp_acars: host appears invalid or unresolvable: {host}")

    # Check send_tcp_acars, should be a list of host:port
    for i in args.send_tcp_acars:
        # try to split host:port
        try:
            host = i.split(':')[0]
            port = i.split(':')[1]
        except IndexError:
            logger.critical(f"send_tcp_acars: host:port expected, got: {i}")
            return False
        # ensure port valid
        try:
            if not valid_tcp_udp_port(port):
                raise ValueError
        except ValueError:
            logger.critical(f"send_tcp_acars: invalid port: {port}")
            return False
        # warn if host appears wrong
        try:
            socket.gethostbyname(host)
        except socket.gaierror:
            logger.warning(f"send_tcp_acars: host appears invalid or unresolvable: {host}")

    # Check send_udp_vdlm2, should be a list of host:port
    for i in args.send_udp_vdlm2:
        # try to split host:port
        try:
            host = i.split(':')[0]
            port = i.split(':')[1]
        except IndexError:
            logger.critical(f"send_udp_vdlm2: host:port expected, got: {i}")
            return False
        # ensure port valid
        try:
            if not valid_tcp_udp_port(port):
                raise ValueError
        except ValueError:
            logger.critical(f"send_udp_vdlm2: invalid port: {port}")
            return False
        # warn if host appears wrong
        try:
            socket.gethostbyname(host)
        except socket.gaierror:
            logger.warning(f"send_udp_vdlm2: host appears invalid or unresolvable: {host}")

    # Check send_tcp_vdlm2, should be a list of host:port
    for i in args.send_tcp_vdlm2:
        # try to split host:port
        try:
            host = i.split(':')[0]
            port = i.split(':')[1]
        except IndexError:
            logger.critical(f"send_tcp_vdlm2: host:port expected, got: {i}")
            return False
        # ensure port valid
        try:
            if not valid_tcp_udp_port(port):
                raise ValueError
        except ValueError:
            logger.critical(f"send_tcp_vdlm2: invalid port: {port}")
            return False
        # warn if host appears wrong
        try:
            socket.gethostbyname(host)
        except socket.gaierror:
            logger.warning(f"send_tcp_vdlm2: host appears invalid or unresolvable: {host}")

    # Check listen_tcp_acars, should be list of valid port numbers
    for i in args.serve_tcp_acars:
        try:
            if not valid_tcp_udp_port(int(i)):
                raise ValueError
        except ValueError:
            logger.critical(f"serve_tcp_acars: invalid port: {i}")
            return False

    # Check serve_tcp_vdlm2, should be list of valid port numbers
    for i in args.serve_tcp_acars:
        try:
            if not valid_tcp_udp_port(int(i)):
                raise ValueError
        except ValueError:
            logger.critical(f"serve_tcp_vdlm2: invalid port: {i}")
            return False

    # Check stats_every, should be an int
    try:
        int(args.stats_every)
    except ValueError:
        logger.critical(f"stats_every: invalid value: {args.stats_every}")
        return False

    # Check verbose, should be an int
    try:
        int(args.verbose)
    except ValueError:
        logger.critical(f"verbose: invalid value: {args.verbose}")
        return False

    # If we're here, all arguments are good
    return True


if __name__ == "__main__":

    # Command line / OS Env
    parser = argparse.ArgumentParser(
        description='Route ACARS/VDLM2 messages'
    )
    parser.add_argument(
        '--listen-udp-acars',
        help='UDP port to listen for ACARS messages. Can be specified multiple times to listen on multiple ports. (default: 5550)',
        type=str,
        nargs='*',
        default=split_env_safely('AR_LISTEN_UDP_ACARS'),
    )
    parser.add_argument(
        '--listen-tcp-acars',
        help='TCP port to listen for ACARS messages. Can be specified multiple times to listen on multiple ports. (default: 5550)',
        type=str,
        nargs='*',
        default=split_env_safely('AR_LISTEN_TCP_ACARS'),
    )
    parser.add_argument(
        '--receive-tcp-acars',
        help='Connect to "host:port" (over TCP) and receive ACARS messages. Can be specified multiple times to receive from multiple sources.',
        type=str,
        nargs='*',
        default=split_env_safely('AR_RECV_TCP_ACARS'),
    )
    parser.add_argument(
        '--listen-udp-vdlm2',
        help='UDP port to listen for VDLM2 messages. Can be specified multiple times to listen on multiple ports. (default: 5555)',
        type=str,
        nargs='*',
        default=split_env_safely('AR_LISTEN_UDP_VDLM2'),
    )
    parser.add_argument(
        '--listen-tcp-vdlm2',
        help='TCP port to listen for VDLM2 messages. Can be specified multiple times to listen on multiple ports. (default: 5550)',
        type=str,
        nargs='*',
        default=split_env_safely('AR_LISTEN_TCP_VDLM2'),
    )
    parser.add_argument(
        '--receive-tcp-vdlm2',
        help='Connect to "host:port" (over TCP) and receive VDLM2 messages. Can be specified multiple times to receive from multiple sources.',
        type=str,
        nargs='*',
        default=split_env_safely('AR_RECV_TCP_VDLM2'),
    )
    parser.add_argument(
        '--receive-zmq-vdlm2',
        help='Connect to a ZeroMQ publisher at "host:port" (over TCP) and receive VDLM2 messages as a subscriber. Can be specified multiple times to receive from multiple sources.',
        type=str,
        nargs='*',
        default=split_env_safely('AR_RECV_ZMQ_VDLM2'),
    )
    parser.add_argument(
        '--send-udp-acars',
        help='Send ACARS messages via UDP datagram to "host:port". Can be specified multiple times to send to multiple clients.',
        type=str,
        nargs='*',
        default=split_env_safely('AR_SEND_UDP_ACARS'),
    )
    parser.add_argument(
        '--send-tcp-acars',
        help='Send ACARS messages via TCP to "host:port". Can be specified multiple times to send to multiple clients.',
        type=str,
        nargs='*',
        default=split_env_safely('AR_SEND_TCP_ACARS'),
    )
    parser.add_argument(
        '--serve-tcp-acars',
        help='Serve ACARS messages on TCP "port". Can be specified multiple times to serve on multiple ports.',
        type=str,
        nargs='*',
        default=split_env_safely('AR_SERVE_TCP_ACARS'),
    )
    parser.add_argument(
        '--send-udp-vdlm2',
        help='Send VDLM2 messages via UDP datagram to "host:port". Can be specified multiple times to send to multiple clients.',
        type=str,
        nargs='*',
        default=split_env_safely('AR_SEND_UDP_VDLM2'),
    )
    parser.add_argument(
        '--send-tcp-vdlm2',
        help='Send VDLM2 messages via TCP to "host:port". Can be specified multiple times to send to multiple clients.',
        type=str,
        nargs='*',
        default=split_env_safely('AR_SEND_TCP_VDLM2'),
    )
    parser.add_argument(
        '--serve-tcp-vdlm2',
        help='Serve VDLM2 messages on TCP "port". Can be specified multiple times to serve on multiple ports.',
        type=str,
        nargs='*',
        default=split_env_safely('AR_SERVE_TCP_VDLM2'),
    )
    parser.add_argument(
        '--stats-every',
        help='Log statistics every N minutes (default: 5)',
        type=int,
        nargs='?',
        default=int(os.getenv('AR_STATS_EVERY', "5")),
    )
    parser.add_argument(
        '--verbose', '-v',
        help='Increase log verbosity. -v = debug. -vv = trace message paths.',
        action='count',
        default=int(os.getenv("AR_VERBOSITY", 0)),
    )
    parser.add_argument(
        '--enable-dedupe',
        help='Enables message deduplication.',
        action='store_true',
        default=env_true_false("AR_ENABLE_DEDUPE"),
    )
    parser.add_argument(
        '--dedupe-window',
        help='The window in seconds for duplicate messages to be dropped (default: 2).',
        type=int,
        nargs='?',
        default=int(os.getenv("AR_DEDUPE_WINDOW", 2)),
    )
    parser.add_argument(
        '--skew-window',
        help='Reject messages with a timestamp greater than +/- this many seconds (default: 1).',
        type=int,
        nargs='?',
        default=int(os.getenv("AR_SKEW_WINDOW", 1)),
    )
    parser.add_argument(
        '--threads-json-deserialiser',
        help=f'Number of threads for JSON deserialisers (default: {os.cpu_count()})',
        type=int,
        nargs='?',
        default=int(os.getenv("AR_THREADS_JSON_DESERIALISER", os.cpu_count())),
    )
    parser.add_argument(
        '--threads-hasher',
        help=f'Number of threads for message hashers (default: {os.cpu_count()})',
        type=int,
        nargs='?',
        default=int(os.getenv("AR_THREADS_HASHER", os.cpu_count())),
    )
    parser.add_argument(
        '--threads-deduper',
        help=f'Number of threads for message dedupers (default: {os.cpu_count()})',
        type=int,
        nargs='?',
        default=int(os.getenv("AR_THREADS_DEDUPER", os.cpu_count())),
    )
    parser.add_argument(
        '--threads-output-queue-populator',
        help=f'Number of threads for output queue populators (default: {os.cpu_count()})',
        type=int,
        nargs='?',
        default=int(os.getenv("AR_OUTPUT_QUEUE_POPULATOR", os.cpu_count())),
    )
    parser.add_argument(
        '--override-station-name',
        help='Overrides station id/name with this value',
        type=str,
        nargs='?',
        default=os.getenv("AR_OVERRIDE_STATION_NAME", None),
    )
    args = parser.parse_args()

    # configure logging: create trace level
    logging_TRACE = logging.DEBUG - 1
    logging.addLevelName(logging_TRACE, 'TRACE')
    # configure logging: base logger
    baselogger = logging.getLogger('acars_router')

    # configure logging
    logger = baselogger.getChild('core')
    logger_console_handler = logging.StreamHandler()
    baselogger.addHandler(logger_console_handler)
    if args.verbose == 1:
        log_formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] [%(name)s] [%(threadName)s] %(message)s',
            r'%Y-%m-%d %H:%M:%S',
        )
        baselogger.setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
        logger_console_handler.setLevel(logging.DEBUG)
        logger_console_handler.setFormatter(log_formatter)
        logger.debug(f"Command line arguments: {args}")
        logger.debug("DEBUG logging enabled")
    elif args.verbose >= 2:
        log_formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] [%(name)s] [%(threadName)s] %(message)s',
            r'%Y-%m-%d %H:%M:%S',
        )
        baselogger.setLevel(logging_TRACE)
        logger.setLevel(logging_TRACE)
        logger_console_handler.setLevel(logging_TRACE)
        logger_console_handler.setFormatter(log_formatter)
        logger.debug(f"Command line arguments: {args}")
        logger.debug("TRACE logging enabled")
    else:
        log_formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
            r'%Y-%m-%d %H:%M:%S',
        )
        baselogger.setLevel(logging.INFO)
        logger.setLevel(logging.INFO)
        logger_console_handler.setLevel(logging.INFO)
        logger_console_handler.setFormatter(log_formatter)

    # sanity check input, if invalid then bail out
    if not valid_args:
        sys.exit(1)

    # initialise counters
    COUNTERS = ARCounters()

    # initialise threading lock
    lock = threading.Lock()

    # Start stats thread
    threading.Thread(
        target=display_stats,
        daemon=True,
        args=(args.stats_every,),
    ).start()

    # Prepare output queues
    # populate with data (bytes) to send out
    output_acars_queues = list()
    output_vdlm2_queues = list()
    COUNTERS.register_queue_list(output_acars_queues)
    COUNTERS.register_queue_list(output_vdlm2_queues)

    # define inbound message queues
    inbound_acars_message_queue = ARQueue('inbound_acars_message_queue', 100)
    COUNTERS.register_queue(inbound_acars_message_queue)
    inbound_vdlm2_message_queue = ARQueue('inbound_vdlm2_message_queue', 100)
    COUNTERS.register_queue(inbound_vdlm2_message_queue)

    # define intermediate queue for deserialised messages
    deserialised_acars_message_queue = ARQueue('deserialised_acars_message_queue', 100)
    COUNTERS.register_queue(deserialised_acars_message_queue)
    deserialised_vdlm2_message_queue = ARQueue('deserialised_vdlm2_message_queue', 100)
    COUNTERS.register_queue(deserialised_vdlm2_message_queue)

    # define intermediate queue for hashed messages
    hashed_acars_message_queue = ARQueue('hashed_acars_message_queue', 100)
    COUNTERS.register_queue(hashed_acars_message_queue)
    hashed_vdlm2_message_queue = ARQueue('hashed_vdlm2_message_queue', 100)
    COUNTERS.register_queue(hashed_vdlm2_message_queue)

    # define intermediate queue for deduped messages
    deduped_acars_message_queue = ARQueue('deduped_acars_message_queue', 100)
    COUNTERS.register_queue(deduped_acars_message_queue)
    deduped_vdlm2_message_queue = ARQueue('deduped_vdlm2_message_queue', 100)
    COUNTERS.register_queue(deduped_vdlm2_message_queue)

    # recent message buffers for dedupe
    recent_message_queue_acars = collections.deque()
    COUNTERS.register_deque("recent_message_queue_acars", recent_message_queue_acars)
    recent_message_queue_vdlm2 = collections.deque()
    COUNTERS.register_deque("recent_message_queue_vdlm2", recent_message_queue_vdlm2)

    # recent message buffers evictor threads
    threading.Thread(
        target=recent_message_queue_evictor,
        args=(recent_message_queue_acars, "acars", args.dedupe_window),
        daemon=True,
    ).start()
    threading.Thread(
        target=recent_message_queue_evictor,
        args=(recent_message_queue_vdlm2, "vdlm2", args.dedupe_window),
        daemon=True,
    ).start()

    # acars json deserialiser threads
    for _ in range(args.threads_json_deserialiser):
        threading.Thread(
            target=json_validator,
            args=(inbound_acars_message_queue, deserialised_acars_message_queue, 'acars',),
            daemon=True,
        ).start()

    # vdlm2 json deserialiser threads
    for _ in range(args.threads_json_deserialiser):
        threading.Thread(
            target=json_validator,
            args=(inbound_vdlm2_message_queue, deserialised_vdlm2_message_queue, 'vdlm2',),
            daemon=True,
        ).start()

    # acars hasher threads
    for _ in range(args.threads_hasher):
        threading.Thread(
            target=acars_hasher,
            args=(
                deserialised_acars_message_queue,
                hashed_acars_message_queue,
                "ACARS",
                args.skew_window,
            ),
            daemon=True,
        ).start()

    # vdlm2 hasher threads
    for _ in range(args.threads_hasher):
        threading.Thread(
            target=vdlm2_hasher,
            args=(
                deserialised_vdlm2_message_queue,
                hashed_vdlm2_message_queue,
                "VDLM2",
                args.skew_window,
            ),
            daemon=True,
        ).start()

    if args.enable_dedupe:

        # if dedupe enabled: use dedupe queue & start dedupers

        for _ in range(args.threads_deduper):
            threading.Thread(
                target=deduper,
                args=(hashed_acars_message_queue, deduped_acars_message_queue, recent_message_queue_acars, "ACARS",),
                daemon=True,
            ).start()

        for _ in range(args.threads_deduper):
            threading.Thread(
                target=deduper,
                args=(hashed_vdlm2_message_queue, deduped_vdlm2_message_queue, recent_message_queue_vdlm2, "VDLM2",),
                daemon=True,
            ).start()

        # output queue populator: acars
        for _ in range(args.threads_output_queue_populator):
            threading.Thread(
                target=output_queue_populator,
                args=(deduped_acars_message_queue, output_acars_queues, "ACARS", args.override_station_name),
                daemon=True,
            ).start()

        # output queue populator: vdlm2
        for _ in range(args.threads_output_queue_populator):
            threading.Thread(
                target=output_queue_populator,
                args=(deduped_vdlm2_message_queue, output_vdlm2_queues, "VDLM2", args.override_station_name),
                daemon=True,
            ).start()

    else:
        # if dedupe disabled: bypass dedupe queue & don't start dedupers

        # output queue populator: acars
        for _ in range(args.threads_output_queue_populator):
            threading.Thread(
                target=output_queue_populator,
                args=(hashed_acars_message_queue, output_acars_queues, "ACARS", args.override_station_name),
                daemon=True,
            ).start()

        # output queue populator: vdlm2
        for _ in range(args.threads_output_queue_populator):
            threading.Thread(
                target=output_queue_populator,
                args=(hashed_vdlm2_message_queue, output_vdlm2_queues, "VDLM2", args.override_station_name),
                daemon=True,
            ).start()

    # Configure "log on first message" for ACARS
    threading.Thread(
        target=log_on_first_message,
        args=(output_acars_queues, "ACARS"),
        daemon=True,
    ).start()

    # Configure "log on first message" for VDLM2
    threading.Thread(
        target=log_on_first_message,
        args=(output_vdlm2_queues, "VDLM2"),
        daemon=True,
    ).start()

    # acars tcp output (server)
    for port in args.serve_tcp_acars:
        logger.info(f'serving ACARS on TCP port {port}')
        threading.Thread(
            target=TCPServerAcceptor,
            daemon=True,
            args=(int(port), output_acars_queues, "ACARS"),
        ).start()

    # vdlm2 tcp output (server)
    for port in args.serve_tcp_vdlm2:
        logger.info(f'serving VDLM2 on TCP port {port}')
        threading.Thread(
            target=TCPServerAcceptor,
            daemon=True,
            args=(int(port), output_vdlm2_queues, "VDLM2"),
        ).start()

    # acars udp output (sender)
    for c in args.send_udp_acars:
        host = c.split(':')[0]
        port = int(c.split(':')[1])
        logger.info(f'sending ACARS via UDP to {host}:{port}')
        threading.Thread(
            target=UDPSender,
            daemon=True,
            args=(host, port, output_acars_queues, "ACARS"),
        ).start()

    # acars tcp output (sender)
    for c in args.send_tcp_acars:
        host = c.split(':')[0]
        port = int(c.split(':')[1])
        logger.info(f'sending ACARS via TCP to {host}:{port}')
        threading.Thread(
            target=TCPSender,
            daemon=True,
            args=(host, port, output_acars_queues, "ACARS"),
        ).start()

    # vdlm2 udp output (sender)
    for c in args.send_udp_vdlm2:
        host = c.split(':')[0]
        port = int(c.split(':')[1])
        logger.info(f'sending VDLM2 via UDP to {host}:{port}')
        threading.Thread(
            target=UDPSender,
            daemon=True,
            args=(host, port, output_vdlm2_queues, "VDLM2"),
        ).start()

    # vdlm2 tcp output (sender)
    for c in args.send_tcp_vdlm2:
        host = c.split(':')[0]
        port = int(c.split(':')[1])
        logger.info(f'sending VDLM2 via TCP to {host}:{port}')
        threading.Thread(
            target=TCPSender,
            daemon=True,
            args=(host, port, output_vdlm2_queues, "VDLM2"),
        ).start()

    # Prepare inbound UDP receiver threads for ACARS
    for port in args.listen_udp_acars:
        logger.info(f"Listening for ACARS UDP on port {port}")
        threading.Thread(
            target=ThreadedUDPServer(
                ("0.0.0.0", int(port)),
                InboundUDPMessageHandler,
                inbound_message_queue=inbound_acars_message_queue,
                protoname="ACARS",
            ).serve_forever,
            daemon=True,
        ).start()

    # Prepare inbound TCP receiver threads for ACARS
    for port in args.listen_tcp_acars:
        logger.info(f"Listening for ACARS TCP on port {port}")
        threading.Thread(
            target=ThreadedTCPServer(
                ("0.0.0.0", int(port)),
                InboundTCPMessageHandler,
                inbound_message_queue=inbound_acars_message_queue,
                protoname="ACARS",
            ).serve_forever,
            daemon=True,
        ).start()

    # Prepare inbound UDP receiver threads for VDLM2
    for port in args.listen_udp_vdlm2:
        logger.info(f"Listening for VDLM2 UDP on port {port}")
        threading.Thread(
            target=ThreadedUDPServer(
                ("0.0.0.0", int(port)),
                InboundUDPMessageHandler,
                inbound_message_queue=inbound_vdlm2_message_queue,
                protoname="VDLM2",
            ).serve_forever,
            daemon=True,
        ).start()

    # Prepare inbound TCP receiver threads for VDLM2
    for port in args.listen_tcp_vdlm2:
        logger.info(f"Listening for VDLM2 TCP on port {port}")
        threading.Thread(
            target=ThreadedTCPServer(
                ("0.0.0.0", int(port)),
                InboundTCPMessageHandler,
                inbound_message_queue=inbound_vdlm2_message_queue,
                protoname="VDLM2",
            ).serve_forever,
            daemon=True,
        ).start()

    # Prepare inbound TCP client receiver threads for ACARS
    for s in args.receive_tcp_acars:
        logger.info(f"Receiving ACARS TCP from {s.split(':')[0]}:{s.split(':')[1]}")
        threading.Thread(
            target=TCPReceiver,
            args=(s.split(':')[0], int(s.split(':')[1]), inbound_acars_message_queue, "ACARS"),
            daemon=True,
        ).start()

    # Prepare inbound TCP client receiver threads for VDLM2
    for s in args.receive_tcp_vdlm2:
        logger.info(f"Receiving VDLM2 TCP from {s.split(':')[0]}:{s.split(':')[1]}")
        threading.Thread(
            target=TCPReceiver,
            args=(s.split(':')[0], int(s.split(':')[1]), inbound_vdlm2_message_queue, "VDLM2"),
            daemon=True,
        ).start()

    # Prepare inbound ZMQ client receiver threads for VDLM2
    for s in args.receive_zmq_vdlm2:
        logger.info(f"Receiving VDLM2 ZMQ from {s.split(':')[0]}:{s.split(':')[1]}")
        threading.Thread(
            target=ZMQReceiver,
            args=(s.split(':')[0], int(s.split(':')[1]), inbound_vdlm2_message_queue, "VDLM2"),
            daemon=True,
        ).start()

    # Main loop
    while True:
        time.sleep(10)
