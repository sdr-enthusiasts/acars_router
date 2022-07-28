#!/usr/bin/env python3

import argparse
from distutils.util import strtobool
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
import signal
import random


# if we get unexpected exceptions in threads, quit the program
def threading_excepthook_override(args):
    threading.__excepthook__(args)
    sys.stderr.write("acars_router: exiting for FATAL condition: Uncaught exception in a thread!\n")
    os._exit(1)


threading.excepthook = threading_excepthook_override
global_queue_size = 100

# HELPER FUNCTIONS #


def reassemble_json(old_data, new_data, logger):
    """ Provides complete or undecodable messages as an array and possibly a partial message or None if the messages are complete """
    logger.log(logging_TRACE, f"reassemble_json: old_data: {old_data} new_data: {new_data}")

    lines = []
    if old_data is None:
        data = new_data
    else:
        # we have old data, check if we can decode first line of the new data on its own
        new_split = new_data.splitlines()
        if len(new_split) == 0:
            data = old_data + new_data
        else:
            try:

                test = json.loads(new_split[0])
                if (type(test) != dict):
                    raise json.JSONDecodeError("not a dict", new_split[0], 0)
                # first line of new data decoded on its own, return the old partial message on its own
                lines += [old_data]
                data = new_data
                logger.debug(f"reassembly: discarding: {old_data}")
            except json.JSONDecodeError as e:
                logger.log(logging_TRACE, f"will try reassembly! ({e})")
                logger.debug(f"reassembly: old_data len: {len(old_data)}, new_data len: {len(new_data)}")
                data = old_data + new_data

    # remove a leading newline to avoid returning empty lines
    if len(data) > 0 and data[0] == '\n':
        data = data[1:]

    lines += data.splitlines()

    if len(lines) == 0:
        return ([], None)

    last = lines[-1]

    if len(last) == 0:
        return (lines[:-1], None)

    # if the last character is a newline, assume last line is is a complete message
    if len(new_data) > 0 and new_data[-1] == '\n':
        return (lines, None)

    # check if the last line is a complete json message
    try:
        test = json.loads(last)
        if (type(test) != dict):
            raise json.JSONDecodeError("not a dict", last, 0)
        # last line complete, return lines
        return (lines, None)
    except json.JSONDecodeError as e:
        logger.log(logging_TRACE, f"will try reassembly! ({e})")
        return (lines[:-1], last)


def test_reassemble_json():
    """ Test json reassembly """
    samples = [('{' + f' "msg": {i}, "dummy": "{i}"' + '}') for i in range(0, 100)]
    logger = baselogger.getChild('testcases')

    partial = None

    for msg in samples[:4]:
        lines, partial = reassemble_json(partial, msg[:5], logger)
        for line in lines:
            print("got line: " + line)

        lines, partial = reassemble_json(partial, msg[5:], logger)
        for line in lines:
            print("got line: " + line)

    concat = '\n'.join(samples)

    partial = None

    while len(concat) > 0:
        split = random.randint(0, 3 * len(samples[0]))
        msg = concat[:split]
        concat = concat[split:]

        lines, partial = reassemble_json(partial, msg, logger)
        for line in lines:
            print("got line: " + line)
            if line[0] != '{' or line[-1] != '}':
                print("-------------------------------------------------------------")
                print("FAIL")
                print("-------------------------------------------------------------")
                os._exit(1)

    partial = None
    msg = '\n{ "msg": 58, "dummy": "58"}\n{ "msg": 59'
    lines, partial = reassemble_json(partial, msg, logger)
    for line in lines:
        print("got line: " + line)


# HELPER CLASSES #


class ARQueue(queue.Queue):
    """
    A subclass of queue.Queue, allowing us to name the queue.
    """
    def __init__(self, name: str, maxsize: int = 0):
        self.name = name
        super().__init__(maxsize=maxsize)

    def put_or_die(self, item):
        try:
            self.put(item, timeout=1)
        except queue.Full as e:
            logger.error(f"queue full: {self.name} {e}")
            sys.stderr.write("acars_router: exiting for FATAL condition: One of the queues is full, this means something is wrong, better start over fresh!\n")
            os._exit(1)


class ARCounters():
    """
    A class to be used for counters
    """
    def __init__(self, stats_file: str = None):

        # Stats file
        self.stats_file = stats_file

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

        # messages outside skew window
        self.skew_exceeded_acars = 0
        self.skew_exceeded_vdlm2 = 0

        self.queue_lists = list()
        self.standalone_queues = list()
        self.standalone_deque = list()

        self.last_thread_count = 0

    def save_stats_file(self):
        if self.stats_file:

            # prepare output with counters
            output_dict = {
                'messages_received_total_acars': self.listen_udp_acars + self.listen_tcp_acars + self.receive_tcp_acars,
                'messages_received_total_vdlm2': self.listen_udp_vdlm2 + self.listen_tcp_vdlm2 + self.receive_tcp_vdlm2 + self.receive_zmq_vdlm2,
                'messages_received_by_listen_udp_acars': self.listen_udp_acars,
                'messages_received_by_listen_tcp_acars': self.listen_tcp_acars,
                'messages_received_by_receive_tcp_acars': self.receive_tcp_acars,
                'messages_received_by_listen_udp_vdlm2': self.listen_udp_vdlm2,
                'messages_received_by_listen_tcp_vdlm2': self.listen_tcp_vdlm2,
                'messages_received_by_receive_tcp_vdlm2': self.receive_tcp_vdlm2,
                'messages_received_by_receive_zmq_vdlm2': self.receive_zmq_vdlm2,
                'messages_received_invalid_json_acars': self.invalid_json_acars,
                'messages_received_invalid_json_vdlm2': self.invalid_json_vdlm2,
                'messages_received_duplicate_acars': self.duplicate_acars,
                'messages_received_duplicate_vdlm2': self.duplicate_vdlm2,
                'messages_received_skew_exceeded_acars': self.skew_exceeded_acars,
                'messages_received_skew_exceeded_vdlm2': self.skew_exceeded_vdlm2,
            }

            # prepare output with queue depths
            for q in self.standalone_queues:
                logger.log(logging.DEBUG, f"Queue depth of {q.name}: {q.qsize()}")
                output_dict[f'queue_depth_{q.name}'] = int(f'{q.qsize()}')
            for queue_list in self.queue_lists:
                for q in queue_list:
                    output_dict[f'queue_depth_{q.name}'] = int(f'{q.qsize()}')
            for dq in self.standalone_deque:
                dqlen = len(dq[1])
                output_dict[f'queue_depth_{dq[0]}'] = int(f'{dqlen}')

            # turn dict into json
            output_json = json.dumps(
                output_dict,
                separators=(',', ':'),
                sort_keys=True,
            )

            # write file
            with open(self.stats_file, 'w') as f:
                f.write(output_json)

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
        if self.skew_exceeded_acars > 0:
            logger.log(level, f"Skew exceeded ACARS messages dropped: {self.skew_exceeded_acars}")
        if self.skew_exceeded_vdlm2 > 0:
            logger.log(level, f"Skew exceeded VDLM2 messages dropped: {self.skew_exceeded_vdlm2}")

        thread_count = threading.active_count()
        if thread_count != self.last_thread_count:
            self.last_thread_count = thread_count
            logger.log(level, f"Active threads: {thread_count}")

        # Log queue depths (only if non zero)
        for q in self.standalone_queues:
            qs = q.qsize()
            if qs > 0:
                logger.log(logging.DEBUG, f"Queue depth of {q.name}: {q.qsize()}")
        for queue_list in self.queue_lists:
            for q in queue_list:
                qs = q.qsize()
                if qs > 0:
                    logger.log(logging.DEBUG, f"Queue depth of {q.name}: {q.qsize()}")
        for dq in self.standalone_deque:
            dqlen = len(dq[1])
            if dqlen > 0:
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


udp_partial_dict = {}


class InboundUDPMessageHandler(socketserver.BaseRequestHandler):
    """ Multi-threaded UDP server to receive ACARS/VDLM2 messages """
    def __init__(self, request, client_address, server):
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

        address = (host, port)

        # prepare logging
        logger = baselogger.getChild(f'input.udp.{self.protoname}.{host}:{port}')

        data = data.decode()

        partial = udp_partial_dict.get(address)

        if partial:
            del udp_partial_dict[address]

        lines, partial = reassemble_json(partial, data, logger)

        if partial:
            udp_partial_dict[address] = partial

        for line in lines:
            # prepare data to be enqueued into input queue
            incoming_data = {
                'raw_json': copy.deepcopy(line),            # raw json data
                'src_host': copy.deepcopy(host),            # src host
                'src_port': copy.deepcopy(port),            # src port
                'src_name': f'input.udp.{self.protoname}',  # function originating the data
                'msg_uuid': uuid.uuid1(),                   # unique identifier for this message
            }

            # trace logging
            logger.log(logging_TRACE, f"in: {host}:{port}/udp; out: {self.inbound_message_queue.name}; data: {incoming_data}")

            # enqueue the data
            self.inbound_message_queue.put_or_die(incoming_data)

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
        logger = baselogger.getChild(f'input.tcpserver.{self.protoname}.{host}:{port}')
        logger.info("connection established")

        partial = None

        # loop until the session is disconnected (we receive no data)
        while True:

            # read data from socket
            data = self.request.recv(65527)

            # if nothing returned from the socket, then "disconnect"
            if not data:
                break

            data = data.decode()

            lines, partial = reassemble_json(partial, data, logger)

            for line in lines:
                # prepare data to be enqueued into input queue
                incoming_data = {
                    'raw_json': copy.deepcopy(line),                                # raw json data
                    'src_host': copy.deepcopy(host),                                # src host
                    'src_port': copy.deepcopy(port),                                # src port
                    'src_name': f'input.tcpserver.{self.protoname}.{host}:{port}',  # function originating the data
                    'msg_uuid': uuid.uuid1(),                                       # unique identifier for this message
                }

                # trace logging
                logger.log(logging_TRACE, f"in: {host}:{port}/tcp; out: {self.inbound_message_queue.name}; data: {incoming_data}")

                # enqueue the data
                self.inbound_message_queue.put_or_die(incoming_data)

                # increment counters
                COUNTERS.increment(f'listen_tcp_{self.protoname}')

        # if broken out of the loop, then "connection lost"
        logger.info("connection lost")


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
                        data = sock.recv(65527)

                    except socket.timeout:
                        continue

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
                            inbound_message_queue.put_or_die(incoming_data)

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
            inbound_message_queue.put_or_die(incoming_data)

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

    message_count = 0

    # pop items off queue forever
    while True:
        # pop data from queue (blocks until an item can be popped)
        data = in_queue.get()
        in_queue.task_done()

        # deal with multiple JSON messages that throw json.JSONDecodeError: Extra data: line L column N (char C)
        raw_json = copy.deepcopy(data['raw_json'])

        # initially, we attempt to decode the whole message
        decode_to_char = len(raw_json)

        # counter to prevent getting stuck in an infinite loop (shouldn't happen as everything is in try/except, but just to be sure)
        decode_attempts = 0

        # while there is data left to decode:
        while len(raw_json) > 0:

            # ensure we're not stuck in an infinite loop (unlikely there'd be 100 parts of json in a message. I've only ever seen up to 2.)
            if decode_attempts > 100:
                logger.error(f"infinite loop deserialising; raw_json: {raw_json}")
                break
            else:
                decode_attempts += 1

            # attempt to deserialise
            try:
                to_decode = raw_json[:decode_to_char]
                deserialised_json = json.loads(to_decode)

            # if there is extra data, attempt to decode as much as we can next iteration of loop
            except json.JSONDecodeError as e:
                logger.log(logging_TRACE, f"message contains extra data: {data}: {e}, attempting to decode to character {e.pos}, then will attempt remaining data")
                if e.pos > 0 and e.pos < decode_to_char:
                    decode_to_char = e.pos
                    continue
                else:
                    logger.error(f"json decoding failed, reattempt impossible: invalid JSON received via {data['src_name']} (possible reason: UDP packet loss, consider using TCP)")
                    logger.debug(f"e.pos: {e.pos} decode_to_char: {decode_to_char} exception: {e} to_decode: {to_decode} raw_json: {raw_json}")
                    COUNTERS.increment(f'invalid_json_{protoname}')
                    break

            # if an exception, log and continue
            except Exception as e:
                logger.error(f"invalid JSON received via {data['src_name']} exception: {e} to_decode: {to_decode}")
                COUNTERS.increment(f'invalid_json_{protoname}')
                break

            # if there was no exception:

            # remove the json we've already serialised from the input
            raw_json = raw_json[decode_to_char:]
            decode_to_char = len(raw_json)

            # ensure json.loads resulted in a dict
            if type(deserialised_json) != dict:
                logger.error(f"invalid JSON received via {data['src_name']} json.loads returned non-dict object")
                logger.debug(f"to_decode: {to_decode}")
                COUNTERS.increment(f'invalid_json_{protoname}')

            # if it is a dict...
            else:
                if message_count == 0:
                    logger.info(f"Receiving {protoname} messages!")
                message_count += 1

                # build output message object
                data_out = copy.deepcopy(data)

                # add part of raw json which was decoded
                data_out['raw_json'] = to_decode

                # add deserialised_json
                data_out['json'] = deserialised_json

                # trace logging
                logger.log(logging_TRACE, f"in: {in_queue.name}; out: {out_queue.name}; data: {data_out}")

                # enqueue the data
                out_queue.put_or_die(data_out)


def within_acceptable_skew(timestamp: int, skew_window_secs: int):
    """
    Helper function that determines whether timestamp (in nanoseconds) is within +/- skew_window_secs
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

        try:

            # ensure message has a timestamp
            if 'timestamp' not in data['json']:
                logger.error(f"message does not contain 'timestamp' field: {data['json']}, dropping message")
                continue

            # create timestamp (in nanoseconds) from message timestamp
            data['msgtime_ns'] = int(float(data['json']['timestamp']) * 1e9)

            # drop messages with timestamp outside of max skew range
            if 'msgtime_ns' in data and not within_acceptable_skew(data['msgtime_ns'], skew_window_secs):
                logger.warning(f"message timestamp outside acceptable skew window: {data['msgtime_ns'] * 1e-9} (now: {time.time()})")
                logger.debug(f"message timestamp outside acceptable skew window: {data}")
                # increment counters
                COUNTERS.increment(f'skew_exceeded_{protoname}')
                continue

            # copy object so we don't molest original data
            data_to_hash = copy.deepcopy(data['json'])

            # remove feeder-specific data so we only hash data unique to the message
            if 'error' in data_to_hash:
                del(data_to_hash['error'])
            else:
                logger.debug(f"message does not contain expected 'error' field: {data_to_hash}")
            if 'level' in data_to_hash:
                del(data_to_hash['level'])
            else:
                logger.debug(f"message does not contain expected 'level' field: {data_to_hash}")
            if 'station_id' in data_to_hash:
                del(data_to_hash['station_id'])
            else:
                logger.debug(f"message does not contain expected 'station_id' field: {data_to_hash}")
            if 'timestamp' in data_to_hash:
                del(data_to_hash['timestamp'])
            else:
                logger.debug(f"message does not contain expected 'timestamp' field: {data_to_hash}")
            if 'channel' in data_to_hash:
                del(data_to_hash['channel'])
            else:
                logger.debug(f"message does not contain expected 'channel' field: {data_to_hash}")

            if 'app' in data_to_hash:
                del(data_to_hash['app'])

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
            out_queue.put_or_die(data)

        except Exception as e:
            logger.error(f"Exception when hashing this message: {data.get('json')}\n{e}")
            break


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

        try:
            # determine whether data is from dumpvdl2 (preferred) or vdlm2dec
            data['format'] = "unknown"
            if 'timestamp' in data['json']:
                data['format'] = "vdlm2dec"
            if 'vdl2' in data['json']:
                if 'app' in data['json']['vdl2']:
                    if 'name' in data['json']['vdl2']['app']:
                        if data['json']['vdl2']['app']['name'].lower() == "dumpvdl2":
                            data['format'] = "dumpvdl2"

            # create timestamp:
            if data['format'] == "dumpvdl2":

                # ensure message has a timestamp
                if 't' not in data['json']['vdl2']:
                    logger.error(f"message does not contain 't' field: {data['json']}, dropping message")
                    continue

                # create timestamp from t.sec & t.usec
                data['msgtime_ns'] = (int(data['json']['vdl2']['t']['sec']) * 1e9) + (int(data['json']['vdl2']['t']['usec']) * 1000)

            elif data['format'] == "vdlm2dec":

                # ensure message has a timestamp
                if 'timestamp' not in data['json']:
                    logger.error(f"message does not contain 'timestamp' field: {data['json']}, dropping message")
                    continue

                # create timestamp (in nanoseconds) from message timestamp
                data['msgtime_ns'] = int(float(data['json']['timestamp']) * 1e9)

            else:
                logger.error(f"unknown vdl message format: {data['json']}")

            # drop messages with timestamp outside of max skew range
            if 'msgtime_ns' in data and not within_acceptable_skew(data['msgtime_ns'], skew_window_secs):
                logger.warning(f"message timestamp outside acceptable skew window: {data['msgtime_ns'] * 1e-9} (now: {time.time()})")
                logger.debug(f"message timestamp outside acceptable skew window: {data}")
                continue

            # copy object so we don't molest original data
            data_to_hash = copy.deepcopy(data['json'])

            # remove feeder-specific data so we only hash data unique to the message
            if data['format'] == "dumpvdl2":

                if 'app' in data_to_hash['vdl2']:
                    del(data_to_hash['vdl2']['app'])
                else:
                    logger.debug(f"message does not contain expected 'app' field: {data_to_hash}")
                if 'freq_skew' in data_to_hash['vdl2']:
                    del(data_to_hash['vdl2']['freq_skew'])
                else:
                    logger.debug(f"message does not contain expected 'freq_skew' field: {data_to_hash}")
                if 'hdr_bits_fixed' in data_to_hash['vdl2']:
                    del(data_to_hash['vdl2']['hdr_bits_fixed'])
                else:
                    logger.debug(f"message does not contain expected 'hdr_bits_fixed' field: {data_to_hash}")
                if 'noise_level' in data_to_hash['vdl2']:
                    del(data_to_hash['vdl2']['noise_level'])
                else:
                    logger.debug(f"message does not contain expected 'noise_level' field: {data_to_hash}")
                if 'octets_corrected_by_fec' in data_to_hash['vdl2']:
                    del(data_to_hash['vdl2']['octets_corrected_by_fec'])
                else:
                    logger.debug(f"message does not contain expected 'octets_corrected_by_fec' field: {data_to_hash}")
                if 'sig_level' in data_to_hash['vdl2']:
                    del(data_to_hash['vdl2']['sig_level'])
                else:
                    logger.debug(f"message does not contain expected 'sig_level' field: {data_to_hash}")
                if 'station' in data_to_hash['vdl2']:
                    del(data_to_hash['vdl2']['station'])
                else:
                    logger.debug(f"message does not contain expected 'station' field: {data_to_hash}")
                if 't' in data_to_hash['vdl2']:
                    del(data_to_hash['vdl2']['t'])
                else:
                    logger.debug(f"message does not contain expected 't' field: {data_to_hash}")

            elif data['format'] == "vdlm2dec":

                # remove feeder-specific data so we only hash data unique to the message
                if 'app' in data_to_hash:
                    del(data_to_hash['app'])
                if 'error' in data_to_hash:
                    del(data_to_hash['error'])
                if 'level' in data_to_hash:
                    del(data_to_hash['level'])
                if 'station_id' in data_to_hash:
                    del(data_to_hash['station_id'])
                if 'timestamp' in data_to_hash:
                    del(data_to_hash['timestamp'])
                else:
                    logger.debug(f"message does not contain expected 'timestamp' field: {data_to_hash}")
                if 'channel' in data_to_hash:
                    del(data_to_hash['channel'])
                else:
                    logger.debug(f"message does not contain expected 'channel' field: {data_to_hash}")

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
            out_queue.put_or_die(data)

        except Exception as e:
            logger.error(f"Exception when hashing this message: {data.get('json')}\n{e}")
            break


def deduper(
    in_queue: ARQueue,
    out_queue: ARQueue,
    recent_message_queue: collections.deque,
    protoname: str,
    dedupe_window_secs: float,
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

            # evict items older than 2 seconds
            evict_cutoff = (time.time_ns() - (dedupe_window_secs * 1e9))
            while len(recent_message_queue) > 0 and recent_message_queue[0]['msgtime_ns'] <= evict_cutoff:
                evictedmsg = recent_message_queue.popleft()
                logger.log(logging_TRACE, f"evicted: {evictedmsg}")

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
            out_queue.put_or_die(data)


# OUTPUTTING MESSAGES #


def output_queue_populator(in_queue: ARQueue, out_queues: list, protoname: str, station_name_override: str = None, label_proxy: bool = True):
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

        # label proxy messages

        if label_proxy:
            # VDLM via dumpvdl2
            if 'vdl2' in data['json']:
                if 'app' not in data['json']['vdl2']:
                    data['json']['vdl2']['app'] = {}

                data['json']['vdl2']['app']['proxied'] = True
                data['json']['vdl2']['app']['proxied_by'] = "acars_router"
            # acarsdec **OR** VDLM2 via vdl2dec
            else:
                if 'app' not in data['json']:
                    data['json']['app'] = {}

                data['json']['app']['proxied'] = True
                data['json']['app']['proxied_by'] = "acars_router"

        # serialise json
        data['out_json'] = bytes(
            json.dumps(
                data['json'],
                separators=(',', ':'),
                sort_keys=True,
            ) + '\n', 'utf-8')

        # put copy of message into each output queue
        for output_queue in out_queues:

            # trace logging
            logger.log(logging_TRACE, f"in: {in_queue.name}; out: {output_queue.name}; data: {data}")

            # enqueue a copy of the data
            output_queue.put_or_die(copy.deepcopy(data))


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
    q = ARQueue(qname, global_queue_size)
    output_queues.append(q)

    suppress_errors_until = 0
    suppress_duration = 15
    sock = None
    while True:

        if sock is not None:
            try:
                sock.close()
            except Exception as e:
                logger.error(f"socket error: {e}")

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
                msg = data['out_json']
                # UDP in python only works up to 8192 bytes, send message in chunks if necessary
                # this only works if the receiver can reassemble the messages and the order
                # stays correct while in transit
                data_sent = 0
                total = len(msg)
                chunk_size = 8192
                while data_sent < total:
                    sock.sendto(msg[data_sent:(data_sent + chunk_size)], (host, port))
                    data_sent += chunk_size

                # trace
                logger.log(logging_TRACE, f"in: {qname}; out: {host}:{port}/udp; data: {data}")

            except Exception as e:

                now = time.time()
                if now > suppress_errors_until:
                    logger.error(f"error sending to {host}:{port}: {e} (suppressing for {suppress_duration} seconds)")
                    suppress_errors_until = now + suppress_duration

                # break inside loop, set up a new socket
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
    q = ARQueue(qname, global_queue_size)
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
            conn.sendall(data['out_json'])

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

    sock = None
    next_reconnect = 0
    # Loop connect
    while True:

        if sock is not None:
            try:
                sock.close()
            except Exception as e:
                logger.error(f"socket error: {e}")

        now = time.time()
        wait_time = next_reconnect - now
        if wait_time > 0:
            time.sleep(wait_time)

        # make sure we don't reconnect too quickly
        next_reconnect = now + 10

        try:
            # create socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        except Exception as e:
            logger.error(f"socket error: {e}")
            continue

        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            # set socket timeout to 5 seconds
            sock.settimeout(5)

            # attempt connection
            sock.connect((host, port))

        except ConnectionRefusedError:
            logger.error("connection refused")
            continue

        except Exception as e:
            logger.error(f"connection error: {e}")
            continue

        logger.info("connection established")

        # Create an output queue for this instance of the function & add to output queue
        qname = f'output.tcpclient.{protoname}.{host}:{port}'
        q = ARQueue(qname, global_queue_size)

        logger.debug(f"registering output queue: {qname}")
        output_queues.append(q)

        # Loop to send messages from output queue
        while True:

            # Pop a message from the output queue
            data = q.get()
            q.task_done()

            # try to send the message to the remote host
            try:
                sock.sendall(data['out_json'])

            except Exception as e:
                logger.error(f"error sending to {host}:{port}: {e}")
                break

        # finally, let the user know client has disconnected
        logger.info("connection lost")

        # clean up
        # remove this instance's queue from the output queue
        logger.debug(f"deregistering output queue: {qname}")
        output_queues.remove(q)

        # delete our queue
        del(q)


def ZMQServer(port: int, output_queues: list, protoname: str):
    """
    Process to send ACARS/VDLM2 messages to TCP clients.
    Intended to be run in a thread.
    """
    protoname = protoname.lower()

    # prepare logging
    logger = baselogger.getChild(f'output.zmqserver.{protoname}:{port}')
    logger.debug("spawned")

    while True:
        # Create an output queue for this instance of the function & add to output queue
        qname = f'output.zmqserver.{protoname}:{port}'
        logger.debug(f"registering output queue: {qname}")
        q = ARQueue(qname, global_queue_size)
        output_queues.append(q)

        # Set up zmq context
        context = zmq.Context()

        # This is our public endpoint for subscribers
        backend = context.socket(zmq.PUB)
        backend.bind(f"tcp://0.0.0.0:{port}")

        # Loop to send messages from output queue
        while True:

            # Pop a message from the output queue
            data = q.get()
            q.task_done()

            # try to send the message to the remote host
            try:
                backend.send_multipart([data['out_json'], ])

                # trace
                logger.log(logging_TRACE, f"in: {qname}; out: {port}/zmq; data: {data}")

            except Exception as e:
                logger.error(f"error sending: {e}")
                break

        # clean up
        # remove this instance's queue from the output queue
        logger.debug(f"deregistering output queue: {qname}")
        output_queues.remove(q)
        # delete our queue
        del(q)

    logger.info("server stopped")


# HELPER FUNCTIONS #

def save_stats_file(
    secs: int = 10,
):
    """
    Saves the status of the global counters into JSON file
    Intended to be run in a thread, as this function will run forever.

    Arguments:
    mins -- the number of minutes between logging stats
    """
    logger = baselogger.getChild('save_stats_file')
    logger.debug("spawned")
    while True:
        time.sleep(secs)
        COUNTERS.save_stats_file()


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
    logger = baselogger.getChild('display_stats')
    logger.debug("spawned")
    while True:
        time.sleep(mins * 60)
        COUNTERS.log(logger, loglevel)


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


def valid_tcp_udp_port(num: int):
    """
    Returns True if num is a valid TCP/UDP port number, else return False.
    """
    if type(num) == int:
        if 1 <= int(num) <= 65535:
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
    for i in args.receive_tcp_acars:
        # try to split host:port
        try:
            host = i.split(':')[0]
            port = i.split(':')[1]
        except IndexError:
            logger.critical(f"receive_tcp_acars: host:port expected, got: {i}")
            return False
        # ensure port valid
        try:
            if not valid_tcp_udp_port(int(port)):
                raise ValueError
        except ValueError:
            logger.critical(f"receive_tcp_acars: invalid port: {port}")
            return False
        # warn if host appears wrong
        try:
            socket.gethostbyname(host)
        except socket.gaierror:
            logger.warning(f"receive_tcp_acars: host appears invalid or unresolvable: {host}")

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
            if not valid_tcp_udp_port(int(port)):
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
            if not valid_tcp_udp_port(int(port)):
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
            if not valid_tcp_udp_port(int(port)):
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
            if not valid_tcp_udp_port(int(port)):
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
            if not valid_tcp_udp_port(int(port)):
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
            if not valid_tcp_udp_port(int(port)):
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

    # Check serve_zmq_acars, should be list of valid port numbers
    for i in args.serve_zmq_acars:
        try:
            if not valid_tcp_udp_port(int(i)):
                raise ValueError
        except ValueError:
            logger.critical(f"serve_tcp_acars: invalid port: {i}")
            return False

    # Check serve_tcp_vdlm2, should be list of valid port numbers
    for i in args.serve_tcp_vdlm2:
        try:
            if not valid_tcp_udp_port(int(i)):
                raise ValueError
        except ValueError:
            logger.critical(f"serve_tcp_vdlm2: invalid port: {i}")
            return False

    # Check serve_zmq_vdlm2, should be list of valid port numbers
    for i in args.serve_zmq_vdlm2:
        try:
            if not valid_tcp_udp_port(int(i)):
                raise ValueError
        except ValueError:
            logger.critical(f"serve_zmq_vdlm2: invalid port: {i}")
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

    # Check stats_file
    # Ensure directory exists if variable is set
    if args.stats_file:
        if not (os.path.isdir(os.path.dirname(args.stats_file)) or os.path.ismount(os.path.dirname(args.stats_file))):
            logger.critical(f"stats_file: is not a directory: {os.path.dirname(args.stats_file)}")
            return False

    # If we're here, all arguments are good
    return True


def sigterm_exit(signum, frame):
    sys.stderr.write("acars_router: caught SIGTERM, exiting!\n")
    sys.exit()


def sigint_exit(signum, frame):
    sys.stderr.write("acars_router: caught SIGINT, exiting!\n")
    sys.exit()


if __name__ == "__main__":

    signal.signal(signal.SIGINT, sigint_exit)
    signal.signal(signal.SIGTERM, sigterm_exit)

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
        '--serve-zmq-acars',
        help='Serve ACARS messages as a ZeroMQ publisher on TCP "port". Can be specified multiple times to serve on multiple ports.',
        type=str,
        nargs='*',
        default=split_env_safely('AR_SERVE_ZMQ_ACARS'),
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
        help='Send VDLM2 messages via TCP to "host:port[:format]". Can be specified multiple times to send to multiple clients.',
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
        '--serve-zmq-vdlm2',
        help='Serve VDLM2 messages as a ZeroMQ publisher on TCP "port". Can be specified multiple times to serve on multiple ports.',
        type=str,
        nargs='*',
        default=split_env_safely('AR_SERVE_ZMQ_VDLM2'),
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
        help=f'Number of threads for JSON deserialisers (default: {1})',
        type=int,
        nargs='?',
        default=int(os.getenv("AR_THREADS_JSON_DESERIALISER", 1)),
    )
    parser.add_argument(
        '--threads-hasher',
        help=f'Number of threads for message hashers (default: {1})',
        type=int,
        nargs='?',
        default=int(os.getenv("AR_THREADS_HASHER", 1)),
    )
    parser.add_argument(
        '--threads-deduper',
        help=f'Number of threads for message dedupers (default: {1})',
        type=int,
        nargs='?',
        default=int(os.getenv("AR_THREADS_DEDUPER", 1)),
    )
    parser.add_argument(
        '--threads-output-queue-populator',
        help=f'Number of threads for output queue populators (default: {1})',
        type=int,
        nargs='?',
        default=int(os.getenv("AR_OUTPUT_QUEUE_POPULATOR", 1)),
    )
    parser.add_argument(
        '--override-station-name',
        help='Overrides station id/name with this value',
        type=str,
        nargs='?',
        default=os.getenv("AR_OVERRIDE_STATION_NAME", None),
    )
    parser.add_argument(
        '--stats-file',
        help='Write a JSON-format stats file',
        type=str,
        nargs='?',
        default=os.getenv("AR_STATS_FILE", None),
    )
    parser.add_argument(
        '--run-testcases',
        help='Run some limited test cases',
        action='store_true',
        default=False,
    )

    # Argument to toggle the output of proxy information
    # We can't use default argprase bool operations (action=set_true/false) and using the presence
    # of a flag to toggle behavior from the default
    # because we need to be able to use an ENV variable to flag the value for running
    # in Docker.

    parser.add_argument(
        '--add-proxy-id',
        help='Add a proxy id to the JSON message',
        type=lambda x: bool(strtobool(x)), nargs='?',
        const=True, default=os.getenv("AR_ADD_PROXY_ID", True))

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
    if not valid_args(args):
        sys.exit(1)

    # run testcases when asked
    if args.run_testcases:
        test_reassemble_json()
        sys.exit(0)

    # initialise counters
    COUNTERS = ARCounters(
        stats_file=args.stats_file,
    )

    # initialise threading lock
    lock = threading.Lock()

    # Start stats threads
    threading.Thread(
        target=display_stats,
        daemon=True,
        args=(args.stats_every,),
    ).start()
    threading.Thread(
        target=save_stats_file,
        daemon=True,
        args=(10,),
    ).start()

    # Prepare output queues
    # populate with data (bytes) to send out
    output_acars_queues = list()
    output_vdlm2_queues = list()
    COUNTERS.register_queue_list(output_acars_queues)
    COUNTERS.register_queue_list(output_vdlm2_queues)

    # define inbound message queues
    inbound_acars_message_queue = ARQueue('inbound_acars_message_queue', global_queue_size)
    COUNTERS.register_queue(inbound_acars_message_queue)
    inbound_vdlm2_message_queue = ARQueue('inbound_vdlm2_message_queue', global_queue_size)
    COUNTERS.register_queue(inbound_vdlm2_message_queue)

    # define intermediate queue for deserialised messages
    deserialised_acars_message_queue = ARQueue('deserialised_acars_message_queue', global_queue_size)
    COUNTERS.register_queue(deserialised_acars_message_queue)
    deserialised_vdlm2_message_queue = ARQueue('deserialised_vdlm2_message_queue', global_queue_size)
    COUNTERS.register_queue(deserialised_vdlm2_message_queue)

    # define intermediate queue for hashed messages
    hashed_acars_message_queue = ARQueue('hashed_acars_message_queue', global_queue_size)
    COUNTERS.register_queue(hashed_acars_message_queue)
    hashed_vdlm2_message_queue = ARQueue('hashed_vdlm2_message_queue', global_queue_size)
    COUNTERS.register_queue(hashed_vdlm2_message_queue)

    # define intermediate queue for deduped messages
    deduped_acars_message_queue = ARQueue('deduped_acars_message_queue', global_queue_size)
    COUNTERS.register_queue(deduped_acars_message_queue)
    deduped_vdlm2_message_queue = ARQueue('deduped_vdlm2_message_queue', global_queue_size)
    COUNTERS.register_queue(deduped_vdlm2_message_queue)

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

        # recent message buffers for dedupe
        recent_message_queue_acars = collections.deque()
        COUNTERS.register_deque("recent_message_queue_acars", recent_message_queue_acars)
        recent_message_queue_vdlm2 = collections.deque()
        COUNTERS.register_deque("recent_message_queue_vdlm2", recent_message_queue_vdlm2)

        # if dedupe enabled: use dedupe queue & start dedupers

        for _ in range(args.threads_deduper):
            threading.Thread(
                target=deduper,
                args=(hashed_acars_message_queue, deduped_acars_message_queue, recent_message_queue_acars, "ACARS", args.dedupe_window),
                daemon=True,
            ).start()

        for _ in range(args.threads_deduper):
            threading.Thread(
                target=deduper,
                args=(hashed_vdlm2_message_queue, deduped_vdlm2_message_queue, recent_message_queue_vdlm2, "VDLM2", args.dedupe_window),
                daemon=True,
            ).start()

        # output queue populator: acars
        for _ in range(args.threads_output_queue_populator):
            threading.Thread(
                target=output_queue_populator,
                args=(deduped_acars_message_queue, output_acars_queues, "ACARS", args.override_station_name, args.add_proxy_id),
                daemon=True,
            ).start()

        # output queue populator: vdlm2
        for _ in range(args.threads_output_queue_populator):
            threading.Thread(
                target=output_queue_populator,
                args=(deduped_vdlm2_message_queue, output_vdlm2_queues, "VDLM2", args.override_station_name, args.add_proxy_id),
                daemon=True,
            ).start()

    else:
        # if dedupe disabled: bypass dedupe queue & don't start dedupers

        # output queue populator: acars
        for _ in range(args.threads_output_queue_populator):
            threading.Thread(
                target=output_queue_populator,
                args=(hashed_acars_message_queue, output_acars_queues, "ACARS", args.override_station_name, args.add_proxy_id),
                daemon=True,
            ).start()

        # output queue populator: vdlm2
        for _ in range(args.threads_output_queue_populator):
            threading.Thread(
                target=output_queue_populator,
                args=(hashed_vdlm2_message_queue, output_vdlm2_queues, "VDLM2", args.override_station_name, args.add_proxy_id),
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

    # acars zmq output (server)
    for port in args.serve_zmq_acars:
        logger.info(f'serving ACARS via ZMQ over TCP, port: {port}')
        threading.Thread(
            target=ZMQServer,
            args=(port, output_acars_queues, "ACARS"),
            daemon=True,
        ).start()

    # vdlm2 zmq output (server)
    for port in args.serve_zmq_vdlm2:
        logger.info(f'serving VDLM2 via ZMQ over TCP, port: {port}')
        threading.Thread(
            target=ZMQServer,
            args=(port, output_vdlm2_queues, "VDLM2"),
            daemon=True,
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
    next_udp_partial_clear = 0
    while True:
        time.sleep(10)
        now = time.time()
        if now > next_udp_partial_clear:
            udp_partial_dict.clear()
            # clear partial dict every 10 mins
            next_udp_partial_clear = now + 600
