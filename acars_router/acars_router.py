
import os
import sys
import copy
import argparse
import socket
import socketserver
import threading
lock = threading.Lock()
import queue
import time
import logging

logging.addLevelName(logging.DEBUG - 5, 'TRACE')
baselogger = logging.getLogger('acars_router')

class ARQueue(queue.Queue):
    """
    A subclass of queue.Queue, allowing us to name the queue.
    """
    def __init__(self, name: str, maxsize: int=0):
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

        self.queue_lists = list()
        self.standalone_queues = list()

    def log(self, logger: logging.Logger, level: int):
        if self.listen_udp_acars > 0:
            logger.log(level, f"ACARS messages via UDP listen: {self.listen_udp_acars}")
        if self.listen_tcp_acars > 0:
            logger.log(level, f"ACARS messages via TCP listen: {self.listen_tcp_acars}")
        if self.receive_tcp_acars > 0:
            logger.log(level, f"ACARS messages via TCP receive: {self.receive_tcp_acars}")
        if self.listen_udp_vdlm2 > 0:
            logger.log(level, f"VDLM2 messages via UDP listen: {self.listen_udp_acars}")
        if self.listen_tcp_vdlm2 > 0:
            logger.log(level, f"VDLM2 messages via TCP listen: {self.listen_tcp_acars}")
        if self.receive_tcp_vdlm2 > 0:
            logger.log(level, f"VDLM2 messages via TCP receive: {self.receive_tcp_acars}")

        # Log queue depths (TODO: should probably be debug level)
        for q in self.standalone_queues:
            logger.log(level, f"Queue depth of {q.name}: {q.qsize()}")
        for queue_list in self.queue_lists:
            for q in queue_list:
                logger.log(level, f"Queue depth of {q.name}: {q.qsize()}")
    
    def register_queue_list(self, qlist: list):
        self.queue_lists.append(qlist)

    def register_queue(self, q: ARQueue):
        self.standalone_queues.append(q)

    def increment(self, counter):
        """
        Increment a counter.
        eg: COUNTER.increment('receive_tcp_acars')
        """
        with lock:
            setattr(self, counter, getattr(self, counter)+1)
        return None

COUNTERS = ARCounters()

def display_stats(
    mins: int=5,
    loglevel: int=logging.INFO
):
    """
    Displays the status of the global counters.
    Intended to be run in a thread, as this function will run forever.

    Arguments:
    mins -- the number of minutes between logging stats
    loglevel -- the log level to use when logging statistics
    """
    logger = baselogger.getChild(f'statistics')
    logger.debug("spawned")
    global COUNTERS
    while True:
        time.sleep(mins * 60)
        COUNTERS.log(logger, loglevel)

class ThreadedUDPServer(socketserver.ThreadingMixIn, socketserver.UDPServer):
    """ Mix-in for multi-threaded UDP server """
    def __init__(self, server_address, RequestHandlerClass, bind_and_activate=True, inbound_message_queue=None, protoname=None):
        self.inbound_message_queue = inbound_message_queue
        self.protoname = protoname.lower()
        socketserver.UDPServer.__init__(self, server_address, RequestHandlerClass, bind_and_activate=bind_and_activate)

class InboundUDPMessageHandler(socketserver.BaseRequestHandler):
    """ Multi-threaded UDP server to receive ACARS/VDLM2 messages """
    def __init__(self, request, client_address, server):
        self.logger = baselogger.getChild(f'input.udp.{server.protoname}')
        self.inbound_message_queue = server.inbound_message_queue
        self.protoname = server.protoname
        socketserver.BaseRequestHandler.__init__(self, request, client_address, server)

    def handle(self):
        data = self.request[0].strip()
        socket = self.request[1]
        self.inbound_message_queue.put(data)
        global COUNTERS
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
        global COUNTERS
        self.logger = baselogger.getChild(f'input.tcpserver.{self.protoname}.{self.client_address[0]}:{self.client_address[1]}')
        self.logger.info("connection established")
        while True:
            data = self.request.recv(8192).strip()
            if not data: break
            self.inbound_message_queue.put(data)
            COUNTERS.increment(f'listen_udp_{self.protoname}')
        self.logger.info("connection lost")

def UDPSender(host, port, output_queues: list, protoname: str):
    """
    Threaded function to send ACARS / VDLM2 messages via UDP
    Intended to be run in a thread.
    """
    protoname = protoname.lower()
    logger = baselogger.getChild(f'output.udp.{protoname}.{host}:{port}')
    logger.debug("spawned")
    # Create an output queue for this instance of the function & add to output queue
    q = ARQueue(f'output.udp.{protoname}.{host}:{port}', 100)
    output_queues.append(q)
    # Set up socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.settimeout(1)
    # Loop to send messages from output queue
    while True:
        data = q.get()
        try:
            sock.sendto(data, (host, port))
            logger.log(logging.DEBUG - 5, f"sent {data} to {host}:{port} OK")
        except:
            logger.error("Error sending to {host}:{port}/udp".format(
                host=host,
                port=port,
            ))
        q.task_done()
    # clean up
    # remove this instance's queue from the output queue
    output_queues.remove(q)
    # delete our queue
    del(q)

def TCPServerAcceptor(port: int, output_queues: list, protoname: str):
    """
    Accepts incoming TCP connections to serve ACARS / VDLM2 messages, and spawns a TCPServer for each connection.
    Intended to be run in a thread.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
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
    logger = baselogger.getChild(f'output.tcpserver.{protoname}.{host}:{port}')
    logger.info("connection established")
    # Create an output queue for this instance of the function & add to output queue
    q = ARQueue(f'output.tcpserver.{protoname}.{host}:{port}', 100)
    output_queues.append(q)
    # Set up socket
    conn.settimeout(1)
    connected = True
    # Loop to send messages from output queue
    while connected:
        data = q.get()
        logger.log(logging.DEBUG - 5, f"sending {data} to {host}:{port}")
        try:
            conn.sendall(data)
        except:
            connected = False
        q.task_done()
    # clean up
    # remove this instance's queue from the output queue
    output_queues.remove(q)
    # delete our queue
    del(q)
    # finally, let the user know client has disconnected
    logger.info("connection lost")

def TCPSender(host: str, port: int, output_queues: list, protoname: str):
    """
    Process to send ACARS/VDLM2 messages to a TCP server.
    Intended to be run in a thread.
    """
    protoname = protoname.lower()
    logger = baselogger.getChild(f'output.tcpclient.{protoname}.{host}:{port}')
    logger.debug("spawned")
    # Create an output queue for this instance of the function & add to output queue
    q = ARQueue(f'output.tcpclient.{protoname}.{host}:{port}', 100)
    output_queues.append(q)
    while True:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # clear queue before connecting
            with q.mutex:
                q.queue.clear()
            sock.settimeout(1)
            # attempt connection
            try:
                sock.connect((host, port))
            except ConnectionRefusedError:
                logger.error("connection refused")
                time.sleep(10)
            else:
                logger.info("connection established")
                connected = True
                while connected:
                    data = q.get()
                    logger.log(logging.DEBUG - 5, f"sending {data} to {host}:{port}")
                    try:
                        sock.sendall(data)
                    except:
                        connected = False
                        logger.info("connection lost")
                    q.task_done()

def TCPReceiver(host: str, port: int, inbound_message_queue: ARQueue, protoname: str):
    """
    Process to receive ACARS/VDLM2 messages from a TCP server.
    Intended to be run in a thread.
    """
    protoname = protoname.lower()
    logger = baselogger.getChild(f'input.tcpclient.{protoname}.{host}.{port}')
    logger.debug("spawned")
    # Set up counters
    global COUNTERS
    while True:
        logger.log(logging.DEBUG - 5, f"creating socket")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # attempt connection
            logger.log(logging.DEBUG - 5, f"attempting to connect")
            try:
                sock.connect((host, port))
            except ConnectionRefusedError:
                logger.error("connection refused")
                time.sleep(10)
            else:
                logger.info("connection established")
                while True:
                    data = sock.recv(8192)
                    logger.log(logging.DEBUG - 5, f"received {data} from {host}:{port}")
                    if not data: break
                    inbound_message_queue.put(data)
                    COUNTERS.increment(f'receive_tcp_{protoname}')
                logger.info("connection lost")

def message_processor(in_queue, out_queues, protoname):
    """
    Puts incoming ACARS/VDLM2 messages into each output queue.
    Intended to be run in a thread.
    """
    protoname = protoname.lower()
    logger = baselogger.getChild(f'message_processor.{protoname}')
    logger.debug("spawned")
    while True:
        data = in_queue.get()
        logger.log(logging.DEBUG - 5, f'{data}')
        for q in out_queues:
            q.put(copy.deepcopy(data))
        in_queue.task_done()

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
    data = q.get()
    q.task_done()
    logger.info(f"Receiving {protoname} messages!")
    out_queues.remove(q)
    del(q)

def valid_tcp_udp_port(num: int):
    """
    Returns True if num is a valid TCP/UDP port number, else return False.
    """
    if type(num) == int:
        if 1 >= int(i) <= 65535:
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
        except:
            logger.critical(f"listen_udp_acars: invalid port: {i}")
            return False

    # Check listen_tcp_acars, should be list of valid port numbers
    for i in args.listen_tcp_acars:
        try:
            if not valid_tcp_udp_port(int(i)):
                raise ValueError
        except:
            logger.critical(f"listen_tcp_acars: invalid port: {i}")
            return False

    # Check receive_tcp_acars, should be a list of host:port
    for i in args.listen_tcp_acars:
        # try to split host:port
        try:
            host = i.split(':')[0]
            port = i.split(':')[1]
        except:
            logger.critical(f"listen_tcp_acars: host:port expected, got: {i}")
            return False
        # ensure port valid
        try:
            if not valid_tcp_udp_port(port):
                raise ValueError
        except:
            logger.critical(f"listen_tcp_acars: invalid port: {port}")
            return False
        # warn if host appears wrong
        try:
            x = socket.gethostbyname(host)
        except:
            logger.warning(f"listen_tcp_acars: host appears invalid or unresolvable: {host}")

    # Check listen_udp_vdlm2, should be list of valid port numbers
    for i in args.listen_udp_vdlm2:
        try:
            if not valid_tcp_udp_port(int(i)):
                raise ValueError
        except:
            logger.critical(f"listen_udp_vdlm2: invalid port: {i}")
            return False

    # Check listen_tcp_vdlm2, should be list of valid port numbers
    for i in args.listen_tcp_vdlm2:
        try:
            if not valid_tcp_udp_port(int(i)):
                raise ValueError
        except:
            logger.critical(f"listen_tcp_vdlm2: invalid port: {i}")
            return False

    # Check receive_tcp_vdlm2, should be a list of host:port
    for i in args.receive_tcp_vdlm2:
        # try to split host:port
        try:
            host = i.split(':')[0]
            port = i.split(':')[1]
        except:
            logger.critical(f"receive_tcp_vdlm2: host:port expected, got: {i}")
            return False
        # ensure port valid
        try:
            if not valid_tcp_udp_port(port):
                raise ValueError
        except:
            logger.critical(f"receive_tcp_vdlm2: invalid port: {port}")
            return False
        # warn if host appears wrong
        try:
            x = socket.gethostbyname(host)
        except:
            logger.warning(f"receive_tcp_vdlm2: host appears invalid or unresolvable: {host}")

    # Check send_udp_acars, should be a list of host:port
    for i in args.send_udp_acars:
        # try to split host:port
        try:
            host = i.split(':')[0]
            port = i.split(':')[1]
        except:
            logger.critical(f"send_udp_acars: host:port expected, got: {i}")
            return False
        # ensure port valid
        try:
            if not valid_tcp_udp_port(port):
                raise ValueError
        except:
            logger.critical(f"send_udp_acars: invalid port: {port}")
            return False
        # warn if host appears wrong
        try:
            x = socket.gethostbyname(host)
        except:
            logger.warning(f"send_udp_acars: host appears invalid or unresolvable: {host}")

    # Check send_tcp_acars, should be a list of host:port
    for i in args.send_tcp_acars:
        # try to split host:port
        try:
            host = i.split(':')[0]
            port = i.split(':')[1]
        except:
            logger.critical(f"send_tcp_acars: host:port expected, got: {i}")
            return False
        # ensure port valid
        try:
            if not valid_tcp_udp_port(port):
                raise ValueError
        except:
            logger.critical(f"send_tcp_acars: invalid port: {port}")
            return False
        # warn if host appears wrong
        try:
            x = socket.gethostbyname(host)
        except:
            logger.warning(f"send_tcp_acars: host appears invalid or unresolvable: {host}")

    # Check send_udp_vdlm2, should be a list of host:port
    for i in args.send_udp_vdlm2:
        # try to split host:port
        try:
            host = i.split(':')[0]
            port = i.split(':')[1]
        except:
            logger.critical(f"send_udp_vdlm2: host:port expected, got: {i}")
            return False
        # ensure port valid
        try:
            if not valid_tcp_udp_port(port):
                raise ValueError
        except:
            logger.critical(f"send_udp_vdlm2: invalid port: {port}")
            return False
        # warn if host appears wrong
        try:
            x = socket.gethostbyname(host)
        except:
            logger.warning(f"send_udp_vdlm2: host appears invalid or unresolvable: {host}")

    # Check send_tcp_vdlm2, should be a list of host:port
    for i in args.send_tcp_vdlm2:
        # try to split host:port
        try:
            host = i.split(':')[0]
            port = i.split(':')[1]
        except:
            logger.critical(f"send_tcp_vdlm2: host:port expected, got: {i}")
            return False
        # ensure port valid
        try:
            if not valid_tcp_udp_port(port):
                raise ValueError
        except:
            logger.critical(f"send_tcp_vdlm2: invalid port: {port}")
            return False
        # warn if host appears wrong
        try:
            x = socket.gethostbyname(host)
        except:
            logger.warning(f"send_tcp_vdlm2: host appears invalid or unresolvable: {host}")

    # Check listen_tcp_acars, should be list of valid port numbers
    for i in args.serve_tcp_acars:
        try:
            if not valid_tcp_udp_port(int(i)):
                raise ValueError
        except:
            logger.critical(f"serve_tcp_acars: invalid port: {i}")
            return False

    # Check serve_tcp_vdlm2, should be list of valid port numbers
    for i in args.serve_tcp_acars:
        try:
            if not valid_tcp_udp_port(int(i)):
                raise ValueError
        except:
            logger.critical(f"serve_tcp_vdlm2: invalid port: {i}")
            return False

    # Check stats_every, should be an int
    try:
        int(args.stats_every)
    except:
        logger.critical(f"stats_every: invalid value: {args.stats_every}")
        return False

    # Check verbose, should be an int
    try:
        int(args.verbose)
    except:
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
        default=os.getenv('AR_LISTEN_UDP_ACARS', "5550").split(';'),
    )
    parser.add_argument(
        '--listen-tcp-acars',
        help='TCP port to listen for ACARS messages. Can be specified multiple times to listen on multiple ports. (default: 5550)',
        type=str,
        nargs='*',
        default=os.getenv('AR_LISTEN_TCP_ACARS', "5550").split(';'),
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
        default=os.getenv('AR_LISTEN_UDP_VDLM2', "5555").split(';'),
    )
    parser.add_argument(
        '--listen-tcp-vdlm2',
        help='TCP port to listen for VDLM2 messages. Can be specified multiple times to listen on multiple ports. (default: 5550)',
        type=str,
        nargs='*',
        default=os.getenv('AR_LISTEN_TCP_VDLM2', "5555").split(';'),
    )
    parser.add_argument(
        '--receive-tcp-vdlm2',
        help='Connect to "host:port" (over TCP) and receive VDLM2 messages. Can be specified multiple times to receive from multiple sources.',
        type=str,
        nargs='*',
        default=split_env_safely('AR_RECV_TCP_VDLM2'),
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
        '--serve-tcp-acars',
        help='Serve ACARS messages on TCP "port". Can be specified multiple times to serve on multiple ports.',
        type=str,
        nargs='*',
        default=split_env_safely('AR_SERVE_TCP_ACARS'),
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
        help='Increase log verbosity. -v = debug. -vv = trace (raw packets).',
        action='count',
        default=int(os.getenv("AR_VERBOSITY", 0)),
    )
    args = parser.parse_args()

    # configure logging
    logger = baselogger.getChild('core')
    logger_console_handler = logging.StreamHandler()
    baselogger.addHandler(logger_console_handler)
    if args.verbose == 1:
        log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] [%(name)s] [%(threadName)s] %(message)s', r'%Y-%m-%d %H:%M:%S')
        baselogger.setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
        logger_console_handler.setLevel(logging.DEBUG)
        logger_console_handler.setFormatter(log_formatter)
        logger.debug(f"Command line arguments: {args}")
        logger.debug("DEBUG logging enabled")
    elif args.verbose >= 2:
        log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] [%(name)s] %(message)s', r'%Y-%m-%d %H:%M:%S')
        baselogger.setLevel(logging.DEBUG - 5)
        logger.setLevel(logging.DEBUG - 5)
        logger_console_handler.setLevel(logging.DEBUG - 5)
        logger_console_handler.setFormatter(log_formatter)
        logger.debug(f"Command line arguments: {args}")
        logger.debug("TRACE logging enabled")
    else:
        log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] [%(name)s] %(message)s', r'%Y-%m-%d %H:%M:%S')
        baselogger.setLevel(logging.INFO)
        logger.setLevel(logging.INFO)
        logger_console_handler.setLevel(logging.INFO)
        logger_console_handler.setFormatter(log_formatter)

    # sanity check input, if invalid then bail out
    if not valid_args:
        sys.exit(1)
    
    # Start stats thread
    threading.Thread(
        target=display_stats,
        daemon=True,
        args=(args.stats_every,),
    ).start()

    # Prepare queues
    output_acars_queues = list()
    output_vdlm2_queues = list()
    COUNTERS.register_queue_list(output_acars_queues)
    COUNTERS.register_queue_list(output_vdlm2_queues)

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

    # inbound acars queue & processor
    inbound_acars_message_queue = ARQueue('inbound_acars_message_queue', 100)
    COUNTERS.register_queue(inbound_acars_message_queue)
    threading.Thread(
        target=message_processor,
        daemon=True,
        args=(inbound_acars_message_queue, output_acars_queues, "ACARS",),
    ).start()


    # inbound vdlm2 queue & processor
    inbound_vdlm2_message_queue = ARQueue('inbound_vdlm2_message_queue', 100)
    COUNTERS.register_queue(inbound_vdlm2_message_queue)
    threading.Thread(
        target=message_processor,
        daemon=True,
        args=(inbound_vdlm2_message_queue, output_vdlm2_queues, "VDLM2",),
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
    
    # Main loop
    while True:
        time.sleep(10)
