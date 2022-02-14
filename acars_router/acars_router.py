
import os
import argparse
from re import T
import socket
import socketserver
import threading
import queue
import time
import logging

logging.addLevelName(logging.DEBUG - 5, 'TRACE')
baselogger = logging.getLogger('acars_router')

# stats counters
COUNTER_ACARS_UDP_RECEIVED_TOTAL = 0
COUNTER_ACARS_UDP_RECEIVED_LAST = 0
COUNTER_VDLM2_UDP_RECEIVED_TOTAL = 0
COUNTER_VDLM2_UDP_RECEIVED_LAST = 0
COUNTER_ACARS_TCP_RECEIVED_TOTAL = 0
COUNTER_ACARS_TCP_RECEIVED_LAST = 0
COUNTER_VDLM2_TCP_RECEIVED_TOTAL = 0
COUNTER_VDLM2_TCP_RECEIVED_LAST = 0

def display_stats(mins: int):
    logger = baselogger.getChild(f'statistics')
    logger.debug("spawned")

    global COUNTER_ACARS_UDP_RECEIVED_TOTAL
    global COUNTER_ACARS_UDP_RECEIVED_LAST
    global COUNTER_VDLM2_UDP_RECEIVED_TOTAL
    global COUNTER_VDLM2_UDP_RECEIVED_LAST
    global COUNTER_ACARS_TCP_RECEIVED_TOTAL
    global COUNTER_ACARS_TCP_RECEIVED_LAST
    global COUNTER_VDLM2_TCP_RECEIVED_TOTAL
    global COUNTER_VDLM2_TCP_RECEIVED_LAST

    while True:

        time.sleep(mins * 60)

        logger.info("ACARS messages received UDP last {mins}min / total: {last} / {total}".format(
            mins=mins,
            total=COUNTER_ACARS_UDP_RECEIVED_TOTAL,
            last=COUNTER_ACARS_UDP_RECEIVED_LAST,
        ))
        COUNTER_ACARS_UDP_RECEIVED_LAST = 0

        logger.info("VDLM2 messages received UDP last {mins}min / total: {last} / {total}".format(
            mins=mins,
            total=COUNTER_VDLM2_UDP_RECEIVED_TOTAL,
            last=COUNTER_VDLM2_UDP_RECEIVED_LAST,
        ))
        COUNTER_VDLM2_UDP_RECEIVED_LAST = 0

        logger.info("ACARS messages received TCP last {mins}min / total: {last} / {total}".format(
            mins=mins,
            total=COUNTER_ACARS_TCP_RECEIVED_TOTAL,
            last=COUNTER_ACARS_TCP_RECEIVED_LAST,
        ))
        COUNTER_ACARS_UDP_RECEIVED_LAST = 0

        logger.info("VDLM2 messages received TCP last {mins}min / total: {last} / {total}".format(
            mins=mins,
            total=COUNTER_VDLM2_TCP_RECEIVED_TOTAL,
            last=COUNTER_VDLM2_TCP_RECEIVED_LAST,
        ))
        COUNTER_VDLM2_UDP_RECEIVED_LAST = 0

        



class ThreadedUDPServer(socketserver.ThreadingMixIn, socketserver.UDPServer):
    def __init__(self, server_address, RequestHandlerClass, bind_and_activate=True, inbound_message_queue=None):
        self.inbound_message_queue = inbound_message_queue
        socketserver.UDPServer.__init__(self, server_address, RequestHandlerClass, bind_and_activate=bind_and_activate)

class InboundUDPACARSMessageHandler(socketserver.BaseRequestHandler):
    def __init__(self, request, client_address, server):
        self.logger = baselogger.getChild(f'input.udp.acars')
        self.inbound_message_queue = server.inbound_message_queue
        socketserver.BaseRequestHandler.__init__(self, request, client_address, server)

    def handle(self):
        data = self.request[0].strip()
        socket = self.request[1]
        self.inbound_message_queue.put(data)
        global COUNTER_ACARS_UDP_RECEIVED_TOTAL
        COUNTER_ACARS_UDP_RECEIVED_TOTAL += 1
        global COUNTER_ACARS_UDP_RECEIVED_LAST
        COUNTER_ACARS_UDP_RECEIVED_LAST += 1

class InboundUDPVDLM2MessageHandler(socketserver.BaseRequestHandler):
    def __init__(self, request, client_address, server):
        self.logger = baselogger.getChild(f'input.udp.vdlm2')
        self.inbound_message_queue = server.inbound_message_queue
        socketserver.BaseRequestHandler.__init__(self, request, client_address, server)

    def handle(self):
        data = self.request[0].strip()
        socket = self.request[1]
        self.inbound_message_queue.put(data)
        global COUNTER_VDLM2_UDP_RECEIVED_TOTAL
        COUNTER_VDLM2_UDP_RECEIVED_TOTAL += 1
        global COUNTER_VDLM2_UDP_RECEIVED_LAST
        COUNTER_VDLM2_UDP_RECEIVED_LAST += 1

class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    def __init__(self, server_address, RequestHandlerClass, bind_and_activate=True, inbound_message_queue=None):
        self.inbound_message_queue = inbound_message_queue
        socketserver.UDPServer.__init__(self, server_address, RequestHandlerClass, bind_and_activate=bind_and_activate)

class InboundTCPACARSMessageHandler(socketserver.BaseRequestHandler):
    def __init__(self, request, client_address, server):
        self.logger = baselogger.getChild(f'input.tcp.acars')
        self.logger.debug("spawned")
        self.inbound_message_queue = server.inbound_message_queue
        socketserver.BaseRequestHandler.__init__(self, request, client_address, server)

    def handle(self):
        data = self.request[0].strip()
        socket = self.request[1]
        self.inbound_message_queue.put(data)
        global COUNTER_ACARS_TCP_RECEIVED_TOTAL
        COUNTER_ACARS_TCP_RECEIVED_TOTAL += 1
        global COUNTER_ACARS_TCP_RECEIVED_LAST
        COUNTER_ACARS_TCP_RECEIVED_LAST += 1

class InboundTCPVDLM2MessageHandler(socketserver.BaseRequestHandler):
    def __init__(self, request, client_address, server):
        self.logger = baselogger.getChild(f'input.tcp.vdlm2')
        self.logger.debug("spawned")
        self.inbound_message_queue = server.inbound_message_queue
        socketserver.BaseRequestHandler.__init__(self, request, client_address, server)

    def handle(self):
        data = self.request[0].strip()
        socket = self.request[1]
        self.inbound_message_queue.put(data)
        global COUNTER_VDLM2_TCP_RECEIVED_TOTAL
        COUNTER_VDLM2_TCP_RECEIVED_TOTAL += 1
        global COUNTER_VDLM2_TCP_RECEIVED_LAST
        COUNTER_VDLM2_TCP_RECEIVED_LAST += 1

def UDPSender(host, port, output_queue: queue.Queue, protoname: str):
    protoname = protoname.lower()
    logger = baselogger.getChild(f'output.udp.{protoname}.{host}.{port}')
    logger.debug("spawned")
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.settimeout(1)
    while True:
        data = output_queue.get()
        logger.log(logging.DEBUG - 5, f"sending {data} to {host}:{port}")
        try:
            sock.sendto(data, (host, port))
        except:
            logger.error("Error sending to {host}:{port}/udp".format(
                host=host,
                port=port,
            ))
        output_queue.task_done()

def TCPServerAcceptor(port: int, output_queues: list, protoname: str):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("0.0.0.0", port))
        s.listen()
        while True:
            conn, addr = s.accept()
            # create a new queue for this connection
            output_queues.append(queue.Queue())
            # create new TCPServer for this connection, and pass queue
            threading.Thread(
                target=TCPServer,
                daemon=True,
                name=f"TCPServer.{addr[0]}.{addr[1]}",
                args=(conn, addr, output_queues[-1], output_queues, protoname),
            ).start()

def TCPServer(conn: socket.socket, addr: tuple, out_queue: queue.Queue, queue_list: list, protoname: str):
    host = addr[0]
    port = addr[1]
    protoname = protoname.lower()
    logger = baselogger.getChild(f'output.tcpserver.{protoname}.{host}.{port}')
    logger.info("client connected")
    conn.settimeout(1)
    connected = True
    while connected:
        data = out_queue.get()
        logger.log(logging.DEBUG - 5, f"sending {data} to {host}:{port}")
        try:
            conn.sendall(data)
        except:
            connected = False
        out_queue.task_done()
    # clean up
    queue_list.remove(out_queue)
    del(out_queue)
    logger.info("client disconnected")

def TCPSender(host: str, port: int, out_queue, protoname: str):
    protoname = protoname.lower()
    logger = baselogger.getChild(f'output.tcpclient.{protoname}.{host}.{port}')
    logger.debug("spawned")
    while True:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # clear queue
            with out_queue.mutex:
                out_queue.queue.clear()
            sock.settimeout(1)
            # attempt connection
            try:
                sock.connect((host, port))
            except ConnectionRefusedError:
                logger.error("connection to server refused")
                time.sleep(10)
            else:
                logger.info("connected to server")
                connected = True
                while connected:
                    data = out_queue.get()
                    logger.log(logging.DEBUG - 5, f"sending {data} to {host}:{port}")
                    try:
                        sock.sendall(data)
                    except:
                        connected = False
                        logger.info("disconnected from server")
                    out_queue.task_done()
                
def message_processor(in_queue, out_queues, protoname):
    protoname = protoname.lower()
    logger = baselogger.getChild(f'message_processor.{protoname}')
    logger.debug("spawned")
    while True:
        data = in_queue.get()
        logger.log(logging.DEBUG - 5, f'{data}')
        for q in out_queues:
            q.put(data)
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
        default=os.getenv('AR_SERVE_TCP_ACARS', "15550").split(';')
    )
    parser.add_argument(
        '--serve-tcp-vdlm2',
        help='Serve VDLM2 messages on TCP "port". Can be specified multiple times to serve on multiple ports.',
        type=str,
        nargs='*',
        default=os.getenv('AR_SERVE_TCP_VDLM2', "15555").split(';')
    )
    parser.add_argument(
        '--stats-every',
        help='Print statistics every N minutes (default: 5)',
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
    if args.verbose == 1:
        log_format = '%(asctime)s [%(levelname)s] [%(name)s] [%(threadName)s] %(message)s'
        logging.basicConfig(level=logging.DEBUG, format=log_format)
        logger.debug("DEBUG logging enabled")
    elif args.verbose >= 2:
        log_format = '%(asctime)s [%(levelname)s] [%(name)s] [%(threadName)s] %(message)s'
        logging.basicConfig(level=logging.DEBUG - 5, format=log_format)
        logger.debug("TRACE logging enabled")
    else:
        log_format = '%(asctime)s [%(levelname)s] [%(name)s] %(message)s'
        logging.basicConfig(level=logging.INFO, format=log_format)
    

    # Start stats thread
    threading.Thread(
        target=display_stats,
        daemon=True,
        name="display_stats",
        args=(args.stats_every,),
    ).start()

    # Prepare queues
    output_acars_queues = list()
    output_vdlm2_queues = list()

    # acars tcp output (server)
    for port in args.serve_tcp_acars:
        logger.info(f'serving ACARS on TCP port {port}')
        threading.Thread(
            target=TCPServerAcceptor,
            daemon=True,
            name="ACARS.TCPServerAcceptor",
            args=(int(port), output_acars_queues, "ACARS"),
        ).start()

    # vdlm2 tcp output (server)
    for port in args.serve_tcp_vdlm2:
        logger.info(f'serving VDLM2 on TCP port {port}')
        threading.Thread(
            target=TCPServerAcceptor,
            daemon=True,
            name="VDLM2.TCPServerAcceptor",
            args=(int(port), output_vdlm2_queues, "VDLM2"),
        ).start()

    # acars udp output (sender)
    for c in args.send_udp_acars:
        host = c.split(':')[0]
        port = int(c.split(':')[1])
        logger.info(f'sending ACARS via UDP to {host}:{port}')
        output_acars_queues.append(queue.Queue())
        threading.Thread(
            target=UDPSender,
            daemon=True,
            name="ACARS.UDPSender",
            args=(host, port, output_acars_queues[-1], "ACARS"),
        ).start()

    # acars tcp output (sender)
    for c in args.send_tcp_acars:
        host = c.split(':')[0]
        port = int(c.split(':')[1])
        logger.info(f'sending ACARS via TCP to {host}:{port}')
        output_acars_queues.append(queue.Queue())
        threading.Thread(
            target=TCPSender,
            daemon=True,
            name="ACARS.TCPSender",
            args=(host, port, output_acars_queues[-1], "ACARS"),
        ).start()

    # vdlm2 udp output (sender)
    for c in args.send_udp_vdlm2:
        host = c.split(':')[0]
        port = int(c.split(':')[1])
        logger.info(f'sending VDLM2 via UDP to {host}:{port}')
        output_vdlm2_queues.append(queue.Queue())
        threading.Thread(
            target=UDPSender,
            daemon=True,
            name="VDLM2.UDPSender",
            args=(host, port, output_vdlm2_queues[-1], "VDLM2"),
        ).start()

    # vdlm2 tcp output (sender)
    for c in args.send_tcp_vdlm2:
        host = c.split(':')[0]
        port = int(c.split(':')[1])
        logger.info(f'sending VDLM2 via TCP to {host}:{port}')
        output_vdlm2_queues.append(queue.Queue())
        threading.Thread(
            target=TCPSender,
            daemon=True,
            name="VDLM2.TCPSender",
            args=(host, port, output_vdlm2_queues[-1], "VDLM2"),
        ).start()

    # inbound acars queue & processor
    inbound_acars_message_queue = queue.Queue()
    threading.Thread(
        target=message_processor,
        daemon=True,
        name='acars.message_processor',
        args=(inbound_acars_message_queue, output_acars_queues, "ACARS",),
    ).start()


    # inbound vdlm2 queue & processor
    inbound_vdlm2_message_queue = queue.Queue()
    threading.Thread(
        target=message_processor,
        daemon=True,
        name='vdlm2.message_processor',
        args=(inbound_vdlm2_message_queue, output_vdlm2_queues, "VDLM2",),
    ).start()

    # Prepare inbound UDP receiver threads for ACARS
    for port in args.listen_udp_acars:
        threading.Thread(
            target=ThreadedUDPServer(
                ("0.0.0.0", int(port)),
                InboundUDPACARSMessageHandler,
                inbound_message_queue=inbound_acars_message_queue,
            ).serve_forever,
            daemon=True,
            name="ThreadedUDPServer.InboundUDPACARSMessageHandler",
        ).start()

    # Prepare inbound TCP receiver threads for ACARS
    for port in args.listen_tcp_acars:
        threading.Thread(
            target=ThreadedTCPServer(
                ("0.0.0.0", int(port)),
                InboundTCPACARSMessageHandler,
                inbound_message_queue=inbound_acars_message_queue,
            ).serve_forever,
            daemon=True,
            name="ThreadedTCPServer.InboundTCPACARSMessageHandler",
        ).start()

    # Prepare inbound UDP receiver threads for VDLM2
    for port in args.listen_udp_vdlm2:
        threading.Thread(
            target=ThreadedUDPServer(
                ("0.0.0.0", int(port)),
                InboundUDPVDLM2MessageHandler,
                inbound_message_queue=inbound_vdlm2_message_queue,
            ).serve_forever,
            daemon=True,
            name="ThreadedUDPServer.InboundUDPVDLM2MessageHandler",
        ).start()

    # Prepare inbound TCP receiver threads for VDLM2
    for port in args.listen_tcp_vdlm2:
        threading.Thread(
            target=ThreadedTCPServer(
                ("0.0.0.0", int(port)),
                InboundTCPVDLM2MessageHandler,
                inbound_message_queue=inbound_vdlm2_message_queue,
            ).serve_forever,
            daemon=True,
            name="ThreadedTCPServer.InboundTCPVDLM2MessageHandler",
        ).start()
    
    # Main loop
    while True:
        time.sleep(10)
