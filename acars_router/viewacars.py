#!/usr/bin/env python3

import os
import sys
import json
import argparse
import socket
import time
from pprint import pprint
import threading

def message_handler(sock):
    while True:
        try:
            data = sock.recv(16384)
        except TimeoutError:
            pass
        else:
            j = json.loads(data)
            pprint(j)

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(
        description='View ACARS messages'
    )
    parser.add_argument(
        '--acars_host',
        type=str,
        nargs='?',
        default="127.0.0.1",
    )
    parser.add_argument(
        '--acars_port',
        type=int,
        nargs='?',
        default=15550,
    )
    parser.add_argument(
        '--vdlm2_host',
        type=str,
        nargs='?',
        default="127.0.0.1",
    )
    parser.add_argument(
        '--vdlm2_port',
        type=int,
        nargs='?',
        default=15555,
    )
    args = parser.parse_args()

    acars_sock = socket.create_connection(
        address=(args.acars_host, args.acars_port,),
        timeout=5,
    )
    vdlm2_sock = socket.create_connection(
        address=(args.vdlm2_host, args.vdlm2_port,),
        timeout=5,
    )

    # initialise threading lock
    lock = threading.Lock()

    acars_thread = threading.Thread(
        target=message_handler,
        args=(acars_sock, ),
        daemon=True,
    )
    
    vdlm2_thread = threading.Thread(
        target=message_handler,
        args=(vdlm2_sock, ),
        daemon=True,
    )

    acars_thread.start()
    vdlm2_thread.start()

    acars_thread.join()
    vdlm2_thread.join()
    