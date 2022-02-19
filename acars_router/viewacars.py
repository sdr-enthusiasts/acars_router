#!/usr/bin/env python3

import os
import sys
import json
import argparse
import socket
import time
from pprint import pprint

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(
        description='View ACARS messages'
    )
    parser.add_argument(
        '--host',
        type=str,
        nargs='?',
        default="127.0.0.1",
    )
    parser.add_argument(
        '--port',
        type=int,
        nargs='?',
        default=15550,
    )
    args = parser.parse_args()

    sock = socket.create_connection(
        address=(args.host, args.port,),
        timeout=5,
    )

    while True:
        try:
            data = sock.recv(16384)
        except TimeoutError:
            pass
        else:
            j = json.loads(data)
            pprint(j)