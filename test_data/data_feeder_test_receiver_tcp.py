#!/usr/bin/env python3

# Copyright (c) Mike Nye, Fred Clausen
#
# Licensed under the MIT license: https://opensource.org/licenses/MIT
# Permission is granted to use, copy, modify, and redistribute the work.
# Full license information available in the project LICENSE file.
#

# This program tests the LISTEN aspect of acars_router
# We have to CONNECT to the acars_router and send a message

import json
import random
import socket
import time
import sys
import argparse
from threading import Thread, Event  # noqa: E402
from collections import deque  # noqa: E402

thread_stop_event = Event()


def TCPSocketListener(sock, queue):
    global thread_stop_event

    while not thread_stop_event.is_set():
        try:
            data = sock.recv(3000)
            if data:
                try:
                    data = json.loads(data.decode("utf-8"))
                    queue.append(data)
                except Exception as e:
                    print(f"Invalid data received: {e}")
                    print(f"{data}")
        except socket.timeout:
            pass
        except Exception:
            return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test data feeder")
    parser.add_argument(
        "--check-for-dupes", action="store_true", help="Check for duplicate packets"
    )

    args = parser.parse_args()
    TEST_PASSED = True
    test_messages = []
    received_messages_queue_acars = deque()
    received_messages_queue_vdlm = deque()
    number_of_expected_acars_messages = 0
    number_of_expected_vdlm_messages = 0
    check_for_dupes = args.check_for_dupes

    with open("acars_other", "r") as acars:
        for line in acars:
            test_messages.append(json.loads(line))
            number_of_expected_acars_messages += 1

    with open("vdlm2_other", "r") as vdlm:
        for line in vdlm:
            test_messages.append(json.loads(line))
            number_of_expected_vdlm_messages += 1

    # sort the test_messages array randomly

    random.shuffle(test_messages)

    # Socket ports
    # inputs ACARS
    tcp_acars_port = 15550
    # inputs VDLM
    tcp_vdlm_port = 15555

    # Remote listening ports
    # ACARS
    tcp_acars_remote_port = 15551
    # VDLM
    tcp_vdlm_remote_port = 15556

    remote_ip = "127.0.0.1"

    # This is slow, but we're going to start each socket one at a time, and then move on to the next
    # the idea here is that the router will not be started at first and won't be actively connecting until
    # some indeterminate amount of time

    # TODO: Thread the socket connects, and then we wait for the sockets to all connect

    # VDLM2
    vdlm_sock_receive = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    vdlm_sock_receive.bind((remote_ip, tcp_vdlm_port))
    vdlm_sock_receive.listen(5)
    vdlm_client, addr = vdlm_sock_receive.accept()

    thread_vdlm2_tcp_listener = Thread(
        target=TCPSocketListener,
        args=(vdlm_client, received_messages_queue_vdlm),
    )
    thread_vdlm2_tcp_listener.start()

    # ACARS
    acars_sock_receive = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    acars_sock_receive.bind((remote_ip, tcp_acars_port))
    acars_sock_receive.listen(5)
    acars_client, addr = acars_sock_receive.accept()
    thread_acars_tcp_listener = Thread(
        target=TCPSocketListener,
        args=(acars_client, received_messages_queue_acars),
    )
    thread_acars_tcp_listener.start()

    # create all of the output sockets

    acars_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    acars_sock.bind((remote_ip, tcp_acars_remote_port))
    acars_sock.listen(5)
    acars_client_remote, addr = acars_sock.accept()

    vdlm_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    vdlm_sock.bind((remote_ip, tcp_vdlm_remote_port))
    vdlm_sock.listen(5)
    vdlm_client_remote, addr = vdlm_sock.accept()

    print(
        f"STARTING TCP SEND/RECEIVE {'DUPLICATION ' if check_for_dupes else ''}TEST\n\n"
    )
    message_count = 0
    duplicated = 0
    for message in test_messages:
        print(f"Sending message {message_count + 1}")
        # Randomly decide if the message should be sent twice
        if random.randint(0, 10) < 3:
            send_twice = True
            duplicated += 1
        else:
            send_twice = False

        if "vdl2" in message:
            # replace message["vdlm"]["t"]["sec"] with current unix epoch time
            message["vdl2"]["t"]["sec"] = int(time.time())
            vdlm_client_remote.sendto(
                json.dumps(message).encode() + b"\n", (remote_ip, tcp_vdlm_remote_port)
            )
            if send_twice:
                time.sleep(0.2)
                print("Sending VDLM duplicate")
                vdlm_client_remote.sendto(
                    json.dumps(message).encode() + b"\n",
                    (remote_ip, tcp_vdlm_remote_port),
                )
        else:
            message["timestamp"] = float(time.time())
            acars_client_remote.sendto(
                json.dumps(message).encode() + b"\n", (remote_ip, tcp_acars_remote_port)
            )
            if send_twice:
                time.sleep(0.2)
                print("Sending ACARS duplicate")
                acars_client_remote.sendto(
                    json.dumps(message).encode() + b"\n",
                    (remote_ip, tcp_acars_remote_port),
                )

        message_count += 1
        time.sleep(0.5)

    time.sleep(5)
    print(
        f"TCP SEND/RECEIVE {'DUPLICATION' if check_for_dupes else ''}TEST COMPLETE\n\n"
    )
    print(f"Sent {message_count} original messages")
    print(f"Sent {duplicated} duplicates")
    print(f"Sent {number_of_expected_acars_messages} of non dup ACARS messages")
    print(f"Sent {number_of_expected_vdlm_messages} of non dup VDLM messages")
    print(f"Sent {message_count + duplicated} total messages")
    print(
        f"Expected number of messages {number_of_expected_acars_messages + number_of_expected_vdlm_messages + (duplicated if not check_for_dupes else 0)}"
    )
    print(
        f"Received number of messages {len(received_messages_queue_acars) + len(received_messages_queue_vdlm)}"
    )

    if len(received_messages_queue_acars) + len(
        received_messages_queue_vdlm
    ) == number_of_expected_acars_messages + number_of_expected_vdlm_messages + (
        duplicated if not check_for_dupes else 0
    ):
        print(
            f"TCP SEND/RECEIVE {'DUPLICATION ' if check_for_dupes else ''}TEST PASSED"
        )
    else:
        print(
            f"TCP SEND/RECEIVE {'DUPLICATION ' if check_for_dupes else ''}TEST FAILED"
        )
        TEST_PASSED = False

    # Clean up

    vdlm_client.close()
    vdlm_client = None
    acars_client.close()
    acars_client = None
    acars_client_remote.close()
    acars_client_remote = None
    vdlm_client_remote.close()
    vdlm_client_remote = None

    # stop all threads

    thread_stop_event.set()

    sys.exit(0 if TEST_PASSED else 1)
