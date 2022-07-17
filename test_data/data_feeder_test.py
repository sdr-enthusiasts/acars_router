#!/usr/bin/env python3

import json
import random
import socket
import time
import sys
from threading import Thread, Event  # noqa: E402
from collections import deque  # noqa: E402

thread_stop_event = Event()

def UDPSocketListener(port, queue):
    global thread_stop_event
    while not thread_stop_event.is_set():
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(5)
            sock.bind(('', port))
            data, _ = sock.recvfrom(1024)
            if data:
                queue.append(data)
        except socket.timeout:
            pass
        except Exception as e:
            print(e)

def TCPSocketLisenter(port, queue):
    global thread_stop_event

    while not thread_stop_event.is_set():
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.bind(('', port))
            sock.listen(1)
            conn, addr = sock.accept()
            data = conn.recv(1024)
            if data:
                queue.append(data)
        except socket.timeout:
            pass
        except Exception as e:
            print(e)


if __name__ == "__main__":
    TEST_PASSED = True
    test_messages = []
    received_messages_queue_acars = deque()
    received_messages_queue_vdlm = deque()
    number_of_expected_acars_messages = 0;
    number_of_expected_vdlm_messages = 0;

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
    udp_acars_port = 15550
    tcp_vdlm_port = 25550
    # inputs VDLM
    udp_vdlm_port = 15555
    tcp_acars_port = 25555

    # Remote listening ports
    # ACARS
    udp_acars_remote_port = 15551
    tcp_vdlm_remote_port = 25551
    # VDLM
    udp_vdlm_remote_port = 15556
    tcp_acars_remote_port = 25556

    remote_ip = "127.0.0.1"

    # VDLM2
    thread_vdlm2_udp_listener = Thread(target=UDPSocketListener, args=(udp_vdlm_port, received_messages_queue_vdlm))
    thread_vdlm2_udp_listener.start()

    thread_vdlm2_tcp_listener = Thread(target=TCPSocketLisenter, args=(tcp_vdlm_port, received_messages_queue_vdlm))
    thread_vdlm2_tcp_listener.start()

    # ACARS

    thread_acars_udp_listener = Thread(target=UDPSocketListener, args=(udp_acars_port, received_messages_queue_acars))
    thread_acars_udp_listener.start()

    thread_acars_tcp_listener = Thread(target=TCPSocketLisenter, args=(tcp_acars_port, received_messages_queue_acars))
    thread_acars_tcp_listener.start()

    # create all of the output sockets
    # UDP
    acars_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    acars_sock.bind((remote_ip, 0))

    vdlm_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    vdlm_sock.bind((remote_ip, 0))

    # # TCP
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((remote_ip, 25555))

    print("STARTING UDP SEND/RECEIVE DUPLICATION TEST")
    message_count = 0
    duplicated = 0
    for message in test_messages:
        # UDP
        print(f"Sending message {message_count + 1}")
        # Randomly decide if the message should be sent twice
        if random.randint(0, 10) == 0:
            send_twice = True
            duplicated += 1
        else:
            send_twice = False

        if "vdl2" in message:
            # replace message["vdlm"]["t"]["sec"] with current unix epoch time
            message["vdl2"]["t"]["sec"] = int(time.time())
            vdlm_sock.sendto(json.dumps(message).encode(), (remote_ip, udp_vdlm_remote_port))
            if send_twice:
                print("Sending VDLM duplicate")
                vdlm_sock.sendto(json.dumps(message).encode(), (remote_ip, udp_vdlm_remote_port))
        else:
            pass
            message["timestamp"] = int(time.time())
            acars_sock.sendto(json.dumps(message).encode(), (remote_ip, udp_acars_remote_port))
            if send_twice:
                print("Sending ACARS duplicate")
                acars_sock.sendto(json.dumps(message).encode(), (remote_ip, udp_acars_remote_port))

        message_count += 1
        time.sleep(.5)

    time.sleep(5)
    print("UDP SEND/RECEIVE DUPLICATION TEST COMPLETE\n\n")
    print(f"Sent {message_count} original messages")
    print(f"Sent {duplicated} duplicates")
    print(f"Sent {message_count + duplicated} total messages")
    print(f"Expected number of messages {number_of_expected_acars_messages + number_of_expected_vdlm_messages}")
    print(f"Received number of messages {len(received_messages_queue_acars) + len(received_messages_queue_vdlm)}")

    if len(received_messages_queue_acars) + len(received_messages_queue_vdlm) == number_of_expected_acars_messages + number_of_expected_vdlm_messages:
        print("UDP SEND/RECEIVE DUPLICATION TEST PASSED")
    else:
        print("UDP SEND/RECEIVE DUPLICATION TEST FAILED")
        TEST_PASSED = False

    acars_sock.close()
    vdlm_sock.close()

    # stop all threads

    thread_stop_event.set()

    sys.exit(0 if TEST_PASSED else 1)
