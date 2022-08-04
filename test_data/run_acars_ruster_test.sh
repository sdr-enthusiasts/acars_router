#!/usr/bin/env bash

# Copyright (c) Mike Nye, Fred Clausen
#
# Licensed under the MIT license: https://opensource.org/licenses/MIT
# Permission is granted to use, copy, modify, and redistribute the work.
# Full license information available in the project LICENSE file.
#

task_failed() {
  echo "Test failed"
  pkill acars_router
  exit 1
}

# run cargo test

cargo test

# UDP Tests
# We will also check primary program logic with UDP here. All other tests will not check program logic
# but will just ensure the router accepts / sends data as appropriate.

# run acars_router with out deduping. Connection checking here

echo "UDP Send/Receive without deduping"
cargo run -- --add-proxy-id --listen-udp-acars 15551 --listen-udp-vdlm2 15556 --send-udp-acars 127.0.0.1:15550 --send-udp-vdlm2 127.0.0.1:15555 &
sleep 30
python3 data_feeder_test_udp.py || task_failed

pkill acars_router
echo "UDP Send/Receive without deduping PASS"

echo "UDP Send/Receive with deduping AND small UDP packet size AND data continuity"

cargo run -- --enable-dedupe --max-udp-packet-size 512 --listen-udp-acars 15551 --listen-udp-vdlm2 15556 --send-udp-acars 127.0.0.1:15550 --send-udp-vdlm2 127.0.0.1:15555 &
sleep 3
python3 data_feeder_test_udp.py --check-data-continuity --check-for-no-proxy-id --check-for-dupes || task_failed

pkill acars_router

echo "UDP Send/Receive without deduping AND small UDP packet size AND data continuity PASS"

echo "UDP Send/Receive with deduping AND fragmented packets AND data continuity"
cargo run -- --enable-dedupe --listen-udp-acars 15551 --listen-udp-vdlm2 15556 --send-udp-acars 127.0.0.1:15550 --send-udp-vdlm2 127.0.0.1:15555 &
sleep 3
python3 data_feeder_test_udp.py --check-data-continuity --check-for-no-proxy-id --check-for-dupes --fragment-packets || task_failed
pkill acars_router
echo "UDP Send/Receive with deduping AND fragmented packets AND data continuity PASS"

# primary program logic checks

# run acars_router with deduping

echo "UDP Send/Receive with deduping"
cargo run -- --add-proxy-id --listen-udp-acars 15551 --listen-udp-vdlm2 15556 --send-udp-acars 127.0.0.1:15550 --send-udp-vdlm2 127.0.0.1:15555 --enable-dedupe &
sleep 3
python3 data_feeder_test_udp.py --check-for-dupes || task_failed

pkill acars_router
echo "UDP Send/Receive with deduping PASS"

echo "UDP Send/Receive and verify no proxied field"
cargo run -- --listen-udp-acars 15551 --listen-udp-vdlm2 15556 --send-udp-acars 127.0.0.1:15550 --send-udp-vdlm2 127.0.0.1:15555 --enable-dedupe &
sleep 3
python3 data_feeder_test_udp.py --check-for-dupes --check-for-no-proxy-id || task_failed

pkill acars_router
echo "UDP Send/Receive and verify no proxied field PASS"

echo "UDP Send/Receive and verify station id is replaced"

cargo run -- --add-proxy-id --listen-udp-acars 15551 --listen-udp-vdlm2 15556 --send-udp-acars 127.0.0.1:15550 --send-udp-vdlm2 127.0.0.1:15555 --enable-dedupe --override-station-name TEST &
sleep 3
python3 data_feeder_test_udp.py --check-for-dupes --check-for-station-id TEST || task_failed

pkill acars_router
echo "UDP Send/Receive and verify station id is replaced PASS"

echo "UDP Send/Receive data continuity"

cargo run -- --listen-udp-acars 15551 --listen-udp-vdlm2 15556 --send-udp-acars 127.0.0.1:15550 --send-udp-vdlm2 127.0.0.1:15555 --enable-dedupe &
sleep 3
python3 data_feeder_test_udp.py --check-for-dupes --check-data-continuity --check-for-no-proxy-id || task_failed

pkill acars_router
echo "UDP Send/Receive data continuity PASS"

### UDP COMPLETE

# TCP SEND / LISTEN

echo "TCP Send/Receive with deduping"

cargo run -- --add-proxy-id --listen-tcp-acars 15551 --listen-tcp-vdlm2 15556 --serve-tcp-acars 15550 --serve-tcp-vdlm2 15555 --enable-dedupe &
sleep 3
python3 data_feeder_test_sender_tcp.py --check-for-dupes || task_failed

pkill acars_router
echo "TCP Send/Receive with deduping PASS"

# echo "TCP Listen/Send with deduping"

# cargo run -- --add-proxy-id --receive-tcp-acars 127.0.0.1:15551 --receive-tcp-vdlm2 127.0.0.1:15556 --send-tcp-acars 127.0.0.1:15550 --send-tcp-vdlm2 127.0.0.1:15555 --enable-dedupe &

# python3 data_feeder_test_receiver_tcp.py --check-for-dupes || task_failed

# pkill acars_router
# echo "TCP Listen/Send with deduping PASS"

# ZMQ SEND / LISTEN

echo "ZMQ VDLM Send/UDP ACARS Send and ZMQ Receive"

cargo run -- --add-proxy-id --listen-udp-acars 15551 --receive-zmq-vdlm2 127.0.0.1:15556 --serve-zmq-acars 15550 --serve-zmq-vdlm2 15555 --enable-dedupe &

python3 data_feeder_test_zmq.py --check-for-dupes || task_failed
sleep 3
pkill acars_router
echo "ZMQ VDLM Send/UDP ACARS Send and ZMQ Receive PASS"

echo "ALL TESTS PASSED"
exit 0
