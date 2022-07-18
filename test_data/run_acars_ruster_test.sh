#!/usr/bin/env bash

task_failed() {
  echo "Test failed"
  pkill acars_router
  exit 1
}

router_failed() {
  echo "Router failed"
  pkill acars_router
  exit 1
}

if [[ -z "$ACARS_ROUTER_PATH" ]]; then
  echo "ACARS_ROUTER_PATH is not set"
  exit 1
fi

# run acars_router with out deduping

echo "UDP Send/Receive without deduping"
"$ACARS_ROUTER_PATH" --listen-udp-acars 15551 --listen-udp-vdlm2 15556 --send-udp-acars 127.0.0.1:15550 --send-udp-vdlm2 127.0.0.1:15555 &

python3 data_feeder_test_udp.py || task_failed

pkill acars_router

# run acars_router with deduping

echo "UDP Send/Receive with deduping"
"$ACARS_ROUTER_PATH" --listen-udp-acars 15551 --listen-udp-vdlm2 15556 --send-udp-acars 127.0.0.1:15550 --send-udp-vdlm2 127.0.0.1:15555 --enable-dedupe &

python3 data_feeder_test_udp.py --check-for-dupes || task_failed

pkill acars_router

echo "UDP Send/Receive and verify no proxied field"
"$ACARS_ROUTER_PATH" --listen-udp-acars 15551 --listen-udp-vdlm2 15556 --send-udp-acars 127.0.0.1:15550 --send-udp-vdlm2 127.0.0.1:15555 --enable-dedupe --dont-add-proxy-id &

python3 data_feeder_test_udp.py --check-for-dupes --check-for-no-proxy-id || task_failed

pkill acars_router

echo "UDP Send/Receive and verify station id is replaced"

"$ACARS_ROUTER_PATH" --listen-udp-acars 15551 --listen-udp-vdlm2 15556 --send-udp-acars 127.0.0.1:15550 --send-udp-vdlm2 127.0.0.1:15555 --enable-dedupe --override-station-name TEST &

python3 data_feeder_test_udp.py --check-for-dupes --check-for-station-id TEST || task_failed

pkill acars_router

echo "ALL TESTS PASSED"
exit 0
