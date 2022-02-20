#!/usr/bin/env bash

set -x

socat UDP-LISTEN:15550 OPEN:/tmp/acars.udp.out,creat,append &
SOCAT_PID="$!"
sleep 2
python3 ./acars_router/acars_router.py -vv --skew-window 30 --listen-udp-acars 5550 --send-udp-acars 127.0.0.1:15550 &
sleep 2

while IFS="" read -r p || [ -n "$p" ]; do
  printf '%s\n' "$p" | socat - UDP-DATAGRAM:127.0.0.1:5550
done <./test_data/acars.patched

sleep 2
cat /tmp/acars.udp.out
kill -9 $ALLOWED_STDERRSOCAT_PID