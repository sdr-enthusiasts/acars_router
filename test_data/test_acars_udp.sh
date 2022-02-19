#!/usr/bin/env bash

set -x

socat UDP-LISTEN:15550 CREATE:/tmp/acars.udp.out &
SOCAT_PID="$!"
sleep 2
python3 ./acars_router/acars_router.py -v --listen-udp-acars 5550 --send-udp-acars 127.0.0.1:15550 &
sleep 2

while IFS="" read -r p || [ -n "$p" ]
  printf '%s\n' "$p" | nc -u -v -q 1 -w 5 127.0.0.1 5550
done <./test_data/acars.patched

sleep 2
cat ./tmp/acars.udp.out
kill -9 $ALLOWED_STDERRSOCAT_PID