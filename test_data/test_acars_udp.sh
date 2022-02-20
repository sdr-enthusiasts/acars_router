#!/usr/bin/env bash
set -x

# Start fake destination server
socat -ddd -x -t5 UDP-LISTEN:15550,fork OPEN:/tmp/acars.udp.out.reference,creat,append &
SOCAT_PID="$!"
sleep 1

# Send data bypassing acars_router
while IFS="" read -r p || [ -n "$p" ]; do
    printf '%s\n' "$p" | socat - UDP-DATAGRAM:127.0.0.1:5550;
done <./test_data/acars.patched

# Stop socat
kill -9 $SOCAT_PID

# Start fake destination server
socat -ddd -x -t10 UDP-LISTEN:15550,fork OPEN:/tmp/acars.udp.out,creat,append &
SOCAT_PID="$!"
sleep 1

# Start acars_router
python3 ./acars_router/acars_router.py -vv --skew-window 30 --listen-udp-acars 5550 --send-udp-acars 127.0.0.1:15550 &
sleep 1

# Send test data thru acars_router
while IFS="" read -r p || [ -n "$p" ]; do
    printf '%s\n' "$p" | socat - UDP-DATAGRAM:127.0.0.1:5550;
done <./test_data/acars.patched

# Stop socat
kill -9 $SOCAT_PID

# Check output
diff /tmp/acars.udp.out.reference /tmp/acars.udp.out
echo $?
