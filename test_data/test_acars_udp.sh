#!/usr/bin/env bash
set -xe

# Start fake destination server for reference output
socat -d -x -t5 UDP-LISTEN:25550,fork OPEN:/tmp/acars.udp.out.reference,creat,append &
sleep 1

# Send data bypassing acars_router
while IFS="" read -r p || [ -n "$p" ]; do
    printf '%s\n' "$p" | socat - UDP-DATAGRAM:127.0.0.1:25550;
done <./test_data/acars.patched

# Start fake destination server for acars_router output
socat -d -x -t5 UDP-LISTEN:15550,fork OPEN:/tmp/acars.udp.out,creat,append &
sleep 1

# Start acars_router
python3 ./acars_router/acars_router.py -vv --skew-window 30 --listen-udp-acars 5550 --send-udp-acars 127.0.0.1:15550 &
sleep 1

# Send test data thru acars_router
while IFS="" read -r p || [ -n "$p" ]; do
    printf '%s\n' "$p" | socat - UDP-DATAGRAM:127.0.0.1:5550;
done <./test_data/acars.patched

# Re-format output files
jq -M . < /tmp/acars.udp.out.reference > /tmp/acars.udp.out.reference.reformatted
jq -M . < /tmp/acars.udp.out > /tmp/acars.udp.out.reformatted

# Check output
diff /tmp/acars.udp.out.reference.reformatted /tmp/acars.udp.out.reformatted
exit $?
