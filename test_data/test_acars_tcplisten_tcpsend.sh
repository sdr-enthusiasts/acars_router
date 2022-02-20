#!/usr/bin/env bash
set -xe

# Start fake destination server for reference output
socat -d -t5 TCP-LISTEN:15552,fork OPEN:/tmp/acars.tcplisten.tcpsend.out.reference,creat,append &
sleep 1

# Send data bypassing acars_router
while IFS="" read -r p || [ -n "$p" ]; do
    printf '%s\n' "$p" | socat - TCP:127.0.0.1:15552;
done <./test_data/acars.patched

# Start fake destination server for acars_router output
socat -d -t5 TCP-LISTEN:15553,fork OPEN:/tmp/acars.tcplisten.tcpsend.out,creat,append &
sleep 1

# Start acars_router
python3 ./acars_router/acars_router.py -vv --skew-window 300 --listen-tcp-acars 5553 --send-tcp-acars 127.0.0.1:15553 &
sleep 1

# Send test data thru acars_router
while IFS="" read -r p || [ -n "$p" ]; do
    printf '%s\n' "$p" | socat - TCP:127.0.0.1:5553;
done <./test_data/acars.patched

# Re-format output files
jq -M . < /tmp/acars.tcplisten.tcpsend.out.reference > /tmp/acars.tcplisten.tcpsend.out.reference.reformatted
jq -M . < /tmp/acars.tcplisten.tcpsend.out > /tmp/acars.tcplisten.tcpsend.out.reformatted

# Check output
diff /tmp/acars.tcplisten.tcpsend.out.reference.reformatted /tmp/acars.tcplisten.tcpsend.out.reformatted
exit $?
