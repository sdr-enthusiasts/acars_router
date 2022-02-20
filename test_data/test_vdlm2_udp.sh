#!/usr/bin/env bash
set -xe

# Start fake destination server for reference output
socat -d -x -t5 UDP-LISTEN:25555,fork OPEN:/tmp/vdlm2.udp.out.reference,creat,append &
sleep 1

# Send data bypassing acars_router
while IFS="" read -r p || [ -n "$p" ]; do
    printf '%s\n' "$p" | socat - UDP-DATAGRAM:127.0.0.1:25555;
done <./test_data/vdlm2.patched

# Start fake destination server for acars_router output
socat -d -x -t5 UDP-LISTEN:15555,fork OPEN:/tmp/vdlm2.udp.out,creat,append &
sleep 1

# Start acars_router
python3 ./acars_router/acars_router.py -vv --skew-window 30 --listen-udp-vdlm2 5555 --send-udp-vdlm2 127.0.0.1:15555 &
sleep 1

# Send test data thru acars_router
while IFS="" read -r p || [ -n "$p" ]; do
    printf '%s\n' "$p" | socat - UDP-DATAGRAM:127.0.0.1:5555;
done <./test_data/vdlm2.patched

# Re-format output files
jq -M . < /tmp/vdlm2.udp.out.reference > /tmp/vdlm2.udp.out.reference.reformatted
jq -M . < /tmp/vdlm2.udp.out > /tmp/vdlm2.udp.out.reformatted

# Check output
diff /tmp/vdlm2.udp.out.reference.reformatted /tmp/vdlm2.udp.out.reformatted
exit $?
