#!/usr/bin/env bash
set -xe

PORT1=$(shuf -i 1000-1999 -n 1)
PORT2=$(shuf -i 2000-2999 -n 1)

# Start fake destination server for acars_router output
socat -d -t5 UDP-LISTEN:"${PORT1}",fork OPEN:/tmp/acars.udp.out,creat,append &
sleep 1

# Start acars_router
python3 ./acars_router/acars_router.py -vv --skew-window 300 --listen-udp-acars "${PORT2}" --send-udp-acars 127.0.0.1:"${PORT1}" &
sleep 1

# Send test data thru acars_router
while IFS="" read -r p || [ -n "$p" ]; do
    printf '%s\n' "$p" | socat - UDP-DATAGRAM:127.0.0.1:"${PORT2}";
done <./test_data/acars.patched

# Re-format output files
jq -M . < ./test_data/acars.patched > /tmp/acars.udp.out.reference.reformatted
jq -M . < /tmp/acars.udp.out > /tmp/acars.udp.out.reformatted

# Check output
diff /tmp/acars.udp.out.reference.reformatted /tmp/acars.udp.out.reformatted
exit $?
