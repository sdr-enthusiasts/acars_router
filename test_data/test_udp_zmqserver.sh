#!/usr/bin/env bash

# TODO: Fix this test.

set -xe

PORT1=$(shuf -i 7000-7999 -n 1)
PORT2=$(shuf -i 8000-8999 -n 1)

# Start acars_router
python3 ./acars_router/acars_router.py -vv --add-proxy-id false --skew-window 300 --listen-udp-"$1" "${PORT2}" --serve-zmq-"$1" "${PORT1}" &
sleep 1

# Process to receive output
python3 ./acars_router/viewacars --port "${PORT1}" --protocol zmq | tee /tmp/"$1".zmqserver.out &
sleep 1

# Send test data thru acars_router
while IFS="" read -r p || [ -n "$p" ]; do
  printf '%s\n' "$p" | socat - UDP-DATAGRAM:127.0.0.1:"${PORT2}"
done < ./test_data/"$1".patched

# Re-format output files
jq -M . < ./test_data/"$1".patched > /tmp/"$1".udp.out.reference.reformatted
jq -M . < /tmp/"$1".zmqserver.out > /tmp/"$1".zmqserver.out.reformatted

echo "DEBUGGING"
cat /tmp/"$1".zmqserver.out
echo "END DEBUGGING"

# Check output
diff /tmp/"$1".udp.out.reference.reformatted /tmp/"$1".zmqserver.out.reformatted
exit $?
