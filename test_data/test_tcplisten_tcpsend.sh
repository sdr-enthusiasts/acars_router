#!/usr/bin/env bash
set -xe

PORT1=$(shuf -i 3000-3999 -n 1)
PORT2=$(shuf -i 4000-4999 -n 1)

# Start fake destination server for acars_router output
socat -d -t5 TCP-LISTEN:"${PORT1}",fork OPEN:/tmp/"$1".tcplisten.tcpsend.out,create,append &
sleep 1

# Start acars_router
python3 ./acars_router/acars_router.py -vv --add-proxy-id false --skew-window 300 --listen-tcp-"$1" "${PORT2}" --send-tcp-"$1" 127.0.0.1:"${PORT1}" &
sleep 1

# Send test data thru acars_router
while IFS="" read -r p || [ -n "$p" ]; do
  printf '%s\n' "$p" | socat - TCP:127.0.0.1:"${PORT2}"
done < ./test_data/"$1".patched

# Re-format output files
jq -M . < ./test_data/"$1".patched > /tmp/"$1".tcplisten.tcpsend.out.reference.reformatted
jq -M . < /tmp/"$1".tcplisten.tcpsend.out > /tmp/"$1".tcplisten.tcpsend.out.reformatted

# Check output
diff /tmp/"$1".tcplisten.tcpsend.out.reference.reformatted /tmp/"$1".tcplisten.tcpsend.out.reformatted
exit $?
