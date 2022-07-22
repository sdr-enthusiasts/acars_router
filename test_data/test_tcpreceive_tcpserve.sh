#!/usr/bin/env bash

PORT1=$(shuf -i 5000-5999 -n 1)
PORT2=$(shuf -i 6000-6999 -n 1)

# Start acars_router
timeout 30s python3 ./acars_router/acars_router.py -vv --add-proxy-id false --skew-window 300 --receive-tcp-"$1"="127.0.0.1:${PORT1}" --serve-tcp-"$1" "${PORT2}" &
sleep 1

# Start fake destination server for acars_router output
socat -d -t10 TCP:127.0.0.1:"${PORT2}" OPEN:/tmp/"$1".tcpreceive.tcpsserve.out,create,append &
sleep 1

# Start fake source server(s)
while IFS="" read -r p || [ -n "$p" ]; do
  printf '%s' "$p" | socat -d TCP-LISTEN:"${PORT1}",reuseaddr STDIN
done < ./test_data/"$1".patched
sleep 10

# Re-format output files
jq -M . < ./test_data/"$1".patched > /tmp/"$1".tcpreceive.tcpsserve.out.reference.reformatted
jq -M . < /tmp/"$1".tcpreceive.tcpsserve.out > /tmp/"$1".tcpreceive.tcpsserve.out.reformatted

# Check output
diff /tmp/"$1".tcpreceive.tcpsserve.out.reference.reformatted /tmp/"$1".tcpreceive.tcpsserve.out.reformatted
exit $?
