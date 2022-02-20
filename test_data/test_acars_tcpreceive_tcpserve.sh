#!/usr/bin/env bash

PORT1=$(shuf -i 2000-2999 -n 1)
PORT2=$(shuf -i 3000-3999 -n 1)

# Start acars_router
timeout 30s python3 ./acars_router/acars_router.py -vv --skew-window 300 --receive-tcp-acars="127.0.0.1:${PORT1}" --serve-tcp-acars "${PORT2}" &
sleep 1

# Start fake destination server for acars_router output
socat -d -t10 TCP:127.0.0.1:"${PORT2}" OPEN:/tmp/acars.tcpreceive.tcpsserve.out,creat,append &
sleep 1

# Start fake source server(s)
while IFS="" read -r p || [ -n "$p" ]; do
    printf '%s' "$p" | socat -d TCP-LISTEN:"${PORT1}",reuseaddr STDIN;
done < ./test_data/acars.patched
sleep 10

jq . < /tmp/acars.tcpreceive.tcpsserve.out 

# # Start fake destination server for reference output
# socat -d -t5 TCP-LISTEN:25550,fork OPEN:/tmp/acars.tcplisten.tcpsend.out.reference,creat,append &
# sleep 1

# # Send data bypassing acars_router
# while IFS="" read -r p || [ -n "$p" ]; do
#     printf '%s\n' "$p" | socat - TCP:127.0.0.1:25550;
# done <./test_data/acars.patched

# # Start fake destination server for acars_router output
# socat -d -t5 TCP-LISTEN:15550,fork OPEN:/tmp/acars.tcplisten.tcpsend.out,creat,append &
# sleep 1


# # Send test data thru acars_router
# while IFS="" read -r p || [ -n "$p" ]; do
#     printf '%s\n' "$p" | socat - TCP:127.0.0.1:5550;
# done <./test_data/acars.patched

# # Re-format output files
# jq -M . < /tmp/acars.tcplisten.tcpsend.out.reference > /tmp/acars.tcplisten.tcpsend.out.reference.reformatted
# jq -M . < /tmp/acars.tcplisten.tcpsend.out > /tmp/acars.tcplisten.tcpsend.out.reformatted

# # Check output
# diff /tmp/acars.tcplisten.tcpsend.out.reference.reformatted /tmp/acars.tcplisten.tcpsend.out.reformatted
# exit $?
