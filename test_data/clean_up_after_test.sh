#!/usr/bin/env bash

set -x

# Kill any socat instances
killall -9 socat

# Kill any python3 instances
killall -9 python3

# Clean up /tmp files
rm -rf /tmp/acars*
rm -rf /tmp/vdlm2*

exit 0
