#!/usr/bin/env bash

set -x

ls -la /opt/

# determine which binary to keep
if /opt/acars_router.amd64 --version > /dev/null 2>&1; then
    mv -v /opt/acars_router.amd64 /opt/acars_router
elif /opt/acars_router.arm64 --version > /dev/null 2>&1; then
    mv -v /opt/acars_router.arm64 /opt/acars_router
elif /opt/acars_router.armv7 --version > /dev/null 2>&1; then
    mv -v /opt/acars_router.armv7 /opt/acars_router
else
    >&2 echo "ERROR: Unsupported architecture"
    exit 1
fi
