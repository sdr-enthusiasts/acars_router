#!/usr/bin/env bash
while IFS="" read -r p || [ -n "$p" ]; do
    printf '%s\n' "$p"
    sleep 0.25
done < $1