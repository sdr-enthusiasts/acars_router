#!/usr/bin/env bash
while IFS="" read -r p || [ -n "$p" ]; do
    printf '%s\n' "$p"
done < $1