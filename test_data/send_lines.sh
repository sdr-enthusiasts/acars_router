#!/usr/bin/env bash
while IFS="" read -r p || [ -n "$p" ]; do
  #printf '%s\n' "$p"
  echo "$p"
  sleep 1
done < "$1"
