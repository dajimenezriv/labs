#!/usr/bin/env bash

source "$(dirname "$0")/lib.sh"

psql $DSN -f seed.sql

rule "every listening backend terminated every 5s"
{
  go run . -header
  go run . -mode listen -kill 5s -settle 25s
  go run . -mode poll -interval 5s -kill 5s -settle 25s
} | column -t -s $'\t'
