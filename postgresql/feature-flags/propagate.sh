#!/usr/bin/env bash

source "$(dirname "$0")/lib.sh"

psql $DSN -f seed.sql

rule "propagation and standing cost, 20 instances"
{
  go run . -header
  go run . -mode poll -interval 5s
  go run . -mode poll -interval 5s -idle -settle 30s
  go run . -mode listen
  go run . -mode listen -idle -settle 30s
} | column -t -s $'\t'
