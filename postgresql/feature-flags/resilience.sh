#!/usr/bin/env bash

source "$(dirname "$0")/lib.sh"

psql $DSN -f seed.sql

rule "every listening backend terminated every 5s"
# pg_terminate_backend is the honest stand-in: a failover, a restarted
# pooler, a network blip, a firewall reaping an idle connection. From the
# client it is the same event -- the session is gone and nothing else is
# wrong. The reconnect loop in cache.go is the one everybody writes.
#
# poll is in the table because it has no session to lose. That is not a
# tuning difference, it is the entire structural argument for polling.
{
  go run . -header
  go run . -mode listen -kill 5s -settle 25s
  go run . -mode poll -interval 5s -kill 5s -settle 25s
} | column -t -s $'\t'
