#!/usr/bin/env bash
# The listening session is a session, and sessions end. This is what each
# strategy does about the ones that end without telling anybody.

source "$(dirname "$0")/lib.sh"
build

readonly N=20

rule "1. control: no interference, 30 changes 1s apart"
{
  "$BIN" -header
  "$BIN" -mode listen -instances $N -flips 30 -gap 1s -settle 25s
  "$BIN" -mode resync -watchdog 10s -instances $N -flips 30 -gap 1s -settle 25s
} | column -t -s $'\t'

rule "2. every listening backend terminated every 5s"
# pg_terminate_backend is the honest stand-in: a failover, a restarted
# pooler, a network blip, a firewall reaping an idle connection. From the
# client it is the same event -- the session is gone and nothing else is
# wrong. The reconnect loop in cache.go is the one everybody writes.
#
# poll is in the table because it has no session to lose. That is not a
# tuning difference, it is the entire structural argument for polling.
{
  "$BIN" -header
  "$BIN" -mode listen -instances $N -flips 30 -gap 1s -kill 5s -settle 25s
  "$BIN" -mode resync -watchdog 10s -instances $N -flips 30 -gap 1s -kill 5s -settle 25s
  "$BIN" -mode poll -interval 5s -instances $N -flips 30 -gap 1s -kill 5s -settle 25s
} | column -t -s $'\t'
