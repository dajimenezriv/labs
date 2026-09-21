// The application under migration, and the client that measures it.
//
//	go run . serve    the service: one HTTP API, two database pools, a
//	                  routing switch that moves reads and writes between
//	                  them while it is running
//	go run . load     the workload: pays orders continuously, checks that
//	                  what it wrote it can read back, and records every
//	                  request outcome
//
// The scripts drive the migration; this binary is what "without downtime"
// is measured against.
package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: go run . serve|load [flags]")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "serve":
		serve(os.Args[2:])
	case "load":
		load(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", os.Args[1])
		os.Exit(2)
	}
}
