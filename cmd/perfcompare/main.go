// perfcompare compares local performance result files without running a server.
package main

import (
	"fmt"
	"github.com/n0ot/nvremoted/internal/perf"
	"os"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: perfcompare baseline/results.json candidate/results.json")
		os.Exit(2)
	}
	before, err := perf.ReadReport(os.Args[1])
	if err == nil {
		var after perf.Report
		after, err = perf.ReadReport(os.Args[2])
		if err == nil {
			err = perf.Compare(os.Stdout, before, after)
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
