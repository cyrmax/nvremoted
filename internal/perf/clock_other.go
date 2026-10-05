//go:build !windows

package perf

import "time"

func Now() time.Time                { return time.Now() }
func clockDetails() (string, int64) { return "Go time.Now monotonic", 0 }
