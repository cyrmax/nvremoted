package perf

import (
	"math"
	"time"
)

// Histogram is bounded (~32 KiB), has no locks, and is owned by one worker.
// Nonzero buckets have at most 1% relative width. Values above ~16 years saturate
// the last bucket, while exact moments and extrema remain available.
type Histogram struct {
	buckets            [4096]uint64
	n                  uint64
	min, max, mean, m2 float64
}

const logBucketWidth = 0.009950330853168083 // log(1.01)

func (h *Histogram) Add(d time.Duration) {
	x := math.Max(0, float64(d))
	index := 0
	if x >= 1 {
		index = 1 + int(math.Log(x)/logBucketWidth)
		if index >= len(h.buckets) {
			index = len(h.buckets) - 1
		}
	}
	h.buckets[index]++
	h.n++
	if h.n == 1 || x < h.min {
		h.min = x
	}
	if x > h.max {
		h.max = x
	}
	delta := x - h.mean
	h.mean += delta / float64(h.n)
	h.m2 += delta * (x - h.mean)
}

func (h *Histogram) Merge(other *Histogram) {
	if other.n == 0 {
		return
	}
	if h.n == 0 {
		*h = *other
		return
	}
	n := h.n + other.n
	d := other.mean - h.mean
	h.m2 += other.m2 + d*d*float64(h.n)*float64(other.n)/float64(n)
	h.mean += d * float64(other.n) / float64(n)
	if other.min < h.min {
		h.min = other.min
	}
	if other.max > h.max {
		h.max = other.max
	}
	h.n = n
	for i, count := range other.buckets {
		h.buckets[i] += count
	}
}

func (h *Histogram) quantile(q float64) float64 {
	if h.n == 0 {
		return 0
	}
	rank := uint64(math.Ceil(q * float64(h.n)))
	var seen uint64
	for i, count := range h.buckets {
		seen += count
		if seen >= rank {
			if i == 0 {
				return h.min
			}
			// Upper boundary: never imply finer precision than the binning.
			return math.Min(h.max, math.Max(h.min, math.Exp(float64(i)*logBucketWidth)))
		}
	}
	return h.max
}

func (h *Histogram) Distribution() Distribution {
	d := Distribution{Count: h.n, Min: h.min, Max: h.max, Mean: h.mean,
		P50: h.quantile(.5), P90: h.quantile(.9), P95: h.quantile(.95), P99: h.quantile(.99)}
	if h.n > 0 {
		d.StdDev = math.Sqrt(h.m2 / float64(h.n))
	}
	// At least ten expected observations in the top 0.1%; fewer samples are
	// explicitly insufficient rather than publishing an almost-max percentile.
	if h.n >= 10000 {
		v := h.quantile(.999)
		d.P999 = &v
	}
	return d
}
