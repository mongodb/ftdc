package ftdc

import (
	"math"
	"sort"
	"time"
)

// MetricStat represents basic statistics for a single metric
type MetricStat struct {
	// Median is the median
	Median int

	// MAD is the Median Absolute Deviation
	MAD int
}

// Stats represents basic statistics for a set of metric samples.
type Stats struct {
	Start    time.Time
	End      time.Time
	Metrics  map[string]MetricStat
	NSamples int
}

// Stats produces Stats for the Chunk
func (c *Chunk) Stats() (s Stats) {
	s.NSamples = len(c.Metrics[0].Values)
	s.Metrics = make(map[string]MetricStat)
	m := c.Map()
	for k, v := range m {
		s.Metrics[k] = computeMetricStat(v)
	}
	start := m["start"][0] / 1000
	end := m["start"][s.NSamples-1] / 1000
	s.Start = time.Unix(int64(start), 0)
	s.End = time.Unix(int64(end), 0)
	return
}

// MergeStats computes a time-weighted merge of Stats.
func MergeStats(cs ...Stats) (m Stats) {
	var start int64 = math.MaxInt64
	var end int64 = math.MinInt64
	weights := make([]int, len(cs))
	meds := make(map[string][]int)
	mads := make(map[string][]int)
	for i, s := range cs {
		m.NSamples += s.NSamples
		sStart := s.Start.Unix()
		sEnd := s.End.Unix()
		if sStart < start {
			start = sStart
		}
		if sEnd > end {
			end = sEnd
		}
		weights[i] = int(sEnd - sStart)
		for k, v := range s.Metrics {
			if _, ok := meds[k]; !ok {
				meds[k] = make([]int, len(cs))
				mads[k] = make([]int, len(cs))
			}
			meds[k][i] = v.Median
			mads[k][i] = v.MAD
		}
	}
	m.Start = time.Unix(start, 0)
	m.End = time.Unix(end, 0)
	m.Metrics = make(map[string]MetricStat)
	for k := range meds {
		med := weightedMed(meds[k], weights)
		mad := weightedMed(mads[k], weights)
		m.Metrics[k] = MetricStat{
			Median: med,
			MAD:    mad,
		}
	}
	return
}

func computeMetricStat(l []int) MetricStat {
	sort.Ints(l)
	m := l[len(l)/2]
	dev := make([]int, len(l))
	for i, v := range l {
		dev[i] = v - m
		if dev[i] < 0 {
			dev[i] = -dev[i]
		}
	}
	sort.Ints(dev)
	mad := dev[len(l)/2]
	return MetricStat{
		Median: m,
		MAD:    mad,
	}
}

func weightedMed(l, w []int) int {
	W := 0
	for _, wt := range w {
		W += wt
	}
	// sort(x) ~ sort(l), but w must correspond
	x := &weightedInts{l, w}
	sort.Sort(x)
	k := 0
	sum := W - x.w[0]
	for sum > W/2 {
		k++
		sum -= x.w[k]
	}
	return x.l[k]
}

type weightedInts struct {
	l, w []int
}

func (w *weightedInts) Len() int {
	return len(w.l)
}
func (w *weightedInts) Less(i, j int) bool {
	return w.l[i] < w.l[j]
}
func (w *weightedInts) Swap(i, j int) {
	w.l[i], w.w[i], w.l[j], w.w[j] = w.l[j], w.w[j], w.l[i], w.w[i]
}
