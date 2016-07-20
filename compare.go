package ftdc

import (
	"bytes"
	"fmt"
	"math"
	"sort"
)

// CmpThreshold is the threshold for comparison of metrics used by the
// Proximal function.
var CmpThreshold float64 = 0.3

const badTimePenalty = -0.1

type cmpScore struct {
	num float64
	msg string
}

type cmpScores []cmpScore

func (s cmpScores) Len() int {
	return len(s)
}
func (s cmpScores) Less(i, j int) bool {
	return s[i].num < s[j].num
}
func (s cmpScores) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Proximal computes a measure of deviation between two sets of metric
// statistics. It computes an aggregated score based on compareMetrics
// output, and compares it against the CmpThreshold.
//
// Return values: msg holds errors for non-proximal metrics, score holds the
// numeric rating (1.0 = perfect), and ok is whether the threshold was met.
func Proximal(a, b Stats) (msg string, score float64, ok bool) {
	aCount := float64(a.NSamples)
	bCount := float64(b.NSamples)
	diff := math.Abs(aCount - bCount)
	max := math.Max(aCount, bCount)
	if diff/max > CmpThreshold {
		msg += fmt.Sprintf("sample count not proximal: (%d, %d) are not "+
			"within threshold (%d%%)\n",
			a.NSamples, b.NSamples, int(CmpThreshold*100))
		score = badTimePenalty
	}

	scores := make(cmpScores, 0)
	var sumScores float64
	for key := range a.Metrics {
		if _, ok := b.Metrics[key]; !ok {
			continue
		}
		cmp := compareMetrics(a, b, key)
		scores = append(scores, cmp)
		sumScores += cmp.num
	}
	sort.Sort(scores)

	// score is half the average...
	score = sumScores / (2 * float64(len(scores)))

	// and half the weighted sum of 1/2, 1/4, 1/8, ...
	// with scores from worst to best
	for i, c := range scores {
		score += math.Pow(2, -float64(i+2)) * c.num
	}
	// score is quadratic, so sqrt for linear
	score = math.Sqrt(score)

	// set msg to ordered output of proximality misses
	buf := new(bytes.Buffer)
	for _, c := range scores {
		buf.WriteString(c.msg)
	}
	msg += buf.String()

	ok = score >= (1 - CmpThreshold)
	return
}

// compareMetrics computes a measure of deviation between two samples of the
// same metric. It computes a score of (1 - rx')*(1 - rx''), where rx' and
// rx'' correspond to the relative difference of the first and second
// derivatives of the time-series metric.
func compareMetrics(sa, sb Stats, key string) (score cmpScore) {
	a := sa.Metrics[key]
	b := sb.Metrics[key]
	if a.Median == b.Median {
		score.num = 1
		return
	}
	maxmad := math.Max(math.Abs(float64(a.MAD)), math.Abs(float64(b.MAD)))
	maxmed := math.Max(math.Abs(float64(a.Median)), math.Abs(float64(b.Median)))
	if maxmad == 0 || maxmed == 0 {
		score.num = 1
		return
	}

	relmad := math.Abs(float64(a.MAD-b.MAD)) / maxmad
	relmed := math.Abs(float64(a.Median-b.Median)) / maxmed
	score.num = (1 - relmed) * (1 - relmad)

	if relmad > CmpThreshold {
		score.msg += fmt.Sprintf("metric '%s' not proximal: "+
			"deviations (%d, %d) are not within threshold (%d)\n",
			key, a.MAD, b.MAD, int(CmpThreshold*100))
	}
	if relmed > CmpThreshold {
		score.msg += fmt.Sprintf("metric '%s' not proximal: "+
			"medians (%d, %d) are not within threshold (%d)\n",
			key, a.Median, b.Median, int(CmpThreshold*100))
	}
	return
}
