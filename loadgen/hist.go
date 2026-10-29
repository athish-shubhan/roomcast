package loadgen

import "math"

// Hist is a log-bucketed latency histogram: 20 buckets per decade from
// 10 us to 100 s.
type Hist struct {
	B [161]uint64 `json:"b"`
	N uint64      `json:"n"`
}

const decadeBuckets = 20

func bucket(ms float64) int {
	if ms <= 0.01 {
		return 0
	}
	i := int((math.Log10(ms) + 2) * decadeBuckets)
	return max(0, min(160, i))
}

func (h *Hist) Add(ms float64) {
	h.B[bucket(ms)]++
	h.N++
}

func (h *Hist) Merge(o *Hist) {
	for i := range h.B {
		h.B[i] += o.B[i]
	}
	h.N += o.N
}

// Quantile returns the upper edge of the bucket holding quantile q, in ms.
func (h *Hist) Quantile(q float64) float64 {
	if h.N == 0 {
		return math.NaN()
	}
	target := uint64(math.Ceil(q * float64(h.N)))
	var c uint64
	for i, n := range h.B {
		c += n
		if c >= target {
			return math.Pow(10, float64(i+1)/decadeBuckets-2)
		}
	}
	return math.Inf(1)
}

// Within is the fraction of samples at or below ms (bucket resolution).
func (h *Hist) Within(ms float64) float64 {
	if h.N == 0 {
		return math.NaN()
	}
	var c uint64
	for i, n := range h.B {
		if math.Pow(10, float64(i+1)/decadeBuckets-2) > ms*1.0001 {
			break
		}
		c += n
	}
	return float64(c) / float64(h.N)
}
