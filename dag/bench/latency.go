package bench

import (
	"fmt"
	"math"
	"slices"
	"sync"
	"time"
)

// Result is one measured run: every timed operation's latency, summarised.
type Result struct {
	Name        string
	Concurrency int
	Ops         int
	Wall        time.Duration
	P50, P90    time.Duration
	P99, P999   time.Duration
	Max, Mean   time.Duration
	Throughput  float64 // operations per second across all workers
	Errors      int     // operations whose outcome differed from the expected one
	FirstError  error
}

// Percentile is the nearest-rank percentile of sorted samples (0 < q <= 1).
func Percentile(sorted []int64, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(math.Ceil(q*float64(len(sorted)))) - 1
	if i < 0 {
		i = 0
	}
	return time.Duration(sorted[i])
}

// Run times op under conc workers. Each worker first runs warmup untimed
// operations, then ops timed ones; worker w's i-th call is op(w, i). Each
// latency is one time.Now pair around one call. An op that returns an error
// is counted in Errors and still timed.
func Run(name string, conc, ops, warmup int, op func(worker, i int) error) Result {
	samples := make([][]int64, conc)
	errs := make([]int, conc)
	firstErr := make([]error, conc)
	var ready, done sync.WaitGroup
	start := make(chan struct{})
	ready.Add(conc)
	done.Add(conc)
	for w := 0; w < conc; w++ {
		go func(w int) {
			defer done.Done()
			for i := 0; i < warmup; i++ {
				_ = op(w, i)
			}
			s := make([]int64, ops)
			ready.Done()
			<-start
			for i := 0; i < ops; i++ {
				t0 := time.Now()
				err := op(w, warmup+i)
				s[i] = int64(time.Since(t0))
				if err != nil {
					errs[w]++
					if firstErr[w] == nil {
						firstErr[w] = err
					}
				}
			}
			samples[w] = s
		}(w)
	}
	ready.Wait()
	t0 := time.Now()
	close(start)
	done.Wait()
	wall := time.Since(t0)

	all := make([]int64, 0, conc*ops)
	r := Result{Name: name, Concurrency: conc, Ops: conc * ops, Wall: wall}
	var sum float64
	for w := range samples {
		all = append(all, samples[w]...)
		r.Errors += errs[w]
		if r.FirstError == nil && firstErr[w] != nil {
			r.FirstError = firstErr[w]
		}
	}
	slices.Sort(all)
	for _, v := range all {
		sum += float64(v)
	}
	r.P50 = Percentile(all, 0.50)
	r.P90 = Percentile(all, 0.90)
	r.P99 = Percentile(all, 0.99)
	r.P999 = Percentile(all, 0.999)
	if len(all) > 0 {
		r.Max = time.Duration(all[len(all)-1])
		r.Mean = time.Duration(sum / float64(len(all)))
	}
	r.Throughput = float64(len(all)) / wall.Seconds()
	return r
}

// us renders a duration in microseconds with two decimals.
func us(d time.Duration) string { return fmt.Sprintf("%.2f", float64(d)/1e3) }

// Header is the Markdown table header Row writes under.
const Header = "| Path | Workers | Ops | p50 µs | p90 µs | p99 µs | p99.9 µs | max µs | mean µs | ops/s | wrong |\n" +
	"| :-- | --: | --: | --: | --: | --: | --: | --: | --: | --: | --: |"

// Row is the result as one Markdown table row.
func (r Result) Row() string {
	return fmt.Sprintf("| %s | %d | %d | %s | %s | %s | %s | %s | %s | %.0f | %d |",
		r.Name, r.Concurrency, r.Ops, us(r.P50), us(r.P90), us(r.P99), us(r.P999), us(r.Max), us(r.Mean), r.Throughput, r.Errors)
}
