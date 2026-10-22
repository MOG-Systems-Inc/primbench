package driver

import (
	"fmt"
	"strings"
)

// published is each path's p50 at one worker, in µs, from the paper's run:
// Apple M5 Max, 18 cores, Go 1.27.1, Postgres 18.3 and Valkey 9.1.2 as
// native processes on loopback, 19 October 2026.
var published = map[string][]struct {
	path, label string
	p50         float64
}{
	"eval": {
		{"local: one process", "local", 5.96},
		{"three services, in sequence", "three services", 144.88},
		{"three services, registry in parallel", "registry in parallel", 101.21},
	},
	"write": {
		{"publish only, no checks", "publish only", 46.96},
		{"local checks, then publish", "local checks then publish", 45.67},
		{"three services, then publish", "three services then publish", 188.54},
	},
	"read": {
		{"plain read, no policy (the floor)", "plain", 26.04},
		{"row policy, pipelined (1 round trip)", "policy pipelined", 30.62},
		{"row policy, three round trips", "policy bound", 71.58},
		{"Valkey grant, then filtered query", "Valkey grant then query", 94.83},
	},
}

var sectionNames = map[string]string{"eval": "evaluation", "write": "write", "read": "read"}

type verdict struct {
	text string
	pass bool
}

// verdicts is one line per section that ran: each one-worker p50 beside the
// published one, and PASS when every one is within tol either way and no
// operation in the section came out wrong.
func (m *measurement) verdicts(tol float64) []verdict {
	var out []verdict
	for _, s := range []string{"eval", "write", "read"} {
		if !m.run[s] {
			continue
		}
		byPath := map[string]float64{}
		wrong := 0
		for _, r := range m.results[s] {
			wrong += r.Errors
			if r.Concurrency == 1 {
				byPath[r.Name] = float64(r.P50) / 1e3
			}
		}
		var parts []string
		pass, judged := wrong == 0, 0
		for _, p := range published[s] {
			got, ok := byPath[p.path]
			if !ok {
				continue
			}
			judged++
			ratio := got / p.p50
			if ratio > tol || ratio < 1/tol {
				pass = false
			}
			parts = append(parts, fmt.Sprintf("%s %.1f µs (published %.1f)", p.label, got, p.p50))
		}
		if got, ok := byPath["Postgres grant, then filtered query"]; ok {
			parts = append(parts, fmt.Sprintf("Postgres grant then query %.1f µs (no published number: no Valkey was listening)", got))
		}
		if judged == 0 {
			out = append(out, verdict{fmt.Sprintf("%s: no one-worker row to judge: FAIL", sectionNames[s]), false})
			continue
		}
		result := "PASS"
		if !pass {
			result = "FAIL"
		}
		out = append(out, verdict{fmt.Sprintf("%s: %s; %d wrong; within %gx: %s", sectionNames[s], strings.Join(parts, ", "), wrong, tol, result), pass})
	}
	return out
}
