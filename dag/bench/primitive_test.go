package bench

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MOG-Systems-Inc/primbench/internal/schema"

	"github.com/nats-io/nats.go/jetstream"
)

func newPrimitive(t testing.TB) (*Primitive, *schema.Validator) {
	t.Helper()
	v, err := schema.Load("")
	if err != nil {
		t.Fatal(err)
	}
	p, err := New(v, DevGrants())
	if err != nil {
		t.Fatal(err)
	}
	return p, v
}

// Every action in the workload reaches the refusal it was written for.
func TestMixDecisions(t *testing.T) {
	p, _ := newPrimitive(t)
	for _, a := range Mix() {
		d := p.CheckPublish(a.Identity, a.Raw)
		if d.Refusal != a.Want {
			t.Errorf("%s: refusal %q (%v), want %q", a.Name, d.Refusal, d.Err, a.Want)
		}
	}
}

func TestDecisionDetail(t *testing.T) {
	p, _ := newPrimitive(t)
	d := p.CheckPublish("gpu-worker", Record("asset", "nd_asset0000000001"))
	if d.Refusal != Allowed || d.Stream != "DAG_ASSET" || d.Subject != "tenant.acme.dag.asset.nd_asset0000000001" {
		t.Fatalf("GPU worker asset: %+v", d)
	}
	d = p.CheckPublish("authority", Record("unknown", "nd_widget00000001"))
	if d.Refusal != NoStream || d.Subject != "tenant.acme.dag.unknown.nd_widget00000001" {
		t.Fatalf("unknown kind: %+v", d)
	}
	if d := p.CheckPublish("nobody", Record("tag", "nd_tag0000000001")); d.Refusal != NotGranted {
		t.Fatalf("an identity with no grant: %+v", d)
	}
	if d := p.CheckPublish("authority", []byte(`{"schema_id":"sch_Tag_v1"}`)); d.Refusal != BadRecord {
		t.Fatalf("no id: %+v", d)
	}
	if d := p.CheckSubscribe("gpu-worker", "tenant.acme.dag.task.render.local"); d.Refusal != Allowed {
		t.Fatalf("the GPU worker subscribes its own queue: %+v", d)
	}
	if d := p.CheckSubscribe("gpu-worker", "tenant.acme.dag.task.render.standby"); d.Refusal != NotGranted {
		t.Fatalf("the GPU worker subscribes the standby's queue: %+v", d)
	}
}

// The evaluation reaches the broker's verdict. Each action is published as
// raw bytes, with no check first, on a broker enforcing the same grants and
// streams: a publish the evaluation refuses as not granted is refused by the
// broker with a permissions violation, one it refuses as no stream gets no
// stream's acknowledgement, and one it allows is stored. Schema validation is
// the writer's: the broker stores a record that fails its schema, so an
// action the evaluation refuses only on its schema is stored here.
func TestEvaluationMatchesBroker(t *testing.T) {
	p, _ := newPrimitive(t)
	b, err := StartBroker(DevGrants(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	cases := Mix()
	cases = append(cases,
		Action{Name: "the GPU worker writes an unregistered kind", Identity: "gpu-worker", Raw: Record("unknown", "nd_widget0000000002"), Want: NotGranted},
		Action{Name: "the standby GPU worker writes a tag", Identity: "gpu-worker-standby", Raw: Record("tag", "nd_tagsb0000000001"), Want: NotGranted},
		Action{Name: "an editing workstation writes a tag", Identity: "workstation-render-1", Raw: Record("tag", "nd_tagws0000000001"), Want: NotGranted},
		Action{Name: "the browser account worker writes an edge", Identity: "browser-account", Raw: Record("edge", "ed_edgebrowser00001"), Want: NotGranted},
	)
	for _, a := range cases {
		d := p.CheckPublish(a.Identity, a.Raw)
		if d.Refusal != a.Want {
			t.Errorf("%s: evaluation %q, want %q", a.Name, d.Refusal, a.Want)
			continue
		}
		var mu sync.Mutex
		var async []error
		nc, err := b.Connect(a.Identity, func(err error) { mu.Lock(); async = append(async, err); mu.Unlock() })
		if err != nil {
			t.Fatal(err)
		}
		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
		// No retry: the client otherwise retries a no-stream answer, in case a
		// stream is electing a leader.
		_, perr := js.Publish(ctx, d.Subject, a.Raw, jetstream.WithRetryAttempts(0))
		cancel()
		_ = nc.Flush()
		time.Sleep(20 * time.Millisecond)
		mu.Lock()
		violation := false
		for _, e := range async {
			if strings.Contains(e.Error(), "Permissions Violation") && strings.Contains(e.Error(), d.Subject) {
				violation = true
			}
		}
		mu.Unlock()
		nc.Close()

		var broker Refusal
		switch {
		case violation:
			broker = NotGranted
		case errors.Is(perr, jetstream.ErrNoStreamResponse):
			broker = NoStream
		case perr == nil:
			broker = Allowed
		default:
			t.Errorf("%s: broker answered %v", a.Name, perr)
			continue
		}
		want := a.Want
		if want == SchemaRejected {
			want = Allowed // the broker does not read the schema
		}
		if broker != want {
			t.Errorf("%s: broker %q, evaluation %q", a.Name, broker, a.Want)
		}
	}
}

// The registry stand-in answers as the local validator does.
func TestRegistryHandler(t *testing.T) {
	_, v := newPrimitive(t)
	srv := httptest.NewServer(RegistryHandler(v))
	defer srv.Close()
	r := &Remote{HTTP: NewHTTPClient(1), RegistryURL: srv.URL}
	ctx := context.Background()
	if err := r.validate(ctx, "sch_Tag_v1", Record("tag", "nd_tag0000000001")); err != nil {
		t.Fatalf("valid tag: %v", err)
	}
	if err := r.validate(ctx, "sch_Tag_v1", Record("bad_tag", "nd_tag0000000001")); err == nil {
		t.Fatal("bad tag passed")
	}
	if err := r.validate(ctx, "sch_Widget_v1", Record("unknown", "nd_widget0000001")); err == nil || !strings.Contains(err.Error(), "404") {
		t.Fatalf("unknown schema: %v", err)
	}
	resp, err := http.Get(srv.URL + "/validate/sch_Tag_v1")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET: %d", resp.StatusCode)
	}
}

func TestPercentile(t *testing.T) {
	s := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	for q, want := range map[float64]int64{0.5: 5, 0.9: 9, 0.99: 10, 0.1: 1} {
		if got := Percentile(s, q); int64(got) != want {
			t.Errorf("p%v = %d, want %d", q, got, want)
		}
	}
	r := Run("noop", 2, 100, 10, func(int, int) error { return nil })
	if r.Ops != 200 || r.Errors != 0 {
		t.Fatalf("run: %+v", r)
	}
}

// The steps of the evaluation, one at a time and whole. go test -bench
// reports ns/op for each on the machine it runs on.
func BenchmarkEvaluation(b *testing.B) {
	p, v := newPrimitive(b)
	raw := Record("asset", "nd_asset0000000001")
	subject := "tenant.acme.dag.asset.nd_asset0000000001"
	b.Run("kind_and_subject", func(b *testing.B) {
		for b.Loop() {
			_, _ = SubjectOf("sch_Asset_ProRes_v2", "nd_asset0000000001", "")
		}
	})
	b.Run("grant_match", func(b *testing.B) {
		for b.Loop() {
			_ = p.Granted("gpu-worker", Publish, subject)
		}
	})
	b.Run("stream_lookup", func(b *testing.B) {
		for b.Loop() {
			_ = p.StreamOf(subject)
		}
	})
	b.Run("schema_validate_asset", func(b *testing.B) {
		for b.Loop() {
			_ = v.Validate("sch_Asset_ProRes_v2", raw)
		}
	})
	b.Run("whole_asset", func(b *testing.B) {
		for b.Loop() {
			_ = p.CheckPublish("gpu-worker", raw)
		}
	})
	tag := Record("tag", "nd_tag0000000001")
	b.Run("whole_tag", func(b *testing.B) {
		for b.Loop() {
			_ = p.CheckPublish("authority", tag)
		}
	})
	mix := Mix()
	b.Run("whole_mix", func(b *testing.B) {
		i := 0
		for b.Loop() {
			a := mix[i%len(mix)]
			_ = p.CheckPublish(a.Identity, a.Raw)
			i++
		}
	})
}
