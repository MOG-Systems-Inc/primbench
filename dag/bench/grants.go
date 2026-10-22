package bench

import "github.com/MOG-Systems-Inc/primbench/internal/ledger"

// Grant is one broker identity's permissions: the subject patterns it may
// publish to and subscribe to. It is the grant tuple of the primitive
// (identity, verb, pattern), one Grant per identity.
type Grant struct {
	Identity  string
	Publish   []string
	Subscribe []string
}

// root is the sample tenant's subject root the grants are written against.
const root = ledger.SubjectRoot

func workstation(name string) Grant {
	return Grant{Identity: "workstation-" + name, Publish: []string{
		root + ".ingress.worker.presence.edit." + name,
		root + ".ingress.edit.*",
		root + ".asset.>",
		root + ".edge.>",
		"$JS.API.>",
		"$JS.ACK.>",
	}}
}

func browserWorker(identity string) Grant {
	return Grant{Identity: identity, Publish: []string{
		root + ".ingress.worker.presence.browser.*",
		root + ".ingress.browser.*",
		"$JS.API.>",
		"$JS.ACK.>",
	}}
}

// DevGrants is the benchmark's grant set: the eight identities that reach
// the sample tenant's subject tree. It is a transcription of a production
// cluster's record-family patterns, four to nine publish patterns per
// identity, with the JetStream API subjects collapsed into one $JS.API.>
// pattern. Only the names are generic. Keys are minted per run for the
// in-process broker.
//
//   - authority: the service that writes records for every family.
//   - gpu-worker, gpu-worker-standby: the GPU workers that write assets and
//     edges and take render and analysis tasks.
//   - browser, browser-account: browser workers, which write no record.
//   - workstation-edit, workstation-render-1, workstation-render-2: editing
//     workstations, which write assets and edges and take edit jobs.
//
// The broker creates its streams as an identity of its own, which holds
// every subject and is not one of these grants (StartBroker).
//
// WithInbox appends `_INBOX.>` to every subscribe list, as the cluster's
// configuration renders it.
func DevGrants() []Grant {
	return []Grant{
		{
			Identity:  "authority",
			Publish:   []string{root + ".>", "tasks.vision-model.request", "$JS.API.>", "$JS.ACK.>"},
			Subscribe: []string{root + ".>", "responses.>", "tasks.*.*.*.events.>"},
		},
		{
			Identity: "gpu-worker",
			Publish: []string{
				root + ".asset.>",
				root + ".edge.>",
				root + ".ingress.worker.presence.local",
				root + ".ingress.worker.presence.local.*",
				root + ".ingress.analysis.pose.*",
				root + ".ingress.precompute.>",
				root + ".ingress.encode.*",
				"$JS.API.>",
				"$JS.ACK.>",
			},
			Subscribe: []string{
				root + ".task.render.local",
				root + ".task.analysis.pose.local",
				root + ".task.analysis.*.local",
				root + ".task.encode.local",
			},
		},
		{
			Identity: "gpu-worker-standby",
			Publish: []string{
				root + ".asset.>",
				root + ".edge.>",
				root + ".ingress.worker.presence.standby",
				root + ".ingress.worker.presence.standby.*",
				root + ".ingress.analysis.pose.*",
				"$JS.API.>",
				"$JS.ACK.>",
			},
			Subscribe: []string{
				root + ".task.render.standby",
				root + ".task.analysis.pose.standby",
			},
		},
		browserWorker("browser"),
		browserWorker("browser-account"),
		workstation("edit"),
		workstation("render-1"),
		workstation("render-2"),
	}
}

// WithInbox returns the grants with `_INBOX.>` appended to every subscribe
// list, as the cluster's configuration renders them.
func WithInbox(gs []Grant) []Grant {
	out := make([]Grant, len(gs))
	for i, g := range gs {
		g.Subscribe = append(append([]string{}, g.Subscribe...), "_INBOX.>")
		out[i] = g
	}
	return out
}
