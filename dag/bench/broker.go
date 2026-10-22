package bench

import (
	"context"
	"fmt"
	"time"

	"github.com/MOG-Systems-Inc/primbench/internal/ledger"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

// Broker is an in-process nats-server with JetStream, configured the way a
// production cluster's configuration renders it: one nkey user per identity
// in the global account, each with its publish and subscribe allow lists, and
// every stream of ledger.Streams. It differs from a cluster in three ways:
// one node, so every
// stream has one replica and a write needs no consensus round; file storage
// in a temporary directory on this machine's disk; and keys minted for the
// run, which exist only in memory.
type Broker struct {
	srv  *server.Server
	keys map[string]nkeys.KeyPair
}

// StartBroker starts the broker with grants (WithInbox is applied) and
// creates every stream in ledger.Streams under storeDir.
func StartBroker(grants []Grant, storeDir string) (*Broker, error) {
	b := &Broker{keys: map[string]nkeys.KeyPair{}}
	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  storeDir,
		NoLog:     true,
		NoSigs:    true,
	}
	// The streams are created as an identity of the broker's own, which
	// holds every subject; it is not one of the grants being measured.
	setup := Grant{Identity: setupIdentity, Publish: []string{">"}, Subscribe: []string{">"}}
	for _, g := range append(WithInbox(grants), setup) {
		kp, err := nkeys.CreateUser()
		if err != nil {
			return nil, err
		}
		pub, err := kp.PublicKey()
		if err != nil {
			return nil, err
		}
		b.keys[g.Identity] = kp
		opts.Nkeys = append(opts.Nkeys, &server.NkeyUser{
			Nkey: pub,
			Permissions: &server.Permissions{
				Publish:   &server.SubjectPermission{Allow: g.Publish},
				Subscribe: &server.SubjectPermission{Allow: g.Subscribe},
			},
		})
	}
	srv, err := server.NewServer(opts)
	if err != nil {
		return nil, err
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		srv.Shutdown()
		return nil, fmt.Errorf("broker did not start")
	}
	b.srv = srv
	if err := b.createStreams(); err != nil {
		b.Close()
		return nil, err
	}
	return b, nil
}

// setupIdentity is the broker's own identity, which creates the streams.
const setupIdentity = "broker-setup"

func (b *Broker) createStreams() error {
	nc, err := b.Connect(setupIdentity)
	if err != nil {
		return err
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, s := range ledger.Streams {
		if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
			Name:              s.Name,
			Subjects:          []string{s.Subject},
			Storage:           jetstream.FileStorage,
			Replicas:          1,
			MaxMsgsPerSubject: s.MaxMsgsPerSubject,
			MaxAge:            s.MaxAge,
			Duplicates:        2 * time.Minute,
		}); err != nil {
			return fmt.Errorf("stream %s: %w", s.Name, err)
		}
	}
	return nil
}

// URL is the broker's client URL.
func (b *Broker) URL() string { return b.srv.ClientURL() }

// Connect opens a connection as identity. Asynchronous errors (a refused
// publish arrives as one) go to onErr when it is given.
func (b *Broker) Connect(identity string, onErr ...func(error)) (*nats.Conn, error) {
	kp, ok := b.keys[identity]
	if !ok {
		return nil, fmt.Errorf("no identity %q", identity)
	}
	pub, err := kp.PublicKey()
	if err != nil {
		return nil, err
	}
	opts := []nats.Option{nats.Nkey(pub, kp.Sign), nats.Name("bench-" + identity)}
	if len(onErr) > 0 && onErr[0] != nil {
		h := onErr[0]
		opts = append(opts, nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) { h(err) }))
	}
	return nats.Connect(b.URL(), opts...)
}

// Close stops the broker.
func (b *Broker) Close() {
	if b.srv != nil {
		b.srv.Shutdown()
		b.srv.WaitForShutdown()
	}
}
