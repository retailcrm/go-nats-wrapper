package natswrapper

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

type Provisioner interface {
	Provision(ctx context.Context) error
	Close() error
}

type provisioner struct {
	cfg        *Config
	connection *jetStreamConnection
}

func NewProvisioner(cfg *Config, logger *zap.Logger) (Provisioner, error) {
	ctx := context.Background()

	connection, err := newJetStreamConnection(ctx, cfg, logger)
	if err != nil {
		return nil, err
	}

	return &provisioner{
		cfg:        cfg,
		connection: connection,
	}, nil
}

func (p *provisioner) Provision(ctx context.Context) error {
	requestCtx, cancel := p.connection.operationContext(ctx)
	defer cancel()

	if err := ensureStream(requestCtx, p.connection.js, p.cfg); err != nil {
		return err
	}

	if err := ensureDLQStream(requestCtx, p.connection.js, p.cfg); err != nil {
		return err
	}

	_, err := ensurePullConsumer(requestCtx, p.connection.js, p.cfg)

	return err
}

func (p *provisioner) Close() error {
	return p.connection.Close()
}

func ensureStream(ctx context.Context, js jetstream.JetStream, cfg *Config) error {
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      cfg.Stream,
		Subjects:  []string{cfg.Subject},
		Retention: jetstream.WorkQueuePolicy,
	})

	return err
}

func ensureDLQStream(ctx context.Context, js jetstream.JetStream, cfg *Config) error {
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      cfg.DLQStream,
		Subjects:  []string{cfg.DLQSubject},
		Retention: jetstream.WorkQueuePolicy,
	})

	return err
}

func ensurePullConsumer(ctx context.Context, js jetstream.JetStream, cfg *Config) (jetstream.Consumer, error) {
	return js.CreateOrUpdateConsumer(ctx, cfg.Stream, jetstream.ConsumerConfig{
		Durable:    cfg.consumerName(),
		AckPolicy:  jetstream.AckExplicitPolicy,
		MaxDeliver: cfg.MaxDeliver,
		AckWait:    cfg.ackWait(),
	})
}
