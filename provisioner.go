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
	cfg        ProvisionerConfig
	connection *jetStreamConnection
}

func NewProvisioner(cfg ProvisionerConfig, logger *zap.Logger) (Provisioner, error) {
	ctx := context.Background()

	connection, err := newJetStreamConnection(ctx, cfg.JetStream, logger)
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

	for _, stream := range p.cfg.Streams {
		if err := ensureStream(requestCtx, p.connection.js, stream.Stream); err != nil {
			return err
		}

		if stream.DLQ != nil {
			if err := ensureStream(requestCtx, p.connection.js, *stream.DLQ); err != nil {
				return err
			}
		}

		for _, consumer := range stream.Consumers {
			if _, err := ensurePullConsumer(requestCtx, p.connection.js, stream.Stream.Name, consumer); err != nil {
				return err
			}
		}
	}

	return nil
}

func (p *provisioner) Close() error {
	return p.connection.Close()
}

func ensureStream(ctx context.Context, js jetstream.JetStream, cfg StreamConfig) error {
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      cfg.Name,
		Subjects:  cfg.Subjects,
		Retention: jetstream.WorkQueuePolicy,
	})

	return err
}

func ensurePullConsumer(
	ctx context.Context,
	js jetstream.JetStream,
	stream string,
	cfg ConsumerProvisionConfig,
) (jetstream.Consumer, error) {
	return js.CreateOrUpdateConsumer(ctx, stream, jetstream.ConsumerConfig{
		Durable:    cfg.Durable,
		AckPolicy:  jetstream.AckExplicitPolicy,
		MaxDeliver: cfg.MaxDeliver,
		AckWait:    cfg.AckWait,
	})
}
