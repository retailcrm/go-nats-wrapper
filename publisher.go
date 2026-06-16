package natswrapper

import (
	"context"
	"errors"

	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

var errSubjectRequired = errors.New("nats subject is required")

type StreamPublisher interface {
	Publish(ctx context.Context, subject string, payload []byte, opts ...jetstream.PublishOpt) (*PublishResult, error)
	Close() error
}

type PublishResult struct {
	Stream    string
	Sequence  uint64
	Duplicate bool
}

type streamPublisher struct {
	connection *jetStreamConnection
}

func NewStreamPublisher(cfg StreamPublisherConfig, logger *zap.Logger) (StreamPublisher, error) {
	ctx := context.Background()

	connection, err := newJetStreamConnection(ctx, cfg.JetStream, logger)
	if err != nil {
		return nil, err
	}

	return &streamPublisher{
		connection: connection,
	}, nil
}

func (p *streamPublisher) Publish(
	ctx context.Context,
	subject string,
	payload []byte,
	opts ...jetstream.PublishOpt,
) (*PublishResult, error) {
	if subject == "" {
		return nil, errSubjectRequired
	}

	requestCtx, cancel := p.connection.operationContext(ctx)
	defer cancel()

	ack, err := p.connection.js.Publish(requestCtx, subject, payload, opts...)
	if err != nil {
		return nil, err
	}

	return &PublishResult{
		Stream:    ack.Stream,
		Sequence:  ack.Sequence,
		Duplicate: ack.Duplicate,
	}, nil
}

func (p *streamPublisher) Close() error {
	return p.connection.Close()
}
