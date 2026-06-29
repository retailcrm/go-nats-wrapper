package natswrapper

import (
	"context"

	natsdriver "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

type StreamPublisher interface {
	Publish(ctx context.Context, subject string, payload []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error)
	PublishMsg(ctx context.Context, message *natsdriver.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error)
	Close() error
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
) (*jetstream.PubAck, error) {
	if subject == "" {
		return nil, ErrSubjectRequired
	}

	requestCtx, cancel := p.connection.operationContext(ctx)
	defer cancel()

	ack, err := p.connection.js.Publish(requestCtx, subject, payload, opts...)
	if err != nil {
		return nil, err
	}

	return ack, nil
}

func (p *streamPublisher) PublishMsg(
	ctx context.Context,
	message *natsdriver.Msg,
	opts ...jetstream.PublishOpt,
) (*jetstream.PubAck, error) {
	if message == nil {
		return nil, ErrMessageRequired
	}
	if message.Subject == "" {
		return nil, ErrSubjectRequired
	}

	requestCtx, cancel := p.connection.operationContext(ctx)
	defer cancel()

	ack, err := p.connection.js.PublishMsg(requestCtx, message, opts...)
	if err != nil {
		return nil, err
	}

	return ack, nil
}

func (p *streamPublisher) Close() error {
	return p.connection.Close()
}
