package natswrapper

import (
	"context"
	"errors"

	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

var errMessageRequired = errors.New("nats message is required")

type PullConsumer interface {
	NextMessage(ctx context.Context) (jetstream.Msg, error)
	Ack(ctx context.Context, message jetstream.Msg) error
	Nack(ctx context.Context, message jetstream.Msg) error
	Close() error
}

type pullConsumer struct {
	cfg        PullConsumerConfig
	connection *jetStreamConnection
	consumer   jetstream.Consumer
	messages   jetstream.MessagesContext
	publisher  StreamPublisher
	logger     *zap.Logger
}

func NewPullConsumer(cfg PullConsumerConfig, logger *zap.Logger) (PullConsumer, error) {
	ctx := context.Background()

	connection, err := newJetStreamConnection(ctx, cfg.JetStream, logger)
	if err != nil {
		return nil, err
	}

	requestCtx, cancel := connection.operationContext(ctx)
	defer cancel()

	consumer, err := connection.js.Consumer(requestCtx, cfg.Stream, cfg.Consumer)
	if err != nil {
		_ = connection.Close()

		return nil, err
	}

	if logger == nil {
		logger = zap.NewNop()
	}

	return &pullConsumer{
		cfg:        cfg,
		connection: connection,
		consumer:   consumer,
		publisher:  &streamPublisher{connection: connection},
		logger:     logger,
	}, nil
}

func (c *pullConsumer) NextMessage(ctx context.Context) (jetstream.Msg, error) {
	if c.messages == nil {
		messages, err := c.consumer.Messages()
		if err != nil {
			return nil, err
		}

		c.messages = messages
	}

	return c.messages.Next(jetstream.NextContext(ctx))
}

func (c *pullConsumer) Ack(_ context.Context, message jetstream.Msg) error {
	if message == nil {
		return errMessageRequired
	}

	return message.Ack()
}

func (c *pullConsumer) Nack(ctx context.Context, message jetstream.Msg) error {
	if message == nil {
		return errMessageRequired
	}

	if meta, err := message.Metadata(); err == nil && meta != nil {
		if meta.NumDelivered >= uint64(c.cfg.MaxDeliver) {
			if err = c.publishToDLQ(ctx, message); err != nil {
				return err
			}

			c.logger.Warn(
				"acknowledging a message after publishing it to dlq",
				zap.Uint64("num_delivered", meta.NumDelivered),
				zap.Int("max_deliver", c.cfg.MaxDeliver),
				zap.String("message", string(message.Data())),
			)

			return message.Ack()
		}
	}

	return message.NakWithDelay(c.cfg.NakDelay)
}

func (c *pullConsumer) publishToDLQ(ctx context.Context, message jetstream.Msg) error {
	_, err := c.publisher.Publish(ctx, c.cfg.DLQSubject, message.Data())

	return err
}

func (c *pullConsumer) Close() error {
	if c.messages != nil {
		c.messages.Stop()
	}

	return c.connection.Close()
}
