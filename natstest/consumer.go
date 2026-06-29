package natstest

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/mock"
)

type MockPullConsumer struct {
	mock.Mock
}

func (c *MockPullConsumer) NextMessage(ctx context.Context) (jetstream.Msg, error) {
	args := c.Called(ctx)
	message, _ := args.Get(0).(jetstream.Msg)

	return message, args.Error(1)
}

func (c *MockPullConsumer) Ack(ctx context.Context, message jetstream.Msg) error {
	args := c.Called(ctx, message)

	return args.Error(0)
}

func (c *MockPullConsumer) Nack(ctx context.Context, message jetstream.Msg) error {
	args := c.Called(ctx, message)

	return args.Error(0)
}

func (c *MockPullConsumer) Close() error {
	args := c.Called()

	return args.Error(0)
}
