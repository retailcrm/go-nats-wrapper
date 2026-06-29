package natstest

import (
	"context"

	natsdriver "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/mock"
)

type MockStreamPublisher struct {
	mock.Mock
}

func (p *MockStreamPublisher) Publish(
	ctx context.Context,
	subject string,
	payload []byte,
	opts ...jetstream.PublishOpt,
) (*jetstream.PubAck, error) {
	args := p.Called(append([]any{ctx, subject, payload}, publishOpts(opts)...)...)
	result, _ := args.Get(0).(*jetstream.PubAck)

	return result, args.Error(1)
}

func (p *MockStreamPublisher) PublishMsg(
	ctx context.Context,
	message *natsdriver.Msg,
	opts ...jetstream.PublishOpt,
) (*jetstream.PubAck, error) {
	args := p.Called(append([]any{ctx, message}, publishOpts(opts)...)...)
	result, _ := args.Get(0).(*jetstream.PubAck)

	return result, args.Error(1)
}

func (p *MockStreamPublisher) Close() error {
	args := p.Called()

	return args.Error(0)
}

func publishOpts(opts []jetstream.PublishOpt) []any {
	args := make([]any, len(opts))
	for i, opt := range opts {
		args[i] = opt
	}

	return args
}
