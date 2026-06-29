package natstest

import (
	"context"
	"time"

	natsdriver "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/mock"
)

type MockMessage struct {
	mock.Mock
}

func (m *MockMessage) Metadata() (*jetstream.MsgMetadata, error) {
	args := m.Called()
	meta, _ := args.Get(0).(*jetstream.MsgMetadata)

	return meta, args.Error(1)
}

func (m *MockMessage) Data() []byte {
	args := m.Called()

	return args.Get(0).([]byte)
}

func (m *MockMessage) Headers() natsdriver.Header {
	args := m.Called()
	headers, _ := args.Get(0).(natsdriver.Header)

	return headers
}

func (m *MockMessage) Subject() string {
	args := m.Called()

	return args.String(0)
}

func (m *MockMessage) Reply() string {
	args := m.Called()

	return args.String(0)
}

func (m *MockMessage) Ack() error {
	args := m.Called()

	return args.Error(0)
}

func (m *MockMessage) DoubleAck(ctx context.Context) error {
	args := m.Called(ctx)

	return args.Error(0)
}

func (m *MockMessage) Nak() error {
	args := m.Called()

	return args.Error(0)
}

func (m *MockMessage) NakWithDelay(delay time.Duration) error {
	args := m.Called(delay)

	return args.Error(0)
}

func (m *MockMessage) InProgress() error {
	args := m.Called()

	return args.Error(0)
}

func (m *MockMessage) Term() error {
	args := m.Called()

	return args.Error(0)
}

func (m *MockMessage) TermWithReason(reason string) error {
	args := m.Called(reason)

	return args.Error(0)
}
