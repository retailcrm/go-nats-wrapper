package natswrapper

import (
	"context"
	"errors"
	"testing"
	"time"

	natsdriver "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestPullConsumerNack(t *testing.T) {
	dataProvider := []struct {
		name         string
		message      *testNATSMessage
		wantAcked    bool
		wantNacked   bool
		wantDelay    time.Duration
		wantDLQ      bool
		wantError    string
		wantMetadata bool
	}{
		{
			name:         "nacks message with delay when delivery limit has not been reached",
			message:      newTestNATSMessage(nil, &jetstream.MsgMetadata{NumDelivered: 2}),
			wantNacked:   true,
			wantDelay:    time.Minute,
			wantMetadata: true,
		},
		{
			name:         "publishes message to dlq and acknowledges it when delivery limit has been reached",
			message:      newTestNATSMessage([]byte(`{"id":1}`), &jetstream.MsgMetadata{NumDelivered: 3}),
			wantAcked:    true,
			wantDLQ:      true,
			wantMetadata: true,
		},
		{
			name:         "nacks message with delay when metadata is unavailable",
			message:      newTestNATSMessageWithMetadataError(nil, errors.New("metadata unavailable")),
			wantNacked:   true,
			wantDelay:    time.Minute,
			wantMetadata: true,
		},
	}

	for _, testCase := range dataProvider {
		t.Run(testCase.name, func(t *testing.T) {
			publisher := &testDLQPublisher{}
			consumer := &pullConsumer{
				cfg: PullConsumerConfig{
					Stream:     "events_stream",
					DLQSubject: "events.dlq",
					NakDelay:   time.Minute,
					MaxDeliver: 3,
				},
				publisher: publisher,
				logger:    zap.NewNop(),
			}

			err := consumer.Nack(context.Background(), testCase.message)

			if testCase.wantError != "" {
				require.EqualError(t, err, testCase.wantError)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, testCase.wantAcked, testCase.message.acked)
			assert.Equal(t, testCase.wantNacked, testCase.message.nacked)
			assert.Equal(t, testCase.wantDelay, testCase.message.nakDelay)
			assert.Equal(t, testCase.wantMetadata, testCase.message.metadataCalled)

			if testCase.wantDLQ {
				require.NotNil(t, publisher.message)
				assert.Equal(t, "events.dlq", publisher.subject)
				assert.Equal(t, []byte(`{"id":1}`), publisher.message)
			} else {
				assert.Nil(t, publisher.message)
			}
		})
	}
}

func TestPullConsumerNackDoesNotAcknowledgeMessageWhenDLQPublishFails(t *testing.T) {
	publishErr := errors.New("dlq unavailable")
	publisher := &testDLQPublisher{err: publishErr}
	consumer := &pullConsumer{
		cfg: PullConsumerConfig{
			Stream:     "events_stream",
			DLQSubject: "events.dlq",
			NakDelay:   time.Minute,
			MaxDeliver: 3,
		},
		publisher: publisher,
		logger:    zap.NewNop(),
	}
	message := newTestNATSMessage(nil, &jetstream.MsgMetadata{NumDelivered: 3})

	err := consumer.Nack(context.Background(), message)

	require.ErrorIs(t, err, publishErr)
	assert.False(t, message.acked)
	assert.False(t, message.nacked)
}

func TestPullConsumerNackAcknowledgesMessageWhenDLQIsDisabled(t *testing.T) {
	consumer := &pullConsumer{
		cfg: PullConsumerConfig{
			NakDelay:   time.Minute,
			MaxDeliver: 3,
		},
		logger: zap.NewNop(),
	}
	message := newTestNATSMessage(nil, &jetstream.MsgMetadata{NumDelivered: 3})

	err := consumer.Nack(context.Background(), message)

	require.NoError(t, err)
	assert.True(t, message.acked)
	assert.False(t, message.nacked)
}

func TestPullConsumerNackReturnsErrorWhenMessageIsMissing(t *testing.T) {
	consumer := &pullConsumer{}

	err := consumer.Nack(context.Background(), nil)

	require.ErrorIs(t, err, errMessageRequired)
}

type testNATSMessage struct {
	metadata       *jetstream.MsgMetadata
	metadataErr    error
	data           []byte
	acked          bool
	nacked         bool
	metadataCalled bool
	nakDelay       time.Duration
}

func newTestNATSMessage(data []byte, metadata *jetstream.MsgMetadata) *testNATSMessage {
	return &testNATSMessage{
		data:     data,
		metadata: metadata,
	}
}

func newTestNATSMessageWithMetadataError(data []byte, metadataErr error) *testNATSMessage {
	return &testNATSMessage{
		data:        data,
		metadataErr: metadataErr,
	}
}

func (m *testNATSMessage) Metadata() (*jetstream.MsgMetadata, error) {
	m.metadataCalled = true

	return m.metadata, m.metadataErr
}

func (m *testNATSMessage) Data() []byte {
	return m.data
}

func (m *testNATSMessage) Headers() natsdriver.Header {
	return nil
}

func (m *testNATSMessage) Subject() string {
	return ""
}

func (m *testNATSMessage) Reply() string {
	return ""
}

func (m *testNATSMessage) Ack() error {
	m.acked = true

	return nil
}

func (m *testNATSMessage) DoubleAck(context.Context) error {
	m.acked = true

	return nil
}

func (m *testNATSMessage) Nak() error {
	m.nacked = true

	return nil
}

func (m *testNATSMessage) NakWithDelay(delay time.Duration) error {
	m.nacked = true
	m.nakDelay = delay

	return nil
}

func (m *testNATSMessage) InProgress() error {
	return nil
}

func (m *testNATSMessage) Term() error {
	return nil
}

func (m *testNATSMessage) TermWithReason(string) error {
	return nil
}

type testDLQPublisher struct {
	subject string
	message []byte
	err     error
}

func (p *testDLQPublisher) Publish(
	_ context.Context,
	subject string,
	payload []byte,
	_ ...jetstream.PublishOpt,
) (*PublishResult, error) {
	p.subject = subject
	p.message = append([]byte(nil), payload...)

	return &PublishResult{
		Stream:   "test",
		Sequence: 1,
	}, p.err
}

func (p *testDLQPublisher) Close() error {
	return nil
}
