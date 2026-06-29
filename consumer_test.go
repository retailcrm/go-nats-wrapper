package natswrapper

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/retailcrm/go-nats-wrapper/natstest"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestPullConsumerNack(t *testing.T) {
	metadataErr := errors.New("metadata unavailable")
	publishErr := errors.New("dlq unavailable")
	dataProvider := []struct {
		name      string
		setup     func(*pullConsumer, *natstest.MockMessage, *natstest.MockStreamPublisher) jetstream.Msg
		wantError error
	}{
		{
			name: "nacks message with delay when delivery limit has not been reached",
			setup: func(_ *pullConsumer, message *natstest.MockMessage, _ *natstest.MockStreamPublisher) jetstream.Msg {
				message.On("Metadata").Return(&jetstream.MsgMetadata{NumDelivered: 2}, nil).Once()
				message.On("NakWithDelay", time.Minute).Return(nil).Once()

				return message
			},
		},
		{
			name: "publishes message to dlq and acknowledges it when delivery limit has been reached",
			setup: func(_ *pullConsumer, message *natstest.MockMessage, publisher *natstest.MockStreamPublisher) jetstream.Msg {
				payload := []byte(`{"id":1}`)
				message.On("Metadata").Return(&jetstream.MsgMetadata{NumDelivered: 3}, nil).Once()
				message.On("Data").Return(payload).Twice()
				message.On("Ack").Return(nil).Once()
				publisher.On("Publish", mock.Anything, "events.dlq", payload).Return(&jetstream.PubAck{
					Stream:   "test",
					Sequence: 1,
				}, nil).Once()

				return message
			},
		},
		{
			name: "nacks message with delay when metadata is unavailable",
			setup: func(_ *pullConsumer, message *natstest.MockMessage, _ *natstest.MockStreamPublisher) jetstream.Msg {
				message.On("Metadata").Return((*jetstream.MsgMetadata)(nil), metadataErr).Once()
				message.On("NakWithDelay", time.Minute).Return(nil).Once()

				return message
			},
		},
		{
			name: "does not acknowledge message when dlq publish fails",
			setup: func(_ *pullConsumer, message *natstest.MockMessage, publisher *natstest.MockStreamPublisher) jetstream.Msg {
				payload := []byte(`{"id":1}`)
				message.On("Metadata").Return(&jetstream.MsgMetadata{NumDelivered: 3}, nil).Once()
				message.On("Data").Return(payload).Twice()
				publisher.On("Publish", mock.Anything, "events.dlq", payload).Return((*jetstream.PubAck)(nil), publishErr).Once()

				return message
			},
			wantError: publishErr,
		},
		{
			name: "acknowledges message when dlq is disabled",
			setup: func(consumer *pullConsumer, message *natstest.MockMessage, _ *natstest.MockStreamPublisher) jetstream.Msg {
				consumer.cfg.DLQSubject = ""
				consumer.publisher = nil
				message.On("Metadata").Return(&jetstream.MsgMetadata{NumDelivered: 3}, nil).Once()
				message.On("Data").Return([]byte(nil)).Once()
				message.On("Ack").Return(nil).Once()

				return message
			},
		},
		{
			name: "returns error when message is missing",
			setup: func(_ *pullConsumer, _ *natstest.MockMessage, _ *natstest.MockStreamPublisher) jetstream.Msg {
				return nil
			},
			wantError: ErrMessageRequired,
		},
	}

	for _, testCase := range dataProvider {
		t.Run(testCase.name, func(t *testing.T) {
			message := &natstest.MockMessage{}
			publisher := &natstest.MockStreamPublisher{}
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
			msg := testCase.setup(consumer, message, publisher)

			err := consumer.Nack(context.Background(), msg)

			if testCase.wantError != nil {
				require.ErrorIs(t, err, testCase.wantError)
			} else {
				require.NoError(t, err)
			}

			message.AssertExpectations(t)
			publisher.AssertExpectations(t)
		})
	}
}
