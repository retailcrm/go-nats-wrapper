package natswrapper

import (
	"context"
	"testing"

	natsdriver "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

func TestStreamPublisherPublish(t *testing.T) {
	publisher := &streamPublisher{}

	_, err := publisher.Publish(context.Background(), "", nil)

	require.ErrorIs(t, err, ErrSubjectRequired)
}

func TestStreamPublisherPublishMsg(t *testing.T) {
	dataProvider := []struct {
		name      string
		message   *natsdriver.Msg
		wantError error
	}{
		{
			name:      "returns error when message is missing",
			wantError: ErrMessageRequired,
		},
		{
			name:      "returns error when subject is missing",
			message:   &natsdriver.Msg{},
			wantError: ErrSubjectRequired,
		},
	}

	for _, testCase := range dataProvider {
		t.Run(testCase.name, func(t *testing.T) {
			publisher := &streamPublisher{}

			_, err := publisher.PublishMsg(context.Background(), testCase.message)

			require.ErrorIs(t, err, testCase.wantError)
		})
	}
}
