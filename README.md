# go-nats-wrapper

Reusable JetStream helpers for NATS.

## Usage

```go
connection := natswrapper.ConnectionConfig{
	Host:                "localhost",
	Port:                4222,
	Name:                "service-name",
	ConnectTimeout:      3 * time.Second,
	ReconnectAttempts:   3,
	ReconnectInterval:   time.Second,
	ReconnectBufferSize: 8 << 20,
	OnClosed: func(err error) {
		// Trigger application shutdown or error reporting.
	},
}

jetStream := natswrapper.JetStreamConfig{
	Connection:       connection,
	OperationTimeout: 5 * time.Second,
}

provisioner, err := natswrapper.NewProvisioner(natswrapper.ProvisionerConfig{
	JetStream: jetStream,
	Streams: []natswrapper.StreamProvisionConfig{
		{
			Stream: natswrapper.StreamConfig{
				Name:     "events_stream",
				Subjects: []string{"events.*"},
			},
			DLQ: &natswrapper.StreamConfig{
				Name:     "events_dlq",
				Subjects: []string{"events.dlq"},
			},
			Consumers: []natswrapper.ConsumerProvisionConfig{
				{
					Durable:    "events_consumer",
					MaxDeliver: 3,
					AckWait:    10 * time.Second,
				},
			},
		},
		{
			Stream: natswrapper.StreamConfig{
				Name:     "notifications_stream",
				Subjects: []string{"notifications.*"},
			},
			DLQ: &natswrapper.StreamConfig{
				Name:     "notifications_dlq",
				Subjects: []string{"notifications.dlq"},
			},
			Consumers: []natswrapper.ConsumerProvisionConfig{
				{
					Durable:    "notifications_consumer",
					MaxDeliver: 5,
					AckWait:    30 * time.Second,
				},
			},
		},
	},
}, logger)
if err != nil {
	return err
}
defer provisioner.Close()

if err := provisioner.Provision(ctx); err != nil {
	return err
}

publisher, err := natswrapper.NewStreamPublisher(natswrapper.StreamPublisherConfig{
	JetStream: jetStream,
}, logger)
if err != nil {
	return err
}
defer publisher.Close()

_, err = publisher.Publish(ctx, "events.created", payload)

consumer, err := natswrapper.NewPullConsumer(natswrapper.PullConsumerConfig{
	JetStream:  jetStream,
	Stream:     "events_stream",
	Consumer:   "events_consumer",
	DLQSubject: "events.dlq",
	NakDelay:   time.Minute,
	MaxDeliver: 3,
}, logger)
```
