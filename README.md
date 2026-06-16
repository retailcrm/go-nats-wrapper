# go-nats-wrapper

Reusable JetStream helpers for NATS.

## Usage

```go
cfg := &natswrapper.Config{
	Host:                "localhost",
	Port:                4222,
	Name:                "service-name",
	Stream:              "events_stream",
	Subject:             "events.*",
	DLQStream:           "events_dlq",
	DLQSubject:          "events.dlq",
	ConnectTimeout:      3 * time.Second,
	OperationTimeout:    5 * time.Second,
	ReconnectAttempts:   3,
	ReconnectInterval:   time.Second,
	ReconnectBufferSize: 8 << 20,
	NakDelay:            time.Minute,
	MaxDeliver:          3,
	OnClosed: func(err error) {
		// Trigger application shutdown or error reporting.
	},
}

provisioner, err := natswrapper.NewProvisioner(cfg, logger)
if err != nil {
	return err
}
defer provisioner.Close()

if err := provisioner.Provision(ctx); err != nil {
	return err
}

publisher, err := natswrapper.NewStreamPublisher(cfg, logger)
if err != nil {
	return err
}
defer publisher.Close()

_, err = publisher.Publish(ctx, "events.created", payload)
```

`Consumer` defaults to `Stream`, and `AckWait` defaults to `10s`.
