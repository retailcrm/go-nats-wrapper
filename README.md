# go-nats-wrapper

Reusable helpers for NATS JetStream.

## Main Usage Scenarios

The wrapper is intended for services that use NATS JetStream as a reliable
background task queue. A typical scenario: a client application publishes a task
to a stream, and a consumer reads it from the queue and passes it further - to an
external service, business logic handler, or another asynchronous process.

### Queue Provisioning

`Provisioner` creates or updates JetStream resources from the service
configuration:

* work queue stream with the specified subjects;
* durable pull consumer for workers;
* separate DLQ stream if the queue needs a dead letter queue.

This allows queue definitions to be kept in the application config and NATS
resources to be provisioned with a separate command before starting the API,
producers, and workers.

### Publishing Tasks to a Stream

`StreamPublisher` publishes a payload to a subject and returns a JetStream ack:
stream, sequence, and the duplicate flag. This scenario fits APIs or dispatchers
that need to quickly persist a task in the queue and avoid doing long-running
work synchronously with the request.

Publisher can be used to:

* put an incoming command or event into the subject of the required queue;
* split one incoming task into several internal tasks for different handlers;
* hand off work from a synchronous process to background worker processes.

If the message needs NATS headers, use `PublishMsg`. JetStream publish options
can still be passed together with custom headers.

Tests in downstream services can use mocks from
`github.com/retailcrm/go-nats-wrapper/natstest`: `MockStreamPublisher`,
`MockPullConsumer`, and `MockMessage`.

### Pull-Based Task Processing by Workers

`PullConsumer` hides message retrieval from a durable consumer and gives the
worker three basic operations:

* `NextMessage` - receive the next task;
* `Ack` - acknowledge successful processing;
* `Nack` - return the task to the queue with a `NakDelay` delay.

This scenario is designed for independent worker processes that can be scaled
horizontally. Multiple instances read from one durable consumer, and JetStream
distributes tasks between them.

### Retry and DLQ

On `Nack`, the wrapper checks the message metadata. While the delivery count is
less than `MaxDeliver`, the message is returned to the queue with
`NakWithDelay`. When the delivery limit is reached, the message:

* is published to `DLQSubject`, if it is configured;
* is acknowledged with `Ack` so it does not loop in the main queue.

If DLQ is not configured, the message is simply acknowledged after reaching
`MaxDeliver`. If publishing to DLQ fails, the original message is not acked so
that task loss is not hidden.

### Connection Management

All components use the same JetStream connection configuration: client name,
connection timeout, reconnect policy, reconnect buffer size, user/password, and
TLS files. For long-running workers, `OnClosed` can be provided: the callback is
called after an established connection is closed if reconnect did not help. In
the application, this is convenient to use as a fatal signal for a supervisor or
error observer.

## Usage Examples

```go
connection := natswrapper.ConnectionConfig{
	Host:                "queue",
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
	OperationTimeout: 15 * time.Second,
}

provisioner, err := natswrapper.NewProvisioner(natswrapper.ProvisionerConfig{
	JetStream: jetStream,
	Streams: []natswrapper.StreamProvisionConfig{
		{
			Stream: natswrapper.StreamConfig{
				Name:     "SERVICE_NAME_TASKS",
				Subjects: []string{"service-name.tasks"},
			},
			DLQ: &natswrapper.StreamConfig{
				Name:     "SERVICE_NAME_TASKS_DLQ",
				Subjects: []string{"service-name.tasks.dlq"},
			},
			Consumers: []natswrapper.ConsumerProvisionConfig{
				{
					Durable:    "SERVICE_NAME_TASKS",
					MaxDeliver: 3,
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

_, err = publisher.Publish(ctx, "service-name.tasks", payload)

_, err = publisher.PublishMsg(
	ctx,
	&nats.Msg{
		Subject: "service-name.tasks",
		Data:    payload,
		Header: nats.Header{
			"X-Request-Id": []string{requestID},
		},
	},
)

consumer, err := natswrapper.NewPullConsumer(natswrapper.PullConsumerConfig{
	JetStream:  jetStream,
	Stream:     "SERVICE_NAME_TASKS",
	Consumer:   "SERVICE_NAME_TASKS",
	DLQSubject: "service-name.tasks.dlq",
	NakDelay:   30 * time.Second,
	MaxDeliver: 3,
}, logger)
```
