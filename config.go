package natswrapper

import "time"

type ConnectionConfig struct {
	Host                string
	Port                int
	Name                string
	User                string
	Password            string
	TLSConfigCa         string
	TLSClientCert       string
	TLSClientKey        string
	ConnectTimeout      time.Duration
	ReconnectAttempts   int
	ReconnectInterval   time.Duration
	ReconnectBufferSize int
	OnClosed            func(error)
}

type JetStreamConfig struct {
	Connection       ConnectionConfig
	OperationTimeout time.Duration
}

type StreamConfig struct {
	Name     string
	Subjects []string
}

type PullConsumerConfig struct {
	JetStream  JetStreamConfig
	Stream     string
	Consumer   string
	DLQSubject string
	NakDelay   time.Duration
	MaxDeliver int
}

type StreamPublisherConfig struct {
	JetStream JetStreamConfig
}

type ProvisionerConfig struct {
	JetStream JetStreamConfig
	Streams   []StreamProvisionConfig
}

type StreamProvisionConfig struct {
	Stream    StreamConfig
	DLQ       *StreamConfig
	Consumers []ConsumerProvisionConfig
}

type ConsumerProvisionConfig struct {
	Durable    string
	MaxDeliver int
	AckWait    time.Duration
}
