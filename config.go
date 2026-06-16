package natswrapper

import "time"

const defaultAckWait = 10 * time.Second

type Config struct {
	Host                string
	Port                int
	Name                string
	User                string
	Password            string
	TLSConfigCa         string
	TLSClientCert       string
	TLSClientKey        string
	Stream              string
	Subject             string
	Consumer            string
	DLQStream           string
	DLQSubject          string
	ConnectTimeout      time.Duration
	OperationTimeout    time.Duration
	ReconnectAttempts   int
	ReconnectInterval   time.Duration
	ReconnectBufferSize int
	NakDelay            time.Duration
	MaxDeliver          int
	AckWait             time.Duration
	OnClosed            func(error)
}

func (c *Config) consumerName() string {
	if c.Consumer != "" {
		return c.Consumer
	}

	return c.Stream
}

func (c *Config) ackWait() time.Duration {
	if c.AckWait > 0 {
		return c.AckWait
	}

	return defaultAckWait
}
