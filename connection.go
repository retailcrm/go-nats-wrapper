package natswrapper

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	natsdriver "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

type jetStreamConnection struct {
	cfg     *Config
	nc      *natsdriver.Conn
	js      jetstream.JetStream
	closing *atomic.Bool
}

func newJetStreamConnection(
	ctx context.Context,
	cfg *Config,
	logger *zap.Logger,
) (*jetStreamConnection, error) {
	closing := &atomic.Bool{}

	nc, err := connect(ctx, cfg, logger, closing)
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()

		return nil, err
	}

	return &jetStreamConnection{
		cfg:     cfg,
		nc:      nc,
		js:      js,
		closing: closing,
	}, nil
}

func (c *jetStreamConnection) operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}

	return context.WithTimeout(ctx, c.cfg.OperationTimeout)
}

func (c *jetStreamConnection) Close() error {
	if c.nc == nil {
		return nil
	}

	c.closing.Store(true)

	if err := c.nc.Drain(); err != nil {
		if errors.Is(err, natsdriver.ErrConnectionClosed) {
			return nil
		}

		return err
	}

	return nil
}

func connect(
	ctx context.Context,
	cfg *Config,
	logger *zap.Logger,
	closing *atomic.Bool,
) (*natsdriver.Conn, error) {
	connected := make(chan struct{})
	closed := make(chan struct{})
	ready := &atomic.Bool{}
	signalConnected := closeOnce(connected)
	signalClosed := closeOnce(closed)

	nc, err := natsdriver.Connect(
		serverURL(cfg),
		connectOptions(cfg, logger, signalConnected, signalClosed, ready, closing)...,
	)
	if err != nil {
		return nil, err
	}

	if err = waitConnected(ctx, nc, connected, closed); err != nil {
		nc.Close()

		return nil, err
	}

	ready.Store(true)

	return nc, nil
}

func serverURL(cfg *Config) string {
	return fmt.Sprintf("nats://%s:%d", cfg.Host, cfg.Port)
}

func connectOptions(
	cfg *Config,
	logger *zap.Logger,
	signalConnected func(),
	signalClosed func(),
	ready *atomic.Bool,
	closing *atomic.Bool,
) []natsdriver.Option {
	if logger == nil {
		logger = zap.NewNop()
	}

	options := []natsdriver.Option{
		natsdriver.Name(cfg.Name),
		natsdriver.Timeout(cfg.ConnectTimeout),
		natsdriver.RetryOnFailedConnect(true),
		natsdriver.MaxReconnects(cfg.ReconnectAttempts),
		natsdriver.ReconnectWait(cfg.ReconnectInterval),
		natsdriver.ConnectHandler(func(_ *natsdriver.Conn) {
			signalConnected()
		}),
		natsdriver.DisconnectErrHandler(func(_ *natsdriver.Conn, err error) {
			if err != nil {
				logger.Warn("disconnected from nats", zap.Error(err))
			}
		}),
		natsdriver.ReconnectErrHandler(func(_ *natsdriver.Conn, err error) {
			logger.Warn("failed to reconnect to nats", zap.Error(err))
		}),
		natsdriver.ReconnectHandler(func(conn *natsdriver.Conn) {
			logger.Info("reconnected to nats", zap.String("url", conn.ConnectedUrl()))
		}),
		natsdriver.ClosedHandler(func(conn *natsdriver.Conn) {
			signalClosed()
			err := conn.LastError()
			logger.Warn("nats connection closed", zap.Error(err))

			if cfg.OnClosed == nil || !ready.Load() || closing.Load() {
				return
			}
			if err == nil {
				err = natsdriver.ErrConnectionClosed
			}

			cfg.OnClosed(fmt.Errorf("nats connection closed: %w", err))
		}),
		natsdriver.ReconnectBufSize(cfg.ReconnectBufferSize),
	}

	if cfg.User != "" || cfg.Password != "" {
		options = append(options, natsdriver.UserInfo(cfg.User, cfg.Password))
	}

	if cfg.TLSConfigCa != "" {
		options = append(options, natsdriver.RootCAs(cfg.TLSConfigCa))
	}

	if cfg.TLSClientCert != "" {
		options = append(options, natsdriver.ClientCert(cfg.TLSClientCert, cfg.TLSClientKey))
	}

	return options
}

func waitConnected(
	ctx context.Context,
	nc *natsdriver.Conn,
	connected <-chan struct{},
	closed <-chan struct{},
) error {
	if nc.IsConnected() {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	for {
		if nc.IsConnected() {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-connected:
			return nil
		case <-closed:
			if err := nc.LastError(); err != nil {
				return err
			}

			return natsdriver.ErrConnectionClosed
		}
	}
}

func closeOnce(ch chan struct{}) func() {
	once := sync.Once{}

	return func() {
		once.Do(func() {
			close(ch)
		})
	}
}
