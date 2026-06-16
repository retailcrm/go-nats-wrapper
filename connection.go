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
	cfg     JetStreamConfig
	nc      *natsdriver.Conn
	js      jetstream.JetStream
	closing *atomic.Bool
}

func newJetStreamConnection(
	ctx context.Context,
	cfg JetStreamConfig,
	logger *zap.Logger,
) (*jetStreamConnection, error) {
	closing := &atomic.Bool{}

	nc, err := connect(ctx, cfg.Connection, logger, closing)
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
	cfg ConnectionConfig,
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

func serverURL(cfg ConnectionConfig) string {
	return fmt.Sprintf("nats://%s:%d", cfg.Host, cfg.Port)
}

func connectOptions(
	cfg ConnectionConfig,
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
		// Client name visible in NATS monitoring and server logs.
		natsdriver.Name(cfg.Name),
		// Timeout for a single TCP connection attempt.
		natsdriver.Timeout(cfg.ConnectTimeout),
		// Allows the client to enter reconnecting state if the first connection fails.
		natsdriver.RetryOnFailedConnect(true),
		// Limits reconnect attempts for the initial connection and later reconnects.
		natsdriver.MaxReconnects(cfg.ReconnectAttempts),
		// Delay between reconnect attempts.
		natsdriver.ReconnectWait(cfg.ReconnectInterval),
		// Signals successful initial connection, including after RetryOnFailedConnect.
		natsdriver.ConnectHandler(func(_ *natsdriver.Conn) {
			signalConnected()
		}),
		// Logs connection loss after an established connection was interrupted.
		natsdriver.DisconnectErrHandler(func(_ *natsdriver.Conn, err error) {
			if err != nil {
				logger.Warn("disconnected from nats", zap.Error(err))
			}
		}),
		// Logs each failed reconnect attempt.
		natsdriver.ReconnectErrHandler(func(_ *natsdriver.Conn, err error) {
			logger.Warn("failed to reconnect to nats", zap.Error(err))
		}),
		// Logs successful reconnects.
		natsdriver.ReconnectHandler(func(conn *natsdriver.Conn) {
			logger.Info("reconnected to nats", zap.String("url", conn.ConnectedUrl()))
		}),
		// Logs final connection close after reconnect attempts are exhausted or Close is called.
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

			// убиваем процессы, если реконнект не принес результат
			cfg.OnClosed(fmt.Errorf("nats connection closed: %w", err))
		}),
		// Limits outbound messages buffered by the client while reconnecting.
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
