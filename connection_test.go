package natswrapper

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	natsdriver "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestConnectOptions(t *testing.T) {
	caPath, certPath, keyPath := writeTLSFiles(t)

	dataProvider := []struct {
		name     string
		cfg      *Config
		wantUser string
		wantTLS  bool
		wantCert bool
	}{
		{
			name: "does not enable tls without configuration",
			cfg:  connectionConfig(),
		},
		{
			name: "enables authenticated mutual tls from certificate files",
			cfg: func() *Config {
				cfg := connectionConfig()
				cfg.User = "nats-user"
				cfg.Password = "nats-password"
				cfg.TLSConfigCa = caPath
				cfg.TLSClientCert = certPath
				cfg.TLSClientKey = keyPath

				return cfg
			}(),
			wantUser: "nats-user",
			wantTLS:  true,
			wantCert: true,
		},
	}

	for _, testCase := range dataProvider {
		t.Run(testCase.name, func(t *testing.T) {
			opts := natsdriver.GetDefaultOptions()

			for _, option := range connectOptions(
				testCase.cfg,
				zap.NewNop(),
				func() {},
				func() {},
				&atomic.Bool{},
				&atomic.Bool{},
			) {
				require.NoError(t, option(&opts))
			}

			assert.Equal(t, testCase.wantUser, opts.User)
			assert.Equal(t, testCase.cfg.Password, opts.Password)
			assert.Equal(t, testCase.wantTLS, opts.Secure)
			assert.Equal(t, testCase.wantTLS, opts.RootCAsCB != nil)
			assert.Equal(t, testCase.wantCert, opts.TLSCertCB != nil)
		})
	}
}

func connectionConfig() *Config {
	return &Config{
		Name:                "go-nats-wrapper-test",
		ConnectTimeout:      3 * time.Second,
		ReconnectAttempts:   3,
		ReconnectInterval:   time.Second,
		ReconnectBufferSize: 8388608,
	}
}

func writeTLSFiles(t *testing.T) (string, string, string) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "go-nats-wrapper-test"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}

	certificate, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)

	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.pem")
	certPath := filepath.Join(dir, "client.pem")
	keyPath := filepath.Join(dir, "client-key.pem")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	require.NoError(t, os.WriteFile(caPath, certPEM, 0o600))
	require.NoError(t, os.WriteFile(certPath, certPEM, 0o600))
	require.NoError(t, os.WriteFile(keyPath, keyPEM, 0o600))

	return caPath, certPath, keyPath
}
