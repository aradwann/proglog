package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// our tests use a few different *tls.Config configurations and SetupTLSConfig allow us to get each type of *rls.Config with
// one function call. these are the different configurations
// - Client *tls.Config is set up to verify the server's certificate with the client's by setting the *tls.Config's RootCA
// - Client *tls.Config is set up to verify the server's certificate and allow the server to verify the client's certificate by setting its RootCAs and its Cerificates
// - Server *tls.Config is set up verify the client's cerfifcate by setting its ClientCAs, Certificate, and ClientAuth mode set to tls.RequireAndVerifyCert.

func SetupTLSConfig(cfg TLSConfig) (*tls.Config, error) {
	var err error
	tlsConfig := &tls.Config{}
	if cfg.CertFile != "" && cfg.KeyFile != "" {
		tlsConfig.Certificates = make([]tls.Certificate, 1)
		tlsConfig.Certificates[0], err = tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, err
		}
	}
	if cfg.CAFile != "" {
		b, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, err
		}
		ca := x509.NewCertPool()
		ok := ca.AppendCertsFromPEM([]byte(b))
		if !ok {
			return nil, fmt.Errorf(
				"failed to parse root certificate: %q",
				cfg.CAFile,
			)
		}
		if cfg.Server {
			tlsConfig.ClientCAs = ca
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		} else {
			tlsConfig.RootCAs = ca
		}

		tlsConfig.ServerName = cfg.ServerAddress
	}

	return tlsConfig, nil
}

type TLSConfig struct {
	CertFile      string
	KeyFile       string
	CAFile        string
	ServerAddress string
	Server        bool
}
