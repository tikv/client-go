package config

// Security is SSL configuration.
type Security struct {
	SSLCA   string `toml:"ssl-ca" json:"ssl-ca"`
	SSLCert string `toml:"ssl-cert" json:"ssl-cert"`
	SSLKey  string `toml:"ssl-key" json:"ssl-key"`
}

// EnableOpenTracing is the flag to enable open tracing.
var EnableOpenTracing = false
