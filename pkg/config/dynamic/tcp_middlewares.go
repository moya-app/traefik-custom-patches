package dynamic

// +k8s:deepcopy-gen=true

// TCPMiddleware holds the TCPMiddleware configuration.
type TCPMiddleware struct {
	InFlightConn   *TCPInFlightConn   `json:"inFlightConn,omitempty" toml:"inFlightConn,omitempty" yaml:"inFlightConn,omitempty" export:"true"`
	IPWhiteList    *TCPIPWhiteList    `json:"ipWhiteList,omitempty" toml:"ipWhiteList,omitempty" yaml:"ipWhiteList,omitempty" export:"true"`
	StreamCompress *TCPStreamCompress `json:"streamCompress,omitempty" toml:"streamCompress,omitempty" yaml:"streamCompress,omitempty" export:"true"`
}

// +k8s:deepcopy-gen=true

// TCPInFlightConn holds the TCP InFlightConn middleware configuration.
// This middleware prevents services from being overwhelmed with high load,
// by limiting the number of allowed simultaneous connections for one IP.
// More info: https://doc.traefik.io/traefik/v2.10/middlewares/tcp/inflightconn/
type TCPInFlightConn struct {
	// Amount defines the maximum amount of allowed simultaneous connections.
	// The middleware closes the connection if there are already amount connections opened.
	Amount int64 `json:"amount,omitempty" toml:"amount,omitempty" yaml:"amount,omitempty" export:"true"`
}

// +k8s:deepcopy-gen=true

// TCPIPWhiteList holds the TCP IPWhiteList middleware configuration.
// This middleware accepts/refuses connections based on the client IP.
type TCPIPWhiteList struct {
	// SourceRange defines the allowed IPs (or ranges of allowed IPs by using CIDR notation).
	SourceRange []string `json:"sourceRange,omitempty" toml:"sourceRange,omitempty" yaml:"sourceRange,omitempty"`
}

// +k8s:deepcopy-gen=true

// TCPStreamCompress holds the TCP StreamCompress middleware configuration.
// This middleware adds a layer of compression to the TCP stream.
type TCPStreamCompress struct {
	// Algorithm defines the compression algorithm to use.
	Algorithm string `json:"algorithm,omitempty" toml:"algorithm,omitempty" yaml:"algorithm,omitempty"`
	// Dictionary is an optional path to a zstd dictionary file
	Dictionary string `json:"dictionary,omitempty" toml:"dictionary,omitempty" yaml:"dictionary,omitempty"`
	// Level is the compression level to use
	Level string `json:"level,omitempty" toml:"level,omitempty" yaml:"level,omitempty"`
}
