package square9

type Client interface {
	// Close releases the license(s) consumed by the Client.  After closing,
	// the Client can no longer be used.
	Close() error
}
