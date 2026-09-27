package session

import (
	"testing"

	"github.com/guoyyit/multiplex-fair-transport-go/config"
	"github.com/guoyyit/multiplex-fair-transport-go/transport"
)

func pairConns(t *testing.T) (transport.Conn, transport.Conn) {
	t.Helper()
	return transport.Pipe()
}

func newSessionOn(t *testing.T, c transport.Conn, cfg config.Config, client bool) *Session {
	t.Helper()
	return New(c, cfg, client, nil)
}
