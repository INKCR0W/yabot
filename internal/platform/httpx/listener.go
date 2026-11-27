package httpx

import (
	"errors"
	"net"
	"sync"
)

type limitedListener struct {
	net.Listener
	slots  chan struct{}
	server *Server
}

func (l *limitedListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		select {
		case l.slots <- struct{}{}:
			return &limitedConn{Conn: conn, listener: l}, nil
		default:
			l.server.stats.connectionsRejected.Inc()
			if err := conn.Close(); err != nil {
				l.server.recordServerError("close rejected HTTP connection failed")
			}
		}
	}
}

type limitedConn struct {
	net.Conn
	listener  *limitedListener
	closeOnce sync.Once
	closeErr  error
}

func (c *limitedConn) Close() error {
	c.closeOnce.Do(func() {
		c.closeErr = c.Conn.Close()
		<-c.listener.slots
		if c.closeErr != nil {
			c.listener.server.recordServerError("close HTTP connection failed")
		}
	})
	return c.closeErr
}

func (c *limitedConn) CloseWrite() error {
	// net/http half-closes TCP connections when rejecting oversized input.
	if conn, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return conn.CloseWrite()
	}
	return errors.ErrUnsupported
}
