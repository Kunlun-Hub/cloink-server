package client

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/netbirdio/netbird/shared/relay/messages"
)

// ErrDeadlineNotSupported is returned by the Conn deadline methods. The relay
// data plane has no deadline support; callers get an explicit error instead
// of crashing the process.
var ErrDeadlineNotSupported = errors.New("relay conn: deadlines not supported")

// Conn represent a connection to a relayed remote peer.
type Conn struct {
	dstID       messages.PeerID
	messageChan chan Msg
	// reregistered holds at most one pending signal, see PeerReregistered.
	reregistered chan struct{}
	instanceURL  *RelayAddr
	ctx          context.Context
	writeFn      func(messages.PeerID, []byte) (int, error)
	closeFn      func(messages.PeerID) error
	localAddrFn  func() net.Addr
}

// Context returns a context that is cancelled when the connection is torn down,
// either by Close or by the relay client losing the server connection. The
// cancellation cause carries the reason, see ErrServerDisconnected and
// ErrPeerDisconnected.
func (c *Conn) Context() context.Context {
	return c.ctx
}

// PeerReregistered reports that the remote peer registered on the relay again
// while this connection was open. Signals that arrive before the previous one
// is consumed are merged into it. The channel is never closed.
func (c *Conn) PeerReregistered() <-chan struct{} {
	return c.reregistered
}

func (c *Conn) Write(p []byte) (n int, err error) {
	return c.writeFn(c.dstID, p)
}

func (c *Conn) Read(b []byte) (n int, err error) {
	m, ok := <-c.messageChan
	if !ok {
		return 0, net.ErrClosed
	}

	n = copy(b, m.Payload)
	m.Free()
	return n, nil
}

func (c *Conn) Close() error {
	return c.closeFn(c.dstID)
}

func (c *Conn) LocalAddr() net.Addr {
	return c.localAddrFn()
}

func (c *Conn) RemoteAddr() net.Addr {
	return c.instanceURL
}

func (c *Conn) SetDeadline(t time.Time) error {
	return ErrDeadlineNotSupported
}

func (c *Conn) SetReadDeadline(t time.Time) error {
	return ErrDeadlineNotSupported
}

func (c *Conn) SetWriteDeadline(t time.Time) error {
	return ErrDeadlineNotSupported
}
