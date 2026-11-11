package quic

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/quic-go/quic-go"

	netErr "github.com/netbirdio/netbird/shared/relay/client/dialer/net"
)

const (
	Network = "quic"
)

type Addr struct {
	addr string
}

func (a Addr) Network() string {
	return Network
}

func (a Addr) String() string {
	return a.addr
}

type Conn struct {
	session *quic.Conn
	ctx     context.Context
}

func NewConn(session *quic.Conn) net.Conn {
	return &Conn{
		session: session,
		ctx:     context.Background(),
	}
}

func (c *Conn) Read(b []byte) (n int, err error) {
	dgram, err := c.session.ReceiveDatagram(c.ctx)
	if err != nil {
		return 0, c.remoteCloseErrHandling(err)
	}

	n = copy(b, dgram)
	return n, nil
}

func (c *Conn) Write(b []byte) (int, error) {
	if err := c.session.SendDatagram(b); err != nil {
		return 0, c.writeErrHandling(err, len(b))
	}
	return len(b), nil
}

// Protocol returns the transport name for this connection.
func (c *Conn) Protocol() string {
	return Network
}

// PacketsReceived returns the total number of QUIC packets received on the
// underlying session, including transport-level ACK packets.
func (c *Conn) PacketsReceived() uint64 {
	if c.session == nil {
		return 0
	}
	return c.session.ConnectionStats().PacketsReceived
}

// RTT returns QUIC's smoothed network RTT estimate for the active path.
func (c *Conn) RTT() time.Duration {
	if c.session == nil {
		return 0
	}
	stats := c.session.ConnectionStats()
	if stats.SmoothedRTT > 0 {
		return stats.SmoothedRTT
	}
	return stats.LatestRTT
}

func (c *Conn) RemoteAddr() net.Addr {
	return c.session.RemoteAddr()
}

func (c *Conn) LocalAddr() net.Addr {
	if c.session != nil {
		return c.session.LocalAddr()
	}
	return Addr{addr: "unknown"}
}

func (c *Conn) SetReadDeadline(t time.Time) error {
	return fmt.Errorf("SetReadDeadline is not implemented")
}

func (c *Conn) SetWriteDeadline(t time.Time) error {
	return fmt.Errorf("SetWriteDeadline is not implemented")
}

func (c *Conn) SetDeadline(t time.Time) error {
	return nil
}

func (c *Conn) Close() error {
	return c.session.CloseWithError(0, "normal closure")
}

func (c *Conn) remoteCloseErrHandling(err error) error {
	var appErr *quic.ApplicationError
	if errors.As(err, &appErr) && appErr.ErrorCode == 0x0 {
		return netErr.ErrClosedByServer
	}
	return err
}

// writeErrHandling normalizes SendDatagram errors. A datagram that exceeds the
// path's QUIC packet budget is mapped to ErrDatagramTooLarge (annotated with the
// datagram size and path budget) so the relay client can fall back to a
// non-datagram transport.
func (c *Conn) writeErrHandling(err error, size int) error {
	var tooLarge *quic.DatagramTooLargeError
	if errors.As(err, &tooLarge) {
		return fmt.Errorf("%w: %d byte datagram over path budget %d", netErr.ErrDatagramTooLarge, size, tooLarge.MaxDatagramPayloadSize)
	}
	return c.remoteCloseErrHandling(err)
}
