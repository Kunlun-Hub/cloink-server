package client

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type fakeRTTConn struct {
	net.Conn
	rtt time.Duration
}

func (c fakeRTTConn) RTT() time.Duration { return c.rtt }

type fakePingConn struct {
	net.Conn
	pinged bool
}

func (c *fakePingConn) PingRTT(context.Context) (time.Duration, error) {
	c.pinged = true
	return 9 * time.Millisecond, nil
}

func TestMeasureTransportRTTPrefersTransportSample(t *testing.T) {
	rtt, err := measureTransportRTT(context.Background(), fakeRTTConn{rtt: 7 * time.Millisecond}, time.Second)
	require.NoError(t, err)
	require.Equal(t, 7*time.Millisecond, rtt)
}

func TestMeasureTransportRTTUsesWebSocketPing(t *testing.T) {
	conn := &fakePingConn{}
	rtt, err := measureTransportRTT(context.Background(), conn, time.Second)
	require.NoError(t, err)
	require.True(t, conn.pinged)
	require.Equal(t, 9*time.Millisecond, rtt)
}

func TestMeasureTransportRTTFallsBackToHandshake(t *testing.T) {
	rtt, err := measureTransportRTT(context.Background(), struct{ net.Conn }{}, 42*time.Millisecond)
	require.NoError(t, err)
	require.Equal(t, 42*time.Millisecond, rtt)
}

func TestRelayRTTMillisecondsRoundsHealthySubMillisecondSample(t *testing.T) {
	require.Zero(t, relayRTTMilliseconds(0))
	require.Equal(t, int64(1), relayRTTMilliseconds(500*time.Microsecond))
	require.Equal(t, int64(1), relayRTTMilliseconds(time.Millisecond))
	require.Equal(t, int64(2), relayRTTMilliseconds(time.Millisecond+time.Nanosecond))
	require.Equal(t, int64(70), relayRTTMilliseconds(70*time.Millisecond))
}
