package client

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The relay Conn is handed to callers as a net.Conn. Deadline methods must
// return an explicit error instead of panicking and crashing the process.
func TestConnDeadlinesReturnError(t *testing.T) {
	c := &Conn{}
	require.ErrorIs(t, c.SetDeadline(time.Now().Add(time.Second)), ErrDeadlineNotSupported)
	require.ErrorIs(t, c.SetReadDeadline(time.Now().Add(time.Second)), ErrDeadlineNotSupported)
	require.ErrorIs(t, c.SetWriteDeadline(time.Now().Add(time.Second)), ErrDeadlineNotSupported)
}
