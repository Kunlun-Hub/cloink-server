package client

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Multiple disconnect events may arrive for the same home Relay. Only one
// reconnect round may run at a time; duplicates must return without starting a
// second picker.
func TestGuardStartReconnectTrysSingleFlight(t *testing.T) {
	guard := NewGuard(&ServerPicker{}, time.Second, nil)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	firstDone := make(chan struct{})
	go func() {
		guard.StartReconnectTrys(ctx, nil)
		close(firstDone)
	}()

	require.Eventually(t, guard.reconnecting.Load, time.Second, 10*time.Millisecond,
		"first reconnect round should become active")

	secondDone := make(chan struct{})
	go func() {
		guard.StartReconnectTrys(context.Background(), nil)
		close(secondDone)
	}()

	select {
	case <-secondDone:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("duplicate StartReconnectTrys did not return immediately")
	}

	cancel()
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("active reconnect round did not stop after cancellation")
	}
	require.False(t, guard.reconnecting.Load())
}
