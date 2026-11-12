package netevents

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type recorderStub struct{}

func (recorderStub) SetNetworkAvailable(bool) {}

func TestWaitSettledAfterOutage(t *testing.T) {
	const budget = 1500 * time.Millisecond
	const settleWindow = 200 * time.Millisecond
	const outage = 2 * settleWindow

	m := NewManager(recorderStub{})
	m.SetNetworkAvailable(false)

	start := time.Now()
	go func() {
		time.Sleep(outage)
		m.SetNetworkAvailable(true)
	}()

	ok := m.WaitSettled(context.Background(), budget, settleWindow)
	elapsed := time.Since(start)

	assert.True(t, ok, "recovered network must let the caller proceed")
	assert.GreaterOrEqual(t, elapsed, outage+settleWindow, "an online verdict must hold a full settle window before it is trusted")
}

func TestNetworkChangedCoalescesTransitions(t *testing.T) {
	m := NewManager(recorderStub{})
	m.NotifyNetworkChange()
	m.NotifyNetworkChange()

	select {
	case <-m.NetworkChanged():
	case <-time.After(time.Second):
		t.Fatal("network change was not emitted")
	}

	select {
	case <-m.NetworkChanged():
		t.Fatal("burst network changes should coalesce into one notification")
	case <-time.After(20 * time.Millisecond):
	}
}
