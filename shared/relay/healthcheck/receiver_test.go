package healthcheck

import (
	"context"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
)

func TestNewReceiver(t *testing.T) {

	opts := ReceiverOptions{
		HeartbeatTimeout: 5 * time.Second,
	}
	r := NewReceiverWithOpts(log.WithContext(context.Background()), opts)
	defer r.Stop()

	select {
	case <-r.OnTimeout:
		t.Error("unexpected timeout")
	case <-time.After(1 * time.Second):
		// Test passes if no timeout received
	}
}

func TestNewReceiverNotReceive(t *testing.T) {
	opts := ReceiverOptions{
		HeartbeatTimeout: 1 * time.Second,
	}
	r := NewReceiverWithOpts(log.WithContext(context.Background()), opts)
	defer r.Stop()

	select {
	case <-r.OnTimeout:
		// Test passes if timeout is received
	case <-time.After(2 * time.Second):
		t.Error("timeout not received")
	}
}

func TestNewReceiverAck(t *testing.T) {
	opts := ReceiverOptions{
		HeartbeatTimeout: 150 * time.Millisecond,
	}
	r := NewReceiverWithOpts(log.WithContext(context.Background()), opts)
	defer r.Stop()

	deadline := time.Now().Add(450 * time.Millisecond)
	for time.Now().Before(deadline) {
		r.Heartbeat()
		time.Sleep(40 * time.Millisecond)
		select {
		case <-r.OnTimeout:
			t.Fatal("unexpected timeout while heartbeats were arriving")
		default:
		}
	}

	select {
	case <-r.OnTimeout:
	case <-time.After(time.Second):
		t.Fatal("timeout not received after heartbeats stopped")
	}
}

func TestReceiverHealthCheckAttemptThreshold(t *testing.T) {
	t.Run("Default attempt threshold", func(t *testing.T) {
		receiver := NewReceiverWithOpts(log.WithField("test_name", "default"), ReceiverOptions{
			HeartbeatTimeout: 60 * time.Millisecond,
			AttemptThreshold: defaultAttemptThreshold,
		})
		defer receiver.Stop()

		select {
		case <-receiver.OnTimeout:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("timeout not received")
		}
	})

	t.Run("Custom attempt threshold", func(t *testing.T) {
		receiver := NewReceiverWithOpts(log.WithField("test_name", "custom"), ReceiverOptions{
			HeartbeatTimeout: 60 * time.Millisecond,
			AttemptThreshold: 3,
		})
		defer receiver.Stop()

		select {
		case <-receiver.OnTimeout:
			t.Fatal("timed out before the attempt threshold")
		case <-time.After(100 * time.Millisecond):
		}
		select {
		case <-receiver.OnTimeout:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("timeout not received")
		}
	})

	t.Run("Heartbeat resets the timeout", func(t *testing.T) {
		receiver := NewReceiverWithOpts(log.WithField("test_name", "reset"), ReceiverOptions{
			HeartbeatTimeout: 100 * time.Millisecond,
		})
		defer receiver.Stop()

		time.Sleep(50 * time.Millisecond)
		receiver.Heartbeat()

		select {
		case <-receiver.OnTimeout:
			t.Fatal("heartbeat did not reset the timeout")
		case <-time.After(60 * time.Millisecond):
		}
		select {
		case <-receiver.OnTimeout:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("timeout not received after reset")
		}
	})
}
