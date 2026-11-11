package healthcheck

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	defaultAttemptThreshold = 1

	defaultHealthCheckInterval = 2 * time.Second
	defaultHealthCheckTimeout  = 4 * time.Second
)

type SenderOptions struct {
	HealthCheckInterval time.Duration
	HealthCheckTimeout  time.Duration
	AttemptThreshold    int
}

// Sender is a healthcheck sender
// It will send healthcheck signal to the receiver
// If the receiver does not receive the signal in a certain time, it will send a timeout signal and stop to work
// It will also stop if the context is canceled
type Sender struct {
	// HealthCheck is a channel to send health check signal to the peer
	HealthCheck chan struct{}
	// Timeout is a channel to the health check signal is not received in a certain time
	Timeout chan struct{}

	log                 *log.Entry
	healthCheckInterval time.Duration
	timeout             time.Duration

	ack              chan struct{}
	attemptThreshold int
}

func NewSenderWithOpts(log *log.Entry, opts SenderOptions) *Sender {
	if opts.HealthCheckInterval <= 0 {
		opts.HealthCheckInterval = defaultHealthCheckInterval
	}
	if opts.HealthCheckTimeout <= 0 {
		opts.HealthCheckTimeout = defaultHealthCheckTimeout
	}
	if opts.AttemptThreshold <= 0 {
		opts.AttemptThreshold = defaultAttemptThreshold
	}
	hc := &Sender{
		HealthCheck:         make(chan struct{}, 1),
		Timeout:             make(chan struct{}, 1),
		log:                 log,
		healthCheckInterval: opts.HealthCheckInterval,
		timeout:             opts.HealthCheckInterval + opts.HealthCheckTimeout,
		ack:                 make(chan struct{}, 1),
		attemptThreshold:    opts.AttemptThreshold,
	}

	return hc
}

// NewSender creates a new healthcheck sender
func NewSender(log *log.Entry) *Sender {
	opts := SenderOptions{
		HealthCheckInterval: defaultHealthCheckInterval,
		HealthCheckTimeout:  defaultHealthCheckTimeout,
		AttemptThreshold:    getAttemptThresholdFromEnv(),
	}
	return NewSenderWithOpts(log, opts)
}

// OnHCResponse sends an acknowledgment signal to the sender
func (hc *Sender) OnHCResponse() {
	select {
	case hc.ack <- struct{}{}:
	default:
	}
}

func (hc *Sender) StartHealthCheck(ctx context.Context) {
	ticker := time.NewTicker(hc.healthCheckInterval)
	defer ticker.Stop()

	timeoutTimer := time.NewTimer(hc.timeout)
	defer timeoutTimer.Stop()

	defer close(hc.HealthCheck)
	defer close(hc.Timeout)

	resetTimeout := func() {
		if !timeoutTimer.Stop() {
			select {
			case <-timeoutTimer.C:
			default:
			}
		}
		timeoutTimer.Reset(hc.timeout)
	}

	failureCounter := 0
	for {
		select {
		case <-ticker.C:
			select {
			case hc.HealthCheck <- struct{}{}:
			default:
			}
		case <-hc.ack:
			failureCounter = 0
			resetTimeout()
		case <-timeoutTimer.C:
			// A response may have arrived at the same time as the deadline.
			select {
			case <-hc.ack:
				failureCounter = 0
				resetTimeout()
				continue
			default:
			}
			failureCounter++
			if failureCounter < hc.attemptThreshold {
				hc.log.Warnf("Health check failed attempt %d.", failureCounter)
				resetTimeout()
				continue
			}
			select {
			case hc.Timeout <- struct{}{}:
			default:
			}
			return
		case <-ctx.Done():
			return
		}
	}
}
