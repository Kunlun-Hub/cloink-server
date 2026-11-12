package client

import (
	"slices"
	"sync"
	"time"
)

const (
	relayQualitySampleWindow  = 5
	relayQualityOutcomeWindow = 10
)

// relayDataPlaneQuality is a snapshot of the relay session that is actually
// carrying traffic. Probe RTT only proves that a fresh handshake works; this
// snapshot reflects the established data plane and is what failover decisions
// should trust for the current home relay.
type relayDataPlaneQuality struct {
	RTT                 time.Duration
	P95                 time.Duration
	Jitter              time.Duration
	LossPercent         int
	Samples             int
	ConsecutiveFailures int
	LastSuccess         time.Time
	LastFailure         time.Time
}

type relayLiveQualityTracker struct {
	mu                  sync.Mutex
	samples             []time.Duration
	outcomes            []bool
	rttEWMA             time.Duration
	jitterEWMA          time.Duration
	lastSample          time.Duration
	lastSuccess         time.Time
	lastFailure         time.Time
	consecutiveFailures int
	now                 func() time.Time
}

func (q *relayLiveQualityTracker) clock() time.Time {
	if q.now != nil {
		return q.now()
	}
	return time.Now()
}

func (q *relayLiveQualityTracker) reset() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.samples = nil
	q.outcomes = nil
	q.rttEWMA = 0
	q.jitterEWMA = 0
	q.lastSample = 0
	q.lastSuccess = time.Time{}
	q.lastFailure = time.Time{}
	q.consecutiveFailures = 0
}

func (q *relayLiveQualityTracker) recordSuccess(rtt time.Duration) {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := q.clock()
	q.lastSuccess = now
	q.consecutiveFailures = 0
	q.recordOutcomeLocked(true)
	if rtt <= 0 {
		return
	}
	if q.lastSample > 0 {
		jitter := absDuration(rtt - q.lastSample)
		if q.jitterEWMA == 0 {
			q.jitterEWMA = jitter
		} else {
			q.jitterEWMA = time.Duration(float64(jitter)*relayRTTEWMAAlpha + float64(q.jitterEWMA)*(1-relayRTTEWMAAlpha))
		}
	}
	q.lastSample = rtt
	if q.rttEWMA == 0 {
		q.rttEWMA = rtt
	} else {
		q.rttEWMA = time.Duration(float64(rtt)*relayRTTEWMAAlpha + float64(q.rttEWMA)*(1-relayRTTEWMAAlpha))
	}
	q.samples = append(q.samples, rtt)
	if len(q.samples) > relayQualitySampleWindow {
		q.samples = q.samples[len(q.samples)-relayQualitySampleWindow:]
	}
}

func (q *relayLiveQualityTracker) recordFailure() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.lastFailure = q.clock()
	q.consecutiveFailures++
	q.recordOutcomeLocked(false)
}

func (q *relayLiveQualityTracker) recordOutcomeLocked(success bool) {
	q.outcomes = append(q.outcomes, success)
	if len(q.outcomes) > relayQualityOutcomeWindow {
		q.outcomes = q.outcomes[len(q.outcomes)-relayQualityOutcomeWindow:]
	}
}

func (q *relayLiveQualityTracker) snapshot() relayDataPlaneQuality {
	q.mu.Lock()
	defer q.mu.Unlock()

	quality := relayDataPlaneQuality{
		RTT:                 q.rttEWMA,
		Jitter:              q.jitterEWMA,
		Samples:             len(q.samples),
		ConsecutiveFailures: q.consecutiveFailures,
		LastSuccess:         q.lastSuccess,
		LastFailure:         q.lastFailure,
	}
	if len(q.samples) > 0 {
		ordered := slices.Clone(q.samples)
		slices.Sort(ordered)
		quality.P95 = ordered[len(ordered)-1]
	}
	if len(q.outcomes) > 0 {
		losses := 0
		for _, success := range q.outcomes {
			if !success {
				losses++
			}
		}
		quality.LossPercent = losses * 100 / len(q.outcomes)
	}
	return quality
}

func (q *relayLiveQualityTracker) failedRecently(window time.Duration) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.consecutiveFailures > 0 {
		return true
	}
	return window > 0 && !q.lastFailure.IsZero() && q.clock().Sub(q.lastFailure) <= window
}

func (c *Client) LiveQuality() relayDataPlaneQuality {
	if c == nil {
		return relayDataPlaneQuality{}
	}
	return c.liveQuality.snapshot()
}

func (c *Client) dataPlaneFailedRecently(window time.Duration) bool {
	if c == nil {
		return false
	}
	return c.liveQuality.failedRecently(window)
}
