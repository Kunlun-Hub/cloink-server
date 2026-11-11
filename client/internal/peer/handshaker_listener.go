package peer

import (
	"context"
	"sync"
)

type callbackFunc func(ctx context.Context, remoteOfferAnswer *OfferAnswer)

func (oa *OfferAnswer) SessionIDString() string {
	if oa.SessionID == nil {
		return "unknown"
	}
	return oa.SessionID.String()
}

type AsyncOfferListener struct {
	fn      callbackFunc
	running bool
	latest  *OfferAnswer
	cancel  context.CancelFunc
	mu      sync.Mutex
}

func NewAsyncOfferListener(fn callbackFunc) *AsyncOfferListener {
	return &AsyncOfferListener{
		fn: fn,
	}
}

func (o *AsyncOfferListener) Notify(remoteOfferAnswer *OfferAnswer) {
	o.mu.Lock()
	// Store the latest offer.
	o.latest = remoteOfferAnswer

	// A newer signal supersedes any connection attempt started for the previous
	// one. Cancel it instead of letting it block progress until its timeout.
	if o.running {
		if o.cancel != nil {
			o.cancel()
		}
		o.mu.Unlock()
		return
	}

	// Start processing.
	ctx, cancel := context.WithCancel(context.Background())
	o.running = true
	o.cancel = cancel
	o.mu.Unlock()

	// Process in a goroutine to avoid blocking the caller.
	go o.run(ctx, cancel, remoteOfferAnswer)
}

func (o *AsyncOfferListener) run(ctx context.Context, cancel context.CancelFunc, remoteOfferAnswer *OfferAnswer) {
	for {
		o.fn(ctx, remoteOfferAnswer)
		cancel()

		o.mu.Lock()
		remoteOfferAnswer = o.latest
		if remoteOfferAnswer == nil {
			// No more work to do.
			o.running = false
			o.cancel = nil
			o.mu.Unlock()
			return
		}
		// Clear the latest to mark it as being processed.
		o.latest = nil
		ctx, cancel = context.WithCancel(context.Background())
		o.cancel = cancel
		o.mu.Unlock()
	}
}
