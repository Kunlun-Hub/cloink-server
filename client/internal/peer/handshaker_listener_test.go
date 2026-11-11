package peer

import (
	"context"
	"testing"
	"time"
)

func Test_newOfferListener(t *testing.T) {
	dummyOfferAnswer := &OfferAnswer{}
	runChan := make(chan struct{}, 10)

	longRunningFn := func(_ context.Context, remoteOfferAnswer *OfferAnswer) {
		time.Sleep(1 * time.Second)
		runChan <- struct{}{}
	}

	hl := NewAsyncOfferListener(longRunningFn)

	hl.Notify(dummyOfferAnswer)
	hl.Notify(dummyOfferAnswer)
	hl.Notify(dummyOfferAnswer)

	// Wait for exactly 2 callbacks
	for i := 0; i < 2; i++ {
		select {
		case <-runChan:
		case <-time.After(3 * time.Second):
			t.Fatal("Timeout waiting for callback")
		}
	}

	// Verify no additional callbacks happen
	select {
	case <-runChan:
		t.Fatal("Unexpected additional callback")
	case <-time.After(100 * time.Millisecond):
		t.Log("Correctly received exactly 2 callbacks")
	}
}

func Test_newOfferListenerCancelsSupersededAttempt(t *testing.T) {
	firstStarted := make(chan *OfferAnswer, 1)
	firstCanceled := make(chan struct{}, 1)
	processed := make(chan *OfferAnswer, 1)

	fn := func(ctx context.Context, offer *OfferAnswer) {
		if offer.WgListenPort == 1111 {
			firstStarted <- offer
			<-ctx.Done()
			firstCanceled <- struct{}{}
			return
		}
		processed <- offer
	}
	hl := NewAsyncOfferListener(fn)
	hl.Notify(&OfferAnswer{WgListenPort: 1111})

	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first callback did not start")
	}

	second := &OfferAnswer{WgListenPort: 2222}
	hl.Notify(second)

	select {
	case <-firstCanceled:
	case <-time.After(time.Second):
		t.Fatal("superseded callback was not canceled")
	}
	select {
	case got := <-processed:
		if got != second {
			t.Fatalf("processed offer %p, want latest %p", got, second)
		}
	case <-time.After(time.Second):
		t.Fatal("latest callback was not processed")
	}
}
