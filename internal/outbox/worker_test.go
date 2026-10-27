package outbox

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"linkpulse/internal/event"
)

type storeStub struct {
	claim func(context.Context, int, time.Duration) ([]Item, error)
	ack   func(context.Context, Item) error
	retry func(context.Context, Item, time.Time, string) error
}

func (s storeStub) Claim(ctx context.Context, size int, lease time.Duration) ([]Item, error) {
	if s.claim == nil {
		return nil, nil
	}
	return s.claim(ctx, size, lease)
}
func (s storeStub) Ack(ctx context.Context, item Item) error {
	if s.ack == nil {
		return nil
	}
	return s.ack(ctx, item)
}
func (s storeStub) Retry(ctx context.Context, item Item, next time.Time, reason string) error {
	if s.retry == nil {
		return nil
	}
	return s.retry(ctx, item, next, reason)
}

type senderFunc func(context.Context, event.Click) error

func (f senderFunc) TrackClick(ctx context.Context, event event.Click) error { return f(ctx, event) }

func testItem() Item {
	return Item{Event: event.Click{
		ID: "c14a43d6-c8fd-4fa2-ae0d-5be0d85910e5", LinkID: 42, Code: "abc123", UserAgent: "agent", Referer: "https://example.org/",
		OccurredAt: time.Date(2026, 1, 2, 23, 59, 59, 123000000, time.UTC),
	}, Attempts: 1, LeaseToken: "owner-1"}
}

func TestDeliveryRetriesPreserveEventAndBackoff(t *testing.T) {
	item := testItem()
	var sent, retried, acknowledged int
	var expectedDelay time.Duration
	before := time.Now().UTC()
	w := New(storeStub{
		retry: func(_ context.Context, got Item, next time.Time, reason string) error {
			retried++
			if got.Event != item.Event || got.LeaseToken != item.LeaseToken {
				t.Fatalf("retry changed payload/ownership: %+v", got)
			}
			if reason != "analytics unavailable" {
				t.Errorf("reason=%q", reason)
			}
			if next.Before(before.Add(expectedDelay)) || next.After(time.Now().UTC().Add(expectedDelay)) {
				t.Errorf("retry_at=%v delay=%v", next, expectedDelay)
			}
			return nil
		},
		ack: func(_ context.Context, got Item) error {
			acknowledged++
			if got.Event != item.Event {
				t.Error("ack changed identity")
			}
			return nil
		},
	}, senderFunc(func(ctx context.Context, got event.Click) error {
		sent++
		if got != item.Event {
			t.Fatalf("delivery changed persisted event: %+v", got)
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Error("delivery has no deadline")
		}
		if sent <= 2 {
			return errors.New("analytics unavailable")
		}
		return nil
	}), Options{MinBackoff: time.Second, MaxBackoff: time.Minute})
	for attempt := 1; attempt <= 3; attempt++ {
		item.Attempts = attempt
		expectedDelay = time.Duration(attempt) * time.Second
		before = time.Now().UTC()
		if err := w.deliver(context.Background(), item); err != nil {
			t.Fatal(err)
		}
	}
	if sent != 3 || retried != 2 || acknowledged != 1 {
		t.Fatalf("sent=%d retries=%d acks=%d", sent, retried, acknowledged)
	}
}

func TestAckFailureAllowsSafeRedelivery(t *testing.T) {
	item := testItem()
	var sends, acks, retries int
	ackError := errors.New("database disconnected after remote commit")
	w := New(storeStub{
		ack: func(context.Context, Item) error {
			acks++
			if acks == 1 {
				return ackError
			}
			return nil
		},
		retry: func(context.Context, Item, time.Time, string) error { retries++; return nil },
	}, senderFunc(func(_ context.Context, e event.Click) error {
		sends++
		if e != item.Event {
			t.Error("redelivery changed identity")
		}
		return nil
	}), Options{})
	if err := w.deliver(context.Background(), item); !errors.Is(err, ackError) {
		t.Fatalf("error=%v", err)
	}
	// An unacknowledged lease can expire and be reclaimed. Sending its original
	// identity again is the recoverable crash/ambiguous-ack path.
	item.LeaseToken = "owner-2"
	item.Attempts++
	if err := w.deliver(context.Background(), item); err != nil {
		t.Fatal(err)
	}
	if sends != 2 || acks != 2 || retries != 0 {
		t.Fatalf("sends=%d acks=%d retries=%d", sends, acks, retries)
	}
}

func TestDeliveryDeadlineRetriesAndPreservesEvent(t *testing.T) {
	item := testItem()
	var retries int
	w := New(storeStub{retry: func(_ context.Context, got Item, _ time.Time, _ string) error {
		retries++
		if got.Event != item.Event {
			t.Error("changed event")
		}
		return nil
	}}, senderFunc(func(ctx context.Context, _ event.Click) error {
		<-ctx.Done()
		return ctx.Err()
	}), Options{DeliveryTimeout: 10 * time.Millisecond})
	if err := w.deliver(context.Background(), item); err != nil {
		t.Fatal(err)
	}
	if retries != 1 {
		t.Fatalf("retries=%d", retries)
	}
}

func TestRetryPersistenceFailureIsReturned(t *testing.T) {
	storeError := errors.New("retry update failed")
	w := New(storeStub{retry: func(context.Context, Item, time.Time, string) error { return storeError }}, senderFunc(func(context.Context, event.Click) error { return errors.New("offline") }), Options{})
	if err := w.deliver(context.Background(), testItem()); !errors.Is(err, storeError) {
		t.Fatalf("error=%v", err)
	}
}

func TestRunRecoversAfterClaimFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var claims, acks int
	w := New(storeStub{
		claim: func(context.Context, int, time.Duration) ([]Item, error) {
			claims++
			if claims == 1 {
				return nil, errors.New("postgres restarting")
			}
			return []Item{testItem()}, nil
		},
		ack: func(context.Context, Item) error { acks++; cancel(); return nil },
	}, senderFunc(func(context.Context, event.Click) error { return nil }), Options{PollInterval: time.Millisecond})
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not recover or stop")
	}
	if claims != 2 || acks != 1 {
		t.Fatalf("claims=%d acks=%d", claims, acks)
	}
}

func TestRunStopsWhileIdle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	claimed := make(chan struct{})
	w := New(storeStub{claim: func(context.Context, int, time.Duration) ([]Item, error) { close(claimed); return nil, nil }}, senderFunc(func(context.Context, event.Click) error { t.Error("unexpected send"); return nil }), Options{PollInterval: time.Hour})
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	select {
	case <-claimed:
	case <-time.After(time.Second):
		t.Fatal("worker never polled")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("idle timer blocked shutdown")
	}
}

func TestRunCancelsInflightAndLeavesClaimsUnacknowledged(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	var sends, acks, retries int
	w := New(storeStub{
		claim: func(context.Context, int, time.Duration) ([]Item, error) { return []Item{testItem(), testItem()}, nil },
		ack:   func(context.Context, Item) error { acks++; return nil },
		retry: func(context.Context, Item, time.Time, string) error { retries++; return nil },
	}, senderFunc(func(ctx context.Context, _ event.Click) error {
		sends++
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}), Options{DeliveryTimeout: time.Hour})
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("delivery did not start")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("inflight RPC blocked shutdown")
	}
	if sends != 1 || acks != 0 || retries != 0 {
		t.Fatalf("sends=%d acks=%d retries=%d", sends, acks, retries)
	}
}

func TestRunStopsDuringClaim(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	w := New(storeStub{claim: func(ctx context.Context, _ int, _ time.Duration) ([]Item, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}}, nil, Options{})
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("claim did not start")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("claim blocked shutdown")
	}
}

func TestBackoffCapsBeforeOverflow(t *testing.T) {
	for _, tc := range []struct {
		attempt                int
		minimum, maximum, want time.Duration
	}{
		{0, time.Second, time.Minute, time.Second},
		{1, time.Second, time.Minute, time.Second},
		{2, time.Second, time.Minute, 2 * time.Second},
		{3, time.Second, time.Minute, 4 * time.Second},
		{7, time.Second, time.Minute, time.Minute},
		{math.MaxInt, time.Second, time.Minute, time.Minute},
		{math.MaxInt, time.Duration(math.MaxInt64/2 + 1), time.Duration(math.MaxInt64), time.Duration(math.MaxInt64)},
	} {
		if got := backoff(tc.attempt, tc.minimum, tc.maximum); got != tc.want {
			t.Errorf("attempt=%d min=%v max=%v got=%v want=%v", tc.attempt, tc.minimum, tc.maximum, got, tc.want)
		}
	}
}

func TestLeaseCoversSerialBatchIncludingAcknowledgements(t *testing.T) {
	w := New(nil, nil, Options{BatchSize: 5, DeliveryTimeout: time.Second, LeaseDuration: time.Millisecond})
	if w.options.LeaseDuration < time.Duration(w.options.BatchSize)*(w.options.DeliveryTimeout+2*time.Second) {
		t.Fatalf("lease=%v cannot cover serial batch", w.options.LeaseDuration)
	}
	if w.options.MinBackoff <= 0 || w.options.MaxBackoff < w.options.MinBackoff {
		t.Fatalf("invalid normalized backoff: %+v", w.options)
	}
}
