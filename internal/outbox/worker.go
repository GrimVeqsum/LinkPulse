// Package outbox delivers committed redirect events with bounded concurrency.
package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"linkpulse/internal/event"
)

// ErrLeaseLost means another worker owns the event. A stale owner must not acknowledge it.
var ErrLeaseLost = errors.New("outbox lease lost")

type Item struct {
	Event      event.Click
	Attempts   int
	LeaseToken string
}

type Store interface {
	Claim(context.Context, int, time.Duration) ([]Item, error)
	Ack(context.Context, Item) error
	Retry(context.Context, Item, time.Time, string) error
}

type Sender interface {
	TrackClick(context.Context, event.Click) error
}

type Options struct {
	PollInterval    time.Duration
	BatchSize       int
	LeaseDuration   time.Duration
	DeliveryTimeout time.Duration
	MinBackoff      time.Duration
	MaxBackoff      time.Duration
}

type Worker struct {
	store   Store
	sender  Sender
	options Options
}

func New(store Store, sender Sender, options Options) *Worker {
	if options.PollInterval <= 0 {
		options.PollInterval = 250 * time.Millisecond
	}
	if options.BatchSize <= 0 {
		options.BatchSize = 10
	}
	if options.DeliveryTimeout <= 0 {
		options.DeliveryTimeout = 2 * time.Second
	}
	// Claims are processed serially. Include database acknowledgement time and margin
	// so healthy batches do not routinely outlive their lease.
	minimumLease := time.Duration(options.BatchSize)*(options.DeliveryTimeout+2*time.Second) + 5*time.Second
	if options.LeaseDuration < minimumLease {
		options.LeaseDuration = minimumLease
	}
	if options.MinBackoff <= 0 {
		options.MinBackoff = time.Second
	}
	if options.MaxBackoff < options.MinBackoff {
		options.MaxBackoff = max(time.Minute, options.MinBackoff)
	}
	return &Worker{store: store, sender: sender, options: options}
}

// Run owns no goroutines. Its caller cancels the context and joins Run before
// closing dependencies. Unacknowledged claims become available after lease expiry.
func (w *Worker) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		items, err := w.store.Claim(ctx, w.options.BatchSize, w.options.LeaseDuration)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Error("claim outbox events", "error", err)
			if !wait(ctx, w.options.PollInterval) {
				return nil
			}
			continue
		}
		for _, item := range items {
			if ctx.Err() != nil {
				return nil
			}
			if err := w.deliver(ctx, item); err != nil {
				slog.Error("update outbox delivery", "event_id", item.Event.ID, "error", err)
			}
		}
		if len(items) < w.options.BatchSize && !wait(ctx, w.options.PollInterval) {
			return nil
		}
	}
}

func (w *Worker) deliver(ctx context.Context, item Item) error {
	deliveryCtx, cancel := context.WithTimeout(ctx, w.options.DeliveryTimeout)
	err := w.sender.TrackClick(deliveryCtx, item.Event)
	cancel()
	if ctx.Err() != nil {
		// Never remove an event on cancellation: delivery may have committed remotely.
		return nil
	}
	if err == nil {
		if err := w.store.Ack(ctx, item); err != nil {
			return fmt.Errorf("acknowledge event: %w", err)
		}
		return nil
	}
	next := time.Now().UTC().Add(backoff(item.Attempts, w.options.MinBackoff, w.options.MaxBackoff))
	slog.Warn("outbox delivery failed", "event_id", item.Event.ID, "attempt", item.Attempts, "retry_at", next, "error", err)
	return w.store.Retry(ctx, item, next, err.Error())
}

// backoff is capped before multiplication to avoid overflow after prolonged outages.
func backoff(attempts int, minimum, maximum time.Duration) time.Duration {
	delay := minimum
	for i := 1; i < attempts && delay < maximum; i++ {
		if delay > maximum/2 {
			return maximum
		}
		delay *= 2
	}
	return min(delay, maximum)
}

func wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
