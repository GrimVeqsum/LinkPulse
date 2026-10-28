package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func TestWaitForClickHouseRecoversFromTemporaryUnavailability(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		failures := []error{
			fmt.Errorf("connect: %w", &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}),
			fmt.Errorf("handshake: %w", &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}),
			io.EOF,
		}
		calls := 0
		started := time.Now()
		err := waitForClickHouse(ctx, func(pingCtx context.Context) error {
			if pingCtx != ctx {
				t.Fatal("startup context was not passed to the database ping")
			}
			calls++
			if calls <= len(failures) {
				return failures[calls-1]
			}
			return nil
		})
		if err != nil || calls != len(failures)+1 {
			t.Fatalf("recovery: calls=%d err=%v", calls, err)
		}
		if elapsed := time.Since(started); elapsed <= 0 || elapsed >= 30*time.Second {
			t.Fatalf("recovery must wait between attempts within the startup budget: %v", elapsed)
		}
	})
}

func TestWaitForClickHouseStopsAtStartupDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const timeout = 350 * time.Millisecond
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		calls := 0
		started := time.Now()
		err := waitForClickHouse(ctx, func(context.Context) error {
			calls++
			return syscall.ECONNREFUSED
		})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("startup deadline was not preserved: %v", err)
		}
		if calls < 2 || time.Since(started) != timeout {
			t.Fatalf("unavailable dependency must retry only within the startup budget: calls=%d elapsed=%v", calls, time.Since(started))
		}
	})
}

func TestWaitForClickHouseRejectsCanceledContextBeforePing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := waitForClickHouse(ctx, func(context.Context) error {
		t.Fatal("an already canceled startup must not contact the database")
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("startup cancellation was not preserved: %v", err)
	}
}

func TestWaitForClickHouseCancelsInFlightPing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		pingStarted := make(chan struct{})
		finished := make(chan error, 1)
		calls := 0
		go func() {
			finished <- waitForClickHouse(ctx, func(pingCtx context.Context) error {
				calls++
				close(pingStarted)
				<-pingCtx.Done()
				return pingCtx.Err()
			})
		}()
		<-pingStarted
		synctest.Wait()
		cancel()
		if err := <-finished; !errors.Is(err, context.Canceled) {
			t.Fatalf("in-flight ping did not observe cancellation: %v", err)
		}
		if calls != 1 {
			t.Fatalf("startup ping retried after cancellation: %d calls", calls)
		}
	})
}

func TestWaitForClickHouseCancelsRetryDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		pingStarted := make(chan struct{})
		finished := make(chan error, 1)
		calls := 0
		started := time.Now()
		go func() {
			finished <- waitForClickHouse(ctx, func(context.Context) error {
				calls++
				if calls == 1 {
					close(pingStarted)
				}
				return syscall.ECONNREFUSED
			})
		}()
		<-pingStarted
		synctest.Wait()
		cancel()
		if err := <-finished; !errors.Is(err, context.Canceled) {
			t.Fatalf("retry delay did not observe cancellation: %v", err)
		}
		if calls != 1 || time.Since(started) != 0 {
			t.Fatalf("cancellation must interrupt the retry delay immediately: calls=%d elapsed=%v", calls, time.Since(started))
		}
	})
}

func TestWaitForClickHouseCancellationWinsOverSuccessfulPing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := waitForClickHouse(ctx, func(context.Context) error {
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled startup must not proceed after a successful ping: %v", err)
	}
}

func TestWaitForClickHouseFailsFastOnPermanentErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"credentials", &clickhouse.Exception{Code: 516, Message: "Authentication failed"}},
		{"database", &clickhouse.Exception{Code: 81, Message: "Database does not exist"}},
		{"hostname", &net.DNSError{Err: "no such host", Name: "invalid.clickhouse", IsNotFound: true}},
		{"configuration", errors.New("unsupported connection configuration")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				calls := 0
				started := time.Now()
				err := waitForClickHouse(ctx, func(context.Context) error {
					calls++
					return fmt.Errorf("connect: %w", tc.err)
				})
				if !errors.Is(err, tc.err) {
					t.Fatalf("permanent startup error was not preserved: %v", err)
				}
				if calls != 1 || time.Since(started) != 0 {
					t.Fatalf("permanent startup errors must fail without waiting: calls=%d elapsed=%v", calls, time.Since(started))
				}
			})
		})
	}
}
