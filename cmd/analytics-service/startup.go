package main

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"syscall"
	"time"
)

// The driver's native handshake uses its own socket deadline rather than ctx.
// Close startup connections on cancellation, then release this guard after
// migrations so the startup deadline cannot close pooled runtime connections.
func startupDialContext(startupCtx context.Context) (func(context.Context, string) (net.Conn, error), func()) {
	var mu sync.Mutex
	var connections []net.Conn
	finished := false
	closed := make(chan struct{})
	stop := context.AfterFunc(startupCtx, func() {
		defer close(closed)
		mu.Lock()
		defer mu.Unlock()
		for _, connection := range connections {
			_ = connection.Close()
		}
	})
	finish := sync.OnceFunc(func() {
		if !stop() {
			<-closed
		}
		mu.Lock()
		finished = true
		connections = nil
		mu.Unlock()
	})
	dial := func(ctx context.Context, address string) (net.Conn, error) {
		connection, err := (&net.Dialer{Timeout: 3 * time.Second}).DialContext(ctx, "tcp", address)
		if err != nil {
			return nil, err
		}
		mu.Lock()
		defer mu.Unlock()
		if !finished {
			if err := startupCtx.Err(); err != nil {
				_ = connection.Close()
				return nil, err
			}
			connections = append(connections, connection)
		}
		return connection, nil
	}
	return dial, finish
}

// waitForClickHouse shares the caller's startup deadline with migrations. Compose
// can report ClickHouse healthy while its entrypoint's temporary, loopback-only
// server is running, before the final server accepts connections from peers.
func waitForClickHouse(ctx context.Context, ping func(context.Context) error) error {
	delay := 100 * time.Millisecond
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := ping(ctx)
		if ctx.Err() != nil {
			return errors.Join(ctx.Err(), err)
		}
		if err == nil || !retryableClickHouseConnectionError(err) {
			return err
		}
		slog.Warn("ClickHouse unavailable during startup; retrying", "error", err, "retry_in", delay)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(ctx.Err(), err)
		case <-timer.C:
		}
		delay = min(2*delay, time.Second)
	}
}

func retryableClickHouseConnectionError(err error) bool {
	// Retry transport failures only. Server exceptions (including invalid
	// credentials, unknown databases and invalid settings) fail immediately.
	for _, transient := range []error{
		syscall.ECONNREFUSED, syscall.ECONNRESET, syscall.ECONNABORTED,
		syscall.EHOSTUNREACH, syscall.ENETUNREACH, syscall.EPIPE,
		io.EOF, io.ErrUnexpectedEOF, driver.ErrBadConn,
	} {
		if errors.Is(err, transient) {
			return true
		}
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return dnsErr.IsTimeout || dnsErr.IsTemporary
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}
