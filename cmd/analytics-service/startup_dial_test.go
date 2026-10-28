package main

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func TestStartupDialContextInterruptsClickHouseHandshake(t *testing.T) {
	for _, tc := range []struct {
		name     string
		deadline bool
		wantErr  error
	}{
		{"cancellation", false, context.Canceled},
		{"deadline", true, context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			listener := startupTestListener(t)
			startupCtx, cancel := context.WithCancel(context.Background())
			if tc.deadline {
				cancel()
				startupCtx, cancel = context.WithTimeout(context.Background(), 500*time.Millisecond)
			}
			dial, finish := startupDialContext(startupCtx)
			db := clickhouse.OpenDB(&clickhouse.Options{
				Addr:        []string{listener.Addr().String()},
				DialContext: dial,
				DialTimeout: time.Minute,
				ReadTimeout: time.Minute,
			})
			t.Cleanup(func() {
				cancel()
				finish()
				_ = db.Close()
			})
			finished := make(chan error, 1)
			go func() { finished <- waitForClickHouse(startupCtx, db.PingContext) }()
			serverConnection := acceptStartupTestConnection(t, listener)
			// Receiving Hello proves that dial completed and the real native driver
			// reached its handshake. The fake server deliberately never answers.
			if _, err := io.ReadFull(serverConnection, make([]byte, 1)); err != nil {
				t.Fatalf("read ClickHouse Hello: %v", err)
			}
			if !tc.deadline {
				cancel()
			}
			select {
			case err := <-finished:
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("handshake cancellation error: got %v, want %v", err, tc.wantErr)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("startup cancellation did not interrupt the driver's minute-long handshake timeout")
			}
		})
	}
}

func TestStartupDialContextFinishReleasesRuntimeConnections(t *testing.T) {
	listener := startupTestListener(t)
	startupCtx, cancel := context.WithCancel(context.Background())
	dial, finish := startupDialContext(startupCtx)
	t.Cleanup(func() { cancel(); finish() })
	connection, err := dial(startupCtx, listener.Addr().String())
	if err != nil {
		t.Fatalf("dial during startup: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	serverConnection := acceptStartupTestConnection(t, listener)

	finish()
	finish()
	cancel()
	assertStartupConnectionUsable(t, connection, serverConnection)

	runtimeCtx, runtimeCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer runtimeCancel()
	runtimeConnection, err := dial(runtimeCtx, listener.Addr().String())
	if err != nil {
		t.Fatalf("dial after startup context cancellation: %v", err)
	}
	t.Cleanup(func() { _ = runtimeConnection.Close() })
	runtimeServerConnection := acceptStartupTestConnection(t, listener)
	assertStartupConnectionUsable(t, runtimeConnection, runtimeServerConnection)
}

func TestStartupDialContextRejectsLateConnectionAfterCancellation(t *testing.T) {
	listener := startupTestListener(t)
	startupCtx, cancel := context.WithCancel(context.Background())
	dial, finish := startupDialContext(startupCtx)
	t.Cleanup(finish)
	cancel()
	dialCtx, dialCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer dialCancel()
	connection, err := dial(dialCtx, listener.Addr().String())
	if connection != nil {
		_ = connection.Close()
		t.Fatal("a connection created after startup cancellation must not be returned")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("late startup dial error: %v", err)
	}
	serverConnection := acceptStartupTestConnection(t, listener)
	if _, err := serverConnection.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("rejected startup connection was not closed: %v", err)
	}
}

func startupTestListener(t *testing.T) *net.TCPListener {
	t.Helper()
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	if err := listener.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return listener
}

func acceptStartupTestConnection(t *testing.T, listener *net.TCPListener) net.Conn {
	t.Helper()
	connection, err := listener.Accept()
	if err != nil {
		t.Fatalf("accept startup connection: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	if err := connection.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return connection
}

func assertStartupConnectionUsable(t *testing.T, connection, peer net.Conn) {
	t.Helper()
	if err := connection.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	const payload = "runtime"
	if _, err := io.WriteString(connection, payload); err != nil {
		t.Fatalf("write after releasing startup guard: %v", err)
	}
	received := make([]byte, len(payload))
	if _, err := io.ReadFull(peer, received); err != nil {
		t.Fatalf("read after releasing startup guard: %v", err)
	}
	if string(received) != payload {
		t.Fatalf("payload after releasing startup guard: %q", received)
	}
}
