package client_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	analyticsv1 "linkpulse/gen/analytics/v1"
	"linkpulse/internal/analytics/client"
	"linkpulse/internal/event"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type testServer struct {
	analyticsv1.UnimplementedAnalyticsServiceServer
	track func(context.Context, *analyticsv1.TrackClickRequest) (*analyticsv1.TrackClickResponse, error)
	stats func(context.Context, *analyticsv1.GetLinkStatsRequest) (*analyticsv1.GetLinkStatsResponse, error)
}

func (s *testServer) TrackClick(ctx context.Context, r *analyticsv1.TrackClickRequest) (*analyticsv1.TrackClickResponse, error) {
	return s.track(ctx, r)
}
func (s *testServer) GetLinkStats(ctx context.Context, r *analyticsv1.GetLinkStatsRequest) (*analyticsv1.GetLinkStatsResponse, error) {
	return s.stats(ctx, r)
}

func newClient(t *testing.T, s *testServer) (*client.Client, *health.Server) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	analyticsv1.RegisterAnalyticsServiceServer(server, s)
	healthServer := health.NewServer()
	healthv1.RegisterHealthServer(server, healthServer)
	go func() { _ = server.Serve(listener) }()
	conn, err := grpc.NewClient("passthrough:///analytics", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(); server.Stop(); _ = listener.Close() })
	return client.New(conn), healthServer
}

func click() event.Click {
	return event.Click{ID: "c14a43d6-c8fd-4fa2-ae0d-5be0d85910e5", LinkID: 42, Code: "abc123", UserAgent: "agent", Referer: "https://example.org/", OccurredAt: time.Date(2026, 1, 2, 23, 59, 59, 123000000, time.UTC)}
}

func TestTrackClickPreservesRetryPayloadAndDeadline(t *testing.T) {
	var mu sync.Mutex
	var ids []string
	e := click()
	c, _ := newClient(t, &testServer{track: func(ctx context.Context, r *analyticsv1.TrackClickRequest) (*analyticsv1.TrackClickResponse, error) {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > 2100*time.Millisecond {
			t.Error("missing bounded RPC deadline")
		}
		if r.GetEventId() != e.ID || r.GetLinkId() != e.LinkID || r.GetCode() != e.Code || r.GetUserAgent() != e.UserAgent || r.GetReferer() != e.Referer || !r.GetOccurredAt().AsTime().Equal(e.OccurredAt) {
			t.Errorf("changed persisted payload: %v", r)
		}
		mu.Lock()
		defer mu.Unlock()
		ids = append(ids, r.GetEventId())
		return &analyticsv1.TrackClickResponse{Success: true}, nil
	}})
	for i := 0; i < 2; i++ {
		if err := c.TrackClick(context.Background(), e); err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(ids) != 2 || ids[0] != ids[1] {
		t.Fatalf("ids=%v", ids)
	}
}

func TestTrackClickFailureNotAcknowledged(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response *analyticsv1.TrackClickResponse
		err      error
	}{
		{"unavailable", nil, status.Error(codes.Unavailable, "offline")},
		{"negative acknowledgement", &analyticsv1.TrackClickResponse{Success: false}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := newClient(t, &testServer{track: func(context.Context, *analyticsv1.TrackClickRequest) (*analyticsv1.TrackClickResponse, error) {
				return tc.response, tc.err
			}})
			if err := c.TrackClick(context.Background(), click()); status.Code(err) != codes.Unavailable {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestRPCDeadlineAndCancellation(t *testing.T) {
	c, _ := newClient(t, &testServer{
		track: func(ctx context.Context, _ *analyticsv1.TrackClickRequest) (*analyticsv1.TrackClickResponse, error) {
			<-ctx.Done()
			return nil, status.FromContextError(ctx.Err()).Err()
		},
		stats: func(ctx context.Context, _ *analyticsv1.GetLinkStatsRequest) (*analyticsv1.GetLinkStatsResponse, error) {
			<-ctx.Done()
			return nil, status.FromContextError(ctx.Err()).Err()
		},
	})
	for _, method := range []string{"track", "stats"} {
		t.Run(method, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			var err error
			if method == "track" {
				err = c.TrackClick(ctx, click())
			} else {
				_, err = c.GetLinkStats(ctx, 42)
			}
			if status.Code(err) != codes.DeadlineExceeded {
				t.Fatalf("error=%v", err)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.TrackClick(ctx, click()); status.Code(err) != codes.Canceled {
		t.Fatalf("canceled error=%v", err)
	}
}

func TestGetStatsAndHealth(t *testing.T) {
	c, h := newClient(t, &testServer{stats: func(ctx context.Context, r *analyticsv1.GetLinkStatsRequest) (*analyticsv1.GetLinkStatsResponse, error) {
		if r.GetLinkId() != 42 {
			t.Errorf("link_id=%d", r.GetLinkId())
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Error("missing deadline")
		}
		return &analyticsv1.GetLinkStatsResponse{TotalClicks: 11, TodayClicks: 3}, nil
	}})
	got, err := c.GetLinkStats(context.Background(), 42)
	if err != nil || got.TotalClicks != 11 || got.TodayClicks != 3 {
		t.Fatalf("stats=%+v error=%v", got, err)
	}
	h.SetServingStatus("", healthv1.HealthCheckResponse_NOT_SERVING)
	if err := c.Ready(context.Background()); status.Code(err) != codes.Unavailable {
		t.Fatalf("health error=%v", err)
	}
	h.SetServingStatus("", healthv1.HealthCheckResponse_SERVING)
	if err := c.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
}
