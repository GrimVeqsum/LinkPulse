package grpc

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	analyticsv1 "linkpulse/gen/analytics/v1"
	"linkpulse/internal/analytics/repository"
	"linkpulse/internal/event"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type repositoryStub struct {
	save  func(context.Context, event.Click) error
	stats func(context.Context, int64) (repository.LinkStats, error)
}

func (r repositoryStub) SaveClick(ctx context.Context, event event.Click) error {
	if r.save == nil {
		return nil
	}
	return r.save(ctx, event)
}
func (r repositoryStub) GetLinkStats(ctx context.Context, id int64) (repository.LinkStats, error) {
	if r.stats == nil {
		return repository.LinkStats{}, nil
	}
	return r.stats(ctx, id)
}

func validRequest() *analyticsv1.TrackClickRequest {
	return &analyticsv1.TrackClickRequest{
		EventId: "c14a43d6-c8fd-4fa2-ae0d-5be0d85910e5", LinkId: 42,
		Code: "ab3DeF", UserAgent: "test-agent", Referer: "https://example.org/",
		OccurredAt: timestamppb.New(time.Date(2026, 1, 2, 23, 59, 59, 123000000, time.UTC)),
	}
}

func TestTrackClickValidation(t *testing.T) {
	tests := []struct {
		name   string
		change func(*analyticsv1.TrackClickRequest)
	}{
		{"negative link", func(r *analyticsv1.TrackClickRequest) { r.LinkId = -1 }},
		{"empty id", func(r *analyticsv1.TrackClickRequest) { r.EventId = "" }},
		{"zero id", func(r *analyticsv1.TrackClickRequest) { r.EventId = "00000000-0000-0000-0000-000000000000" }},
		{"uppercase id", func(r *analyticsv1.TrackClickRequest) { r.EventId = strings.ToUpper(r.EventId) }},
		{"non v4 id", func(r *analyticsv1.TrackClickRequest) { r.EventId = "c14a43d6-c8fd-1fa2-ae0d-5be0d85910e5" }},
		{"invalid variant", func(r *analyticsv1.TrackClickRequest) { r.EventId = "c14a43d6-c8fd-4fa2-0e0d-5be0d85910e5" }},
		{"short code", func(r *analyticsv1.TrackClickRequest) { r.Code = "abc" }},
		{"non ascii code", func(r *analyticsv1.TrackClickRequest) { r.Code = "abcЖ5" }},
		{"missing timestamp", func(r *analyticsv1.TrackClickRequest) { r.OccurredAt = nil }},
		{"invalid timestamp", func(r *analyticsv1.TrackClickRequest) { r.OccurredAt = &timestamppb.Timestamp{Seconds: 253402300800} }},
		{"database timestamp range", func(r *analyticsv1.TrackClickRequest) {
			r.OccurredAt = timestamppb.New(time.Date(1800, 1, 1, 0, 0, 0, 0, time.UTC))
		}},
		{"large agent", func(r *analyticsv1.TrackClickRequest) { r.UserAgent = strings.Repeat("x", 4097) }},
		{"large referer", func(r *analyticsv1.TrackClickRequest) { r.Referer = strings.Repeat("x", 4097) }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := proto.Clone(validRequest()).(*analyticsv1.TrackClickRequest)
			tc.change(r)
			s := New(repositoryStub{save: func(context.Context, event.Click) error { t.Fatal("invalid event reached storage"); return nil }})
			if _, err := s.TrackClick(context.Background(), r); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("code=%v, err=%v", status.Code(err), err)
			}
		})
	}
	if _, err := New(repositoryStub{}).TrackClick(context.Background(), nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("nil request: %v", err)
	}
}

func TestTrackClickPreservesDeliveryIdentity(t *testing.T) {
	r := validRequest()
	var received []event.Click
	s := New(repositoryStub{save: func(_ context.Context, e event.Click) error { received = append(received, e); return nil }})
	for i := 0; i < 2; i++ {
		response, err := s.TrackClick(context.Background(), r)
		if err != nil || !response.GetSuccess() {
			t.Fatalf("track=%v %v", response, err)
		}
	}
	if len(received) != 2 || received[0] != received[1] {
		t.Fatalf("retry changed identity: %+v", received)
	}
	if received[0].ID != r.EventId || !received[0].OccurredAt.Equal(r.OccurredAt.AsTime()) {
		t.Fatalf("payload changed: %+v", received[0])
	}
}

func TestBackendErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want codes.Code
	}{
		{"canceled", context.Canceled, codes.Canceled},
		{"deadline", context.DeadlineExceeded, codes.DeadlineExceeded},
		{"database", errors.New("private database credentials must not leak"), codes.Unavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := New(repositoryStub{save: func(context.Context, event.Click) error { return tc.err }, stats: func(context.Context, int64) (repository.LinkStats, error) { return repository.LinkStats{}, tc.err }})
			_, err := s.TrackClick(context.Background(), validRequest())
			if status.Code(err) != tc.want {
				t.Fatalf("TrackClick: %v", err)
			}
			if strings.Contains(err.Error(), "private") {
				t.Fatalf("leaked backend error: %v", err)
			}
			_, err = s.GetLinkStats(context.Background(), &analyticsv1.GetLinkStatsRequest{LinkId: 42})
			if status.Code(err) != tc.want {
				t.Fatalf("GetLinkStats: %v", err)
			}
		})
	}
}

func TestGetLinkStats(t *testing.T) {
	s := New(repositoryStub{stats: func(_ context.Context, id int64) (repository.LinkStats, error) {
		if id != 42 {
			t.Fatalf("id=%d", id)
		}
		return repository.LinkStats{TotalClicks: 11, TodayClicks: 3}, nil
	}})
	if _, err := s.GetLinkStats(context.Background(), nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("nil request: %v", err)
	}
	got, err := s.GetLinkStats(context.Background(), &analyticsv1.GetLinkStatsRequest{LinkId: 42})
	if err != nil || got.GetTotalClicks() != 11 || got.GetTodayClicks() != 3 {
		t.Fatalf("stats=%v err=%v", got, err)
	}
}

func TestCanceledRequestDoesNotWrite(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s := New(repositoryStub{save: func(context.Context, event.Click) error { t.Fatal("canceled request reached storage"); return nil }})
	_, err := s.TrackClick(ctx, validRequest())
	if status.Code(err) != codes.Canceled {
		t.Fatalf("error=%v", err)
	}
}
