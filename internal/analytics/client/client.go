package client

import (
	"context"
	"fmt"
	"time"

	analyticsv1 "linkpulse/gen/analytics/v1"
	"linkpulse/internal/event"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const rpcTimeout = 2 * time.Second

type Client struct {
	client analyticsv1.AnalyticsServiceClient
	health healthv1.HealthClient
}

type LinkStats struct {
	TotalClicks uint64
	TodayClicks uint64
}

func New(connection grpc.ClientConnInterface) *Client {
	return &Client{
		client: analyticsv1.NewAnalyticsServiceClient(connection),
		health: healthv1.NewHealthClient(connection),
	}
}

// TrackClick sends the immutable persisted payload, including its original
// timestamp. A retry must reuse the same ID; a new ID represents a new click.
func (c *Client) TrackClick(ctx context.Context, event event.Click) error {
	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	response, err := c.client.TrackClick(ctx, &analyticsv1.TrackClickRequest{
		EventId:    event.ID,
		LinkId:     event.LinkID,
		Code:       event.Code,
		UserAgent:  event.UserAgent,
		Referer:    event.Referer,
		OccurredAt: timestamppb.New(event.OccurredAt.UTC()),
	})
	if err != nil {
		return fmt.Errorf("track click: %w", err)
	}
	if !response.GetSuccess() {
		return status.Error(codes.Unavailable, "analytics did not acknowledge the event")
	}
	return nil
}

func (c *Client) GetLinkStats(ctx context.Context, linkID int64) (LinkStats, error) {
	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	response, err := c.client.GetLinkStats(ctx, &analyticsv1.GetLinkStatsRequest{LinkId: linkID})
	if err != nil {
		return LinkStats{}, fmt.Errorf("get link stats: %w", err)
	}
	return LinkStats{TotalClicks: response.GetTotalClicks(), TodayClicks: response.GetTodayClicks()}, nil
}

// Ready checks the health service, which in turn checks ClickHouse.
func (c *Client) Ready(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	response, err := c.health.Check(ctx, &healthv1.HealthCheckRequest{})
	if err != nil {
		return fmt.Errorf("analytics health: %w", err)
	}
	if response.GetStatus() != healthv1.HealthCheckResponse_SERVING {
		return status.Error(codes.Unavailable, "analytics is not serving")
	}
	return nil
}
