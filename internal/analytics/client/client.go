package client

import (
	"context"
	"time"

	analyticsv1 "linkpulse/gen/analytics/v1"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Client struct {
	client analyticsv1.AnalyticsServiceClient
}

type LinkStats struct {
	TotalClicks uint64
	TodayClicks uint64
}

func New(
	connection grpc.ClientConnInterface,
) *Client {
	return &Client{
		client: analyticsv1.NewAnalyticsServiceClient(
			connection,
		),
	}
}

func (c *Client) TrackClick(
	ctx context.Context,
	linkID int64,
	code string,
	userAgent string,
	referer string,
) error {
	_, err := c.client.TrackClick(
		ctx,
		&analyticsv1.TrackClickRequest{
			LinkId:    linkID,
			Code:      code,
			UserAgent: userAgent,
			Referer:   referer,
			OccurredAt: timestamppb.New(
				time.Now().UTC(),
			),
		},
	)

	return err
}

func (c *Client) GetLinkStats(
	ctx context.Context,
	linkID int64,
) (LinkStats, error) {
	response, err := c.client.GetLinkStats(
		ctx,
		&analyticsv1.GetLinkStatsRequest{
			LinkId: linkID,
		},
	)

	if err != nil {
		return LinkStats{}, err
	}

	return LinkStats{
		TotalClicks: response.GetTotalClicks(),
		TodayClicks: response.GetTodayClicks(),
	}, nil
}
