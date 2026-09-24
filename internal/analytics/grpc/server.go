package grpc

import (
	"context"
	"time"

	analyticsv1 "linkpulse/gen/analytics/v1"
	"linkpulse/internal/analytics/repository"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	analyticsv1.UnimplementedAnalyticsServiceServer

	repository *repository.Repository
}

func New(
	repository *repository.Repository,
) *Server {
	return &Server{
		repository: repository,
	}
}

func (s *Server) TrackClick(
	ctx context.Context,
	request *analyticsv1.TrackClickRequest,
) (*analyticsv1.TrackClickResponse, error) {
	if request.GetLinkId() <= 0 {
		return nil, status.Error(
			codes.InvalidArgument,
			"link_id must be positive",
		)
	}

	occurredAt := time.Now().UTC()

	if request.GetOccurredAt() != nil {
		occurredAt = request.
			GetOccurredAt().
			AsTime().
			UTC()
	}

	event := repository.ClickEvent{
		LinkID:     request.GetLinkId(),
		Code:       request.GetCode(),
		UserAgent:  request.GetUserAgent(),
		Referer:    request.GetReferer(),
		OccurredAt: occurredAt,
	}

	if err := s.repository.SaveClick(
		ctx,
		event,
	); err != nil {
		return nil, status.Error(
			codes.Internal,
			"failed to save click",
		)
	}

	return &analyticsv1.TrackClickResponse{
		Success: true,
	}, nil
}

func (s *Server) GetLinkStats(
	ctx context.Context,
	request *analyticsv1.GetLinkStatsRequest,
) (*analyticsv1.GetLinkStatsResponse, error) {
	if request.GetLinkId() <= 0 {
		return nil, status.Error(
			codes.InvalidArgument,
			"link_id must be positive",
		)
	}

	stats, err := s.repository.GetLinkStats(
		ctx,
		request.GetLinkId(),
	)

	if err != nil {
		return nil, status.Error(
			codes.Internal,
			"failed to get link stats",
		)
	}

	return &analyticsv1.GetLinkStatsResponse{
		TotalClicks: stats.TotalClicks,
		TodayClicks: stats.TodayClicks,
	}, nil
}
