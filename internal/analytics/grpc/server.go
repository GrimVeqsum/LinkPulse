package grpc

import (
	"context"
	"errors"
	"log"

	analyticsv1 "linkpulse/gen/analytics/v1"
	"linkpulse/internal/analytics/repository"
	"linkpulse/internal/event"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Repository interface {
	SaveClick(context.Context, event.Click) error
	GetLinkStats(context.Context, int64) (repository.LinkStats, error)
}

type Server struct {
	analyticsv1.UnimplementedAnalyticsServiceServer
	repository Repository
}

func New(repository Repository) *Server {
	return &Server{repository: repository}
}

func (s *Server) TrackClick(ctx context.Context, request *analyticsv1.TrackClickRequest) (*analyticsv1.TrackClickResponse, error) {
	if request.GetLinkId() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "link_id must be positive")
	}
	id, err := uuid.Parse(request.GetEventId())
	if err != nil || id.Version() != 4 || id.Variant() != uuid.RFC4122 || id.String() != request.GetEventId() {
		return nil, status.Error(codes.InvalidArgument, "event_id must be a canonical UUIDv4")
	}
	if !validCode(request.GetCode()) {
		return nil, status.Error(codes.InvalidArgument, "code must contain six ASCII letters or digits")
	}
	if len(request.GetUserAgent()) > 4096 || len(request.GetReferer()) > 4096 {
		return nil, status.Error(codes.InvalidArgument, "event metadata exceeds 4096 bytes")
	}
	if request.GetOccurredAt() == nil || request.GetOccurredAt().CheckValid() != nil {
		return nil, status.Error(codes.InvalidArgument, "occurred_at must be a valid timestamp")
	}
	// ClickHouse DateTime64(3) has a narrower range than protobuf Timestamp.
	occurredAt := request.GetOccurredAt().AsTime().UTC()
	if occurredAt.Year() < 1900 || occurredAt.Year() > 2299 {
		return nil, status.Error(codes.InvalidArgument, "occurred_at must be within 1900..2299")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if err := s.repository.SaveClick(ctx, event.Click{
		ID: id.String(), LinkID: request.GetLinkId(), Code: request.GetCode(),
		UserAgent: request.GetUserAgent(), Referer: request.GetReferer(), OccurredAt: occurredAt,
	}); err != nil {
		return nil, backendError("save click", err)
	}
	return &analyticsv1.TrackClickResponse{Success: true}, nil
}

func (s *Server) GetLinkStats(ctx context.Context, request *analyticsv1.GetLinkStatsRequest) (*analyticsv1.GetLinkStatsResponse, error) {
	if request.GetLinkId() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "link_id must be positive")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	stats, err := s.repository.GetLinkStats(ctx, request.GetLinkId())
	if err != nil {
		return nil, backendError("get link stats", err)
	}
	return &analyticsv1.GetLinkStatsResponse{TotalClicks: stats.TotalClicks, TodayClicks: stats.TodayClicks}, nil
}

func backendError(operation string, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	log.Printf("analytics %s: %v", operation, err)
	return status.Error(codes.Unavailable, "analytics storage unavailable")
}

func validCode(code string) bool {
	if len(code) != 6 {
		return false
	}
	for _, ch := range code {
		if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9') {
			return false
		}
	}
	return true
}
