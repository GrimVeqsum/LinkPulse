package handler

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"

	analyticsclient "linkpulse/internal/analytics/client"
	"linkpulse/internal/link/repository"
	"linkpulse/internal/link/service"
)

type Handler struct {
	service   *service.Service
	analytics *analyticsclient.Client
}

func New(
	service *service.Service,
	analytics *analyticsclient.Client,
) *Handler {
	return &Handler{
		service:   service,
		analytics: analytics,
	}
}

func (h *Handler) RegisterRoutes(
	mux *http.ServeMux,
) {
	mux.HandleFunc(
		"GET /health",
		h.health,
	)

	mux.HandleFunc(
		"POST /links",
		h.createLink,
	)

	mux.HandleFunc(
		"GET /links/{code}/stats",
		h.getStats,
	)

	mux.HandleFunc(
		"GET /{code}",
		h.redirect,
	)
}

type createLinkRequest struct {
	URL string `json:"url"`
}

type createLinkResponse struct {
	Code      string    `json:"code"`
	URL       string    `json:"url"`
	CreatedAt time.Time `json:"created_at"`
}

type statsResponse struct {
	Code        string `json:"code"`
	TotalClicks uint64 `json:"total_clicks"`
	TodayClicks uint64 `json:"today_clicks"`
}

func (h *Handler) health(
	w http.ResponseWriter,
	r *http.Request,
) {
	w.WriteHeader(http.StatusOK)

	_, _ = w.Write([]byte("OK"))
}

func (h *Handler) createLink(
	w http.ResponseWriter,
	r *http.Request,
) {
	var request createLinkRequest

	if err := json.NewDecoder(
		r.Body,
	).Decode(&request); err != nil {
		http.Error(
			w,
			"invalid request",
			http.StatusBadRequest,
		)
		return
	}

	link, err := h.service.CreateLink(
		r.Context(),
		request.URL,
	)

	if err != nil {
		if errors.Is(
			err,
			service.ErrInvalidURL,
		) {
			http.Error(
				w,
				"invalid url",
				http.StatusBadRequest,
			)
			return
		}

		http.Error(
			w,
			"internal server error",
			http.StatusInternalServerError,
		)
		return
	}

	response := createLinkResponse{
		Code:      link.Code,
		URL:       link.TargetURL,
		CreatedAt: link.CreatedAt,
	}

	writeJSON(
		w,
		http.StatusCreated,
		response,
	)
}

func (h *Handler) redirect(
	w http.ResponseWriter,
	r *http.Request,
) {
	code := r.PathValue("code")

	link, err := h.service.GetByCode(
		r.Context(),
		code,
	)

	if err != nil {
		if errors.Is(
			err,
			repository.ErrLinkNotFound,
		) {
			http.Error(
				w,
				"link not found",
				http.StatusNotFound,
			)
			return
		}

		http.Error(
			w,
			"internal server error",
			http.StatusInternalServerError,
		)
		return
	}

	userAgent := r.UserAgent()
	referer := r.Referer()

	go func() {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			time.Second,
		)
		defer cancel()

		if err := h.analytics.TrackClick(
			ctx,
			link.ID,
			link.Code,
			userAgent,
			referer,
		); err != nil {
			log.Printf(
				"failed to track click: %v",
				err,
			)
		}
	}()

	http.Redirect(
		w,
		r,
		link.TargetURL,
		http.StatusFound,
	)
}

func (h *Handler) getStats(
	w http.ResponseWriter,
	r *http.Request,
) {
	code := r.PathValue("code")

	link, err := h.service.GetByCode(
		r.Context(),
		code,
	)

	if err != nil {
		if errors.Is(
			err,
			repository.ErrLinkNotFound,
		) {
			http.Error(
				w,
				"link not found",
				http.StatusNotFound,
			)
			return
		}

		http.Error(
			w,
			"internal server error",
			http.StatusInternalServerError,
		)
		return
	}

	ctx, cancel := context.WithTimeout(
		r.Context(),
		2*time.Second,
	)
	defer cancel()

	stats, err := h.analytics.GetLinkStats(
		ctx,
		link.ID,
	)

	if err != nil {
		http.Error(
			w,
			"analytics unavailable",
			http.StatusServiceUnavailable,
		)
		return
	}

	writeJSON(
		w,
		http.StatusOK,
		statsResponse{
			Code:        link.Code,
			TotalClicks: stats.TotalClicks,
			TodayClicks: stats.TodayClicks,
		},
	)
}

func writeJSON(
	w http.ResponseWriter,
	status int,
	value any,
) {
	w.Header().Set(
		"Content-Type",
		"application/json",
	)

	w.WriteHeader(status)

	_ = json.NewEncoder(w).Encode(value)
}
