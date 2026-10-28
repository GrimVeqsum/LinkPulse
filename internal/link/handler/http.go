package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"

	analyticsclient "linkpulse/internal/analytics/client"
	"linkpulse/internal/event"
	"linkpulse/internal/link/repository"
	"linkpulse/internal/link/service"
)

const (
	maxRequestBytes  = 16 << 10
	maxMetadataBytes = 4096
	requestTimeout   = 3 * time.Second
)

type analyticsStats interface {
	GetLinkStats(context.Context, int64) (analyticsclient.LinkStats, error)
}

type eventStore interface {
	Enqueue(context.Context, event.Click) error
}

type Handler struct {
	service   *service.Service
	analytics analyticsStats
	outbox    eventStore
	ready     func(context.Context) error
}

func New(service *service.Service, analytics analyticsStats, outbox eventStore, ready func(context.Context) error) *Handler {
	return &Handler{service: service, analytics: analytics, outbox: outbox, ready: ready}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /health", h.health)
	mux.HandleFunc("GET /ready", h.withTimeout(h.readiness))
	mux.HandleFunc("POST /links", h.withTimeout(h.createLink))
	mux.HandleFunc("GET /links/{code}/stats", h.withTimeout(h.getStats))
	mux.HandleFunc("GET /{code}", h.withTimeout(h.redirect))
}

func (h *Handler) withTimeout(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
		defer cancel()
		next(w, r.WithContext(ctx))
	}
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

func (h *Handler) health(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

func (h *Handler) readiness(w http.ResponseWriter, r *http.Request) {
	// Analytics can be offline while redirects are safely accepted into the outbox.
	if h.ready == nil || h.ready(r.Context()) != nil {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

func (h *Handler) createLink(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		} else {
			http.Error(w, "invalid request", http.StatusBadRequest)
		}
		return
	}
	request, err := decodeCreateRequest(body)
	if err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	link, err := h.service.CreateLink(r.Context(), request.URL)
	if err != nil {
		if errors.Is(err, service.ErrInvalidURL) {
			http.Error(w, "invalid url", http.StatusBadRequest)
			return
		}
		h.writeRepositoryError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, createLinkResponse{Code: link.Code, URL: link.TargetURL, CreatedAt: link.CreatedAt})
}

// Decode exactly one object containing one string field. Reject duplicate keys,
// unknown fields, null, trailing objects and type mismatches rather than silently
// choosing one interpretation of an ambiguous request.
func decodeCreateRequest(body []byte) (createLinkRequest, error) {
	if !utf8.Valid(body) {
		return createLinkRequest{}, errors.New("request body must be valid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return createLinkRequest{}, errors.New("expected JSON object")
	}
	var request createLinkRequest
	seenURL := false
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil || token != "url" || seenURL {
			return createLinkRequest{}, errors.New("unexpected or duplicate field")
		}
		seenURL = true
		var value any
		if err := decoder.Decode(&value); err != nil {
			return createLinkRequest{}, err
		}
		var ok bool
		request.URL, ok = value.(string)
		if !ok {
			return createLinkRequest{}, errors.New("url must be a string")
		}
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') || !seenURL {
		return createLinkRequest{}, errors.New("missing url or closing object")
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return createLinkRequest{}, errors.New("unexpected trailing JSON")
	}
	return request, nil
}

func (h *Handler) redirect(w http.ResponseWriter, r *http.Request) {
	link, err := h.service.GetByCode(r.Context(), r.PathValue("code"))
	if err != nil {
		h.writeRepositoryError(w, err)
		return
	}
	userAgent := r.UserAgent()
	referer := r.Referer()
	if len(userAgent) > maxMetadataBytes || len(referer) > maxMetadataBytes || !utf8.ValidString(userAgent) || !utf8.ValidString(referer) {
		http.Error(w, "invalid request metadata", http.StatusBadRequest)
		return
	}
	id, err := uuid.NewRandom()
	if err != nil {
		log.Printf("generate click identity: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	if err := h.outbox.Enqueue(r.Context(), event.Click{
		ID: id.String(), LinkID: link.ID, Code: link.Code,
		UserAgent: userAgent, Referer: referer, OccurredAt: time.Now().UTC(),
	}); err != nil {
		log.Printf("persist click event: %v", err)
		http.Error(w, "analytics persistence unavailable", http.StatusServiceUnavailable)
		return
	}
	http.Redirect(w, r, link.TargetURL, http.StatusFound)
}

func (h *Handler) getStats(w http.ResponseWriter, r *http.Request) {
	link, err := h.service.GetByCode(r.Context(), r.PathValue("code"))
	if err != nil {
		h.writeRepositoryError(w, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	stats, err := h.analytics.GetLinkStats(ctx, link.ID)
	if err != nil {
		log.Printf("get link stats: %v", err)
		http.Error(w, "analytics unavailable", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, statsResponse{Code: link.Code, TotalClicks: stats.TotalClicks, TodayClicks: stats.TodayClicks})
}

func (h *Handler) writeRepositoryError(w http.ResponseWriter, err error) {
	if errors.Is(err, repository.ErrLinkNotFound) {
		http.Error(w, "link not found", http.StatusNotFound)
		return
	}
	log.Printf("link repository: %v", err)
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		http.Error(w, "database unavailable", http.StatusServiceUnavailable)
		return
	}
	http.Error(w, "internal server error", http.StatusInternalServerError)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
