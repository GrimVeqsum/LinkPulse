package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"linkpulse/internal/link/repository"
	"linkpulse/internal/link/service"
)

type Handler struct {
	service *service.Service
}

func New(service *service.Service) *Handler {
	return &Handler{
		service: service,
	}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /health", h.health)
	mux.HandleFunc("POST /links", h.createLink)
	mux.HandleFunc("GET /{code}", h.redirect)
}

type createLinkRequest struct {
	URL string `json:"url"`
}

type createLinkResponse struct {
	Code      string    `json:"code"`
	URL       string    `json:"url"`
	CreatedAt time.Time `json:"created_at"`
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

	decoder := json.NewDecoder(r.Body)

	if err := decoder.Decode(&request); err != nil {
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
		if errors.Is(err, service.ErrInvalidURL) {
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

	w.Header().Set(
		"Content-Type",
		"application/json",
	)

	w.WriteHeader(http.StatusCreated)

	_ = json.NewEncoder(w).Encode(response)
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
		if errors.Is(err, repository.ErrLinkNotFound) {
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

	http.Redirect(
		w,
		r,
		link.TargetURL,
		http.StatusFound,
	)
}
