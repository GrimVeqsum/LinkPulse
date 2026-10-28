package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	analyticsclient "linkpulse/internal/analytics/client"
	"linkpulse/internal/event"
	"linkpulse/internal/link/model"
	"linkpulse/internal/link/repository"
	"linkpulse/internal/link/service"
)

type fakeRepository struct {
	create func(context.Context, *model.Link) error
	get    func(context.Context, string) (model.Link, error)
}

func (r fakeRepository) Create(ctx context.Context, link *model.Link) error {
	if r.create != nil {
		return r.create(ctx, link)
	}
	link.ID = 42
	link.CreatedAt = time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)
	return nil
}

func (r fakeRepository) GetByCode(ctx context.Context, code string) (model.Link, error) {
	if r.get != nil {
		return r.get(ctx, code)
	}
	return model.Link{ID: 42, Code: code, TargetURL: "https://example.com/destination"}, nil
}

type fakeAnalytics func(context.Context, int64) (analyticsclient.LinkStats, error)

func (f fakeAnalytics) GetLinkStats(ctx context.Context, id int64) (analyticsclient.LinkStats, error) {
	return f(ctx, id)
}

type fakeOutbox func(context.Context, event.Click) error

func (f fakeOutbox) Enqueue(ctx context.Context, click event.Click) error { return f(ctx, click) }

func testMux(repo fakeRepository, analytics fakeAnalytics, outbox fakeOutbox, ready func(context.Context) error) *http.ServeMux {
	if analytics == nil {
		analytics = func(context.Context, int64) (analyticsclient.LinkStats, error) {
			return analyticsclient.LinkStats{TotalClicks: 5, TodayClicks: 2}, nil
		}
	}
	if outbox == nil {
		outbox = func(context.Context, event.Click) error { return nil }
	}
	if ready == nil {
		ready = func(context.Context) error { return nil }
	}
	mux := http.NewServeMux()
	New(service.New(repo), analytics, outbox, ready).RegisterRoutes(mux)
	return mux
}

func perform(mux http.Handler, method, path, body string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(method, path, strings.NewReader(body)))
	return w
}

func TestCreateLink(t *testing.T) {
	w := perform(testMux(fakeRepository{}, nil, nil, nil), "POST", "/links", `{"url":"https://example.com/path"}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if w.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("content-type=%q", w.Header().Get("Content-Type"))
	}
	var response createLinkResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Code) != 6 || response.URL != "https://example.com/path" || response.CreatedAt.IsZero() {
		t.Fatalf("response=%+v", response)
	}
}

func TestCreateLinkRejectsBadInput(t *testing.T) {
	tests := []struct {
		name, body string
		status     int
	}{
		{"empty", "", http.StatusBadRequest},
		{"broken", `{"url":`, http.StatusBadRequest},
		{"null", `null`, http.StatusBadRequest},
		{"array", `[]`, http.StatusBadRequest},
		{"missing URL", `{}`, http.StatusBadRequest},
		{"null URL", `{"url":null}`, http.StatusBadRequest},
		{"numeric URL", `{"url":42}`, http.StatusBadRequest},
		{"unknown field", `{"url":"https://example.com","extra":true}`, http.StatusBadRequest},
		{"duplicate URL", `{"url":"https://example.com","url":"https://other.example"}`, http.StatusBadRequest},
		{"trailing object", `{"url":"https://example.com"} {}`, http.StatusBadRequest},
		{"trailing garbage", `{"url":"https://example.com"} false`, http.StatusBadRequest},
		{"invalid URL", `{"url":"javascript:alert(1)"}`, http.StatusBadRequest},
		{"invalid UTF8 URL path", "{\"url\":\"https://example.com/a\xffb\"}", http.StatusBadRequest},
		{"URL too long", `{"url":"https://example.com/` + strings.Repeat("a", 8192) + `"}`, http.StatusBadRequest},
		{"body too long", `{"url":"https://example.com/` + strings.Repeat("a", maxRequestBytes) + `"}`, http.StatusRequestEntityTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			mux := testMux(fakeRepository{create: func(context.Context, *model.Link) error { calls++; return nil }}, nil, nil, nil)
			w := perform(mux, "POST", "/links", tt.body)
			if w.Code != tt.status || calls != 0 {
				t.Fatalf("status=%d body=%s repository calls=%d", w.Code, w.Body.String(), calls)
			}
		})
	}
}

func TestCreateLinkRepositoryError(t *testing.T) {
	for _, tt := range []struct {
		name   string
		err    error
		status int
	}{
		{"storage", errors.New("connection credentials must not leak"), http.StatusInternalServerError},
		{"deadline", context.DeadlineExceeded, http.StatusServiceUnavailable},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mux := testMux(fakeRepository{create: func(context.Context, *model.Link) error { return tt.err }}, nil, nil, nil)
			w := perform(mux, "POST", "/links", `{"url":"https://example.com"}`)
			if w.Code != tt.status || strings.Contains(w.Body.String(), tt.err.Error()) {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
		})
	}
}

type redirectWitness struct {
	*httptest.ResponseRecorder
	persisted *bool
	t         *testing.T
}

func (w redirectWitness) WriteHeader(status int) {
	if status == http.StatusFound && !*w.persisted {
		w.t.Error("redirect sent before event persisted")
	}
	w.ResponseRecorder.WriteHeader(status)
}

func TestRedirectPersistsBeforeResponse(t *testing.T) {
	persisted := false
	var stored event.Click
	mux := testMux(fakeRepository{}, nil, func(ctx context.Context, click event.Click) error {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > requestTimeout {
			t.Error("outbox operation has no bounded deadline")
		}
		stored = click
		persisted = true
		return nil
	}, nil)
	request := httptest.NewRequest("GET", "/abc123", nil)
	request.Header.Set("User-Agent", "test-client")
	request.Header.Set("Referer", "https://source.example/page")
	recorder := httptest.NewRecorder()
	before := time.Now()
	mux.ServeHTTP(redirectWitness{recorder, &persisted, t}, request)
	if recorder.Code != http.StatusFound || recorder.Header().Get("Location") != "https://example.com/destination" {
		t.Fatalf("status=%d location=%q", recorder.Code, recorder.Header().Get("Location"))
	}
	id, err := uuid.Parse(stored.ID)
	if err != nil || id.Version() != 4 || id == uuid.Nil {
		t.Fatalf("event ID=%q err=%v", stored.ID, err)
	}
	if stored.LinkID != 42 || stored.Code != "abc123" || stored.UserAgent != "test-client" || stored.Referer != "https://source.example/page" || stored.OccurredAt.Location() != time.UTC || stored.OccurredAt.Before(before) || stored.OccurredAt.After(time.Now()) {
		t.Fatalf("stored event=%+v", stored)
	}
}

func TestRedirectPersistenceFailureReturns503(t *testing.T) {
	calls := 0
	mux := testMux(fakeRepository{}, nil, func(context.Context, event.Click) error { calls++; return errors.New("postgres offline") }, nil)
	w := perform(mux, "GET", "/abc123", "")
	if w.Code != http.StatusServiceUnavailable || w.Header().Get("Location") != "" || calls != 1 {
		t.Fatalf("status=%d location=%q calls=%d", w.Code, w.Header().Get("Location"), calls)
	}
}

func TestRedirectRejectsOversizedMetadata(t *testing.T) {
	for _, header := range []string{"User-Agent", "Referer"} {
		t.Run(header, func(t *testing.T) {
			calls := 0
			mux := testMux(fakeRepository{}, nil, func(context.Context, event.Click) error { calls++; return nil }, nil)
			r := httptest.NewRequest("GET", "/abc123", nil)
			r.Header.Set(header, strings.Repeat("x", maxMetadataBytes+1))
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, r)
			if w.Code != http.StatusBadRequest || calls != 0 || w.Header().Get("Location") != "" {
				t.Fatalf("status=%d calls=%d", w.Code, calls)
			}
		})
	}
}

func TestRedirectRejectsInvalidUTF8Metadata(t *testing.T) {
	for _, header := range []string{"User-Agent", "Referer"} {
		t.Run(header, func(t *testing.T) {
			calls := 0
			mux := testMux(fakeRepository{}, nil, func(context.Context, event.Click) error { calls++; return nil }, nil)
			r := httptest.NewRequest("GET", "/abc123", nil)
			r.Header.Set(header, string([]byte{0xff}))
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, r)
			if w.Code != http.StatusBadRequest || calls != 0 || w.Header().Get("Location") != "" {
				t.Fatalf("status=%d calls=%d", w.Code, calls)
			}
		})
	}
}

func TestLinkNotFoundAndRepositoryFailures(t *testing.T) {
	for _, path := range []string{"/abc123", "/links/abc123/stats"} {
		for _, tt := range []struct {
			name   string
			err    error
			status int
		}{
			{"missing", repository.ErrLinkNotFound, http.StatusNotFound},
			{"database", errors.New("database failure"), http.StatusInternalServerError},
			{"deadline", context.DeadlineExceeded, http.StatusServiceUnavailable},
		} {
			t.Run(path+"/"+tt.name, func(t *testing.T) {
				mux := testMux(fakeRepository{get: func(context.Context, string) (model.Link, error) { return model.Link{}, tt.err }}, nil, nil, nil)
				w := perform(mux, "GET", path, "")
				if w.Code != tt.status {
					t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
				}
			})
		}
	}
	mux := testMux(fakeRepository{get: func(context.Context, string) (model.Link, error) {
		t.Fatal("invalid code reached repository")
		return model.Link{}, nil
	}}, nil, nil, nil)
	for _, path := range []string{"/short", "/invalid-code", "/links/short/stats"} {
		if w := perform(mux, "GET", path, ""); w.Code != http.StatusNotFound {
			t.Fatalf("path=%s status=%d", path, w.Code)
		}
	}
}

func TestGetStats(t *testing.T) {
	w := perform(testMux(fakeRepository{}, nil, nil, nil), "GET", "/links/abc123/stats", "")
	var response statsResponse
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Code != "abc123" || response.TotalClicks != 5 || response.TodayClicks != 2 {
		t.Fatalf("response=%+v", response)
	}
}

func TestStatsDependencyErrorsAndDeadline(t *testing.T) {
	for _, failure := range []error{errors.New("rpc unavailable"), context.DeadlineExceeded, context.Canceled} {
		mux := testMux(fakeRepository{}, func(ctx context.Context, id int64) (analyticsclient.LinkStats, error) {
			deadline, ok := ctx.Deadline()
			if !ok || time.Until(deadline) > 2*time.Second || id != 42 {
				t.Fatalf("deadline=%v bounded=%v id=%d", deadline, ok, id)
			}
			return analyticsclient.LinkStats{}, failure
		}, nil, nil)
		if w := perform(mux, "GET", "/links/abc123/stats", ""); w.Code != http.StatusServiceUnavailable {
			t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
		}
	}
}

func TestHealthReadinessAndMethods(t *testing.T) {
	readyCalls := 0
	mux := testMux(fakeRepository{}, nil, nil, func(ctx context.Context) error {
		readyCalls++
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("readiness missing deadline")
		}
		return errors.New("postgres offline")
	})
	if w := perform(mux, "GET", "/health", ""); w.Code != http.StatusOK || w.Body.String() != "OK" || readyCalls != 0 {
		t.Fatalf("health=%d body=%s readiness calls=%d", w.Code, w.Body.String(), readyCalls)
	}
	if w := perform(mux, "GET", "/ready", ""); w.Code != http.StatusServiceUnavailable || readyCalls != 1 {
		t.Fatalf("readiness=%d calls=%d", w.Code, readyCalls)
	}
	if w := perform(testMux(fakeRepository{}, nil, nil, nil), "GET", "/ready", ""); w.Code != http.StatusOK {
		t.Fatalf("ready=%d", w.Code)
	}
	for _, path := range []string{"/health", "/ready", "/abc123", "/links/abc123/stats"} {
		if w := perform(mux, "DELETE", path, ""); w.Code != http.StatusMethodNotAllowed {
			t.Fatalf("path=%s status=%d", path, w.Code)
		}
	}
}

func TestConcurrentRedirects(t *testing.T) {
	var mu sync.Mutex
	ids := map[string]bool{}
	mux := testMux(fakeRepository{}, nil, func(_ context.Context, click event.Click) error {
		mu.Lock()
		defer mu.Unlock()
		if ids[click.ID] {
			return fmt.Errorf("reused event identity %s", click.ID)
		}
		ids[click.ID] = true
		return nil
	}, nil)
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if w := perform(mux, "GET", "/abc123", ""); w.Code != http.StatusFound {
				t.Errorf("status=%d", w.Code)
			}
		}()
	}
	wg.Wait()
	if len(ids) != 32 {
		t.Fatalf("events=%d", len(ids))
	}
}
