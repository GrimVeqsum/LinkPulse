//go:build integration

package integration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"

	analyticsv1 "linkpulse/gen/analytics/v1"
	analyticsclient "linkpulse/internal/analytics/client"
	analyticsgrpc "linkpulse/internal/analytics/grpc"
	analyticsrepo "linkpulse/internal/analytics/repository"
	"linkpulse/internal/event"
	"linkpulse/internal/link/handler"
	"linkpulse/internal/link/model"
	linkrepo "linkpulse/internal/link/repository"
	"linkpulse/internal/link/service"
	"linkpulse/internal/outbox"
	"linkpulse/migrations"
)

// These tests require explicit configuration and real databases. They never
// silently skip when invoked with -tags=integration. Every fixture owns its own
// generated schema/database, keeping app-service workers and tests independent.
type fixture struct {
	pool       *pgxpool.Pool
	db         *sql.DB
	links      *linkrepo.PostgresRepository
	clicks     *analyticsrepo.Repository
	service    *service.Service
	client     *analyticsclient.Client
	server     *grpc.Server
	address    string
	http       *httptest.Server
	httpClient *http.Client
}

func required(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("%s is required; use docker-compose.test.yml", name)
	}
	return value
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	postgresURL := required(t, "TEST_POSTGRES_URL")
	admin, err := pgxpool.New(ctx, postgresURL)
	if err != nil {
		t.Fatal(err)
	}
	name := "lp_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, `CREATE SCHEMA "`+name+`"`); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		if _, err := admin.Exec(cleanupCtx, `DROP SCHEMA "`+name+`" CASCADE`); err != nil {
			t.Errorf("clean temporary PostgreSQL schema: %v", err)
		}
		admin.Close()
	})
	cfg, err := pgxpool.ParseConfig(postgresURL)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = name
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := migrations.UpPostgres(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if err := migrations.UpPostgres(ctx, pool); err != nil {
		t.Fatalf("restart PostgreSQL migrator with existing history: %v", err)
	}
	options := &clickhouse.Options{
		Addr: []string{required(t, "TEST_CLICKHOUSE_ADDR")},
		Auth: clickhouse.Auth{
			Database: required(t, "TEST_CLICKHOUSE_DATABASE"),
			Username: required(t, "TEST_CLICKHOUSE_USER"),
			Password: required(t, "TEST_CLICKHOUSE_PASSWORD"),
		},
		DialTimeout: 3 * time.Second,
		// Queries must use UTC even when the database session has another zone.
		Settings: clickhouse.Settings{"session_timezone": "Asia/Tokyo"},
	}
	chAdmin := clickhouse.OpenDB(options)
	if _, err := chAdmin.ExecContext(ctx, "CREATE DATABASE "+name); err != nil {
		chAdmin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		if _, err := chAdmin.ExecContext(cleanupCtx, "DROP DATABASE "+name); err != nil {
			t.Errorf("clean temporary ClickHouse database: %v", err)
		}
		chAdmin.Close()
	})
	options.Auth.Database = name
	db := clickhouse.OpenDB(options)
	t.Cleanup(func() { db.Close() })
	if err := migrations.UpClickHouse(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err := migrations.UpClickHouse(ctx, db); err != nil {
		t.Fatalf("restart ClickHouse migrator with existing history: %v", err)
	}
	f := &fixture{pool: pool, db: db, links: linkrepo.NewPostgresRepository(pool), clicks: analyticsrepo.New(db)}
	f.service = service.New(f.links)
	f.startAnalytics(t)
	t.Cleanup(func() {
		if f.server != nil {
			f.server.Stop()
		}
	})
	connection, err := grpc.NewClient(f.address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: backoff.Config{BaseDelay: 10 * time.Millisecond, Multiplier: 1.2, Jitter: 0.1, MaxDelay: 50 * time.Millisecond}, MinConnectTimeout: 100 * time.Millisecond}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.Close() })
	f.client = analyticsclient.New(connection)
	mux := http.NewServeMux()
	handler.New(f.service, f.client, f.links, f.links.Ping).RegisterRoutes(mux)
	f.http = httptest.NewServer(mux)
	t.Cleanup(f.http.Close)
	f.httpClient = &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return f
}

func (f *fixture) startAnalytics(t *testing.T) {
	t.Helper()
	address := f.address
	if address == "" {
		address = "127.0.0.1:0"
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	f.address = listener.Addr().String()
	f.server = grpc.NewServer()
	analyticsv1.RegisterAnalyticsServiceServer(f.server, analyticsgrpc.New(f.clicks))
	server := f.server
	go func() { _ = server.Serve(listener) }()
}

func (f *fixture) startWorker(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	worker := outbox.New(f.links, f.client, outbox.Options{
		PollInterval: 10 * time.Millisecond, BatchSize: 4,
		DeliveryTimeout: 200 * time.Millisecond,
		MinBackoff:      50 * time.Millisecond, MaxBackoff: 200 * time.Millisecond,
	})
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("stop worker: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("worker did not stop after cancellation")
		}
	})
}

func (f *fixture) create(t *testing.T) model.Link {
	t.Helper()
	response, err := f.httpClient.Post(f.http.URL+"/links", "application/json", bytes.NewBufferString(`{"url":"https://example.org/documentation"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d", response.StatusCode)
	}
	var body struct {
		Code string `json:"code"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	link, err := f.service.GetByCode(context.Background(), body.Code)
	if err != nil {
		t.Fatal(err)
	}
	return link
}

func (f *fixture) pending(linkID int64) (int, error) {
	var count int
	err := f.pool.QueryRow(context.Background(), "SELECT count(*) FROM click_outbox WHERE link_id=$1", linkID).Scan(&count)
	return count, err
}

func eventually(t *testing.T, check func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		ok, err := check()
		if ok && err == nil {
			return
		}
		lastErr = err
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("condition did not become true: last error: %v", lastErr)
}

func (f *fixture) expectStats(t *testing.T, link model.Link, total uint64, occurrences ...time.Time) {
	t.Helper()
	eventually(t, func() (bool, error) {
		now := time.Now().UTC()
		start := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
		end := start.Add(24 * time.Hour)
		var today uint64
		for _, at := range occurrences {
			if !at.Before(start) && at.Before(end) {
				today++
			}
		}
		response, err := f.httpClient.Get(f.http.URL + "/links/" + link.Code + "/stats")
		if err != nil {
			return false, err
		}
		defer response.Body.Close()
		var body struct {
			Total uint64 `json:"total_clicks"`
			Today uint64 `json:"today_clicks"`
		}
		if response.StatusCode != http.StatusOK {
			return false, fmt.Errorf("stats status %d", response.StatusCode)
		}
		if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
			return false, err
		}
		if !time.Now().UTC().Before(end) {
			// Retry if UTC midnight occurred during this particular request.
			return false, nil
		}
		if body.Total != total || body.Today != today {
			return false, fmt.Errorf("got total=%d today=%d; want %d/%d", body.Total, body.Today, total, today)
		}
		return true, nil
	})
}

func clickFor(link model.Link, at time.Time) event.Click {
	return event.Click{ID: uuid.NewString(), LinkID: link.ID, Code: link.Code, UserAgent: "integration-test", Referer: "https://example.org", OccurredAt: at.UTC()}
}

func TestDurablePipelineAndAnalyticsRecovery(t *testing.T) {
	f := newFixture(t)
	link := f.create(t)
	response, err := f.httpClient.Get(f.http.URL + "/" + link.Code)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusFound || response.Header.Get("Location") != link.TargetURL {
		t.Fatalf("redirect status=%d location=%q", response.StatusCode, response.Header.Get("Location"))
	}
	// No worker has been started: a 302 already implies one committed event.
	if count, err := f.pending(link.ID); err != nil || count != 1 {
		t.Fatalf("pending after redirect=%d err=%v", count, err)
	}
	var firstAt time.Time
	if err := f.pool.QueryRow(context.Background(), "SELECT occurred_at FROM click_outbox WHERE link_id=$1", link.ID).Scan(&firstAt); err != nil {
		t.Fatal(err)
	}
	f.startWorker(t)
	f.expectStats(t, link, 1, firstAt)
	eventually(t, func() (bool, error) { count, err := f.pending(link.ID); return count == 0, err })

	f.server.Stop()
	f.server = nil
	response, err = f.httpClient.Get(f.http.URL + "/" + link.Code)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusFound {
		t.Fatalf("analytics outage must still redirect: %d", response.StatusCode)
	}
	eventually(t, func() (bool, error) {
		var attempts int
		var lastError string
		err := f.pool.QueryRow(context.Background(), "SELECT attempts, last_error FROM click_outbox WHERE link_id=$1", link.ID).Scan(&attempts, &lastError)
		return attempts >= 2 && lastError != "", err
	})
	if count, err := f.pending(link.ID); err != nil || count != 1 {
		t.Fatalf("pending during outage=%d err=%v", count, err)
	}
	var secondAt time.Time
	if err := f.pool.QueryRow(context.Background(), "SELECT occurred_at FROM click_outbox WHERE link_id=$1", link.ID).Scan(&secondAt); err != nil {
		t.Fatal(err)
	}
	response, err = f.httpClient.Get(f.http.URL + "/links/" + link.Code + "/stats")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("stats during outage = %d", response.StatusCode)
	}
	f.startAnalytics(t)
	f.expectStats(t, link, 2, firstAt, secondAt)
	eventually(t, func() (bool, error) { count, err := f.pending(link.ID); return count == 0, err })
}

func TestRedeliveryAfterCommittedInsertWithoutAcknowledgement(t *testing.T) {
	f := newFixture(t)
	link := f.create(t)
	original := clickFor(link, time.Now())
	if err := f.links.Enqueue(context.Background(), original); err != nil {
		t.Fatal(err)
	}
	claimed, err := f.links.Claim(context.Background(), 1, 80*time.Millisecond)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("first claim=%v err=%v", claimed, err)
	}
	if err := f.client.TrackClick(context.Background(), claimed[0].Event); err != nil {
		t.Fatal(err)
	}
	// Simulate death after remote COMMIT but before PostgreSQL acknowledgement.
	var redelivered outbox.Item
	eventually(t, func() (bool, error) {
		items, err := f.links.Claim(context.Background(), 1, time.Second)
		if len(items) == 1 {
			redelivered = items[0]
			return true, err
		}
		return false, err
	})
	if redelivered.Event.ID != original.ID || !redelivered.Event.OccurredAt.Equal(claimed[0].Event.OccurredAt) {
		t.Fatalf("redelivery changed immutable payload: %+v", redelivered.Event)
	}
	if err := f.client.TrackClick(context.Background(), redelivered.Event); err != nil {
		t.Fatal(err)
	}
	if err := f.links.Ack(context.Background(), claimed[0]); !errors.Is(err, outbox.ErrLeaseLost) {
		t.Fatalf("stale acknowledgement = %v", err)
	}
	if err := f.links.Ack(context.Background(), redelivered); err != nil {
		t.Fatal(err)
	}
	var rows uint64
	if err := f.db.QueryRowContext(context.Background(), "SELECT count() FROM click_events_v2 WHERE event_id=?", original.ID).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 2 {
		t.Fatalf("expected two physical deliveries, got %d", rows)
	}
	f.expectStats(t, link, 1, redelivered.Event.OccurredAt)
}

func TestConcurrentRedirectsAndClaims(t *testing.T) {
	f := newFixture(t)
	link := f.create(t)
	const requests = 24
	var group sync.WaitGroup
	for range requests {
		group.Add(1)
		go func() {
			defer group.Done()
			response, err := f.httpClient.Get(f.http.URL + "/" + link.Code)
			if err != nil {
				t.Error(err)
				return
			}
			response.Body.Close()
			if response.StatusCode != http.StatusFound {
				t.Errorf("redirect status %d", response.StatusCode)
			}
		}()
	}
	group.Wait()
	if count, err := f.pending(link.ID); err != nil || count != requests {
		t.Fatalf("pending=%d err=%v", count, err)
	}
	results := make(chan []outbox.Item, 2)
	for range 2 {
		go func() {
			items, err := f.links.Claim(context.Background(), requests/2, time.Minute)
			if err != nil {
				t.Error(err)
			}
			results <- items
		}()
	}
	seen := make(map[string]bool)
	var occurrences []time.Time
	for range 2 {
		for _, item := range <-results {
			if seen[item.Event.ID] {
				t.Fatalf("event claimed by two workers: %s", item.Event.ID)
			}
			seen[item.Event.ID] = true
			occurrences = append(occurrences, item.Event.OccurredAt)
			if err := f.client.TrackClick(context.Background(), item.Event); err != nil {
				t.Fatal(err)
			}
			if err := f.links.Ack(context.Background(), item); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(seen) != requests {
		t.Fatalf("claimed=%d want=%d", len(seen), requests)
	}
	f.expectStats(t, link, requests, occurrences...)
}

func TestUTCDayBoundariesAndLegacyHistory(t *testing.T) {
	f := newFixture(t)
	link := f.create(t)
	now := time.Now().UTC()
	midnight := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	for _, at := range []time.Time{midnight.Add(-time.Millisecond), midnight, midnight.Add(24 * time.Hour)} {
		click := clickFor(link, at)
		if err := f.clicks.SaveClick(context.Background(), click); err != nil {
			t.Fatal(err)
		}
		// With a non-UTC ClickHouse session timezone, the persisted instant must
		// still retain UTC millisecond precision (especially just before midnight).
		var storedMillis int64
		if err := f.db.QueryRowContext(context.Background(),
			"SELECT toUnixTimestamp64Milli(occurred_at) FROM click_events_v2 WHERE event_id=?", click.ID).Scan(&storedMillis); err != nil {
			t.Fatal(err)
		}
		if storedMillis != at.UnixMilli() {
			t.Fatalf("stored event time=%d want=%d (session timezone must not shift UTC)", storedMillis, at.UnixMilli())
		}
		if at.Equal(midnight) {
			for range 2 {
				if err := f.clicks.SaveClick(context.Background(), click); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	for _, at := range []time.Time{midnight.Add(-time.Millisecond), midnight} {
		if _, err := f.db.ExecContext(context.Background(), "INSERT INTO click_events (link_id,code,user_agent,referer,occurred_at) VALUES (?,?,?,?,fromUnixTimestamp64Milli(?, 'UTC'))", link.ID, link.Code, "legacy", "", at.UnixMilli()); err != nil {
			t.Fatal(err)
		}
	}
	f.expectStats(t, link, 5, midnight.Add(-time.Millisecond), midnight, midnight.Add(24*time.Hour), midnight.Add(-time.Millisecond), midnight)
}

func TestUnavailablePostgresOutboxPreventsRedirect(t *testing.T) {
	f := newFixture(t)
	link := f.create(t)
	// A separate closed real pool makes only the durable-save step fail.
	failedPool, err := pgxpool.New(context.Background(), required(t, "TEST_POSTGRES_URL"))
	if err != nil {
		t.Fatal(err)
	}
	failedPool.Close()
	mux := http.NewServeMux()
	handler.New(f.service, f.client, linkrepo.NewPostgresRepository(failedPool), f.links.Ping).RegisterRoutes(mux)
	request := httptest.NewRequest(http.MethodGet, "/"+link.Code, nil)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable || response.Header().Get("Location") != "" {
		t.Fatalf("failed persistence status=%d location=%q", response.Code, response.Header().Get("Location"))
	}
	if count, err := f.pending(link.ID); err != nil || count != 0 {
		t.Fatalf("pending=%d err=%v", count, err)
	}
}

func TestDeployedApplication(t *testing.T) {
	base := required(t, "TEST_HTTP_URL")
	client := &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	for _, path := range []string{"/health", "/ready"} {
		response, err := client.Get(base + path)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("%s status=%d", path, response.StatusCode)
		}
	}
	response, err := client.Post(base+"/links", "application/json", strings.NewReader(`{"url":"https://example.org/deployed"}`))
	if err != nil {
		t.Fatal(err)
	}
	var link struct {
		Code string `json:"code"`
	}
	if err := json.NewDecoder(response.Body).Decode(&link); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusCreated || link.Code == "" {
		t.Fatalf("create status=%d code=%q", response.StatusCode, link.Code)
	}
	response, err = client.Get(base + "/" + link.Code)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusFound || response.Header.Get("Location") != "https://example.org/deployed" {
		t.Fatalf("redirect status=%d", response.StatusCode)
	}
	eventually(t, func() (bool, error) {
		response, err := client.Get(base + "/links/" + link.Code + "/stats")
		if err != nil {
			return false, err
		}
		defer response.Body.Close()
		var stats struct {
			Total uint64 `json:"total_clicks"`
		}
		err = json.NewDecoder(response.Body).Decode(&stats)
		return response.StatusCode == http.StatusOK && stats.Total == 1, err
	})
}
