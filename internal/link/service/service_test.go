package service

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"linkpulse/internal/link/model"
	"linkpulse/internal/link/repository"
)

type fakeRepository struct {
	create func(context.Context, *model.Link) error
	get    func(context.Context, string) (model.Link, error)
}

func (r fakeRepository) Create(ctx context.Context, link *model.Link) error {
	return r.create(ctx, link)
}

func (r fakeRepository) GetByCode(ctx context.Context, code string) (model.Link, error) {
	return r.get(ctx, code)
}

func TestGenerateCode(t *testing.T) {
	for range 100 {
		code, err := generateCode()
		if err != nil {
			t.Fatal(err)
		}
		if !isValidCode(code) {
			t.Fatalf("invalid generated code %q", code)
		}
	}
}

func TestURLValidation(t *testing.T) {
	tests := []struct {
		name  string
		url   string
		valid bool
	}{
		{"https", "https://example.com/path?q=a%20b#fragment", true},
		{"http", "http://example.com:8080/", true},
		{"localhost", "http://localhost/health", true},
		{"ipv4", "http://127.0.0.1:8080/path", true},
		{"ipv6", "https://[2001:db8::1]:443/path", true},
		{"idn", "https://пример.рф/путь", true},
		{"trailing dot", "https://example.com./", true},
		{"empty", "", false},
		{"relative", "/path", false},
		{"scheme relative", "//example.com/path", false},
		{"ftp", "ftp://example.com/file", false},
		{"javascript", "javascript:alert(1)", false},
		{"missing host", "https:///path", false},
		{"missing hostname", "http://:80/", false},
		{"credentials", "https://user:password@example.com/", false},
		{"empty credentials", "https://@example.com/", false},
		{"invalid escape", "https://example.com/%zz", false},
		{"literal space", "https://example.com/a b", false},
		{"unicode whitespace", "https://example.com/a\u00a0b", false},
		{"control character", "https://example.com/a\nheader", false},
		{"invalid UTF8 path", "https://example.com/a\xffb", false},
		{"empty port", "https://example.com:", false},
		{"out of range port", "https://example.com:65536/", false},
		{"zero port", "https://example.com:0/", false},
		{"negative port", "https://example.com:-1/", false},
		{"text port", "https://example.com:invalid/", false},
		{"unbracketed ipv6", "https://2001:db8::1/", false},
		{"bracketed hostname", "https://[example.com]/", false},
		{"empty label", "https://example..com/", false},
		{"bad label", "https://-example.com/", false},
		{"only dot", "https://./", false},
		{"too long", "https://example.com/" + strings.Repeat("a", maxURLLength), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isValidURL(tt.url); got != tt.valid {
				t.Errorf("isValidURL() = %v, want %v", got, tt.valid)
			}
		})
	}
}

func TestCreateLinkRetriesCollisions(t *testing.T) {
	calls := 0
	createdAt := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)
	s := New(fakeRepository{create: func(_ context.Context, link *model.Link) error {
		calls++
		if calls < 3 {
			return repository.ErrCodeAlreadyExists
		}
		link.ID = 42
		link.CreatedAt = createdAt
		return nil
	}})
	codes := []string{"aaaaaa", "bbbbbb", "cccccc"}
	s.generateCode = func() (string, error) { return codes[calls], nil }
	link, err := s.CreateLink(context.Background(), "  https://example.com/path  ")
	if err != nil {
		t.Fatal(err)
	}
	if calls != 3 || link.Code != "cccccc" || link.TargetURL != "https://example.com/path" || link.ID != 42 || !link.CreatedAt.Equal(createdAt) {
		t.Fatalf("unexpected result after collisions: calls=%d link=%+v", calls, link)
	}
}

func TestCreateLinkCollisionLimit(t *testing.T) {
	calls := 0
	s := New(fakeRepository{create: func(context.Context, *model.Link) error {
		calls++
		return repository.ErrCodeAlreadyExists
	}})
	_, err := s.CreateLink(context.Background(), "https://example.com")
	if err == nil || calls != maxGenerateAttempts {
		t.Fatalf("err=%v calls=%d, want failure after %d attempts", err, calls, maxGenerateAttempts)
	}
}

func TestCreateLinkSkipsReservedCode(t *testing.T) {
	generated, stored := 0, 0
	s := New(fakeRepository{create: func(_ context.Context, link *model.Link) error {
		stored++
		if link.Code == "health" {
			t.Fatal("reserved code reached repository")
		}
		return nil
	}})
	s.generateCode = func() (string, error) {
		generated++
		if generated == 1 {
			return "health", nil
		}
		return "abc123", nil
	}
	link, err := s.CreateLink(context.Background(), "https://example.com")
	if err != nil || link.Code != "abc123" || generated != 2 || stored != 1 {
		t.Fatalf("link=%+v err=%v generated=%d stored=%d", link, err, generated, stored)
	}
}

func TestCreateLinkErrors(t *testing.T) {
	failure := errors.New("storage unavailable")
	t.Run("repository", func(t *testing.T) {
		calls := 0
		s := New(fakeRepository{create: func(context.Context, *model.Link) error { calls++; return failure }})
		_, err := s.CreateLink(context.Background(), "https://example.com")
		if !errors.Is(err, failure) || calls != 1 {
			t.Fatalf("err=%v calls=%d", err, calls)
		}
	})
	t.Run("entropy", func(t *testing.T) {
		s := New(fakeRepository{})
		s.generateCode = func() (string, error) { return "", failure }
		_, err := s.CreateLink(context.Background(), "https://example.com")
		if !errors.Is(err, failure) {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("invalid URL", func(t *testing.T) {
		_, err := New(fakeRepository{}).CreateLink(context.Background(), "ftp://example.com")
		if !errors.Is(err, ErrInvalidURL) {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("invalid UTF8 URL", func(t *testing.T) {
		_, err := New(fakeRepository{}).CreateLink(context.Background(), "https://example.com/a\xffb")
		if !errors.Is(err, ErrInvalidURL) {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := New(fakeRepository{}).CreateLink(ctx, "https://example.com")
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err=%v", err)
		}
	})
}

func TestGetByCodeValidation(t *testing.T) {
	for _, code := range []string{"", "short", "toolong", "abc-12", "éabc1", "abc_12"} {
		_, err := New(fakeRepository{}).GetByCode(context.Background(), code)
		if !errors.Is(err, repository.ErrLinkNotFound) {
			t.Fatalf("code=%q err=%v", code, err)
		}
	}
	failure := errors.New("database error")
	_, err := New(fakeRepository{get: func(_ context.Context, code string) (model.Link, error) {
		if code != "abc123" {
			t.Fatalf("code=%q", code)
		}
		return model.Link{}, failure
	}}).GetByCode(context.Background(), "abc123")
	if !errors.Is(err, failure) {
		t.Fatalf("err=%v", err)
	}
}
