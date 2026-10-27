package service

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"linkpulse/internal/link/model"
	"linkpulse/internal/link/repository"
)

const (
	codeLength          = 6
	maxGenerateAttempts = 5
	maxURLLength        = 8192
)

const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var ErrInvalidURL = errors.New("invalid url")

type Service struct {
	repository   repository.Repository
	generateCode func() (string, error)
}

func New(repository repository.Repository) *Service {
	return &Service{
		repository:   repository,
		generateCode: generateCode,
	}
}

func (s *Service) CreateLink(
	ctx context.Context,
	targetURL string,
) (model.Link, error) {
	targetURL = strings.TrimSpace(targetURL)

	if !isValidURL(targetURL) {
		return model.Link{}, ErrInvalidURL
	}

	for i := 0; i < maxGenerateAttempts; i++ {
		if err := ctx.Err(); err != nil {
			return model.Link{}, err
		}
		code, err := s.generateCode()
		if err != nil {
			return model.Link{}, err
		}
		// /health is a reserved six-character route, so it cannot be a link.
		if code == "health" {
			continue
		}

		link := model.Link{
			Code:      code,
			TargetURL: targetURL,
		}

		err = s.repository.Create(ctx, &link)

		if err == nil {
			return link, nil
		}

		if errors.Is(err, repository.ErrCodeAlreadyExists) {
			continue
		}

		return model.Link{}, err
	}

	return model.Link{}, errors.New("failed to generate unique code")
}

func (s *Service) GetByCode(
	ctx context.Context,
	code string,
) (model.Link, error) {
	if !isValidCode(code) {
		return model.Link{}, repository.ErrLinkNotFound
	}

	return s.repository.GetByCode(ctx, code)
}

func generateCode() (string, error) {
	result := make([]byte, codeLength)

	for i := range result {
		randomIndex, err := rand.Int(
			rand.Reader,
			big.NewInt(int64(len(alphabet))),
		)
		if err != nil {
			return "", fmt.Errorf("generate random code: %w", err)
		}

		result[i] = alphabet[randomIndex.Int64()]
	}

	return string(result), nil
}

func isValidURL(value string) bool {
	if value == "" || len(value) > maxURLLength || !utf8.ValidString(value) || strings.ContainsFunc(value, func(r rune) bool {
		return unicode.IsSpace(r) || unicode.IsControl(r)
	}) {
		return false
	}

	parsedURL, err := url.Parse(value)
	if err != nil {
		return false
	}

	if parsedURL.Host == "" || parsedURL.User != nil || parsedURL.Opaque != "" {
		return false
	}

	if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
		return false
	}

	hostname := parsedURL.Hostname()
	if !isValidHostname(hostname) || strings.HasSuffix(parsedURL.Host, ":") {
		return false
	}
	if strings.HasPrefix(parsedURL.Host, "[") {
		if _, err := netip.ParseAddr(hostname); err != nil {
			return false
		}
	}
	if port := parsedURL.Port(); port != "" {
		value, err := strconv.Atoi(port)
		if err != nil || value < 1 || value > 65535 {
			return false
		}
	}
	if strings.Contains(hostname, ":") && !strings.HasPrefix(parsedURL.Host, "[") {
		return false
	}

	return true
}

func isValidHostname(hostname string) bool {
	if _, err := netip.ParseAddr(hostname); err == nil {
		return true
	}
	if hostname == "" || len(hostname) > 253 {
		return false
	}
	for _, label := range strings.Split(strings.TrimSuffix(hostname, "."), ".") {
		if label == "" || len(label) > 63 || strings.HasPrefix(label, "-") || strings.HasSuffix(label, "-") {
			return false
		}
		for _, char := range label {
			if !unicode.IsLetter(char) && !unicode.IsDigit(char) && char != '-' {
				return false
			}
		}
	}
	return true
}

func isValidCode(code string) bool {
	if len(code) != codeLength {
		return false
	}
	for _, char := range code {
		if !strings.ContainsRune(alphabet, char) {
			return false
		}
	}
	return true
}
