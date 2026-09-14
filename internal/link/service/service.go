package service

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"net/url"
	"strings"

	"linkpulse/internal/link/model"
	"linkpulse/internal/link/repository"
)

const (
	codeLength          = 6
	maxGenerateAttempts = 5
)

const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var ErrInvalidURL = errors.New("invalid url")

type Service struct {
	repository repository.Repository
}

func New(repository repository.Repository) *Service {
	return &Service{
		repository: repository,
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
		code, err := generateCode()
		if err != nil {
			return model.Link{}, err
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
	if code == "" {
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
	parsedURL, err := url.Parse(value)
	if err != nil {
		return false
	}

	if parsedURL.Host == "" {
		return false
	}

	if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
		return false
	}

	return true
}
