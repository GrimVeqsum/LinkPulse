package repository

import (
	"context"
	"errors"

	"linkpulse/internal/link/model"
)

var (
	ErrLinkNotFound      = errors.New("link not found")
	ErrCodeAlreadyExists = errors.New("code already exists")
)

type Repository interface {
	Create(ctx context.Context, link *model.Link) error
	GetByCode(ctx context.Context, code string) (model.Link, error)
}
